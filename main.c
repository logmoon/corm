#include "thirdparty/sqlite/sqlite3.h"
#define LU_LOGGER_ENABLED
#define LU_LOGGER_SHOW_SOURCE
#define LU_LOGGER_SHOW_DATE
#define LU_LOGGER_LOG_TO_FILE
#define LU_LOG_FILE_LEVEL_WARN
#include "log.utils/lu_logger.h"
#include "log.utils/lu_arena.h"
#include "log.utils/lu_darray.h"
#include "log.utils/lu_string.h"

typedef enum {
    FIELD_TYPE_INT,
    FIELD_TYPE_INT64,
    FIELD_TYPE_FLOAT,
    FIELD_TYPE_DOUBLE,
    FIELD_TYPE_STRING,
    FIELD_TYPE_BOOL,
    FIELD_TYPE_BLOB,
    FIELD_TYPE_FOREIGN_KEY
} field_type_e;

typedef struct {
    const char* name;
    size_t offset;
    field_type_e type;
    int flags;
    size_t max_length;
    const char* foreign_table;
    const char* foreign_field;
    void* default_value;
} field_info_t;

typedef struct {
    const char* table_name;
    size_t struct_size;
    field_info_t* fields;
    u64 field_count;
    const char* primary_key_field;
} model_meta_t;

typedef struct {
    sqlite3* db;
    model_meta_t** models;
    u64 model_count;
    u64 model_capacity;
    arena_t* arena;
} corm_db_t;

// Field flags
#define PRIMARY_KEY (1 << 0)
#define NOT_NULL    (1 << 1)
#define UNIQUE      (1 << 2)
#define AUTO_INC    (1 << 3)

// Macro expansions for different contexts
#define FIELD_STRUCT(c_type, name, field_type, flags) c_type name;

#define FIELD_INFO(c_type, name, field_type, flags) \
    {#name, offsetof(ModelType, name), field_type, flags, 0, NULL, NULL, NULL},

#define FIELD_COUNT(c_type, name, field_type, flags) +1

#define DEFINE_MODEL(ModelName, ...) \
    typedef struct ModelName##_struct { \
        __VA_ARGS__(FIELD_STRUCT) \
    } ModelName; \
    typedef ModelName ModelType; \
    static field_info_t ModelName##_fields[] = { \
        __VA_ARGS__(FIELD_INFO) \
    }; \
    static model_meta_t ModelName##_meta = { \
        #ModelName, \
        sizeof(ModelName), \
        ModelName##_fields, \
        0 __VA_ARGS__(FIELD_COUNT), \
        NULL \
    };

// Type-specific field macros
#define FIELD_INT(F, name, flags)     F(int, name, FIELD_TYPE_INT, flags)
#define FIELD_INT64(F, name, flags)   F(long long, name, FIELD_TYPE_INT64, flags)
#define FIELD_FLOAT(F, name, flags)   F(float, name, FIELD_TYPE_FLOAT, flags)
#define FIELD_DOUBLE(F, name, flags)  F(double, name, FIELD_TYPE_DOUBLE, flags)
#define FIELD_STRING(F, name, flags)  F(char*, name, FIELD_TYPE_STRING, flags)
#define FIELD_BOOL(F, name, flags)    F(bool, name, FIELD_TYPE_BOOL, flags)

// =============================================================================
// ORM IMPLEMENTATION
// =============================================================================

corm_db_t* corm_init(arena_t* arena, const char* db_filepath) {
    corm_db_t* db = arena_alloc_struct(arena, corm_db_t);
    if (db == NULL) {
        log_error("Couldn't allocate db object");
        return NULL;
    }
    
    db->arena = arena;
    db->model_count = 0;
    db->model_capacity = 64;
    db->models = arena_alloc_array(arena, model_meta_t*, db->model_capacity);
    
    int rc = sqlite3_open(db_filepath, &db->db);
    if (rc != SQLITE_OK) {
        log_error("Cannot open database: %s", sqlite3_errmsg(db->db));
        sqlite3_close(db->db);
        return NULL;
    }
    
    return db;
}

void corm_register_model(corm_db_t* db, model_meta_t* meta) {
    if (db->model_count >= db->model_capacity) {
        log_error("Model capacity exceeded");
        return;
    }
    
    // Find primary key field
    for (u64 i = 0; i < meta->field_count; i++) {
        if (meta->fields[i].flags & PRIMARY_KEY) {
            meta->primary_key_field = meta->fields[i].name;
            break;
        }
    }
    
    db->models[db->model_count++] = meta;
}

const char* corm_sql_type(field_info_t* field) {
    switch (field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return "INTEGER";
        case FIELD_TYPE_INT64:
            return "BIGINT";
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            return "REAL";
        case FIELD_TYPE_STRING:
            return field->max_length > 0 ? "VARCHAR" : "TEXT";
        case FIELD_TYPE_BLOB:
            return "BLOB";
        default:
            return "TEXT";
    }
}

bool corm_create_table(corm_db_t* db, model_meta_t* meta) {
    temp_t temp = arena_start_temp(db->arena);
    
    // Start with "CREATE TABLE IF NOT EXISTS tablename ("
    string_t sql = str_fmt(db->arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);
    
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        // Add comma separator
        if (i > 0) {
            sql = str_cat(db->arena, sql, STR_LIT(", "));
        }
        
        // Add field definition
        string_t field_def = str_fmt(db->arena, "%s %s", field->name, corm_sql_type(field));
        sql = str_cat(db->arena, sql, field_def);
        
        // Add constraints
        if (field->flags & PRIMARY_KEY) {
            sql = str_cat(db->arena, sql, STR_LIT(" PRIMARY KEY"));
        }
        if (field->flags & AUTO_INC) {
            sql = str_cat(db->arena, sql, STR_LIT(" AUTOINCREMENT"));
        }
        if (field->flags & NOT_NULL && !(field->flags & PRIMARY_KEY)) {
            sql = str_cat(db->arena, sql, STR_LIT(" NOT NULL"));
        }
        if (field->flags & UNIQUE) {
            sql = str_cat(db->arena, sql, STR_LIT(" UNIQUE"));
        }
    }
    
    sql = str_cat(db->arena, sql, STR_LIT(");"));
    
    log_info("Creating table: %s", str_to_c(sql));
    
    char* err_msg = NULL;
    int rc = sqlite3_exec(db->db, str_to_c(sql), NULL, NULL, &err_msg);
    
    arena_end_temp(temp);
    
    if (rc != SQLITE_OK) {
        log_error("SQL error: %s", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    return true;
}

bool corm_sync(corm_db_t* db) {
    for (u64 i = 0; i < db->model_count; i++) {
        if (!corm_create_table(db, db->models[i])) {
            return false;
        }
    }
    return true;
}

bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
    temp_t temp = arena_start_temp(db->arena);
    
    // Check if record exists
    bool is_update = false;
    void* pk_value = NULL;
    
    for (u64 i = 0; i < meta->field_count; i++) {
        if (meta->fields[i].flags & PRIMARY_KEY) {
            pk_value = (char*)instance + meta->fields[i].offset;
            if (meta->fields[i].type == FIELD_TYPE_INT) {
                is_update = (*(int*)pk_value != 0);
            }
            break;
        }
    }
    
    string_t sql;
    
    if (is_update) {
        // UPDATE
        sql = str_fmt(db->arena, "UPDATE %s SET ", meta->table_name);
        
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            if (field->flags & PRIMARY_KEY) continue;
            
            if (!first) {
                sql = str_cat(db->arena, sql, STR_LIT(", "));
            }
            string_t field_set = str_fmt(db->arena, "%s = ?", field->name);
            sql = str_cat(db->arena, sql, field_set);
            first = false;
        }
        
        string_t where = str_fmt(db->arena, " WHERE %s = ?", meta->primary_key_field);
        sql = str_cat(db->arena, sql, where);
    } else {
        // INSERT
        sql = str_fmt(db->arena, "INSERT INTO %s (", meta->table_name);
        
        // Field names
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            if (field->flags & AUTO_INC) continue;
            
            if (!first) {
                sql = str_cat(db->arena, sql, STR_LIT(", "));
            }
            sql = str_cat(db->arena, sql, str_from_cstring(db->arena, field->name));
            first = false;
        }
        
        sql = str_cat(db->arena, sql, STR_LIT(") VALUES ("));
        
        // Placeholders
        first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            if (meta->fields[i].flags & AUTO_INC) continue;
            
            if (!first) {
                sql = str_cat(db->arena, sql, STR_LIT(", "));
            }
            sql = str_cat(db->arena, sql, STR_LIT("?"));
            first = false;
        }
        
        sql = str_cat(db->arena, sql, STR_LIT(")"));
    }
    
    log_info("SQL: %s", str_to_c(sql));
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c(sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        arena_end_temp(temp);
        return false;
    }
    
    // Bind parameters
    int param_idx = 1;
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        if (!is_update && (field->flags & AUTO_INC)) continue;
        if (is_update && (field->flags & PRIMARY_KEY)) continue;
        
        void* field_ptr = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, param_idx++, *(int*)field_ptr);
                break;
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, param_idx++, *(long long*)field_ptr);
                break;
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, param_idx++, *(float*)field_ptr);
                break;
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, param_idx++, *(double*)field_ptr);
                break;
            case FIELD_TYPE_STRING: {
                char* str = *(char**)field_ptr;
                if (str) {
                    sqlite3_bind_text(stmt, param_idx++, str, -1, SQLITE_TRANSIENT);
                } else {
                    sqlite3_bind_null(stmt, param_idx++);
                }
                break;
            }
            default:
                sqlite3_bind_null(stmt, param_idx++);
                break;
        }
    }
    
    // Bind primary key for UPDATE WHERE
    if (is_update) {
        for (u64 i = 0; i < meta->field_count; i++) {
            if (meta->fields[i].flags & PRIMARY_KEY) {
                void* field_ptr = (char*)instance + meta->fields[i].offset;
                sqlite3_bind_int(stmt, param_idx++, *(int*)field_ptr);
                break;
            }
        }
    }
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    arena_end_temp(temp);
    
    if (rc != SQLITE_DONE) {
        log_error("Failed to execute: %s", sqlite3_errmsg(db->db));
        return false;
    }
    
    // Set auto-increment ID
    if (!is_update) {
        for (u64 i = 0; i < meta->field_count; i++) {
            if (meta->fields[i].flags & AUTO_INC) {
                void* field_ptr = (char*)instance + meta->fields[i].offset;
                *(int*)field_ptr = (int)sqlite3_last_insert_rowid(db->db);
                break;
            }
        }
    }
    
    return true;
}

void* corm_find(corm_db_t* db, model_meta_t* meta, int pk_value) {
    if (!meta->primary_key_field) {
        log_error("Model has no primary key");
        return NULL;
    }
    
    string_t sql = str_fmt(db->arena, "SELECT * FROM %s WHERE %s = ?", 
                          meta->table_name, meta->primary_key_field);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c(sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare: %s", sqlite3_errmsg(db->db));
        return NULL;
    }
    
    sqlite3_bind_int(stmt, 1, pk_value);
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return NULL;
    }
    
    void* instance = arena_alloc(db->arena, meta->struct_size);
    
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        void* field_ptr = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                *(int*)field_ptr = sqlite3_column_int(stmt, i);
                break;
            case FIELD_TYPE_INT64:
                *(long long*)field_ptr = sqlite3_column_int64(stmt, i);
                break;
            case FIELD_TYPE_FLOAT:
            case FIELD_TYPE_DOUBLE:
                *(double*)field_ptr = sqlite3_column_double(stmt, i);
                break;
            case FIELD_TYPE_STRING: {
                const unsigned char* text = sqlite3_column_text(stmt, i);
                if (text) {
                    size_t len = strlen((const char*)text);
                    char* str = arena_alloc_array(db->arena, char, len + 1);
                    strcpy(str, (const char*)text);
                    *(char**)field_ptr = str;
                } else {
                    *(char**)field_ptr = NULL;
                }
                break;
            }
            default:
                break;
        }
    }
    
    sqlite3_finalize(stmt);
    return instance;
}

// =============================================================================
// MODELS
// =============================================================================

#define USER_FIELDS(F) \
    FIELD_INT(F, id, PRIMARY_KEY | AUTO_INC) \
    FIELD_STRING(F, name, NOT_NULL) \
    FIELD_STRING(F, pwd_hash, NOT_NULL)

DEFINE_MODEL(User, USER_FIELDS)

// =============================================================================
// MAIN
// =============================================================================

int main() {
    arena_t* arena = arena_create(MiB(10));
    corm_db_t* db = corm_init(arena, "test.db");
    
    corm_register_model(db, &User_meta);
    corm_sync(db);
    
    User user = {0};
    user.name = "John Doe";
    user.pwd_hash = "hash123";
    
    log_info("Saving user...");
    corm_save(db, &User_meta, &user);
    log_info("User saved with ID: %d", user.id);
    
    User* found = corm_find(db, &User_meta, user.id);
    if (found) {
        log_info("Found user: id=%d, name=%s", found->id, found->name);
    }
    
    sqlite3_close(db->db);
    arena_destroy(arena);
    return 0;
}
