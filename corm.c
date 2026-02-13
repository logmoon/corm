#include "corm.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#define CORM_LOG_ERROR(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n", ##__VA_ARGS__)
#define CORM_LOG_WARN(fmt, ...)  fprintf(stderr, "[WARN] " fmt "\n", ##__VA_ARGS__)
#define CORM_LOG_INFO(fmt, ...)  fprintf(stdout, "[INFO] " fmt "\n", ##__VA_ARGS__)

#define CORM_KIB(n) ((uint64_t)(n) << 10)
#define CORM_MIB(n) ((uint64_t)(n) << 20)
#define CORM_GIB(n) ((uint64_t)(n) << 30)

#define CORM_ARENA_DEFAULT_ALIGN (sizeof(void*))
#define CORM_ALIGN_UP(n, align) (((uint64_t)(n) + ((uint64_t)(align) - 1)) & ~((uint64_t)(align) - 1))

struct corm_arena_t {
    uint8_t* region;
    uint64_t size;
    uint64_t used;
};

typedef struct {
    corm_arena_t* arena;
    uint64_t checkpoint;
} corm_temp_t;

typedef struct {
    uint8_t* str;
    uint64_t size;
} corm_string_t;

#define CORM_STR_LIT(s) (corm_string_t){ (uint8_t*)(s), sizeof((s)) - 1 }

static inline corm_arena_t* corm_arena_create(uint64_t size) {
    if (size == 0) return NULL;
    
    corm_arena_t* arena = (corm_arena_t*)malloc(sizeof(corm_arena_t));
    if (!arena) return NULL;
    
    arena->region = (uint8_t*)malloc(size);
    if (!arena->region) {
        free(arena);
        return NULL;
    }
    
    arena->size = size;
    arena->used = 0;
    
    return arena;
}

static inline void* corm_arena_alloc(corm_arena_t* arena, uint64_t size) {
    if (!arena || size == 0) return NULL;
    
    uint64_t aligned_pos = CORM_ALIGN_UP(arena->used, CORM_ARENA_DEFAULT_ALIGN);
    uint64_t new_used = aligned_pos + size;
    
    if (new_used > arena->size) {
        return NULL;
    }
    
    arena->used = new_used;
    void* result = arena->region + aligned_pos;
    
    memset(result, 0, size);
    
    return result;
}

static inline corm_temp_t corm_arena_start_temp(corm_arena_t* arena) {
    return (corm_temp_t) {
        .arena = arena,
        .checkpoint = arena->used
    };
}

static inline void corm_arena_end_temp(corm_temp_t temp) {
    temp.arena->used = temp.checkpoint;
}

static inline void corm_arena_destroy(corm_arena_t* arena) {
    if (!arena) return;
    if (arena->region) {
        free(arena->region);
    }
    free(arena);
}

static inline corm_string_t corm_str_fmt(corm_arena_t* arena, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);

    if (len < 0) return (corm_string_t){0};

    uint8_t* data = (uint8_t*)corm_arena_alloc(arena, len + 1);
    
    va_start(args, fmt);
    vsnprintf((char*)data, len + 1, fmt, args);
    va_end(args);

    data[len] = 0;
    return (corm_string_t){ data, (uint64_t)len };
}

static inline corm_string_t corm_str_cat(corm_arena_t* arena, corm_string_t a, corm_string_t b) {
    uint64_t size = a.size + b.size;
    uint8_t* data = (uint8_t*)corm_arena_alloc(arena, size + 1);
    
    memcpy(data, a.str, a.size);
    memcpy(data + a.size, b.str, b.size);
    data[size] = 0;
    
    return (corm_string_t){ data, size };
}

static inline const char* corm_str_to_c_safe(corm_arena_t* arena, corm_string_t s) {
    if (s.str && s.str[s.size] == 0) {
        return (const char*)s.str;
    }
    
    uint8_t* data = (uint8_t*)corm_arena_alloc(arena, s.size + 1);
    memcpy(data, s.str, s.size);
    data[s.size] = 0;
    return (const char*)data;
}


static inline void* corm_alloc_fn(corm_db_t* db, size_t size) {
    if (db->allocator.alloc_fn) {
        return db->allocator.alloc_fn(db->allocator.ctx, size);
    }
    return CORM_MALLOC(size);
}

static inline void corm_free_fn(corm_db_t* db, void* ptr) {
    if (db->allocator.alloc_fn) {
        if (db->allocator.free_fn) {
            db->allocator.free_fn(db->allocator.ctx, ptr);
        }
        return;
    }
    CORM_FREE(ptr);
}

corm_db_t* corm_init(const char* db_filepath) {
    return corm_init_with_allocator(db_filepath, NULL, NULL, NULL);
}

corm_db_t* corm_init_with_allocator(const char* db_filepath, void* ctx,
                                    void* (*alloc_fn)(void*, size_t),
                                    void (*free_fn)(void*, void*)) {

    corm_db_t* db = CORM_MALLOC(sizeof(corm_db_t));
    if (db == NULL) {
        CORM_LOG_ERROR("Couldn't allocate db object");
        return NULL;
    }
    
    db->allocator.alloc_fn = alloc_fn;
    db->allocator.free_fn = free_fn;
    db->allocator.ctx = ctx;
    
    db->internal_arena = corm_arena_create(CORM_MIB(1));
    if (db->internal_arena == NULL) {
        CORM_LOG_ERROR("Couldn't create internal arena");
        CORM_FREE(db);
        return NULL;
    }
    
    db->model_count = 0;
    db->model_capacity = CORM_MAX_MODELS;
    
    db->models = corm_alloc_fn(db, sizeof(model_meta_t*) * CORM_MAX_MODELS);
    if (db->models == NULL) {
        CORM_LOG_ERROR("Couldn't allocate models array");
        corm_arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }
    
    int rc = sqlite3_open(db_filepath, &db->db);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Cannot open database: %s", sqlite3_errmsg(db->db));
        sqlite3_close(db->db);
        corm_free_fn(db, db->models);
        corm_arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }

    char* err_msg = NULL;
    rc = sqlite3_exec(db->db, "PRAGMA foreign_keys = ON;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to enable foreign keys: %s", err_msg);
        sqlite3_free(err_msg);
    }
    
    return db;
}

void corm_set_allocator(corm_db_t* db, void* ctx,
                        void* (*alloc_fn)(void*, size_t),
                        void (*free_fn)(void*, void*)) {

    db->allocator.ctx = ctx;
    db->allocator.alloc_fn = alloc_fn;
    db->allocator.free_fn = free_fn;
}

void corm_close(corm_db_t* db) {
    if (!db) return;
    sqlite3_close(db->db);
    corm_arena_destroy(db->internal_arena);
    corm_free_fn(db, db->models);
    CORM_FREE(db);
}

bool corm_register_model(corm_db_t* db, model_meta_t* meta) {
    field_info_t* pk_field = NULL;
    int pk_count = 0;
    
    for (uint64_t i = 0; i < meta->field_count; i++) {
        if (meta->fields[i].flags & PRIMARY_KEY) {
            pk_field = &meta->fields[i];
            pk_count++;
        }
    }
    
    if (pk_count == 0) {
        CORM_LOG_ERROR("Model '%s' must have exactly one PRIMARY_KEY field", meta->table_name);
        return false;
    }
    if (pk_count > 1) {
        CORM_LOG_ERROR("Model '%s' has %d PRIMARY_KEY fields, expected 1", meta->table_name, pk_count);
        return false;
    }
    
    meta->primary_key_field = pk_field;

    if (db->model_count >= db->model_capacity) {
        CORM_LOG_ERROR("Maximum number of models (%d) reached. Define CORM_MAX_MODELS to increase.",
              CORM_MAX_MODELS);
        return false;
    }
    
    db->models[db->model_count++] = meta;
    return true;
}

static corm_string_t corm_sql_type(corm_arena_t* arena, field_info_t* field) {
    switch (field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return CORM_STR_LIT("INTEGER");
        case FIELD_TYPE_INT64:
            return CORM_STR_LIT("BIGINT");
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            return CORM_STR_LIT("REAL");
        case FIELD_TYPE_STRING:
            if (field->max_length > 0) {
                return corm_str_fmt(arena, "VARCHAR(%zu)", field->max_length);
            }
            return CORM_STR_LIT("TEXT");
        case FIELD_TYPE_BLOB:
            return CORM_STR_LIT("BLOB");
        default:
            return CORM_STR_LIT("TEXT");
    }
}

static bool corm_table_exists(corm_db_t* db, const char* table_name) {
    const char* check_sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt* stmt;
    
    int rc = sqlite3_prepare_v2(db->db, check_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, table_name, -1, SQLITE_STATIC);
    
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) > 0;
    }
    
    sqlite3_finalize(stmt);
    return exists;
}

static bool corm_create_table(corm_db_t* db, model_meta_t* meta) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    corm_string_t sql = corm_str_fmt(db->internal_arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);

    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];

        if (field->type == FIELD_TYPE_BELONGS_TO ||
            field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }

        if (i > 0) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
        }

        corm_string_t type = corm_sql_type(db->internal_arena, field);
        corm_string_t new_field = corm_str_fmt(db->internal_arena, "%s %s", field->name, corm_str_to_c_safe(db->internal_arena, type));

        sql = corm_str_cat(db->internal_arena, sql, new_field);
        if (meta->fields[i].flags & PRIMARY_KEY) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" PRIMARY KEY"));
        }
        if (meta->fields[i].flags & NOT_NULL) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" NOT NULL"));
        }
        if (meta->fields[i].flags & UNIQUE) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" UNIQUE"));
        }
        if (meta->fields[i].flags & AUTO_INC) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" AUTOINCREMENT"));
        }
    }

    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];

        if (field->type == FIELD_TYPE_BELONGS_TO) {
            corm_string_t fk = corm_str_fmt(db->internal_arena, ", FOREIGN KEY (%s) REFERENCES %s(id)",
                field->fk_column_name,
                field->target_model_name
            );
            
            switch(field->on_delete) {
                case FK_CASCADE:
                    fk = corm_str_cat(db->internal_arena, fk, CORM_STR_LIT(" ON DELETE CASCADE"));
                    break;
                case FK_SET_NULL:
                    fk = corm_str_cat(db->internal_arena, fk, CORM_STR_LIT(" ON DELETE SET NULL"));
                    break;
                case FK_RESTRICT:
                    fk = corm_str_cat(db->internal_arena, fk, CORM_STR_LIT(" ON DELETE RESTRICT"));
                    break;
                default: break;
            }
            
            sql = corm_str_cat(db->internal_arena, sql, fk);
        }
    }

    sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(");"));

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, corm_str_to_c_safe(db->internal_arena, sql), NULL, NULL, &err_msg);
    corm_arena_end_temp(tmp);

    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("SQL error: %s", err_msg);
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

static bool corm_resolve_relationships(corm_db_t* db) {
    for (size_t i = 0; i < db->model_count; i++) {
        model_meta_t* model = db->models[i];
        
        for (size_t j = 0; j < model->field_count; j++) {
            field_info_t* field = &model->fields[j];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                
                field->related_model = NULL;
                for (size_t k = 0; k < db->model_count; k++) {
                    if (strcmp(db->models[k]->table_name, 
                              field->target_model_name) == 0) {
                        field->related_model = db->models[k];
                        break;
                    }
                }
                
                if (!field->related_model) {
                    CORM_LOG_ERROR("Related model '%s' not found for field '%s'",
                             field->target_model_name, field->name);
                    return false;
                }
            }
        }
    }
    return true;
}
bool corm_sync(corm_db_t* db, corm_sync_mode_e mode) {
    if (!corm_resolve_relationships(db)) {
        return false;
    }

    switch(mode) {
        case CORM_SYNC_SAFE:
        {
            for (uint64_t i = 0; i < db->model_count; ++i) {
                model_meta_t* meta = db->models[i];
                bool existed = corm_table_exists(db, meta->table_name);
                if (!existed) {
                    if (!corm_create_table(db, meta)) {
                        return false;
                    }
                }
            }
        }
        break;
        case CORM_SYNC_DROP:
        {
            sqlite3_exec(db->db, "PRAGMA foreign_keys = OFF;", NULL, NULL, NULL);
            for (uint64_t i = 0; i < db->model_count; ++i) {
                corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
                corm_string_t drop_sql = corm_str_fmt(db->internal_arena, "DROP TABLE IF EXISTS %s;", 
                                           db->models[i]->table_name);
                char* err_msg = NULL;
                int rc = sqlite3_exec(db->db, corm_str_to_c_safe(db->internal_arena, drop_sql), NULL, NULL, &err_msg);
                if (rc != SQLITE_OK) {
                    CORM_LOG_ERROR("Failed to drop table '%s': %s", db->models[i]->table_name, err_msg);
                    sqlite3_free(err_msg);
                    corm_arena_end_temp(tmp);
                    return false;
                }
                corm_arena_end_temp(tmp);
            }
            sqlite3_exec(db->db, "PRAGMA foreign_keys = ON;", NULL, NULL, NULL);

            for (uint64_t i = 0; i < db->model_count; ++i) {
                if (!corm_create_table(db, db->models[i])) {
                    return false;
                }
            }
        }
        break;
        case CORM_SYNC_MIGRATE:
        {
            CORM_LOG_ERROR("CORM_SYNC_MIGRATE is not implemented yet");
            return false;
        }
        break;
    }
    return true;
}

static bool corm_record_exists(corm_db_t* db, model_meta_t* meta, field_info_t* pk_field, void* pk_value) {
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT COUNT(*) FROM %s WHERE %s = ?;", 
                          meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        return false;
    }
    
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        default:
            CORM_LOG_ERROR("Unsupported primary key type");
            sqlite3_finalize(stmt);
            return false;
    }
    
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) > 0;
    }
    
    sqlite3_finalize(stmt);
    return exists;
}
bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    field_info_t* pk_field = meta->primary_key_field;
    if (!pk_field) {
        CORM_LOG_ERROR("Primary key field not found in model '%s'", meta->table_name);
        corm_arena_end_temp(tmp);
        return false;
    }

    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        if (field->validator) {
            void* field_value = (char*)instance + field->offset;
            const char* error_msg = NULL;
            if (!field->validator(field_value, &error_msg)) {
                CORM_LOG_ERROR("Validation failed for field '%s': %s", 
                         field->name, error_msg ? error_msg : "Unknown error");
                corm_arena_end_temp(tmp);
                return false;
            }
        }
    }
    
    void* pk_value = (char*)instance + pk_field->offset;
    bool is_update = corm_record_exists(db, meta, pk_field, pk_value);
    
    corm_string_t sql;
    sqlite3_stmt* stmt;
    
    if (is_update) {
        sql = corm_str_fmt(db->internal_arena, "UPDATE %s SET ", meta->table_name);
        
        bool first = true;
        for (uint64_t i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];

            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            if (field->flags & PRIMARY_KEY || field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
            }
            sql = corm_str_cat(db->internal_arena, sql, corm_str_fmt(db->internal_arena, "%s=?", field->name));
            first = false;
        }
        
        sql = corm_str_cat(db->internal_arena, sql, corm_str_fmt(db->internal_arena, " WHERE %s=?;", pk_field->name));
    } else {
        sql = corm_str_fmt(db->internal_arena, "INSERT INTO %s (", meta->table_name);
        corm_string_t values = CORM_STR_LIT("VALUES (");
        
        bool first = true;
        for (uint64_t i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];

            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }

            if (field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
                values = corm_str_cat(db->internal_arena, values, CORM_STR_LIT(", "));
            }
            
            sql = corm_str_cat(db->internal_arena, sql, corm_str_fmt(db->internal_arena, "%s", field->name));
            values = corm_str_cat(db->internal_arena, values, CORM_STR_LIT("?"));
            first = false;
        }
        
        sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(") "));
        values = corm_str_cat(db->internal_arena, values, CORM_STR_LIT(");"));
        sql = corm_str_cat(db->internal_arena, sql, values);
    }
    
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return false;
    }
    
    int param_idx = 1;
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];

        if (field->type == FIELD_TYPE_BELONGS_TO || 
            field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        if (field->flags & AUTO_INC) {
            continue;
        }
        
        if (is_update && (field->flags & PRIMARY_KEY)) {
            continue;
        }
        
        void* field_value = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, param_idx, *(int*)field_value);
                break;
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, param_idx, *(int64_t*)field_value);
                break;
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, param_idx, (double)(*(float*)field_value));
                break;
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, param_idx, *(double*)field_value);
                break;
            case FIELD_TYPE_STRING: {
                char* str = *(char**)field_value;
                if (str) {
                    sqlite3_bind_text(stmt, param_idx, str, -1, SQLITE_STATIC);
                } else {
                    sqlite3_bind_null(stmt, param_idx);
                }
                break;
            }
            case FIELD_TYPE_BLOB: {
                blob_t* blob = (blob_t*)field_value;
                if (blob && blob->data && blob->size > 0) {
                    sqlite3_bind_blob(stmt, param_idx, blob->data, (int)blob->size, SQLITE_STATIC);
                } else {
                    sqlite3_bind_null(stmt, param_idx);
                }
                break;
            }
            default:
                CORM_LOG_ERROR("Unsupported field type for field '%s'", field->name);
                sqlite3_finalize(stmt);
                corm_arena_end_temp(tmp);
                return false;
        }
        
        param_idx++;
    }
    
    if (is_update) {
        switch (pk_field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, param_idx, *(int*)pk_value);
                break;
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, param_idx, *(int64_t*)pk_value);
                break;
            case FIELD_TYPE_STRING: {
                char* str = *(char**)pk_value;
                sqlite3_bind_text(stmt, param_idx, str, -1, SQLITE_STATIC);
                break;
            }
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, param_idx, (double)(*(float*)pk_value));
                break;
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, param_idx, *(double*)pk_value);
                break;
            default:
                break;
        }
    }
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        CORM_LOG_ERROR("Failed to execute %s: %s", is_update ? "UPDATE" : "INSERT", sqlite3_errmsg(db->db));
        sqlite3_finalize(stmt);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    if (!is_update && (pk_field->flags & AUTO_INC)) {
        int64_t last_id = sqlite3_last_insert_rowid(db->db);
        
        void* pk_ptr = (char*)instance + pk_field->offset;
        if (pk_field->type == FIELD_TYPE_INT) {
            *(int*)pk_ptr = (int)last_id;
        } else if (pk_field->type == FIELD_TYPE_INT64) {
            *(int64_t*)pk_ptr = last_id;
        }
    }
    
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    return true;
}

bool corm_delete(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    if (!db || !meta || !pk_value) {
        CORM_LOG_ERROR("Invalid arguments to corm_delete");
        return false;
    }
    
    if (!meta->primary_key_field) {
        CORM_LOG_ERROR("Model '%s' has no primary key", meta->table_name);
        return false;
    }
    
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "DELETE FROM %s WHERE %s = ?;",
                           meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare DELETE: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return false;
    }
    
    switch (meta->primary_key_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
            
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
            
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
            
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
            
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
            
        default:
            CORM_LOG_ERROR("Unsupported primary key type");
            sqlite3_finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
    }
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    
    if (rc != SQLITE_DONE) {
        CORM_LOG_ERROR("Failed to execute DELETE: %s", sqlite3_errmsg(db->db));
        return false;
    }
    
    int rows_affected = sqlite3_changes(db->db);
    if (rows_affected == 0) {
        CORM_LOG_WARN("No rows deleted (record not found)");
        return false;
    }
    
    return true;
}

void* corm_find(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s = ?;",
                          meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare find query: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    field_info_t* pk_field = meta->primary_key_field;
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        default:
            CORM_LOG_ERROR("Unsupported primary key type");
            sqlite3_finalize(stmt);
            corm_arena_end_temp(tmp);
            return NULL;
    }
    
    void* instance = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        instance = corm_alloc_fn(db, meta->struct_size);
        if (!instance) {
            CORM_LOG_ERROR("Failed to allocate instance");
            sqlite3_finalize(stmt);
            corm_arena_end_temp(tmp);
            return NULL;
        }
        memset(instance, 0, meta->struct_size);
        
        for (uint64_t i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)instance + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                CORM_LOG_WARN("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    CORM_LOG_WARN("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
    }
    
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    
    return instance;
}

void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    corm_string_t count_sql = corm_str_fmt(db->internal_arena, "SELECT COUNT(*) FROM %s;", meta->table_name);
    
    sqlite3_stmt* count_stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, count_sql), -1, &count_stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare count query: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    int record_count = 0;
    if (sqlite3_step(count_stmt) == SQLITE_ROW) {
        record_count = sqlite3_column_int(count_stmt, 0);
    }
    sqlite3_finalize(count_stmt);
    
    if (record_count == 0) {
        if (count) *count = 0;
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    void* instances = corm_alloc_fn(db, meta->struct_size * record_count);
    if (!instances) {
        CORM_LOG_ERROR("Failed to allocate instances array");
        corm_arena_end_temp(tmp);
        return NULL;
    }
    memset(instances, 0, meta->struct_size * record_count);
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT * FROM %s;", meta->table_name);
    
    sqlite3_stmt* stmt;
    rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare find_all query: %s", sqlite3_errmsg(db->db));
        corm_free_fn(db, instances);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < record_count) {
        void* instance = (char*)instances + (idx * meta->struct_size);
        
        for (uint64_t i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)instance + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                CORM_LOG_WARN("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    CORM_LOG_WARN("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    
    if (count) *count = record_count;
    return instances;
}

void* corm_where_raw(corm_db_t* db, model_meta_t* meta, const char* where_clause, 
                     void** params, field_type_e* param_types, size_t param_count, int* count) {
    if (!db || !meta || !where_clause || !count) {
        CORM_LOG_ERROR("Invalid arguments to corm_where_raw");
        return NULL;
    }
    
    *count = 0;
    
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s;", 
                           meta->table_name, where_clause);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare WHERE query: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    for (size_t i = 0; i < param_count; i++) {
        int bind_idx = i + 1;
        
        switch (param_types[i]) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, bind_idx, *(int*)params[i]);
                break;
                
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, bind_idx, *(int64_t*)params[i]);
                break;
                
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, bind_idx, (double)(*(float*)params[i]));
                break;
                
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, bind_idx, *(double*)params[i]);
                break;
                
            case FIELD_TYPE_STRING: {
                char* str = *(char**)params[i];
                sqlite3_bind_text(stmt, bind_idx, str, -1, SQLITE_STATIC);
                break;
            }
            
            case FIELD_TYPE_BLOB: {
                blob_t* blob = (blob_t*)params[i];
                sqlite3_bind_blob(stmt, bind_idx, blob->data, blob->size, SQLITE_STATIC);
                break;
            }
                
            default:
                CORM_LOG_ERROR("Unsupported parameter type %d at index %zu", param_types[i], i);
                sqlite3_finalize(stmt);
                corm_arena_end_temp(tmp);
                return NULL;
        }
    }
    
    int result_count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        result_count++;
    }
    
    if (result_count == 0) {
        sqlite3_finalize(stmt);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    sqlite3_reset(stmt);
    
    void* instances = corm_alloc_fn(db, meta->struct_size * result_count);
    if (!instances) {
        CORM_LOG_ERROR("Failed to allocate instances array");
        sqlite3_finalize(stmt);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    memset(instances, 0, meta->struct_size * result_count);
    
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < result_count) {
        void* inst = (char*)instances + (idx * meta->struct_size);
        
        for (uint64_t i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)inst + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                CORM_LOG_WARN("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    CORM_LOG_WARN("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    
    *count = result_count;
    return instances;
}

static bool corm_load_belongs_to(corm_db_t* db, void* instance,
                                 model_meta_t* meta, field_info_t* field) {
    field_info_t* fk_field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field->fk_column_name) == 0) {
            fk_field = &meta->fields[i];
            break;
        }
    }
    
    if (!fk_field) {
        CORM_LOG_ERROR("Foreign key field '%s' not found in model '%s'", 
                 field->fk_column_name, meta->table_name);
        return false;
    }
    
    if (!field->related_model) {
        CORM_LOG_ERROR("Related model not resolved for field '%s'", field->name);
        return false;
    }
    
    void* fk_value_ptr = (char*)instance + fk_field->offset;
    
    bool is_null = false;
    switch (fk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            is_null = (*(int*)fk_value_ptr == 0);
            break;
        case FIELD_TYPE_INT64:
            is_null = (*(int64_t*)fk_value_ptr == 0);
            break;
        case FIELD_TYPE_STRING:
            is_null = (*(char**)fk_value_ptr == NULL);
            break;
        default:
            break;
    }
    
    if (is_null) {
        void* relation_ptr = (char*)instance + field->offset;
        *(void**)relation_ptr = NULL;
        return true;
    }
    
    void* related_instance = corm_find(db, field->related_model, fk_value_ptr);
    
    if (!related_instance) {
        CORM_LOG_WARN("Related instance not found for field '%s'", field->name);
        return false;
    }
    
    void* relation_ptr = (char*)instance + field->offset;
    *(void**)relation_ptr = related_instance;
    
    return true;
}

static bool corm_load_has_many(corm_db_t* db, void* instance,
                               model_meta_t* meta, field_info_t* field) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    if (!field->related_model) {
        CORM_LOG_ERROR("Related model not resolved for field '%s'", field->name);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    void* pk_value = (char*)instance + meta->primary_key_field->offset;
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s = ?;",
                          field->related_model->table_name,
                          field->fk_column_name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, corm_str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        CORM_LOG_ERROR("Failed to prepare has_many query: %s", sqlite3_errmsg(db->db));
        corm_arena_end_temp(tmp);
        return false;
    }
    
    field_info_t* pk_field = meta->primary_key_field;
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        default:
            CORM_LOG_ERROR("Unsupported primary key type");
            sqlite3_finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
    }
    
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        count++;
    }
    sqlite3_reset(stmt);
    
    void* instances = NULL;
    if (count > 0) {
        instances = corm_alloc_fn(db, field->related_model->struct_size * count);
        if (!instances) {
            CORM_LOG_ERROR("Failed to allocate instances array");
            sqlite3_finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
        }
        memset(instances, 0, field->related_model->struct_size * count);
    }
    
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < count) {
        void* inst = (char*)instances + (idx * field->related_model->struct_size);
        
        for (uint64_t i = 0; i < field->related_model->field_count; i++) {
            field_info_t* rel_field = &field->related_model->fields[i];
            
            if (rel_field->type == FIELD_TYPE_BELONGS_TO || 
                rel_field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)inst + rel_field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), rel_field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                CORM_LOG_WARN("Column '%s' not found in result set", rel_field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (rel_field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    CORM_LOG_WARN("Unsupported field type for field '%s'", rel_field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    corm_arena_end_temp(tmp);
    
    void* relation_ptr = (char*)instance + field->offset;
    *(void**)relation_ptr = instances;
    
    void* count_ptr = (char*)instance + field->count_offset;
    *(int*)count_ptr = count;
    
    return true;
}
bool corm_load_relation(corm_db_t* db, model_meta_t* meta, void* instance, const char* field_name) {
    field_info_t* field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field_name) == 0) {
            field = &meta->fields[i];
            break;
        }
    }
    
    if (!field) {
        CORM_LOG_ERROR("Field '%s' doesn't exist in %s", field_name, meta->table_name);
        return false;
    }
    
    if (field->type == FIELD_TYPE_BELONGS_TO) {
        return corm_load_belongs_to(db, instance, meta, field);
    } else if (field->type == FIELD_TYPE_HAS_MANY) {
        return corm_load_has_many(db, instance, meta, field);
    }
    
    return false;
}

void corm_free(corm_db_t* db, model_meta_t* meta, void* instance) {
    if (!instance) return;
    
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        void* field_ptr = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_STRING: {
                char** str_ptr = (char**)field_ptr;
                if (*str_ptr) {
                    corm_free_fn(db, *str_ptr);
                }
                break;
            }
            case FIELD_TYPE_BLOB: {
                blob_t* blob = (blob_t*)field_ptr;
                if (blob->data) {
                    corm_free_fn(db, blob->data);
                }
                break;
            }
            case FIELD_TYPE_BELONGS_TO: {
                void** related_ptr = (void**)field_ptr;
                if (*related_ptr && field->related_model) {
                    corm_free(db, field->related_model, *related_ptr);
                }
                break;
            }
            case FIELD_TYPE_HAS_MANY: {
                void** related_array = (void**)field_ptr;
                if (*related_array && field->related_model) {
                    void* count_ptr = (char*)instance + field->count_offset;
                    int count = *(int*)count_ptr;
                    
                    for (int j = 0; j < count; j++) {
                        void* inst = (char*)(*related_array) + (j * field->related_model->struct_size);
                        corm_free(db, field->related_model, inst);
                    }
                    
                    corm_free_fn(db, *related_array);
                }
                break;
            }
            default:
                break;
        }
    }
    
    corm_free_fn(db, instance);
}
