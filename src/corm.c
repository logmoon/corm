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

// Arena allocator functions (unchanged from original)
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
    return corm_init_with_backend(corm_backend_sqlite_init(), db_filepath);
}

corm_db_t* corm_init_with_allocator(const char* db_filepath, void* ctx,
                                    void* (*alloc_fn)(void*, size_t),
                                    void (*free_fn)(void*, void*)) {
    return corm_init_with_backend_and_allocator(corm_backend_sqlite_init(), db_filepath, 
                                                ctx, alloc_fn, free_fn);
}

corm_db_t* corm_init_with_backend(const corm_backend_ops_t* backend, 
                                   const char* connection_string) {
    return corm_init_with_backend_and_allocator(backend, connection_string, NULL, NULL, NULL);
}

corm_db_t* corm_init_with_backend_and_allocator(const corm_backend_ops_t* backend,
                                                 const char* connection_string,
                                                 void* ctx,
                                                 void* (*alloc_fn)(void*, size_t),
                                                 void (*free_fn)(void*, void*)) {
    if (!backend) {
        CORM_LOG_ERROR("Backend cannot be NULL");
        return NULL;
    }

    corm_db_t* db = CORM_MALLOC(sizeof(corm_db_t));
    if (db == NULL) {
        CORM_LOG_ERROR("Couldn't allocate db object");
        return NULL;
    }
    
    db->backend = backend;
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
    
    // Connect to database using backend
    char* error = NULL;
    if (!backend->connect(&db->backend_conn, connection_string, &error)) {
        CORM_LOG_ERROR("Cannot connect to database: %s", error ? error : "unknown error");
        if (error) free(error);
        corm_free_fn(db, db->models);
        corm_arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }
    
    CORM_LOG_INFO("Connected to database using backend: %s", backend->name);
    
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
    db->backend->disconnect(db->backend_conn);
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
                     (int)db->model_capacity);
        return false;
    }
    
    db->models[db->model_count++] = meta;
    
    // Resolve relations
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            for (uint64_t j = 0; j < db->model_count; j++) {
                if (strcmp(db->models[j]->table_name, field->target_model_name) == 0) {
                    field->related_model = db->models[j];
                    break;
                }
            }
        }
    }
    
    return true;
}

// Generate CREATE TABLE SQL using backend dialect
static corm_string_t corm_generate_create_table_sql(corm_db_t* db, model_meta_t* meta) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    corm_string_t sql = corm_str_fmt(db->internal_arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);
    
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        // Skip relation fields
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        if (i > 0) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
        }
        
        // Column name and type using backend dialect
        const char* type_name = db->backend->get_type_name(field->type, field->max_length);
        corm_string_t col_def = corm_str_fmt(db->internal_arena, "%s %s", field->name, type_name);
        sql = corm_str_cat(db->internal_arena, sql, col_def);
        
        // Constraints
        if (field->flags & PRIMARY_KEY) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" PRIMARY KEY"));
            if (field->flags & AUTO_INC) {
                const char* auto_inc = db->backend->get_auto_increment();
                if (auto_inc && strlen(auto_inc) > 0) {
                    corm_string_t auto_inc_str = corm_str_fmt(db->internal_arena, " %s", auto_inc);
                    sql = corm_str_cat(db->internal_arena, sql, auto_inc_str);
                }
            }
        }
        
        if (field->flags & NOT_NULL) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" NOT NULL"));
        }
        
        if (field->flags & UNIQUE) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(" UNIQUE"));
        }
    }
    
    sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(");"));
    
    corm_string_t result = sql;
    corm_arena_end_temp(tmp);
    return result;
}

bool corm_sync(corm_db_t* db, corm_sync_mode_e mode) {
    if (mode == CORM_SYNC_DROP) {
        // Drop all tables first
        for (size_t i = 0; i < db->model_count; i++) {
            corm_string_t drop_sql = corm_str_fmt(db->internal_arena, 
                                                  "DROP TABLE IF EXISTS %s;",
                                                  db->models[i]->table_name);
            
            char* error = NULL;
            if (!db->backend->execute(db->backend_conn, corm_str_to_c_safe(db->internal_arena, drop_sql), &error)) {
                CORM_LOG_ERROR("Failed to drop table %s: %s", 
                             db->models[i]->table_name, 
                             error ? error : "unknown error");
                if (error) free(error);
                return false;
            }
        }
    }
    
    // Create tables
    for (size_t i = 0; i < db->model_count; i++) {
        corm_string_t create_sql = corm_generate_create_table_sql(db, db->models[i]);
        
        char* error = NULL;
        if (!db->backend->execute(db->backend_conn, corm_str_to_c_safe(db->internal_arena, create_sql), &error)) {
            CORM_LOG_ERROR("Failed to create table %s: %s", 
                         db->models[i]->table_name,
                         error ? error : "unknown error");
            if (error) free(error);
            return false;
        }
        
        CORM_LOG_INFO("Created table: %s", db->models[i]->table_name);
    }
    
    return true;
}

// Helper function to bind a parameter based on field type
static bool bind_param_by_type(corm_db_t* db, corm_backend_stmt_t stmt, int param_idx,
                                void* value_ptr, field_type_e type) {
    switch (type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return db->backend->bind_int(stmt, param_idx, *(int*)value_ptr);
            
        case FIELD_TYPE_INT64:
            return db->backend->bind_int64(stmt, param_idx, *(int64_t*)value_ptr);
            
        case FIELD_TYPE_FLOAT:
            return db->backend->bind_double(stmt, param_idx, (double)(*(float*)value_ptr));
            
        case FIELD_TYPE_DOUBLE:
            return db->backend->bind_double(stmt, param_idx, *(double*)value_ptr);
            
        case FIELD_TYPE_STRING: {
            char* str = *(char**)value_ptr;
            if (str) {
                return db->backend->bind_string(stmt, param_idx, str, -1);
            } else {
                return db->backend->bind_null(stmt, param_idx);
            }
        }
        
        case FIELD_TYPE_BLOB: {
            blob_t* blob = (blob_t*)value_ptr;
            if (blob->data && blob->size > 0) {
                return db->backend->bind_blob(stmt, param_idx, blob->data, blob->size);
            } else {
                return db->backend->bind_null(stmt, param_idx);
            }
        }
        
        default:
            CORM_LOG_ERROR("Unsupported field type for binding");
            return false;
    }
}

bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    void* pk_value = (char*)instance + meta->primary_key_field->offset;
    int pk_int = (meta->primary_key_field->type == FIELD_TYPE_INT || 
                  meta->primary_key_field->type == FIELD_TYPE_BOOL) 
                  ? *(int*)pk_value : 0;
    
    // Build INSERT SQL with backend-specific placeholders
    corm_string_t sql = corm_str_fmt(db->internal_arena, "INSERT INTO %s (", meta->table_name);
    corm_string_t values_part = CORM_STR_LIT("VALUES (");
    
    int field_idx = 0;
    int param_idx = 1;
    
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        // Skip PK with AUTO_INC and relation fields
        if ((field->flags & (PRIMARY_KEY | AUTO_INC)) == (PRIMARY_KEY | AUTO_INC)) {
            continue;
        }
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        if (field_idx > 0) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
            values_part = corm_str_cat(db->internal_arena, values_part, CORM_STR_LIT(", "));
        }
        
        sql = corm_str_cat(db->internal_arena, sql, corm_str_fmt(db->internal_arena, "%s", field->name));
        
        // Use backend-specific placeholder
        const char* placeholder = db->backend->get_placeholder(param_idx);
        values_part = corm_str_cat(db->internal_arena, values_part, 
                                   corm_str_fmt(db->internal_arena, "%s", placeholder));
        
        field_idx++;
        param_idx++;
    }
    
    sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(") "));
    values_part = corm_str_cat(db->internal_arena, values_part, CORM_STR_LIT(");"));
    sql = corm_str_cat(db->internal_arena, sql, values_part);
    
    // Prepare statement
    corm_backend_stmt_t stmt;
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_LOG_ERROR("Failed to prepare INSERT: %s", error ? error : "unknown error");
        if (error) free(error);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    // Bind parameters
    param_idx = 1;
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        if ((field->flags & (PRIMARY_KEY | AUTO_INC)) == (PRIMARY_KEY | AUTO_INC)) {
            continue;
        }
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        void* field_ptr = (char*)instance + field->offset;
        
        if (!bind_param_by_type(db, stmt, param_idx, field_ptr, field->type)) {
            CORM_LOG_ERROR("Failed to bind parameter %d", param_idx);
            db->backend->finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
        }
        
        param_idx++;
    }
    
    // Execute
    int result = db->backend->step(stmt);
    db->backend->finalize(stmt);
    
    if (result < 0) {
        CORM_LOG_ERROR("Failed to execute INSERT");
        corm_arena_end_temp(tmp);
        return false;
    }
    
    // Get last insert ID if AUTO_INC
    if ((meta->primary_key_field->flags & AUTO_INC) && 
        (meta->primary_key_field->type == FIELD_TYPE_INT || meta->primary_key_field->type == FIELD_TYPE_INT64)) {
        
        int64_t last_id = db->backend->last_insert_id(db->backend_conn);
        
        if (meta->primary_key_field->type == FIELD_TYPE_INT) {
            *(int*)pk_value = (int)last_id;
        } else {
            *(int64_t*)pk_value = last_id;
        }
    }
    
    corm_arena_end_temp(tmp);
    return true;
}

void* corm_find(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    // Build SELECT with backend-specific placeholder
    const char* placeholder = db->backend->get_placeholder(1);
    corm_string_t sql = corm_str_fmt(db->internal_arena, 
                                     "SELECT * FROM %s WHERE %s = %s;",
                                     meta->table_name,
                                     meta->primary_key_field->name,
                                     placeholder);
    
    corm_backend_stmt_t stmt;
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_LOG_ERROR("Failed to prepare SELECT: %s", error ? error : "unknown error");
        if (error) free(error);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    // Bind PK value
    if (!bind_param_by_type(db, stmt, 1, pk_value, meta->primary_key_field->type)) {
        CORM_LOG_ERROR("Failed to bind primary key");
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    // Execute and fetch
    int result = db->backend->step(stmt);
    
    if (result != 1) {
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    
    // Allocate instance
    void* instance = corm_alloc_fn(db, meta->struct_size);
    if (!instance) {
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return NULL;
    }
    memset(instance, 0, meta->struct_size);
    
    // Fill instance from result set
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        void* field_ptr = (char*)instance + field->offset;
        
        // Find column index
        int col_idx = -1;
        int col_count = db->backend->column_count(stmt);
        for (int j = 0; j < col_count; j++) {
            if (strcmp(db->backend->column_name(stmt, j), field->name) == 0) {
                col_idx = j;
                break;
            }
        }
        
        if (col_idx == -1) continue;
        
        // Check for NULL
        if (db->backend->column_type(stmt, col_idx) == 0) {
            continue;
        }
        
        // Extract value based on type
        switch (field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                *(int*)field_ptr = db->backend->column_int(stmt, col_idx);
                break;
                
            case FIELD_TYPE_INT64:
                *(int64_t*)field_ptr = db->backend->column_int64(stmt, col_idx);
                break;
                
            case FIELD_TYPE_FLOAT:
                *(float*)field_ptr = (float)db->backend->column_double(stmt, col_idx);
                break;
                
            case FIELD_TYPE_DOUBLE:
                *(double*)field_ptr = db->backend->column_double(stmt, col_idx);
                break;
                
            case FIELD_TYPE_STRING: {
                const char* text = (const char*)db->backend->column_text(stmt, col_idx);
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
                const void* blob_data = db->backend->column_blob(stmt, col_idx);
                int blob_size = db->backend->column_bytes(stmt, col_idx);
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
                break;
        }
    }
    
    db->backend->finalize(stmt);
    corm_arena_end_temp(tmp);
    
    return instance;
}

// Stub implementations for remaining functions - pattern is the same
void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count) {
    // Similar to corm_find but loops through all results
    // Replace sqlite3_* calls with db->backend->* calls
    *count = 0;
    return NULL; // Stub
}

void* corm_where_raw(corm_db_t* db, model_meta_t* meta, const char* where_clause, 
                     void** params, field_type_e* param_types, size_t param_count, int* count) {
    // Uses backend->prepare, backend->bind_*, backend->step
    *count = 0;
    return NULL; // Stub
}

bool corm_delete(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    // Generates DELETE with backend->get_placeholder
    return false; // Stub
}

bool corm_load_relation(corm_db_t* db, model_meta_t* meta, void* instance, const char* field_name) {
    // Uses backend operations for loading relations
    return false; // Stub
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
