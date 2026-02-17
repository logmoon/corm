#include "corm.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#define CORM_SET_ERROR(db, fmt, ...) \
    snprintf((db)->last_error, sizeof((db)->last_error), fmt, ##__VA_ARGS__)

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

// Arena allocator functions
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
    if (!data) return (corm_string_t){0};

    va_start(args, fmt);
    vsnprintf((char*)data, len + 1, fmt, args);
    va_end(args);

    data[len] = 0;
    return (corm_string_t){ data, (uint64_t)len };
}

static inline corm_string_t corm_str_cat(corm_arena_t* arena, corm_string_t a, corm_string_t b) {
    uint64_t size = a.size + b.size;
    uint8_t* data = (uint8_t*)corm_arena_alloc(arena, size + 1);
    if (!data) return (corm_string_t){0};

    memcpy(data, a.str, a.size);
    memcpy(data + a.size, b.str, b.size);
    data[size] = 0;

    return (corm_string_t){ data, size };
}

static inline const char* corm_str_to_c_safe(corm_arena_t* arena, corm_string_t s) {
    if (!s.str) return NULL;
    if (s.str[s.size] == 0) {
        return (const char*)s.str;
    }

    uint8_t* data = (uint8_t*)corm_arena_alloc(arena, s.size + 1);
    if (!data) return NULL;
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

static inline corm_result_t* corm_result_create(corm_db_t* db, model_meta_t* meta) {
    corm_result_t* result = corm_alloc_fn(db, sizeof(corm_result_t));
    if (!result) return NULL;
    
    result->data = NULL;
    result->count = 0;
    result->meta = meta;
    result->allocation_capacity = 16;
    result->allocation_count = 0;
    result->allocations = corm_alloc_fn(db, sizeof(void*) * result->allocation_capacity);
    
    if (!result->allocations) {
        corm_free_fn(db, result);
        return NULL;
    }
    
    return result;
}

static inline bool corm_result_track(corm_db_t* db, corm_result_t* result, void* ptr) {
    if (!ptr || !result) return false;
    
    if (result->allocation_count >= result->allocation_capacity) {
        size_t new_cap = result->allocation_capacity * 2;
        void** new_arr = corm_alloc_fn(db, sizeof(void*) * new_cap);
        if (!new_arr) return false;
        
        memcpy(new_arr, result->allocations, sizeof(void*) * result->allocation_count);
        corm_free_fn(db, result->allocations);
        result->allocations = new_arr;
        result->allocation_capacity = new_cap;
    }
    
    result->allocations[result->allocation_count++] = ptr;
    return true;
}

static inline void* corm_result_alloc(corm_db_t* db, corm_result_t* result, size_t size) {
    void* ptr = corm_alloc_fn(db, size);
    if (ptr) {
        if (!corm_result_track(db, result, ptr)) {
            corm_free_fn(db, ptr);
            return NULL;
        }
    }
    return ptr;
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
        return NULL;
    }

    corm_db_t* db = CORM_MALLOC(sizeof(corm_db_t));
    if (db == NULL) {
        return NULL;
    }
    
    db->backend = backend;
    db->allocator.alloc_fn = alloc_fn;
    db->allocator.free_fn = free_fn;
    db->allocator.ctx = ctx;
    
    db->internal_arena = corm_arena_create(CORM_MIB(1));
    if (db->internal_arena == NULL) {
        CORM_FREE(db);
        return NULL;
    }
    
    db->model_count = 0;
    db->model_capacity = CORM_MAX_MODELS;
    
    db->models = corm_alloc_fn(db, sizeof(model_meta_t*) * CORM_MAX_MODELS);
    if (db->models == NULL) {
        corm_arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }
    
    // Connect to database using backend
    char* error = NULL;
    if (!backend->connect(&db->backend_conn, connection_string, &error)) {
        CORM_SET_ERROR(db, "Cannot connect to database: %s", error ? error : "unknown error");
        if (error) free(error);
        corm_free_fn(db, db->models);
        corm_arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
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
    db->backend->disconnect(db->backend_conn);
    corm_arena_destroy(db->internal_arena);
    corm_free_fn(db, db->models);
    CORM_FREE(db);
}

const char* corm_get_last_error(corm_db_t* db) {
    if (!db) return "Invalid database handle";
    return db->last_error;
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
        CORM_SET_ERROR(db, "Model '%s' must have exactly one PRIMARY_KEY field", meta->table_name);
        return false;
    }
    if (pk_count > 1) {
        CORM_SET_ERROR(db, "Model '%s' has %d PRIMARY_KEY fields, expected 1", meta->table_name, pk_count);
        return false;
    }
    
    meta->primary_key_field = pk_field;

    if (db->model_count >= db->model_capacity) {
        CORM_SET_ERROR(db, "Maximum number of models (%d) reached. Define CORM_MAX_MODELS to increase.",
                     (int)db->model_capacity);
        return false;
    }
    
    db->models[db->model_count++] = meta;
    return true;
}

static bool extract_field_from_column(corm_db_t* db, corm_result_t* result,
                                      corm_backend_stmt_t stmt, int col_idx,
                                      void* field_ptr, field_type_e type) {
    if (db->backend->column_type(stmt, col_idx) == 0) {
        return true;
    }
    
    switch (type) {
        case FIELD_TYPE_INT:
            *(int*)field_ptr = db->backend->column_int(stmt, col_idx);
            break;
        
        case FIELD_TYPE_BOOL:
            *(bool*)field_ptr = db->backend->column_int(stmt, col_idx) != 0;
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
                char* str = corm_result_alloc(db, result, len + 1);
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
                void* data = corm_result_alloc(db, result, blob_size);
                if (data) {
                    memcpy(data, blob_data, blob_size);
                    ((blob_t*)field_ptr)->data = data;
                    ((blob_t*)field_ptr)->size = blob_size;
                }
            }
            break;
        }
        
        default:
            return false;
    }
    
    return true;
}

static bool bind_param_by_type(corm_db_t* db, corm_backend_stmt_t stmt, int param_idx,
                                void* value_ptr, field_type_e type) {
    switch (type) {
        case FIELD_TYPE_INT:
            return db->backend->bind_int(stmt, param_idx, *(int*)value_ptr);
            
        case FIELD_TYPE_BOOL:
            return db->backend->bind_int(stmt, param_idx, *(bool*)value_ptr ? 1 : 0);
            
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
            CORM_SET_ERROR(db, "Unsupported field type for binding");
            return false;
    }
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
                    CORM_SET_ERROR(db, "Related model '%s' not found for field '%s'",
                             field->target_model_name, field->name);
                    return false;
                }
            }
        }
    }
    return true;
}

static bool corm_table_exists(corm_db_t* db, const char* table_name) {
    return db->backend->table_exists(db->backend_conn, table_name);
}

static bool corm_record_exists(corm_db_t* db, model_meta_t* meta, field_info_t* pk_field, void* pk_value) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    const char* placeholder = db->backend->get_placeholder(1);
    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT COUNT(*) FROM %s WHERE %s = %s;", 
                          meta->table_name, pk_field->name, placeholder);
    
    corm_backend_stmt_t stmt;
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_SET_ERROR(db, "Failed to prepare statement: %s", error ? error : "unknown");
        if (error) free(error);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    if (!bind_param_by_type(db, stmt, 1, pk_value, pk_field->type)) {
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    bool exists = false;
    if (db->backend->step(stmt) == 1) {
        exists = db->backend->column_int(stmt, 0) > 0;
    }
    
    db->backend->finalize(stmt);
    corm_arena_end_temp(tmp);
    return exists;
}

static corm_string_t corm_generate_create_table_sql(corm_db_t* db, model_meta_t* meta) {
    corm_string_t sql = corm_str_fmt(db->internal_arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);
    
    bool first = true;
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        if (field->type == FIELD_TYPE_BELONGS_TO || field->type == FIELD_TYPE_HAS_MANY) {
            continue;
        }
        
        if (!first) {
            sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
        }
        
        char type_buf[128];
        const char* type_name = db->backend->get_type_name(field->type, field->max_length, type_buf, sizeof(type_buf));
        corm_string_t col_def = corm_str_fmt(db->internal_arena, "%s %s", field->name, type_name);
        sql = corm_str_cat(db->internal_arena, sql, col_def);
        
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
        
        first = false;
    }
    
    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];

        if (field->type == FIELD_TYPE_BELONGS_TO) {
            corm_string_t fk = corm_str_fmt(db->internal_arena, ", FOREIGN KEY (%s) REFERENCES %s(%s)",
                field->fk_column_name,
                field->target_model_name,
                field->related_model->primary_key_field->name
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
    return sql;
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
                    corm_string_t create_sql = corm_generate_create_table_sql(db, meta);
                    char* error = NULL;
                    if (!db->backend->execute(db->backend_conn, corm_str_to_c_safe(db->internal_arena, create_sql), &error)) {
                        CORM_SET_ERROR(db, "Failed to create table %s: %s", meta->table_name, error ? error : "unknown error");
                        if (error) free(error);
                        return false;
                    }
                }
            }
        }
        break;
        
        case CORM_SYNC_DROP:
        {
            db->backend->set_foreign_keys(db->backend_conn, false);
            
            for (uint64_t i = 0; i < db->model_count; ++i) {
                corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
                corm_string_t drop_sql = corm_str_fmt(db->internal_arena, "DROP TABLE IF EXISTS %s;", 
                                           db->models[i]->table_name);
                char* error = NULL;
                if (!db->backend->execute(db->backend_conn, corm_str_to_c_safe(db->internal_arena, drop_sql), &error)) {
                    CORM_SET_ERROR(db, "Failed to drop table '%s': %s", db->models[i]->table_name, error ? error : "unknown");
                    if (error) free(error);
                    corm_arena_end_temp(tmp);
                    return false;
                }
                corm_arena_end_temp(tmp);
            }
            
            db->backend->set_foreign_keys(db->backend_conn, true);

            for (uint64_t i = 0; i < db->model_count; ++i) {
                corm_string_t create_sql = corm_generate_create_table_sql(db, db->models[i]);
                char* error = NULL;
                if (!db->backend->execute(db->backend_conn, corm_str_to_c_safe(db->internal_arena, create_sql), &error)) {
                    CORM_SET_ERROR(db, "Failed to create table %s: %s", db->models[i]->table_name, error ? error : "unknown error");
                    if (error) free(error);
                    return false;
                }
            }
        }
        break;
        
        case CORM_SYNC_MIGRATE:
        {
            CORM_SET_ERROR(db, "CORM_SYNC_MIGRATE is not implemented yet");
            return false;
        }
        break;
    }
    return true;
}

bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    field_info_t* pk_field = meta->primary_key_field;
    if (!pk_field) {
        CORM_SET_ERROR(db, "Primary key field not found in model '%s'", meta->table_name);
        corm_arena_end_temp(tmp);
        return false;
    }

    for (uint64_t i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        if (field->validator) {
            void* field_value = (char*)instance + field->offset;
            const char* error_msg = NULL;
            if (!field->validator(instance, field_value, &error_msg)) {
                CORM_SET_ERROR(db, "Validation failed for field '%s': %s", 
                         field->name, error_msg ? error_msg : "Unknown error");
                corm_arena_end_temp(tmp);
                return false;
            }
        }
    }
    
    void* pk_value = (char*)instance + pk_field->offset;
    bool is_update = corm_record_exists(db, meta, pk_field, pk_value);
    
    corm_string_t sql;
    corm_backend_stmt_t stmt;
    
    if (is_update) {
        sql = corm_str_fmt(db->internal_arena, "UPDATE %s SET ", meta->table_name);
        
        bool first = true;
        int param_idx = 1;
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
            
            const char* placeholder = db->backend->get_placeholder(param_idx);
            sql = corm_str_cat(db->internal_arena, sql, 
                              corm_str_fmt(db->internal_arena, "%s=%s", field->name, placeholder));
            first = false;
            param_idx++;
        }
        
        const char* where_placeholder = db->backend->get_placeholder(param_idx);
        sql = corm_str_cat(db->internal_arena, sql, 
                          corm_str_fmt(db->internal_arena, " WHERE %s=%s;", pk_field->name, where_placeholder));
    } else {
        sql = corm_str_fmt(db->internal_arena, "INSERT INTO %s (", meta->table_name);
        corm_string_t values = CORM_STR_LIT("VALUES (");
        
        bool first = true;
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
            
            if (!first) {
                sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(", "));
                values = corm_str_cat(db->internal_arena, values, CORM_STR_LIT(", "));
            }
            
            sql = corm_str_cat(db->internal_arena, sql, corm_str_fmt(db->internal_arena, "%s", field->name));
            
            const char* placeholder = db->backend->get_placeholder(param_idx);
            values = corm_str_cat(db->internal_arena, values, 
                                 corm_str_fmt(db->internal_arena, "%s", placeholder));
            first = false;
            param_idx++;
        }
        
        sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(") "));
        values = corm_str_cat(db->internal_arena, values, CORM_STR_LIT(");"));
        sql = corm_str_cat(db->internal_arena, sql, values);
    }
    
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_SET_ERROR(db, "Failed to prepare %s: %s", is_update ? "UPDATE" : "INSERT", error ? error : "unknown");
        if (error) free(error);
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
        
        if (!bind_param_by_type(db, stmt, param_idx, field_value, field->type)) {
            CORM_SET_ERROR(db, "Failed to bind parameter %d for field '%s'", param_idx, field->name);
            db->backend->finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
        }
        
        param_idx++;
    }
    
    if (is_update) {
        if (!bind_param_by_type(db, stmt, param_idx, pk_value, pk_field->type)) {
            db->backend->finalize(stmt);
            corm_arena_end_temp(tmp);
            return false;
        }
    }
    
    int result = db->backend->step(stmt);
    if (result < 0) {
        const char* backend_err = db->backend->get_error(db->backend_conn);
        CORM_SET_ERROR(db, "Failed to execute %s: %s", 
                       is_update ? "UPDATE" : "INSERT",
                       backend_err ? backend_err : "unknown error");
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    if (!is_update && (pk_field->flags & AUTO_INC)) {
        int64_t last_id = db->backend->last_insert_id(db->backend_conn);
        
        void* pk_ptr = (char*)instance + pk_field->offset;
        if (pk_field->type == FIELD_TYPE_INT) {
            *(int*)pk_ptr = (int)last_id;
        } else if (pk_field->type == FIELD_TYPE_INT64) {
            *(int64_t*)pk_ptr = last_id;
        }
    }
    
    db->backend->finalize(stmt);
    corm_arena_end_temp(tmp);
    return true;
}

corm_query_t* corm_query(corm_db_t* db, model_meta_t* meta) {
    if (!db || !meta) return NULL;

    corm_query_t* q = corm_alloc_fn(db, sizeof(corm_query_t));
    if (!q) return NULL;

    q->db          = db;
    q->meta        = meta;
    q->where_clause = NULL;
    q->params      = NULL;
    q->param_types = NULL;
    q->param_count = 0;
    q->order_by    = NULL;
    q->limit       = -1;
    q->offset      = 0;

    return q;
}

void corm_query_where(corm_query_t* q, const char* clause,
                      void** params, field_type_e* types, size_t count) {
    q->where_clause = clause;
    q->params       = params;
    q->param_types  = types;
    q->param_count  = count;
}

void corm_query_order_by(corm_query_t* q, const char* order_by) {
    q->order_by = order_by;
}

void corm_query_limit(corm_query_t* q, int limit) {
    q->limit = limit;
}

void corm_query_offset(corm_query_t* q, int offset) {
    q->offset = offset;
}

corm_result_t* corm_query_exec(corm_query_t* q) {
    if (!q) return NULL;

    corm_db_t*    db   = q->db;
    model_meta_t* meta = q->meta;

    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);

    corm_string_t sql = corm_str_fmt(db->internal_arena, "SELECT * FROM %s", meta->table_name);

    if (q->where_clause) {
        // translate each ? to the backend's placeholder
        corm_string_t where = CORM_STR_LIT("");
        const char* cur = q->where_clause;
        int param_idx = 1;
        while (*cur) {
            const char* next = strchr(cur, '?');
            if (!next) {
                where = corm_str_cat(db->internal_arena, where,
                        (corm_string_t){ (uint8_t*)cur, (uint64_t)strlen(cur) });
                break;
            }
            where = corm_str_cat(db->internal_arena, where,
                    (corm_string_t){ (uint8_t*)cur, (uint64_t)(next - cur) });
            const char* ph = db->backend->get_placeholder(param_idx++);
            where = corm_str_cat(db->internal_arena, where,
                    (corm_string_t){ (uint8_t*)ph, (uint64_t)strlen(ph) });
            cur = next + 1;
        }
        sql = corm_str_cat(db->internal_arena, sql,
              corm_str_fmt(db->internal_arena, " WHERE %.*s", (int)where.size, where.str));
    }

    if (q->order_by) {
        sql = corm_str_cat(db->internal_arena, sql,
              corm_str_fmt(db->internal_arena, " ORDER BY %s", q->order_by));
    }

    if (q->limit != -1 || q->offset > 0) {
        const char* limit_str = db->backend->get_limit_syntax(q->limit, q->offset);
        sql = corm_str_cat(db->internal_arena, sql,
              corm_str_fmt(db->internal_arena, " %s", limit_str));
    }

    sql = corm_str_cat(db->internal_arena, sql, CORM_STR_LIT(";"));

    corm_backend_stmt_t stmt;
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_SET_ERROR(db, "Failed to prepare query: %s", error ? error : "unknown");
        if (error) free(error);
        corm_free_fn(db, q);
        corm_arena_end_temp(tmp);
        return NULL;
    }

    for (size_t i = 0; i < q->param_count; i++) {
        if (!bind_param_by_type(db, stmt, (int)(i + 1), q->params[i], q->param_types[i])) {
            CORM_SET_ERROR(db, "Failed to bind parameter %zu", i);
            db->backend->finalize(stmt);
            corm_free_fn(db, q);
            corm_arena_end_temp(tmp);
            return NULL;
        }
    }

    int col_count = db->backend->column_count(stmt);
    int* col_map = corm_arena_alloc(db->internal_arena, sizeof(int) * meta->field_count);
    if (!col_map) {
        db->backend->finalize(stmt);
        corm_free_fn(db, q);
        corm_arena_end_temp(tmp);
        return NULL;
    }

    for (uint64_t i = 0; i < meta->field_count; i++) {
        col_map[i] = -1;
        if (meta->fields[i].type == FIELD_TYPE_BELONGS_TO ||
            meta->fields[i].type == FIELD_TYPE_HAS_MANY) continue;

        for (int j = 0; j < col_count; j++) {
            if (strcmp(db->backend->column_name(stmt, j), meta->fields[i].name) == 0) {
                col_map[i] = j;
                break;
            }
        }
    }

    corm_result_t* res = corm_result_create(db, meta);
    if (!res) {
        db->backend->finalize(stmt);
        corm_free_fn(db, q);
        corm_arena_end_temp(tmp);
        return NULL;
    }

    size_t capacity = 16;
    void* instances = corm_alloc_fn(db, meta->struct_size * capacity);
    if (!instances) {
        CORM_SET_ERROR(db, "Failed to allocate instances array");
        corm_free_fn(db, res->allocations);
        corm_free_fn(db, res);
        db->backend->finalize(stmt);
        corm_free_fn(db, q);
        corm_arena_end_temp(tmp);
        return NULL;
    }

    size_t count = 0;
    while (db->backend->step(stmt) == 1) {
        if (count >= capacity) {
            size_t new_cap = capacity * 2;
            void* grown = corm_alloc_fn(db, meta->struct_size * new_cap);
            if (!grown) {
                CORM_SET_ERROR(db, "Failed to grow instances array");
                corm_free_fn(db, instances);
                corm_free_fn(db, res->allocations);
                corm_free_fn(db, res);
                db->backend->finalize(stmt);
                corm_free_fn(db, q);
                corm_arena_end_temp(tmp);
                return NULL;
            }
            memcpy(grown, instances, meta->struct_size * count);
            corm_free_fn(db, instances);
            instances = grown;
            capacity  = new_cap;
        }

        void* inst = (char*)instances + (count * meta->struct_size);
        memset(inst, 0, meta->struct_size);

        for (uint64_t i = 0; i < meta->field_count; i++) {
            if (col_map[i] == -1) continue;
            void* field_ptr = (char*)inst + meta->fields[i].offset;
            extract_field_from_column(db, res, stmt, col_map[i], field_ptr, meta->fields[i].type);
        }

        count++;
    }

    db->backend->finalize(stmt);
    corm_free_fn(db, q);
    corm_arena_end_temp(tmp);

    if (count == 0) {
        corm_free_fn(db, instances);
        corm_free_fn(db, res->allocations);
        corm_free_fn(db, res);
        return NULL;
    }

    res->data  = instances;
    res->count = (int)count;

    return res;
}

bool corm_delete(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    if (!db || !meta || !pk_value) {
        CORM_SET_ERROR(db, "Invalid arguments to corm_delete");
        return false;
    }
    
    if (!meta->primary_key_field) {
        CORM_SET_ERROR(db, "Model '%s' has no primary key", meta->table_name);
        return false;
    }
    
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);
    
    const char* placeholder = db->backend->get_placeholder(1);
    corm_string_t sql = corm_str_fmt(db->internal_arena, "DELETE FROM %s WHERE %s = %s;",
                           meta->table_name, meta->primary_key_field->name, placeholder);
    
    corm_backend_stmt_t stmt;
    char* error = NULL;
    if (!db->backend->prepare(db->backend_conn, &stmt, corm_str_to_c_safe(db->internal_arena, sql), &error)) {
        CORM_SET_ERROR(db, "Failed to prepare DELETE: %s", error ? error : "unknown");
        if (error) free(error);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    if (!bind_param_by_type(db, stmt, 1, pk_value, meta->primary_key_field->type)) {
        CORM_SET_ERROR(db, "Failed to bind primary key");
        db->backend->finalize(stmt);
        corm_arena_end_temp(tmp);
        return false;
    }
    
    int result = db->backend->step(stmt);
    db->backend->finalize(stmt);
    corm_arena_end_temp(tmp);
    
    if (result < 0) {
        const char* backend_err = db->backend->get_error(db->backend_conn);
        CORM_SET_ERROR(db, "Failed to execute DELETE: %s", backend_err ? backend_err : "unknown error");
        return false;
    }
    
    return true;
}

static corm_result_t* corm_load_belongs_to(corm_db_t* db, void* instance, model_meta_t* meta,
								 field_info_t* field) {
    field_info_t* fk_field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field->fk_column_name) == 0) {
            fk_field = &meta->fields[i];
            break;
        }
    }
    
    if (!fk_field) {
        CORM_SET_ERROR(db, "Foreign key field '%s' not found in model '%s'", 
                 field->fk_column_name, meta->table_name);
        return NULL;
    }
    
    if (!field->related_model) {
        CORM_SET_ERROR(db, "Related model not resolved for field '%s'", field->name);
        return NULL;
    }
    
    void* fk_value_ptr = (char*)instance + fk_field->offset;
    
    bool is_null = false;
    switch (fk_field->type) {
        case FIELD_TYPE_INT:
            is_null = (*(int*)fk_value_ptr == 0);
            break;
        case FIELD_TYPE_BOOL:
            is_null = (*(bool*)fk_value_ptr == 0);
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
        return NULL;
    }
    
    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);

    const char* placeholder = db->backend->get_placeholder(1);
    corm_string_t where = corm_str_fmt(db->internal_arena, "%s = %s",
                                       field->related_model->primary_key_field->name,
                                       placeholder);

    void* params[]          = { fk_value_ptr };
    field_type_e types[]    = { fk_field->type };

    corm_query_t* q = corm_query(db, field->related_model);
    if (!q) {
        corm_arena_end_temp(tmp);
        return NULL;
    }

    corm_query_where(q, (const char*)where.str, params, types, 1);
    corm_result_t* related_result = corm_query_exec(q);

    corm_arena_end_temp(tmp);

    if (!related_result) {
        return NULL;
    }

    void* relation_ptr = (char*)instance + field->offset;
    *(void**)relation_ptr = related_result->data;

    return related_result;
}

static corm_result_t* corm_load_has_many(corm_db_t* db, void* instance, model_meta_t* meta,
										field_info_t* field) {
    
    if (!field->related_model) {
        CORM_SET_ERROR(db, "Related model not resolved for field '%s'", field->name);
        return NULL;
    }
    
    void* pk_value = (char*)instance + meta->primary_key_field->offset;

    corm_temp_t tmp = corm_arena_start_temp(db->internal_arena);

    const char* placeholder = db->backend->get_placeholder(1);
    corm_string_t where = corm_str_fmt(db->internal_arena, "%s = %s",
                                       field->fk_column_name, placeholder);

    void* params[]          = { pk_value };
    field_type_e types[]    = { meta->primary_key_field->type };

    corm_query_t* q = corm_query(db, field->related_model);
    if (!q) {
        corm_arena_end_temp(tmp);
        *(void**)((char*)instance + field->offset)          = NULL;
        *(int*)((char*)instance + field->count_offset)      = 0;
        return NULL;
    }

    corm_query_where(q, (const char*)where.str, params, types, 1);
    corm_result_t* result = corm_query_exec(q);

    corm_arena_end_temp(tmp);
    
    void* relation_ptr = (char*)instance + field->offset;
	void* count_ptr = (char*)instance + field->count_offset;

	if (result) {
        *(void**)relation_ptr = result->data;
        *(int*)count_ptr = result->count;
    } else {
        *(void**)relation_ptr = NULL;
        *(int*)count_ptr = 0;
    }
    
    return result;
}

corm_result_t* corm_load_relation(corm_db_t* db, model_meta_t* meta, void* instance, const char* field_name) {
    field_info_t* field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field_name) == 0) {
            field = &meta->fields[i];
            break;
        }
    }
    
    if (!field) {
        CORM_SET_ERROR(db, "Field '%s' doesn't exist in %s", field_name, meta->table_name);
        return NULL;
    }
    
    if (field->type == FIELD_TYPE_BELONGS_TO) {
        return corm_load_belongs_to(db, instance, meta, field);
    } else if (field->type == FIELD_TYPE_HAS_MANY) {
        return corm_load_has_many(db, instance, meta, field);
    }
    
    return NULL;
}

void corm_free_result(corm_db_t* db, corm_result_t* result) {
    if (!result) return;
    
	if (result->allocations) {
        for (size_t i = 0; i < result->allocation_count; i++) {
            if (result->allocations[i]) {
                corm_free_fn(db, result->allocations[i]);
            }
        }
        corm_free_fn(db, result->allocations);
    }
    
	if (result->data) {
        corm_free_fn(db, result->data);
    }

    corm_free_fn(db, result);
}
