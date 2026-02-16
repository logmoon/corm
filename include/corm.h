#ifndef CORM_H_
#define CORM_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>

#include "corm_backend.h"

#ifndef CORM_MAX_MODELS
#define CORM_MAX_MODELS 128
#endif

#ifndef CORM_MALLOC
#define CORM_MALLOC malloc
#endif

#ifndef CORM_FREE
#define CORM_FREE free
#endif

typedef struct corm_arena_t corm_arena_t;

typedef enum field_type_e {
    FIELD_TYPE_INT,
    FIELD_TYPE_INT64,
    FIELD_TYPE_FLOAT,
    FIELD_TYPE_DOUBLE,
    FIELD_TYPE_STRING,
    FIELD_TYPE_BOOL,
    FIELD_TYPE_BLOB,
    FIELD_TYPE_BELONGS_TO,
    FIELD_TYPE_HAS_MANY
} field_type_e;

typedef struct {
    void* data;
    size_t size;
} blob_t;

typedef enum {
    FK_NO_ACTION,
    FK_CASCADE,
    FK_SET_NULL,
    FK_RESTRICT
} fk_delete_action_e;

typedef struct model_meta_t model_meta_t;

typedef bool (*validator_fn)(void* value, const char** error_msg);

typedef struct field_info_t {
    const char* name;
    size_t offset;
    field_type_e type;

    int flags;
    size_t max_length;
    validator_fn validator;

    const char* target_model_name;
    const char* fk_column_name;
    size_t count_offset;
    fk_delete_action_e on_delete;
    model_meta_t* related_model;
} field_info_t;

typedef struct model_meta_t {
    const char* table_name;
    size_t struct_size;
    field_info_t* fields;
    size_t field_count;
    field_info_t* primary_key_field;
} model_meta_t;

typedef struct {
    void* (*alloc_fn)(void* ctx, size_t size);
    void (*free_fn)(void* ctx, void* ptr);
    void* ctx;
} corm_allocator_t;

typedef struct corm_db_t {
    corm_backend_conn_t backend_conn;
    const corm_backend_ops_t* backend;
    corm_arena_t* internal_arena;
    corm_allocator_t allocator;
    model_meta_t** models;
    size_t model_count;
    size_t model_capacity;
} corm_db_t;

typedef struct corm_result_t {
    void* data;
    int count;
    model_meta_t* meta;
    void** allocations;
    size_t allocation_count;
    size_t allocation_capacity;
} corm_result_t;

#define NO_FLAGS 0
enum {
    PRIMARY_KEY = (1 << 0),
    NOT_NULL    = (1 << 1),
    UNIQUE      = (1 << 2),
    AUTO_INC    = (1 << 3),
};

typedef enum {
    CORM_SYNC_SAFE,
    CORM_SYNC_DROP,
    CORM_SYNC_MIGRATE,
} corm_sync_mode_e;

#define _NARGS_IMPL(_1,_2,_3,_4,_5,N,...) N
#define _NARGS(...) _NARGS_IMPL(__VA_ARGS__, 5, 4, 3, 2, 1)

#define _CONCAT(a, b) a##b
#define _DISPATCH(prefix, N) _CONCAT(prefix, N)

#define _BASE_FIELD(stype, fname, ftype) \
    .name = #fname, \
    .offset = offsetof(stype, fname), \
    .type = ftype, \
    .flags = 0, \
    .max_length = 0, \
    .validator = NULL, \
    .target_model_name = NULL, \
    .fk_column_name = NULL, \
    .count_offset = 0, \
    .on_delete = FK_NO_ACTION, \
    .related_model = NULL

#define _F_INT_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT) }
#define _F_INT_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT), .flags = fflags }
#define _F_INT_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT), .flags = fflags, .validator = fval }
#define F_INT(...) _DISPATCH(_F_INT_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_INT64_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT64) }
#define _F_INT64_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT64), .flags = fflags }
#define _F_INT64_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_INT64), .flags = fflags, .validator = fval }
#define F_INT64(...) _DISPATCH(_F_INT64_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_FLOAT_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_FLOAT) }
#define _F_FLOAT_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_FLOAT), .flags = fflags }
#define _F_FLOAT_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_FLOAT), .flags = fflags, .validator = fval }
#define F_FLOAT(...) _DISPATCH(_F_FLOAT_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_DOUBLE_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_DOUBLE) }
#define _F_DOUBLE_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_DOUBLE), .flags = fflags }
#define _F_DOUBLE_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_DOUBLE), .flags = fflags, .validator = fval }
#define F_DOUBLE(...) _DISPATCH(_F_DOUBLE_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_STRING_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING) }
#define _F_STRING_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING), .flags = fflags }
#define _F_STRING_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING), .flags = fflags, .validator = fval }
#define F_STRING(...) _DISPATCH(_F_STRING_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_STRING_LEN_3(stype, fname, max_len) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING), .max_length = max_len }
#define _F_STRING_LEN_4(stype, fname, max_len, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING), .max_length = max_len, .flags = fflags }
#define _F_STRING_LEN_5(stype, fname, max_len, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_STRING), .max_length = max_len, .flags = fflags, .validator = fval }
#define F_STRING_LEN(...) _DISPATCH(_F_STRING_LEN_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_BOOL_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_BOOL) }
#define _F_BOOL_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_BOOL), .flags = fflags }
#define _F_BOOL_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_BOOL), .flags = fflags, .validator = fval }
#define F_BOOL(...) _DISPATCH(_F_BOOL_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define _F_BLOB_2(stype, fname) { _BASE_FIELD(stype, fname, FIELD_TYPE_BLOB) }
#define _F_BLOB_3(stype, fname, fflags) { _BASE_FIELD(stype, fname, FIELD_TYPE_BLOB), .flags = fflags }
#define _F_BLOB_4(stype, fname, fflags, fval) { _BASE_FIELD(stype, fname, FIELD_TYPE_BLOB), .flags = fflags, .validator = fval }
#define F_BLOB(...) _DISPATCH(_F_BLOB_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define F_BELONGS_TO(stype, fname, target, fk) \
    { _BASE_FIELD(stype, fname, FIELD_TYPE_BELONGS_TO), .target_model_name = #target, .fk_column_name = #fk }

#define MANY(name) \
    int name##_count; \
    void* name
#define F_HAS_MANY(stype, fname, target, fk) \
    { _BASE_FIELD(stype, fname, FIELD_TYPE_HAS_MANY), .target_model_name = #target, .fk_column_name = #fk, .count_offset = offsetof(stype, fname##_count) }

#define F_HAS_MANY_COUNT(stype, fname, cname, target, fk) \
    { _BASE_FIELD(stype, fname, FIELD_TYPE_HAS_MANY), .target_model_name = #target, .fk_column_name = #fk, .count_offset = offsetof(stype, cname) }

#define DEFINE_MODEL(name, stype, ...) \
    static field_info_t name##_fields[] = {__VA_ARGS__}; \
    static model_meta_t name##_model = { \
        .table_name = #name, \
        .struct_size = sizeof(stype), \
        .fields = name##_fields, \
        .field_count = sizeof(name##_fields) / sizeof(field_info_t), \
        .primary_key_field = NULL \
    }

corm_db_t* corm_init(const char* db_filepath);

corm_db_t* corm_init_with_allocator(const char* db_filepath, void* ctx,
                                    void* (*alloc_fn)(void*, size_t),
                                    void (*free_fn)(void*, void*));

corm_db_t* corm_init_with_backend(const corm_backend_ops_t* backend, 
                                   const char* connection_string);

corm_db_t* corm_init_with_backend_and_allocator(const corm_backend_ops_t* backend,
                                                 const char* connection_string,
                                                 void* ctx,
                                                 void* (*alloc_fn)(void*, size_t),
                                                 void (*free_fn)(void*, void*));

void corm_set_allocator(corm_db_t* db, void* ctx,
                        void* (*alloc_fn)(void*, size_t),
                        void (*free_fn)(void*, void*));

void corm_close(corm_db_t* db);

bool corm_register_model(corm_db_t* db, model_meta_t* meta);
bool corm_sync(corm_db_t* db, corm_sync_mode_e mode);

bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance);
bool corm_delete(corm_db_t* db, model_meta_t* meta, void* pk_value);

corm_result_t* corm_find(corm_db_t* db, model_meta_t* meta, void* pk_value);
corm_result_t* corm_find_all(corm_db_t* db, model_meta_t* meta);
corm_result_t* corm_where_raw(corm_db_t* db, model_meta_t* meta, const char* where_clause, void** params, field_type_e* param_types, size_t param_count);

corm_result_t* corm_load_relation(corm_db_t* db, model_meta_t* meta, void* instance, const char* field_name);

void corm_free_result(corm_db_t* db, corm_result_t* result);

#endif // CORM_H_
