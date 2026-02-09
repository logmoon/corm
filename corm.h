#ifndef CORM_H_
#define CORM_H_

#define LU_LOGGER_ENABLED
#define LU_LOGGER_SHOW_SOURCE
#define LU_LOGGER_SHOW_DATE
#define LU_LOGGER_LOG_TO_FILE
#define LU_LOG_FILE_LEVEL_WARN
#include "log.utils/lu_logger.h"
#include "log.utils/lu_string.h"
#include "thirdparty/sqlite/sqlite3.h"

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
    void* data;
    size_t size;
} blob_t;

typedef bool (*validator_fn)(void* value, const char** error_msg);
typedef struct {
    const char* name;
    size_t offset;
    field_type_e type;
    int flags;
    size_t max_length;
    const char* foreign_table;
    const char* foreign_field;
    void* default_value;
	validator_fn validator;
} field_info_t;

typedef struct {
    const char* table_name;
    size_t struct_size;
    field_info_t* fields;
    size_t field_count;
	field_info_t* primary_key_field;
} model_meta_t;

typedef struct {
    sqlite3* db;
    model_meta_t** models;
    u64 model_count;
    u64 model_capacity;
    arena_t* arena;
} corm_db_t;

// Flags
#define NO_FLAGS 0
enum {
    PRIMARY_KEY = (1 << 0),
    NOT_NULL    = (1 << 1),
    UNIQUE      = (1 << 2),
    AUTO_INC    = (1 << 3),
};

typedef enum {
    CORM_SYNC_SAFE,		// CREATE IF NOT EXISTS
    CORM_SYNC_DROP,     // DROP + CREATE (will destroy data)
    CORM_SYNC_MIGRATE,  // ALTER TABLE to match schema
} corm_sync_mode_e;


// =============================================================================
// MODEL DEFINITION API
// =============================================================================

// Argument counting helper
#define _NARGS_IMPL(_1,_2,_3,_4,_5,N,...) N
#define _NARGS(...) _NARGS_IMPL(__VA_ARGS__, 5, 4, 3, 2, 1)

#define _CONCAT(a, b) a##b
#define _DISPATCH(prefix, N) _CONCAT(prefix, N)

// F_INT
#define _F_INT_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_INT_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_INT_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_INT(...) _DISPATCH(_F_INT_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_INT64
#define _F_INT64_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT64, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_INT64_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT64, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_INT64_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT64, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_INT64(...) _DISPATCH(_F_INT64_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_FLOAT
#define _F_FLOAT_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_FLOAT, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_FLOAT_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_FLOAT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_FLOAT_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_FLOAT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_FLOAT(...) _DISPATCH(_F_FLOAT_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_DOUBLE
#define _F_DOUBLE_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_DOUBLE, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_DOUBLE_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_DOUBLE, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_DOUBLE_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_DOUBLE, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_DOUBLE(...) _DISPATCH(_F_DOUBLE_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_STRING
#define _F_STRING_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_STRING_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_STRING_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_STRING(...) _DISPATCH(_F_STRING_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_STRING_LEN
#define _F_STRING_LEN_3(stype, fname, max_len) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = 0, .max_length = (max_len), .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_STRING_LEN_4(stype, fname, max_len, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = fflags, .max_length = (max_len), .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_STRING_LEN_5(stype, fname, max_len, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = fflags, .max_length = (max_len), .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_STRING_LEN(...) _DISPATCH(_F_STRING_LEN_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_BOOL
#define _F_BOOL_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BOOL, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_BOOL_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BOOL, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = NULL}

#define _F_BOOL_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BOOL, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL, .validator = fvalidator}

#define F_BOOL(...) _DISPATCH(_F_BOOL_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

// F_BLOB
#define _F_BLOB_2(stype, fname) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BLOB, \
     .flags = 0, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, \
     .default_value = NULL, .validator = NULL}

#define _F_BLOB_3(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BLOB, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, \
     .default_value = NULL, .validator = NULL}

#define _F_BLOB_4(stype, fname, fflags, fvalidator) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BLOB, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, \
     .default_value = NULL, .validator = fvalidator}

#define F_BLOB(...) _DISPATCH(_F_BLOB_, _NARGS(__VA_ARGS__))(__VA_ARGS__)

#define DEFINE_MODEL(name, stype, ...) \
    static field_info_t name##_fields[] = {__VA_ARGS__}; \
    static model_meta_t name##_model = { \
        .table_name = #name, \
        .struct_size = sizeof(stype), \
        .fields = name##_fields, \
        .field_count = sizeof(name##_fields) / sizeof(field_info_t), \
        .primary_key_field = NULL \
    }

// =============================================================================
// ORM FUNCTION DEFS
// =============================================================================

corm_db_t* corm_init(arena_t* arena, const char* db_filepath);
void corm_deinit(arena_t* arena, corm_db_t* db);

bool corm_register_model(corm_db_t* db, model_meta_t* meta);
bool corm_sync(corm_db_t* db, corm_sync_mode_e mode);

bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance);
bool corm_delete(corm_db_t* db, model_meta_t* meta, int pk_value);

void* corm_find(corm_db_t* db, model_meta_t* meta, int pk_value);
void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count);
void* corm_where(corm_db_t* db, model_meta_t* meta, const char* where_clause, int* count);

#endif
