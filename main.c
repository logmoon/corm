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

// TASK(20260129-122159) - Add custom validators
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

enum {
    PRIMARY_KEY = (1 << 0),
    NOT_NULL    = (1 << 1),
    UNIQUE      = (1 << 2),
    AUTO_INC    = (1 << 3),
};

// =============================================================================
// MODEL DEFINITION API
// =============================================================================

#define F_INT(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

#define F_INT64(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_INT64, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

#define F_FLOAT(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_FLOAT, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

#define F_DOUBLE(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_DOUBLE, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

#define F_STRING(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_STRING, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

#define F_BOOL(stype, fname, fflags) \
    {.name = #fname, .offset = offsetof(stype, fname), .type = FIELD_TYPE_BOOL, \
     .flags = fflags, .max_length = 0, .foreign_table = NULL, .foreign_field = NULL, .default_value = NULL}

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

void corm_register_model(corm_db_t* db, model_meta_t* meta);
// This is an internal thing, I think?
// But we can still expose it I guess
bool corm_create_table(corm_db_t* db, model_meta_t* meta);
// Creates the tables from the cached model metas
bool corm_sync(corm_db_t* db);

// TASK(20260129-122159) - Rewrite base functions and add in new ones#Notes
bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance);
bool corm_delete(corm_db_t* db, model_meta_t* meta, int pk_value);

void* corm_find(corm_db_t* db, model_meta_t* meta, int pk_value);
void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count);
void* corm_where(corm_db_t* db, model_meta_t* meta, const char* where_clause, int* count);

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

void corm_deinit(arena_t* arena, corm_db_t* db) {
    (void)arena;
    sqlite3_close(db->db);
}

// TASK(20260129-122159) - Rewrite base functions and add in new ones

// =============================================================================
// MODEL DEFINITIONS
// =============================================================================

typedef struct {
    int id;
    char* name;
    char* pwd_hash;
} User;

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(User, name, NOT_NULL),
    F_STRING(User, pwd_hash, NOT_NULL)
);

typedef struct {
    int id;
    char* title;
    char* content;
} Post;

DEFINE_MODEL(Post, Post,
    F_INT(Post, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Post, title, NOT_NULL),
    F_STRING(Post, content, 0)
);

// =============================================================================
// MAIN
// =============================================================================

int main() {
    arena_t* arena = arena_create(MiB(10));
    corm_db_t* db = corm_init(arena, "test.db");
    
    // Register models
    corm_register_model(db, &User_model);
    corm_register_model(db, &Post_model);
    corm_sync(db);
    
    // Test User
    User user = {0};
    user.name = "John Doe";
    user.pwd_hash = "hash123";
    
    log_info("Saving user...");
    corm_save(db, &User_model, &user);
    log_info("User saved with ID: %d", user.id);
    
    User* found_user = corm_find(db, &User_model, user.id);
    if (found_user) {
        log_info("Found user: id=%d, name=%s", found_user->id, found_user->name);
    }
    
    // Test Post
    Post post = {0};
    post.title = "My First Post";
    post.content = "This is the content of my first post!";
    
    log_info("Saving post...");
    corm_save(db, &Post_model, &post);
    log_info("Post saved with ID: %d", post.id);
    
    Post* found_post = corm_find(db, &Post_model, post.id);
    if (found_post) {
        log_info("Found post: id=%d, title=%s", 
                 found_post->id, found_post->title);
    }
    
    corm_deinit(arena, db);
    arena_destroy(arena);
    return 0;
}
