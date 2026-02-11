---
title: DB Abstraction
state: open
priority: 1
created: 20260210-235159
---
# DB Abstraction
Something like:
```c
// Later, when you support MySQL/Postgres:
typedef struct {
    // Backend-specific implementation
    void* (*find_by)(void* db_handle, model_meta_t* meta, 
                     filter_t* filters, size_t count, int* result_count);
    void* (*where_raw)(void* db_handle, model_meta_t* meta,
                       const char* clause, int* result_count);
    // ...
} db_backend_ops_t;

typedef struct {
    void* handle;  // sqlite3* or PGconn* or MYSQL*
    db_backend_ops_t* ops;
    // ...
} corm_db_t;
```
