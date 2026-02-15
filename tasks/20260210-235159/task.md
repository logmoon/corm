---
title: DB Abstraction
state: closed
priority: 1
created: 20260210-235159
---
# DB Abstraction
Something like:
```c
// Later, when you support MySQL/Postgres:
typedef struct {
    // Backend-specific implementation
    // ... ?
} db_backend_ops_t;

typedef struct {
    void* handle;  // sqlite3* or PGconn* or MYSQL*
    db_backend_ops_t* ops;
    // ...
} corm_db_t;
```
