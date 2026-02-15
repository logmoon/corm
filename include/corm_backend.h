#ifndef CORM_BACKEND_H_
#define CORM_BACKEND_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct corm_db_t corm_db_t;
typedef struct model_meta_t model_meta_t;
typedef struct field_info_t field_info_t;
typedef enum field_type_e field_type_e;

typedef void* corm_backend_conn_t;
typedef void* corm_backend_stmt_t;

typedef struct corm_backend_ops_t {
    // Backend identification
    const char* name; // "sqlite", "postgres", etc...
    
    // Connection management
    bool (*connect)(corm_backend_conn_t* conn, const char* connection_string, char** error);
    void (*disconnect)(corm_backend_conn_t conn);
    const char* (*get_error)(corm_backend_conn_t conn);
    
    // SQL execution
    bool (*execute)(corm_backend_conn_t conn, const char* sql, char** error);
    
    // Prepared statements
    bool (*prepare)(corm_backend_conn_t conn, corm_backend_stmt_t* stmt, 
                    const char* sql, char** error);
    void (*finalize)(corm_backend_stmt_t stmt);
    bool (*reset)(corm_backend_stmt_t stmt);
    
    // Parameter binding
    bool (*bind_int)(corm_backend_stmt_t stmt, int index, int value);
    bool (*bind_int64)(corm_backend_stmt_t stmt, int index, int64_t value);
    bool (*bind_double)(corm_backend_stmt_t stmt, int index, double value);
    bool (*bind_string)(corm_backend_stmt_t stmt, int index, const char* value, int length);
    bool (*bind_blob)(corm_backend_stmt_t stmt, int index, const void* value, int length);
    bool (*bind_null)(corm_backend_stmt_t stmt, int index);
    
    // Execution and results
    int (*step)(corm_backend_stmt_t stmt); // Returns: 1=row, 0=done, -1=error
    int (*column_count)(corm_backend_stmt_t stmt);
    const char* (*column_name)(corm_backend_stmt_t stmt, int index);
    int (*column_type)(corm_backend_stmt_t stmt, int index); // 0=null, 1=int, 2=float, 3=text, 4=blob
    
    // Column data retrieval
    int (*column_int)(corm_backend_stmt_t stmt, int index);
    int64_t (*column_int64)(corm_backend_stmt_t stmt, int index);
    double (*column_double)(corm_backend_stmt_t stmt, int index);
    const unsigned char* (*column_text)(corm_backend_stmt_t stmt, int index);
    const void* (*column_blob)(corm_backend_stmt_t stmt, int index);
    int (*column_bytes)(corm_backend_stmt_t stmt, int index);
    
    // Last insert ID
    int64_t (*last_insert_id)(corm_backend_conn_t conn);
    
    // Transaction support
    bool (*begin_transaction)(corm_backend_conn_t conn);
    bool (*commit)(corm_backend_conn_t conn);
    bool (*rollback)(corm_backend_conn_t conn);
    
    // SQL dialect functions (for generating database-specific SQL)
    const char* (*get_type_name)(field_type_e type, size_t max_length, char* buf, size_t buf_size);
    const char* (*get_auto_increment)();
    const char* (*get_placeholder)(int index);
    bool (*supports_returning)();
    const char* (*get_limit_syntax)(int limit, int offset);
    
    // Database-specific utilities
    bool (*table_exists)(corm_backend_conn_t conn, const char* table_name);
    bool (*set_foreign_keys)(corm_backend_conn_t conn, bool enabled);
    
} corm_backend_ops_t;

// Backend registration - built-in backend(s)
const corm_backend_ops_t* corm_backend_sqlite_init();
// const corm_backend_ops_t* corm_backend_postgresql_init();

#endif // CORM_BACKEND_H_
