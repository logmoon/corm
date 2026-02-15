#include "corm_backend.h"
#include "corm.h"

#include "thirdparty/sqlite/sqlite3.h"

#include <string.h>
#include <stdio.h>

static bool sqlite_connect(corm_backend_conn_t* conn, const char* connection_string, char** error) {
    sqlite3* db = NULL;
    int rc = sqlite3_open(connection_string, &db);
    
    if (rc != SQLITE_OK) {
        if (error) {
            const char* err_msg = sqlite3_errmsg(db);
            *error = strdup(err_msg);
        }
        sqlite3_close(db);
        return false;
    }
    
    // Enable foreign keys
    char* err_msg = NULL;
    rc = sqlite3_exec(db, "PRAGMA foreign_keys = ON;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        if (error && err_msg) {
            *error = strdup(err_msg);
        }
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return false;
    }
    
    *conn = db;
    return true;
}

static void sqlite_disconnect(corm_backend_conn_t conn) {
    if (conn) {
        sqlite3_close((sqlite3*)conn);
    }
}

static const char* sqlite_get_error(corm_backend_conn_t conn) {
    return sqlite3_errmsg((sqlite3*)conn);
}

static bool sqlite_execute(corm_backend_conn_t conn, const char* sql, char** error) {
    char* err_msg = NULL;
    int rc = sqlite3_exec((sqlite3*)conn, sql, NULL, NULL, &err_msg);
    
    if (rc != SQLITE_OK) {
        if (error && err_msg) {
            *error = strdup(err_msg);
        }
        sqlite3_free(err_msg);
        return false;
    }
    
    return true;
}

static bool sqlite_prepare(corm_backend_conn_t conn, corm_backend_stmt_t* stmt,
                           const char* sql, char** error) {
    sqlite3_stmt* s = NULL;
    int rc = sqlite3_prepare_v2((sqlite3*)conn, sql, -1, &s, NULL);
    
    if (rc != SQLITE_OK) {
        if (error) {
            *error = strdup(sqlite3_errmsg((sqlite3*)conn));
        }
        return false;
    }
    
    *stmt = s;
    return true;
}

static void sqlite_finalize(corm_backend_stmt_t stmt) {
    if (stmt) {
        sqlite3_finalize((sqlite3_stmt*)stmt);
    }
}

static bool sqlite_reset(corm_backend_stmt_t stmt) {
    return sqlite3_reset((sqlite3_stmt*)stmt) == SQLITE_OK;
}

static bool sqlite_bind_int(corm_backend_stmt_t stmt, int index, int value) {
    return sqlite3_bind_int((sqlite3_stmt*)stmt, index, value) == SQLITE_OK;
}

static bool sqlite_bind_int64(corm_backend_stmt_t stmt, int index, int64_t value) {
    return sqlite3_bind_int64((sqlite3_stmt*)stmt, index, value) == SQLITE_OK;
}

static bool sqlite_bind_double(corm_backend_stmt_t stmt, int index, double value) {
    return sqlite3_bind_double((sqlite3_stmt*)stmt, index, value) == SQLITE_OK;
}

static bool sqlite_bind_string(corm_backend_stmt_t stmt, int index, const char* value, int length) {
    return sqlite3_bind_text((sqlite3_stmt*)stmt, index, value, length, SQLITE_STATIC) == SQLITE_OK;
}

static bool sqlite_bind_blob(corm_backend_stmt_t stmt, int index, const void* value, int length) {
    return sqlite3_bind_blob((sqlite3_stmt*)stmt, index, value, length, SQLITE_STATIC) == SQLITE_OK;
}

static bool sqlite_bind_null(corm_backend_stmt_t stmt, int index) {
    return sqlite3_bind_null((sqlite3_stmt*)stmt, index) == SQLITE_OK;
}

static int sqlite_step(corm_backend_stmt_t stmt) {
    int rc = sqlite3_step((sqlite3_stmt*)stmt);
    
    if (rc == SQLITE_ROW) return 1;
    if (rc == SQLITE_DONE) return 0;
    return -1;
}

static int sqlite_column_count(corm_backend_stmt_t stmt) {
    return sqlite3_column_count((sqlite3_stmt*)stmt);
}

static const char* sqlite_column_name(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_name((sqlite3_stmt*)stmt, index);
}

static int sqlite_column_type(corm_backend_stmt_t stmt, int index) {
    int type = sqlite3_column_type((sqlite3_stmt*)stmt, index);
    
    switch (type) {
        case SQLITE_NULL: return 0;
        case SQLITE_INTEGER: return 1;
        case SQLITE_FLOAT: return 2;
        case SQLITE_TEXT: return 3;
        case SQLITE_BLOB: return 4;
        default: return 0;
    }
}

static int sqlite_column_int(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_int((sqlite3_stmt*)stmt, index);
}

static int64_t sqlite_column_int64(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_int64((sqlite3_stmt*)stmt, index);
}

static double sqlite_column_double(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_double((sqlite3_stmt*)stmt, index);
}

static const unsigned char* sqlite_column_text(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_text((sqlite3_stmt*)stmt, index);
}

static const void* sqlite_column_blob(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_blob((sqlite3_stmt*)stmt, index);
}

static int sqlite_column_bytes(corm_backend_stmt_t stmt, int index) {
    return sqlite3_column_bytes((sqlite3_stmt*)stmt, index);
}

static int64_t sqlite_last_insert_id(corm_backend_conn_t conn) {
    return sqlite3_last_insert_rowid((sqlite3*)conn);
}

static bool sqlite_begin_transaction(corm_backend_conn_t conn) {
    char* err = NULL;
    return sqlite3_exec((sqlite3*)conn, "BEGIN TRANSACTION;", NULL, NULL, &err) == SQLITE_OK;
}

static bool sqlite_commit(corm_backend_conn_t conn) {
    char* err = NULL;
    return sqlite3_exec((sqlite3*)conn, "COMMIT;", NULL, NULL, &err) == SQLITE_OK;
}

static bool sqlite_rollback(corm_backend_conn_t conn) {
    char* err = NULL;
    return sqlite3_exec((sqlite3*)conn, "ROLLBACK;", NULL, NULL, &err) == SQLITE_OK;
}

// SQLite SQL dialect functions
static const char* sqlite_get_type_name(field_type_e type, size_t max_length) {
    switch (type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return "INTEGER";
        case FIELD_TYPE_INT64:
            return "INTEGER";
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            return "REAL";
        case FIELD_TYPE_STRING:
            return max_length > 0 ? "TEXT" : "TEXT";
        case FIELD_TYPE_BLOB:
            return "BLOB";
        default:
            return "TEXT";
    }
}

static const char* sqlite_get_auto_increment() {
    return "AUTOINCREMENT";
}

static const char* sqlite_get_placeholder(int index) {
    return "?";
}

static bool sqlite_supports_returning() {
    return true; // SQLite 3.35+ supports RETURNING
}

static const char* sqlite_get_limit_syntax(int limit, int offset) {
    static char buf[64];
    if (offset > 0) {
        snprintf(buf, sizeof(buf), "LIMIT %d OFFSET %d", limit, offset);
    } else {
        snprintf(buf, sizeof(buf), "LIMIT %d", limit);
    }
    return buf;
}

static corm_backend_ops_t sqlite_ops = {
    .name = "sqlite",
    .connect = sqlite_connect,
    .disconnect = sqlite_disconnect,
    .get_error = sqlite_get_error,
    .execute = sqlite_execute,
    .prepare = sqlite_prepare,
    .finalize = sqlite_finalize,
    .reset = sqlite_reset,
    .bind_int = sqlite_bind_int,
    .bind_int64 = sqlite_bind_int64,
    .bind_double = sqlite_bind_double,
    .bind_string = sqlite_bind_string,
    .bind_blob = sqlite_bind_blob,
    .bind_null = sqlite_bind_null,
    .step = sqlite_step,
    .column_count = sqlite_column_count,
    .column_name = sqlite_column_name,
    .column_type = sqlite_column_type,
    .column_int = sqlite_column_int,
    .column_int64 = sqlite_column_int64,
    .column_double = sqlite_column_double,
    .column_text = sqlite_column_text,
    .column_blob = sqlite_column_blob,
    .column_bytes = sqlite_column_bytes,
    .last_insert_id = sqlite_last_insert_id,
    .begin_transaction = sqlite_begin_transaction,
    .commit = sqlite_commit,
    .rollback = sqlite_rollback,
    .get_type_name = sqlite_get_type_name,
    .get_auto_increment = sqlite_get_auto_increment,
    .get_placeholder = sqlite_get_placeholder,
    .supports_returning = sqlite_supports_returning,
    .get_limit_syntax = sqlite_get_limit_syntax,
};

const corm_backend_ops_t* corm_backend_sqlite_init() {
    return &sqlite_ops;
}
