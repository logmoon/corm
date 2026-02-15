#include "corm_backend.h"
#include "corm.h"

#include <libpq-fe.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    PGconn* conn;
    PGresult* result;
    int current_row;
    int row_count;
} postgres_stmt_t;

static bool postgres_connect(corm_backend_conn_t* conn, const char* connection_string, char** error) {
    PGconn* pg_conn = PQconnectdb(connection_string);
    
    if (PQstatus(pg_conn) != CONNECTION_OK) {
        if (error) {
            *error = strdup(PQerrorMessage(pg_conn));
        }
        PQfinish(pg_conn);
        return false;
    }
    
    *conn = pg_conn;
    return true;
}

static void postgres_disconnect(corm_backend_conn_t conn) {
    if (conn) {
        PQfinish((PGconn*)conn);
    }
}

static const char* postgres_get_error(corm_backend_conn_t conn) {
    return PQerrorMessage((PGconn*)conn);
}

static bool postgres_execute(corm_backend_conn_t conn, const char* sql, char** error) {
    PGresult* result = PQexec((PGconn*)conn, sql);
    ExecStatusType status = PQresultStatus(result);
    
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        if (error) {
            *error = strdup(PQerrorMessage((PGconn*)conn));
        }
        PQclear(result);
        return false;
    }
    
    PQclear(result);
    return true;
}

static bool postgres_prepare(corm_backend_conn_t conn, corm_backend_stmt_t* stmt,
                             const char* sql, char** error) {
    postgres_stmt_t* pg_stmt = malloc(sizeof(postgres_stmt_t));
    if (!pg_stmt) {
        if (error) *error = strdup("Failed to allocate statement");
        return false;
    }
    
    pg_stmt->conn = (PGconn*)conn;
    pg_stmt->result = NULL;
    pg_stmt->current_row = -1;
    pg_stmt->row_count = 0;
    
    static int stmt_counter = 0;
    char stmt_name[64];
    snprintf(stmt_name, sizeof(stmt_name), "corm_stmt_%d", stmt_counter++);
    
    PGresult* prep_result = PQprepare((PGconn*)conn, stmt_name, sql, 0, NULL);
    ExecStatusType status = PQresultStatus(prep_result);
    
    if (status != PGRES_COMMAND_OK) {
        if (error) {
            *error = strdup(PQerrorMessage((PGconn*)conn));
        }
        PQclear(prep_result);
        free(pg_stmt);
        return false;
    }
    
    PQclear(prep_result);
    
    char* name_copy = strdup(stmt_name);
    pg_stmt->result = (PGresult*)name_copy;
    
    *stmt = pg_stmt;
    return true;
}

static void postgres_finalize(corm_backend_stmt_t stmt) {
    if (!stmt) return;
    
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    
    if (pg_stmt->result && ((char*)pg_stmt->result)[0] != '\0') {
        if (PQresultStatus(pg_stmt->result) == PGRES_TUPLES_OK || 
            PQresultStatus(pg_stmt->result) == PGRES_COMMAND_OK) {
            PQclear(pg_stmt->result);
        } else {
            free(pg_stmt->result);
        }
    }
    
    free(pg_stmt);
}

static bool postgres_reset(corm_backend_stmt_t stmt) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    pg_stmt->current_row = -1;
    return true;
}

#define MAX_PARAMS 64
static char* param_values[MAX_PARAMS] = {0};
static int param_count = 0;

static void clear_params() {
    for (int i = 0; i < param_count; i++) {
        if (param_values[i]) {
            free(param_values[i]);
            param_values[i] = NULL;
        }
    }
    param_count = 0;
}

static bool postgres_bind_int(corm_backend_stmt_t stmt, int index, int value) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) free(param_values[idx]);
    
    param_values[idx] = malloc(32);
    snprintf(param_values[idx], 32, "%d", value);
    
    if (index > param_count) param_count = index;
    return true;
}

static bool postgres_bind_int64(corm_backend_stmt_t stmt, int index, int64_t value) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) free(param_values[idx]);
    
    param_values[idx] = malloc(32);
    snprintf(param_values[idx], 32, "%lld", (long long)value);
    
    if (index > param_count) param_count = index;
    return true;
}

static bool postgres_bind_double(corm_backend_stmt_t stmt, int index, double value) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) free(param_values[idx]);
    
    param_values[idx] = malloc(64);
    snprintf(param_values[idx], 64, "%.17g", value);
    
    if (index > param_count) param_count = index;
    return true;
}

static bool postgres_bind_string(corm_backend_stmt_t stmt, int index, const char* value, int length) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) free(param_values[idx]);
    
    if (length < 0) length = strlen(value);
    param_values[idx] = malloc(length + 1);
    memcpy(param_values[idx], value, length);
    param_values[idx][length] = 0;
    
    if (index > param_count) param_count = index;
    return true;
}

static bool postgres_bind_blob(corm_backend_stmt_t stmt, int index, const void* value, int length) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) free(param_values[idx]);
    
    size_t encoded_len;
    unsigned char* encoded = PQescapeByteaConn(
        ((postgres_stmt_t*)stmt)->conn, 
        (const unsigned char*)value, 
        length, 
        &encoded_len
    );
    
    param_values[idx] = (char*)encoded;
    
    if (index > param_count) param_count = index;
    return true;
}

static bool postgres_bind_null(corm_backend_stmt_t stmt, int index) {
    if (index > MAX_PARAMS) return false;
    
    int idx = index - 1;
    if (param_values[idx]) {
        free(param_values[idx]);
        param_values[idx] = NULL;
    }
    
    if (index > param_count) param_count = index;
    return true;
}

static int postgres_step(corm_backend_stmt_t stmt) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    
    if (pg_stmt->current_row == -1) {
        char* stmt_name = (char*)pg_stmt->result;
        
        PGresult* result = PQexecPrepared(
            pg_stmt->conn,
            stmt_name,
            param_count,
            (const char* const*)param_values,
            NULL,
            NULL,
            0
        );
        
        ExecStatusType status = PQresultStatus(result);
        
        if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
            PQclear(result);
            clear_params();
            return -1;
        }
        
        free(stmt_name);
        pg_stmt->result = result;
        pg_stmt->row_count = PQntuples(result);
        pg_stmt->current_row = 0;
        
        clear_params();
    }
    
    if (pg_stmt->current_row < pg_stmt->row_count) {
        pg_stmt->current_row++;
        return 1;
    }
    
    return 0;
}

static int postgres_column_count(corm_backend_stmt_t stmt) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    return PQnfields(pg_stmt->result);
}

static const char* postgres_column_name(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    return PQfname(pg_stmt->result, index);
}

static int postgres_column_type(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    
    if (PQgetisnull(pg_stmt->result, row, index)) {
        return 0;
    }
    
    Oid type = PQftype(pg_stmt->result, index);
    
    switch (type) {
        case 16:
        case 20:
        case 21:
        case 23:
            return 1;
        case 700:
        case 701:
            return 2;
        case 25:
        case 1043:
            return 3;
        case 17:
            return 4;
        default:
            return 3;
    }
}

static int postgres_column_int(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    char* value = PQgetvalue(pg_stmt->result, row, index);
    return atoi(value);
}

static int64_t postgres_column_int64(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    char* value = PQgetvalue(pg_stmt->result, row, index);
    return atoll(value);
}

static double postgres_column_double(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    char* value = PQgetvalue(pg_stmt->result, row, index);
    return atof(value);
}

static const unsigned char* postgres_column_text(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    return (const unsigned char*)PQgetvalue(pg_stmt->result, row, index);
}

static const void* postgres_column_blob(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    
    char* encoded = PQgetvalue(pg_stmt->result, row, index);
    size_t decoded_len;
    unsigned char* decoded = PQunescapeBytea((const unsigned char*)encoded, &decoded_len);
    
    return decoded;
}

static int postgres_column_bytes(corm_backend_stmt_t stmt, int index) {
    postgres_stmt_t* pg_stmt = (postgres_stmt_t*)stmt;
    int row = pg_stmt->current_row - 1;
    return PQgetlength(pg_stmt->result, row, index);
}

static int64_t postgres_last_insert_id(corm_backend_conn_t conn) {
    PGresult* result = PQexec((PGconn*)conn, "SELECT lastval();");
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        PQclear(result);
        return 0;
    }
    
    int64_t id = atoll(PQgetvalue(result, 0, 0));
    PQclear(result);
    return id;
}

static bool postgres_begin_transaction(corm_backend_conn_t conn) {
    PGresult* result = PQexec((PGconn*)conn, "BEGIN;");
    bool success = PQresultStatus(result) == PGRES_COMMAND_OK;
    PQclear(result);
    return success;
}

static bool postgres_commit(corm_backend_conn_t conn) {
    PGresult* result = PQexec((PGconn*)conn, "COMMIT;");
    bool success = PQresultStatus(result) == PGRES_COMMAND_OK;
    PQclear(result);
    return success;
}

static bool postgres_rollback(corm_backend_conn_t conn) {
    PGresult* result = PQexec((PGconn*)conn, "ROLLBACK;");
    bool success = PQresultStatus(result) == PGRES_COMMAND_OK;
    PQclear(result);
    return success;
}

static const char* postgres_get_type_name(field_type_e type, size_t max_length, char* buf, size_t buf_size) {
    switch (type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return "INTEGER";
        case FIELD_TYPE_INT64:
            return "BIGINT";
        case FIELD_TYPE_FLOAT:
            return "REAL";
        case FIELD_TYPE_DOUBLE:
            return "DOUBLE PRECISION";
        case FIELD_TYPE_STRING:
            if (max_length > 0) {
                snprintf(buf, buf_size, "VARCHAR(%zu)", max_length);
                return buf;
            }
            return "TEXT";
        case FIELD_TYPE_BLOB:
            return "BYTEA";
        default:
            return "TEXT";
    }
}

static const char* postgres_get_auto_increment() {
    return "";
}

static const char* postgres_get_placeholder(int index) {
    static char buf[16];
    snprintf(buf, sizeof(buf), "$%d", index);
    return buf;
}

static bool postgres_supports_returning() {
    return true;
}

static const char* postgres_get_limit_syntax(int limit, int offset) {
    static char buf[64];
    if (offset > 0) {
        snprintf(buf, sizeof(buf), "LIMIT %d OFFSET %d", limit, offset);
    } else {
        snprintf(buf, sizeof(buf), "LIMIT %d", limit);
    }
    return buf;
}

static bool postgres_table_exists(corm_backend_conn_t conn, const char* table_name) {
    const char* sql = "SELECT COUNT(*) FROM information_schema.tables "
                     "WHERE table_schema = 'public' AND table_name = $1;";
    
    const char* params[1] = { table_name };
    PGresult* result = PQexecParams(
        (PGconn*)conn,
        sql,
        1,
        NULL,
        params,
        NULL,
        NULL,
        0
    );
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        PQclear(result);
        return false;
    }
    
    int count = atoi(PQgetvalue(result, 0, 0));
    PQclear(result);
    return count > 0;
}

static bool postgres_set_foreign_keys(corm_backend_conn_t conn, bool enabled) {
    return true;
}

static corm_backend_ops_t postgres_ops = {
    .name = "postgresql",
    .connect = postgres_connect,
    .disconnect = postgres_disconnect,
    .get_error = postgres_get_error,
    .execute = postgres_execute,
    .prepare = postgres_prepare,
    .finalize = postgres_finalize,
    .reset = postgres_reset,
    .bind_int = postgres_bind_int,
    .bind_int64 = postgres_bind_int64,
    .bind_double = postgres_bind_double,
    .bind_string = postgres_bind_string,
    .bind_blob = postgres_bind_blob,
    .bind_null = postgres_bind_null,
    .step = postgres_step,
    .column_count = postgres_column_count,
    .column_name = postgres_column_name,
    .column_type = postgres_column_type,
    .column_int = postgres_column_int,
    .column_int64 = postgres_column_int64,
    .column_double = postgres_column_double,
    .column_text = postgres_column_text,
    .column_blob = postgres_column_blob,
    .column_bytes = postgres_column_bytes,
    .last_insert_id = postgres_last_insert_id,
    .begin_transaction = postgres_begin_transaction,
    .commit = postgres_commit,
    .rollback = postgres_rollback,
    .get_type_name = postgres_get_type_name,
    .get_auto_increment = postgres_get_auto_increment,
    .get_placeholder = postgres_get_placeholder,
    .supports_returning = postgres_supports_returning,
    .get_limit_syntax = postgres_get_limit_syntax,
    .table_exists = postgres_table_exists,
    .set_foreign_keys = postgres_set_foreign_keys,
};

const corm_backend_ops_t* corm_backend_postgresql_init() {
    return &postgres_ops;
}
