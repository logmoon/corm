#include "corm.h"

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
bool corm_register_model(corm_db_t* db, model_meta_t* meta) {
	// Find and validate primary key
    const char* pk_field = NULL;
    int pk_count = 0;
    
    for (u64 i = 0; i < meta->field_count; i++) {
        if (meta->fields[i].flags & PRIMARY_KEY) {
            pk_field = meta->fields[i].name;
            pk_count++;
        }
    }
    
    if (pk_count == 0) {
        log_error("Model '%s' must have exactly one PRIMARY_KEY field", meta->table_name);
        return false;
    }
    if (pk_count > 1) {
        log_error("Model '%s' has %d PRIMARY_KEY fields, expected 1", meta->table_name, pk_count);
        return false;
    }
    
    meta->primary_key_field = pk_field;

	// Register
	// Not sure if we should resize, or throw an error here
	// Let's just resize for now
    if (db->model_count >= db->model_capacity) {
        u64 new_capacity = db->model_capacity * 2;
        db->models = arena_resize_array_typed(
            db->arena,
            db->models,
            db->model_count,
            new_capacity,
            model_meta_t*
        );
        db->model_capacity = new_capacity;
    }
    
    db->models[db->model_count++] = meta;
	return true;
}

string_t corm_sql_type(arena_t* arena, field_info_t* field) {
    switch (field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            return STR_LIT("INTEGER");
        case FIELD_TYPE_INT64:
            return STR_LIT("BIGINT");
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            return STR_LIT("REAL");
        case FIELD_TYPE_STRING:
            if (field->max_length > 0) {
                return str_fmt(arena, "VARCHAR(%zu)", field->max_length);
            }
            return STR_LIT("TEXT");
        case FIELD_TYPE_BLOB:
            return STR_LIT("BLOB");
        default:
            return STR_LIT("TEXT");
    }
}

bool corm_table_exists(corm_db_t* db, const char* table_name) {
    const char* check_sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt* stmt;
    
    int rc = sqlite3_prepare_v2(db->db, check_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return false;
    }
    
    sqlite3_bind_text(stmt, 1, table_name, -1, SQLITE_STATIC);
    
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) > 0;
    }
    
    sqlite3_finalize(stmt);
    return exists;
}

bool corm_create_table(corm_db_t* db, model_meta_t* meta) {
	temp_t tmp = arena_start_temp(db->arena);
	string_t sql = str_fmt(db->arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);

    for (u64 i = 0; i < meta->field_count; i++) {
		field_info_t* field = &meta->fields[i];

		if (i > 0) {
			sql = str_cat(db->arena, sql, STR_LIT(", "));
		}

		string_t type = corm_sql_type(db->arena, field);
		string_t new_field = str_fmt(db->arena, "%s %s", field->name, str_to_c_safe(db->arena, type));

		sql = str_cat(db->arena, sql, new_field);
        if (meta->fields[i].flags & PRIMARY_KEY) {
			sql = str_cat(db->arena, sql, STR_LIT(" PRIMARY KEY"));
        }
        if (meta->fields[i].flags & NOT_NULL) {
			sql = str_cat(db->arena, sql, STR_LIT(" NOT NULL"));
        }
        if (meta->fields[i].flags & UNIQUE) {
			sql = str_cat(db->arena, sql, STR_LIT(" UNIQUE"));
        }
        if (meta->fields[i].flags & AUTO_INC) {
			sql = str_cat(db->arena, sql, STR_LIT(" AUTOINCREMENT"));
        }
    }

	sql = str_cat(db->arena, sql, STR_LIT(");"));

	// Maybe hide this? Maybe use a versobe flag or sum?
	log_info("  Running: %s", str_to_c(sql));

    char *err_msg = NULL;
	int rc = sqlite3_exec(db->db, str_to_c_safe(db->arena, sql), NULL, NULL, &err_msg);
	arena_end_temp(tmp);

	if (rc != SQLITE_OK) {
		log_error("SQL error: %s", err_msg);
        sqlite3_free(err_msg);
		return false;
	}

	return true;
}

bool corm_sync(corm_db_t* db, corm_sync_mode_e mode) {
	switch(mode) {
		case CORM_SYNC_SAFE:
		{
			log_info("Syncing database (SAFE)");
			for (u64 i = 0; i < db->model_count; ++i) {
				model_meta_t* meta = db->models[i];
				bool existed = corm_table_exists(db, meta->table_name);
				if (existed) {
					log_info("  Table '%s' already exists (preserved existing schema)", meta->table_name);
				} else {
					if (!corm_create_table(db, meta)) {
						return false;
					}
					log_info("  Created table '%s'", meta->table_name);
				}
			}
		}
		break;
		case CORM_SYNC_DROP:
		{
			log_info("Syncing database (DROP)");
			// Drop all tables first
			for (u64 i = 0; i < db->model_count; ++i) {
                temp_t tmp = arena_start_temp(db->arena);
                string_t drop_sql = str_fmt(db->arena, "DROP TABLE IF EXISTS %s;", 
                                           db->models[i]->table_name);
                char* err_msg = NULL;
                int rc = sqlite3_exec(db->db, str_to_c_safe(db->arena, drop_sql), NULL, NULL, &err_msg);
				if (rc != SQLITE_OK) {
					log_error("Failed to drop table '%s': %s", db->models[i]->table_name, err_msg);
					sqlite3_free(err_msg);
					arena_end_temp(tmp);
					return false;
				}
				log_info("  Dropped table '%s'", db->models[i]->table_name);
                arena_end_temp(tmp);
            }
			// Then recreate
			for (u64 i = 0; i < db->model_count; ++i) {
				if (!corm_create_table(db, db->models[i])) {
					return false;
				}
				log_info("  Created table '%s'", db->models[i]->table_name);
			}
		}
		break;
		case CORM_SYNC_MIGRATE:
		{
			log_error("CORM_SYNC_MIGRATE is not implemented yet");
            return false;
		}
		break;
	}
	log_info("Sync complete!");
	return true;
}

// TASK(20260129-122159) - Rewrite base functions and add in new ones#Notes
bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
	return false;
}

bool corm_delete(corm_db_t* db, model_meta_t* meta, int pk_value) {
	return false;
}

void* corm_find(corm_db_t* db, model_meta_t* meta, int pk_value) {
	return NULL;
}

void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count) {
	return NULL;
}

void* corm_where(corm_db_t* db, model_meta_t* meta, const char* where_clause, int* count) {
	return NULL;
}
