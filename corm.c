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
	field_info_t* pk_field = NULL;
    int pk_count = 0;
    
    for (u64 i = 0; i < meta->field_count; i++) {
        if (meta->fields[i].flags & PRIMARY_KEY) {
            pk_field = &meta->fields[i];
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

static bool corm_create_table(corm_db_t* db, model_meta_t* meta) {
	temp_t tmp = arena_start_temp(db->arena);
	string_t sql = str_fmt(db->arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);

    for (u64 i = 0; i < meta->field_count; i++) {
		field_info_t* field = &meta->fields[i];

		if (field->type == FIELD_TYPE_BELONGS_TO ||
			field->type == FIELD_TYPE_HAS_MANY) {
			continue;
		}

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

	for (u64 i = 0; i < meta->field_count; i++) {
		field_info_t* field = &meta->fields[i];

		if (field->type == FIELD_TYPE_BELONGS_TO) {
			string_t fk = str_fmt(db->arena, ", FOREIGN KEY (%s) REFERENCES %s(id)",
				field->fk_column_name,
				field->target_model_name
			);
			
			switch(field->on_delete) {
				case FK_CASCADE:
					fk = str_cat(db->arena, fk, STR_LIT(" ON DELETE CASCADE"));
					break;
				case FK_SET_NULL:
					fk = str_cat(db->arena, fk, STR_LIT(" ON DELETE SET NULL"));
					break;
				case FK_RESTRICT:
					fk = str_cat(db->arena, fk, STR_LIT(" ON DELETE RESTRICT"));
					break;
				default: break;
			}
			
			sql = str_cat(db->arena, sql, fk);
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

static bool corm_resolve_relationships(corm_db_t* db) {
    for (size_t i = 0; i < db->model_count; i++) {
        model_meta_t* model = db->models[i];
        
        for (size_t j = 0; j < model->field_count; j++) {
            field_info_t* field = &model->fields[j];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                
                // Find the related model
                field->related_model = NULL;
                for (size_t k = 0; k < db->model_count; k++) {
                    if (strcmp(db->models[k]->table_name, 
                              field->target_model_name) == 0) {
                        field->related_model = db->models[k];
                        break;
                    }
                }
                
                if (!field->related_model) {
                    log_error("Related model '%s' not found for field '%s'",
                             field->target_model_name, field->name);
                    return false;
                }
            }
        }
    }
    return true;
}
bool corm_sync(corm_db_t* db, corm_sync_mode_e mode) {
	// Resolve relationships first
	if (!corm_resolve_relationships(db)) {
		return false;
	}

	switch(mode) {
		case CORM_SYNC_SAFE:
		{
			log_info("Syncing database (SAFE)");
			for (u64 i = 0; i < db->model_count; ++i) {
				model_meta_t* meta = db->models[i];
				bool existed = corm_table_exists(db, meta->table_name);
				if (existed) {
					log_info("  Table '%s' already exists (preserving existing schema)", meta->table_name);
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

static bool corm_record_exists(corm_db_t* db, model_meta_t* meta, field_info_t* pk_field, void* pk_value) {
    string_t sql = str_fmt(db->arena, "SELECT COUNT(*) FROM %s WHERE %s = ?;", 
                          meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        return false;
    }
    
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        default:
            log_error("Unsupported primary key type");
            sqlite3_finalize(stmt);
            return false;
    }
    
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) > 0;
    }
    
    sqlite3_finalize(stmt);
    return exists;
}
bool corm_save(corm_db_t* db, model_meta_t* meta, void* instance) {
    temp_t tmp = arena_start_temp(db->arena);
    
    // Get pk field
    field_info_t* pk_field = meta->primary_key_field;
    // Lil sanity check
    if (!pk_field) {
        log_error("Primary key field not found in model '%s'", meta->table_name);
        arena_end_temp(tmp);
        return false;
    }

    // Validate custom validators
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        if (field->validator) {
            void* field_value = (char*)instance + field->offset;
            const char* error_msg = NULL;
            if (!field->validator(field_value, &error_msg)) {
                log_error("Validation failed for field '%s': %s", 
                         field->name, error_msg ? error_msg : "Unknown error");
                arena_end_temp(tmp);
                return false;
            }
        }
    }
    
    // Determine insert vs update
    void* pk_value = (char*)instance + pk_field->offset;
    bool is_update = corm_record_exists(db, meta, pk_field, pk_value);
    
    // Build SQL
    string_t sql;
    sqlite3_stmt* stmt;
    
    if (is_update) {
        // UPDATE table SET field1=?, field2=?, ... WHERE pk_field=?
        sql = str_fmt(db->arena, "UPDATE %s SET ", meta->table_name);
        
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            // Skip PK and AUTO_INC fields
            if (field->flags & PRIMARY_KEY || field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = str_cat(db->arena, sql, STR_LIT(", "));
            }
            sql = str_cat(db->arena, sql, str_fmt(db->arena, "%s=?", field->name));
            first = false;
        }
        
        sql = str_cat(db->arena, sql, str_fmt(db->arena, " WHERE %s=?;", pk_field->name));
    } else {
        // INSERT INTO table (field1, field2, ...) VALUES (?, ?, ...)
        sql = str_fmt(db->arena, "INSERT INTO %s (", meta->table_name);
        string_t values = STR_LIT("VALUES (");
        
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            // Skip AUTO_INC fields
            if (field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = str_cat(db->arena, sql, STR_LIT(", "));
                values = str_cat(db->arena, values, STR_LIT(", "));
            }
            
            sql = str_cat(db->arena, sql, str_fmt(db->arena, "%s", field->name));
            values = str_cat(db->arena, values, STR_LIT("?"));
            first = false;
        }
        
        sql = str_cat(db->arena, sql, STR_LIT(") "));
        values = str_cat(db->arena, values, STR_LIT(");"));
        sql = str_cat(db->arena, sql, values);
    }
    
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return false;
    }
    
    // Bind  the values
    int param_idx = 1;
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        
        if (field->flags & AUTO_INC) {
            continue;
        }
        
        // For UPDATE, skip PK in the SET clause, we'll bind it later in WHERE
        if (is_update && (field->flags & PRIMARY_KEY)) {
            continue;
        }
        
        void* field_value = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, param_idx, *(int*)field_value);
                break;
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, param_idx, *(int64_t*)field_value);
                break;
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, param_idx, (double)(*(float*)field_value));
                break;
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, param_idx, *(double*)field_value);
                break;
            case FIELD_TYPE_STRING: {
                char* str = *(char**)field_value;
                if (str) {
                    sqlite3_bind_text(stmt, param_idx, str, -1, SQLITE_STATIC);
                } else {
                    sqlite3_bind_null(stmt, param_idx);
                }
                break;
            }
			case FIELD_TYPE_BLOB: {
				blob_t* blob = (blob_t*)field_value;
				if (blob && blob->data && blob->size > 0) {
					sqlite3_bind_blob(stmt, param_idx, blob->data, (int)blob->size, SQLITE_STATIC);
				} else {
					sqlite3_bind_null(stmt, param_idx);
				}
				break;
			}
            default:
                log_error("Unsupported field type for field '%s'", field->name);
                sqlite3_finalize(stmt);
                arena_end_temp(tmp);
                return false;
        }
        
        param_idx++;
    }
    
    // For UPDATE, bind PK value in WHERE clause
    if (is_update) {
        switch (pk_field->type) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, param_idx, *(int*)pk_value);
                break;
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, param_idx, *(int64_t*)pk_value);
                break;
            case FIELD_TYPE_STRING: {
                char* str = *(char**)pk_value;
                sqlite3_bind_text(stmt, param_idx, str, -1, SQLITE_STATIC);
                break;
            }
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, param_idx, (double)(*(float*)pk_value));
                break;
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, param_idx, *(double*)pk_value);
                break;
            default:
                break;
        }
    }
    
    // Execute
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        log_error("Failed to execute %s: %s", is_update ? "UPDATE" : "INSERT", sqlite3_errmsg(db->db));
        sqlite3_finalize(stmt);
        arena_end_temp(tmp);
        return false;
    }
    
    // For INSERT with AUTO_INC, update the instance with the new ID
    if (!is_update && (pk_field->flags & AUTO_INC)) {
        int64_t last_id = sqlite3_last_insert_rowid(db->db);
        
        // Update the instance with the generated ID
        void* pk_ptr = (char*)instance + pk_field->offset;
        if (pk_field->type == FIELD_TYPE_INT) {
            *(int*)pk_ptr = (int)last_id;
        } else if (pk_field->type == FIELD_TYPE_INT64) {
            *(int64_t*)pk_ptr = last_id;
        }
    }
    
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    return true;
}

bool corm_delete(corm_db_t* db, model_meta_t* meta, void* pk_value) {
	return false;
}

void* corm_find(corm_db_t* db, model_meta_t* meta, void* pk_value) {
	return NULL;
}

void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count) {
	return NULL;
}

void* corm_where_raw(corm_db_t* db, model_meta_t* meta,
					 const char* where_clause, void** params,
					 field_type_e* param_types, size_t param_count,
					 int* count) {
	return NULL;
}

static bool corm_load_belongs_to(corm_db_t* db, void* instance,
								 model_meta_t* meta, field_info_t* field) {
	return false;
}
static bool corm_load_has_many(corm_db_t* db, void* instance,
								 model_meta_t* meta, field_info_t* field) {
	return false;
}
bool corm_load_relation(corm_db_t* db, void* instance, 
                        model_meta_t* meta, const char* field_name) {

    field_info_t* field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field_name) == 0) {
            field = &meta->fields[i];
            break;
        }
    }
    
    if (!field) {
		log_error("Field '%s' doesn't exist in %s", field_name, meta->table_name);
		return false;
	}
    
    if (field->type == FIELD_TYPE_BELONGS_TO) {
        return corm_load_belongs_to(db, instance, meta, field);
    } else if (field->type == FIELD_TYPE_HAS_MANY) {
        return corm_load_has_many(db, instance, meta, field);
    }
    
    return false;
}
