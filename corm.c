#include "corm.h"

// =============================================================================
// ALLOCATION
// =============================================================================
static inline void* corm_alloc_fn(corm_db_t* db, size_t size) {
    if (db->allocator.alloc_fn) {
        return db->allocator.alloc_fn(db->allocator.ctx, size);
    }
    return CORM_MALLOC(size);  // fallback to defined macro
}
static inline void corm_free_fn(corm_db_t* db, void* ptr) {
    // If using a custom allocator
    if (db->allocator.alloc_fn) {
        // Use custom free if provided
        if (db->allocator.free_fn) {
            db->allocator.free_fn(db->allocator.ctx, ptr);
        }
        // Otherwise no-op
        return;
    }
    // No custom allocator, so we use default CORM_FREE
    CORM_FREE(ptr);
}
// =============================================================================
// INIT/CLEANUP
// =============================================================================
corm_db_t* corm_init(const char* db_filepath) {
    return corm_init_with_allocator(db_filepath, NULL, NULL, NULL);
}

corm_db_t* corm_init_with_allocator(const char* db_filepath, void* ctx,
									void* (*alloc_fn)(void*, size_t),
									void (*free_fn)(void*, void*)) {

    corm_db_t* db = CORM_MALLOC(sizeof(corm_db_t));
    if (db == NULL) {
        log_error("Couldn't allocate db object");
        return NULL;
    }
    
    db->allocator.alloc_fn = alloc_fn;
    db->allocator.free_fn = free_fn;
    db->allocator.ctx = ctx;
    
    db->internal_arena = arena_create(MiB(1));
    if (db->internal_arena == NULL) {
        log_error("Couldn't create internal arena");
        CORM_FREE(db);
        return NULL;
    }
    
    db->model_count = 0;
    db->model_capacity = CORM_MAX_MODELS;
    
    db->models = corm_alloc_fn(db, sizeof(model_meta_t*) * CORM_MAX_MODELS);
    if (db->models == NULL) {
        log_error("Couldn't allocate models array");
        arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }
    
    int rc = sqlite3_open(db_filepath, &db->db);
    if (rc != SQLITE_OK) {
        log_error("Cannot open database: %s", sqlite3_errmsg(db->db));
        sqlite3_close(db->db);
        corm_free_fn(db, db->models);
        arena_destroy(db->internal_arena);
        CORM_FREE(db);
        return NULL;
    }

	char* err_msg = NULL;
	rc = sqlite3_exec(db->db, "PRAGMA foreign_keys = ON;", NULL, NULL, &err_msg);
	if (rc != SQLITE_OK) {
		log_error("Failed to enable foreign keys: %s", err_msg);
		sqlite3_free(err_msg);
	}
    
    return db;
}

void corm_set_allocator(corm_db_t* db, void* ctx,
						void* (*alloc_fn)(void*, size_t),
                        void (*free_fn)(void*, void*)) {

    db->allocator.ctx = ctx;
    db->allocator.alloc_fn = alloc_fn;
    db->allocator.free_fn = free_fn;
}

void corm_close(corm_db_t* db) {
    if (!db) return;
    sqlite3_close(db->db);
    arena_destroy(db->internal_arena);
    corm_free_fn(db, db->models);
    CORM_FREE(db);
}

// =============================================================================
// MODEL REGISTRATION
// =============================================================================

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

    if (db->model_count >= db->model_capacity) {
		log_error("Maximum number of models (%d) reached. Define CORM_MAX_MODELS to increase.",
              CORM_MAX_MODELS);
		return false;
    }
    
    db->models[db->model_count++] = meta;
	return true;
}

static string_t corm_sql_type(arena_t* arena, field_info_t* field) {
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

static bool corm_table_exists(corm_db_t* db, const char* table_name) {
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
	temp_t tmp = arena_start_temp(db->internal_arena);
	string_t sql = str_fmt(db->internal_arena, "CREATE TABLE IF NOT EXISTS %s (", meta->table_name);

    for (u64 i = 0; i < meta->field_count; i++) {
		field_info_t* field = &meta->fields[i];

		if (field->type == FIELD_TYPE_BELONGS_TO ||
			field->type == FIELD_TYPE_HAS_MANY) {
			continue;
		}

		if (i > 0) {
			sql = str_cat(db->internal_arena, sql, STR_LIT(", "));
		}

		string_t type = corm_sql_type(db->internal_arena, field);
		string_t new_field = str_fmt(db->internal_arena, "%s %s", field->name, str_to_c_safe(db->internal_arena, type));

		sql = str_cat(db->internal_arena, sql, new_field);
        if (meta->fields[i].flags & PRIMARY_KEY) {
			sql = str_cat(db->internal_arena, sql, STR_LIT(" PRIMARY KEY"));
        }
        if (meta->fields[i].flags & NOT_NULL) {
			sql = str_cat(db->internal_arena, sql, STR_LIT(" NOT NULL"));
        }
		if (meta->fields[i].flags & UNIQUE) {
			sql = str_cat(db->internal_arena, sql, STR_LIT(" UNIQUE"));
        }
        if (meta->fields[i].flags & AUTO_INC) {
			sql = str_cat(db->internal_arena, sql, STR_LIT(" AUTOINCREMENT"));
        }
    }

	for (u64 i = 0; i < meta->field_count; i++) {
		field_info_t* field = &meta->fields[i];

		if (field->type == FIELD_TYPE_BELONGS_TO) {
			string_t fk = str_fmt(db->internal_arena, ", FOREIGN KEY (%s) REFERENCES %s(id)",
				field->fk_column_name,
				field->target_model_name
			);
			
			switch(field->on_delete) {
				case FK_CASCADE:
					fk = str_cat(db->internal_arena, fk, STR_LIT(" ON DELETE CASCADE"));
					break;
				case FK_SET_NULL:
					fk = str_cat(db->internal_arena, fk, STR_LIT(" ON DELETE SET NULL"));
					break;
				case FK_RESTRICT:
					fk = str_cat(db->internal_arena, fk, STR_LIT(" ON DELETE RESTRICT"));
					break;
				default: break;
			}
			
			sql = str_cat(db->internal_arena, sql, fk);
		}
	}

	sql = str_cat(db->internal_arena, sql, STR_LIT(");"));

	// Maybe hide this? Maybe use a versobe flag or sum?
	log_info("  Running: %s", str_to_c(sql));

    char *err_msg = NULL;
	int rc = sqlite3_exec(db->db, str_to_c_safe(db->internal_arena, sql), NULL, NULL, &err_msg);
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

			// Disable ts temporarily so that we can drop
			sqlite3_exec(db->db, "PRAGMA foreign_keys = OFF;", NULL, NULL, NULL);
			// Drop all tables
			for (u64 i = 0; i < db->model_count; ++i) {
                temp_t tmp = arena_start_temp(db->internal_arena);
                string_t drop_sql = str_fmt(db->internal_arena, "DROP TABLE IF EXISTS %s;", 
                                           db->models[i]->table_name);
                char* err_msg = NULL;
                int rc = sqlite3_exec(db->db, str_to_c_safe(db->internal_arena, drop_sql), NULL, NULL, &err_msg);
				if (rc != SQLITE_OK) {
					log_error("Failed to drop table '%s': %s", db->models[i]->table_name, err_msg);
					sqlite3_free(err_msg);
					arena_end_temp(tmp);
					return false;
				}
				log_info("  Dropped table '%s'", db->models[i]->table_name);
                arena_end_temp(tmp);
            }
			// Re-enable
			sqlite3_exec(db->db, "PRAGMA foreign_keys = ON;", NULL, NULL, NULL);

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
    string_t sql = str_fmt(db->internal_arena, "SELECT COUNT(*) FROM %s WHERE %s = ?;", 
                          meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
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
    temp_t tmp = arena_start_temp(db->internal_arena);
    
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
        sql = str_fmt(db->internal_arena, "UPDATE %s SET ", meta->table_name);
        
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];

			// Skip relationship fields
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            // Skip PK and AUTO_INC fields
            if (field->flags & PRIMARY_KEY || field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = str_cat(db->internal_arena, sql, STR_LIT(", "));
            }
            sql = str_cat(db->internal_arena, sql, str_fmt(db->internal_arena, "%s=?", field->name));
            first = false;
        }
        
        sql = str_cat(db->internal_arena, sql, str_fmt(db->internal_arena, " WHERE %s=?;", pk_field->name));
    } else {
        // INSERT INTO table (field1, field2, ...) VALUES (?, ?, ...)
        sql = str_fmt(db->internal_arena, "INSERT INTO %s (", meta->table_name);
        string_t values = STR_LIT("VALUES (");
        
        bool first = true;
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];

			// Skip relationship fields
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }

            // Skip AUTO_INC fields
            if (field->flags & AUTO_INC) {
                continue;
            }
            
            if (!first) {
                sql = str_cat(db->internal_arena, sql, STR_LIT(", "));
                values = str_cat(db->internal_arena, values, STR_LIT(", "));
            }
            
            sql = str_cat(db->internal_arena, sql, str_fmt(db->internal_arena, "%s", field->name));
            values = str_cat(db->internal_arena, values, STR_LIT("?"));
            first = false;
        }
        
        sql = str_cat(db->internal_arena, sql, STR_LIT(") "));
        values = str_cat(db->internal_arena, values, STR_LIT(");"));
        sql = str_cat(db->internal_arena, sql, values);
    }
    
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare statement: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return false;
    }
    
    // Bind  the values
    int param_idx = 1;
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];

		// Skip relationship fields
		if (field->type == FIELD_TYPE_BELONGS_TO || 
			field->type == FIELD_TYPE_HAS_MANY) {
			continue;
		}
        
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
    if (!db || !meta || !pk_value) {
        log_error("Invalid arguments to corm_delete");
        return false;
    }
    
    if (!meta->primary_key_field) {
        log_error("Model '%s' has no primary key", meta->table_name);
        return false;
    }
    
    temp_t tmp = arena_start_temp(db->internal_arena);
    
    string_t sql = str_fmt(db->internal_arena, "DELETE FROM %s WHERE %s = ?;",
                           meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare DELETE: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return false;
    }
    
    // Bind primary key
    switch (meta->primary_key_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
            
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
            
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
            
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
            
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
            
        default:
            log_error("Unsupported primary key type");
            sqlite3_finalize(stmt);
            arena_end_temp(tmp);
            return false;
    }
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    
    if (rc != SQLITE_DONE) {
        log_error("Failed to execute DELETE: %s", sqlite3_errmsg(db->db));
        return false;
    }
    
    int rows_affected = sqlite3_changes(db->db);
    if (rows_affected == 0) {
        log_warn("No rows deleted (record not found)");
        return false;
    }
    
    return true;
}

// =============================================================================
// QUERY FUNCTIONS
// =============================================================================

void* corm_find(corm_db_t* db, model_meta_t* meta, void* pk_value) {
    temp_t tmp = arena_start_temp(db->internal_arena);
    
    string_t sql = str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s = ?;",
                          meta->table_name, meta->primary_key_field->name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare find query: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return NULL;
    }
    
    field_info_t* pk_field = meta->primary_key_field;
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        default:
            log_error("Unsupported primary key type");
            sqlite3_finalize(stmt);
            arena_end_temp(tmp);
            return NULL;
    }
    
    void* instance = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        instance = corm_alloc_fn(db, meta->struct_size);
        if (!instance) {
            log_error("Failed to allocate instance");
            sqlite3_finalize(stmt);
            arena_end_temp(tmp);
            return NULL;
        }
        memset(instance, 0, meta->struct_size);
        
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)instance + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                log_warn("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    log_warn("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
    }
    
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    
    return instance;
}

void* corm_find_all(corm_db_t* db, model_meta_t* meta, int* count) {
    temp_t tmp = arena_start_temp(db->internal_arena);
    
    // First, count how many records we have
    string_t count_sql = str_fmt(db->internal_arena, "SELECT COUNT(*) FROM %s;", meta->table_name);
    
    sqlite3_stmt* count_stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, count_sql), -1, &count_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare count query: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return NULL;
    }
    
    int record_count = 0;
    if (sqlite3_step(count_stmt) == SQLITE_ROW) {
        record_count = sqlite3_column_int(count_stmt, 0);
    }
    sqlite3_finalize(count_stmt);
    
    if (record_count == 0) {
        if (count) *count = 0;
        arena_end_temp(tmp);
        return NULL;
    }
    
    // Allocate array for all instances
    void* instances = corm_alloc_fn(db, meta->struct_size * record_count);
    if (!instances) {
        log_error("Failed to allocate instances array");
        arena_end_temp(tmp);
        return NULL;
    }
    memset(instances, 0, meta->struct_size * record_count);
    
    // Now query all records
    string_t sql = str_fmt(db->internal_arena, "SELECT * FROM %s;", meta->table_name);
    
    sqlite3_stmt* stmt;
    rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare find_all query: %s", sqlite3_errmsg(db->db));
        corm_free_fn(db, instances);
        arena_end_temp(tmp);
        return NULL;
    }
    
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < record_count) {
        void* instance = (char*)instances + (idx * meta->struct_size);
        
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)instance + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                log_warn("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    log_warn("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    
    if (count) *count = record_count;
    return instances;
}

void* corm_where_raw(corm_db_t* db, model_meta_t* meta, const char* where_clause, 
                     void** params, field_type_e* param_types, size_t param_count, int* count) {
    if (!db || !meta || !where_clause || !count) {
        log_error("Invalid arguments to corm_where_raw");
        return NULL;
    }
    
    *count = 0;
    
    temp_t tmp = arena_start_temp(db->internal_arena);
    
    // Build SELECT query
    string_t sql = str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s;", 
                           meta->table_name, where_clause);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare WHERE query: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return NULL;
    }
    
    // Bind parameters
    for (size_t i = 0; i < param_count; i++) {
        int bind_idx = i + 1;
        
        switch (param_types[i]) {
            case FIELD_TYPE_INT:
            case FIELD_TYPE_BOOL:
                sqlite3_bind_int(stmt, bind_idx, *(int*)params[i]);
                break;
                
            case FIELD_TYPE_INT64:
                sqlite3_bind_int64(stmt, bind_idx, *(int64_t*)params[i]);
                break;
                
            case FIELD_TYPE_FLOAT:
                sqlite3_bind_double(stmt, bind_idx, (double)(*(float*)params[i]));
                break;
                
            case FIELD_TYPE_DOUBLE:
                sqlite3_bind_double(stmt, bind_idx, *(double*)params[i]);
                break;
                
            case FIELD_TYPE_STRING: {
                char* str = *(char**)params[i];
                sqlite3_bind_text(stmt, bind_idx, str, -1, SQLITE_STATIC);
                break;
            }
            
            case FIELD_TYPE_BLOB: {
                blob_t* blob = (blob_t*)params[i];
                sqlite3_bind_blob(stmt, bind_idx, blob->data, blob->size, SQLITE_STATIC);
                break;
            }
                
            default:
                log_error("Unsupported parameter type %d at index %zu", param_types[i], i);
                sqlite3_finalize(stmt);
                arena_end_temp(tmp);
                return NULL;
        }
    }
    
    // First pass: count results
    int result_count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        result_count++;
    }
    
    if (result_count == 0) {
        sqlite3_finalize(stmt);
        arena_end_temp(tmp);
        return NULL;
    }
    
    sqlite3_reset(stmt);
    
    // Allocate array for results
    void* instances = corm_alloc_fn(db, meta->struct_size * result_count);
    if (!instances) {
        log_error("Failed to allocate instances array");
        sqlite3_finalize(stmt);
        arena_end_temp(tmp);
        return NULL;
    }
    memset(instances, 0, meta->struct_size * result_count);
    
    // Second pass: populate instances
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < result_count) {
        void* inst = (char*)instances + (idx * meta->struct_size);
        
        for (u64 i = 0; i < meta->field_count; i++) {
            field_info_t* field = &meta->fields[i];
            
            if (field->type == FIELD_TYPE_BELONGS_TO || 
                field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)inst + field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                log_warn("Column '%s' not found in result set", field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    log_warn("Unsupported field type for field '%s'", field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    
    *count = result_count;
    return instances;
}

static bool corm_load_belongs_to(corm_db_t* db, void* instance,
                                 model_meta_t* meta, field_info_t* field) {
    // Find the foreign key field in the current model
    field_info_t* fk_field = NULL;
    for (size_t i = 0; i < meta->field_count; i++) {
        if (strcmp(meta->fields[i].name, field->fk_column_name) == 0) {
            fk_field = &meta->fields[i];
            break;
        }
    }
    
    if (!fk_field) {
        log_error("Foreign key field '%s' not found in model '%s'", 
                 field->fk_column_name, meta->table_name);
        return false;
    }
    
    if (!field->related_model) {
        log_error("Related model not resolved for field '%s'", field->name);
        return false;
    }
    
    // Get the foreign key value from the instance
    void* fk_value_ptr = (char*)instance + fk_field->offset;
    
    // Check if FK is null (for nullable relationships)
    bool is_null = false;
    switch (fk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            is_null = (*(int*)fk_value_ptr == 0);
            break;
        case FIELD_TYPE_INT64:
            is_null = (*(int64_t*)fk_value_ptr == 0);
            break;
        case FIELD_TYPE_STRING:
            is_null = (*(char**)fk_value_ptr == NULL);
            break;
        default:
            break;
    }
    
    if (is_null) {
        // Set relation field to NULL
        void* relation_ptr = (char*)instance + field->offset;
        *(void**)relation_ptr = NULL;
        return true;
    }
    
    // Load the related instance using corm_find
    void* related_instance = corm_find(db, field->related_model, fk_value_ptr);
    
    if (!related_instance) {
        log_warn("Related instance not found for field '%s'", field->name);
        return false;
    }
    
    // Store the pointer in the relationship field
    void* relation_ptr = (char*)instance + field->offset;
    *(void**)relation_ptr = related_instance;
    
    return true;
}

static bool corm_load_has_many(corm_db_t* db, void* instance,
                               model_meta_t* meta, field_info_t* field) {
    temp_t tmp = arena_start_temp(db->internal_arena);
    
    if (!field->related_model) {
        log_error("Related model not resolved for field '%s'", field->name);
        arena_end_temp(tmp);
        return false;
    }
    
    // Get this instance's primary key value
    void* pk_value = (char*)instance + meta->primary_key_field->offset;
    
    // Build query: SELECT * FROM related_table WHERE fk_column = ?
    string_t sql = str_fmt(db->internal_arena, "SELECT * FROM %s WHERE %s = ?;",
                          field->related_model->table_name,
                          field->fk_column_name);
    
    sqlite3_stmt* stmt;
    int rc = sqlite3_prepare_v2(db->db, str_to_c_safe(db->internal_arena, sql), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_error("Failed to prepare has_many query: %s", sqlite3_errmsg(db->db));
        arena_end_temp(tmp);
        return false;
    }
    
    // Bind the primary key value
    field_info_t* pk_field = meta->primary_key_field;
    switch (pk_field->type) {
        case FIELD_TYPE_INT:
        case FIELD_TYPE_BOOL:
            sqlite3_bind_int(stmt, 1, *(int*)pk_value);
            break;
        case FIELD_TYPE_INT64:
            sqlite3_bind_int64(stmt, 1, *(int64_t*)pk_value);
            break;
        case FIELD_TYPE_FLOAT:
            sqlite3_bind_double(stmt, 1, (double)(*(float*)pk_value));
            break;
        case FIELD_TYPE_DOUBLE:
            sqlite3_bind_double(stmt, 1, *(double*)pk_value);
            break;
        case FIELD_TYPE_STRING: {
            char* str = *(char**)pk_value;
            sqlite3_bind_text(stmt, 1, str, -1, SQLITE_STATIC);
            break;
        }
        default:
            log_error("Unsupported primary key type");
            sqlite3_finalize(stmt);
            arena_end_temp(tmp);
            return false;
    }
    
    // First pass: count results
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        count++;
    }
    sqlite3_reset(stmt);
    
    // Allocate array for results
    void* instances = NULL;
    if (count > 0) {
        instances = corm_alloc_fn(db, field->related_model->struct_size * count);
        if (!instances) {
            log_error("Failed to allocate instances array");
            sqlite3_finalize(stmt);
            arena_end_temp(tmp);
            return false;
        }
        memset(instances, 0, field->related_model->struct_size * count);
    }
    
    // Second pass: populate instances
    int idx = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && idx < count) {
        void* inst = (char*)instances + (idx * field->related_model->struct_size);
        
        for (u64 i = 0; i < field->related_model->field_count; i++) {
            field_info_t* rel_field = &field->related_model->fields[i];
            
            if (rel_field->type == FIELD_TYPE_BELONGS_TO || 
                rel_field->type == FIELD_TYPE_HAS_MANY) {
                continue;
            }
            
            void* field_ptr = (char*)inst + rel_field->offset;
            
            int col_idx = -1;
            for (int j = 0; j < sqlite3_column_count(stmt); j++) {
                if (strcmp(sqlite3_column_name(stmt, j), rel_field->name) == 0) {
                    col_idx = j;
                    break;
                }
            }
            
            if (col_idx == -1) {
                log_warn("Column '%s' not found in result set", rel_field->name);
                continue;
            }
            
            if (sqlite3_column_type(stmt, col_idx) == SQLITE_NULL) {
                continue;
            }
            
            switch (rel_field->type) {
                case FIELD_TYPE_INT:
                case FIELD_TYPE_BOOL:
                    *(int*)field_ptr = sqlite3_column_int(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_INT64:
                    *(int64_t*)field_ptr = sqlite3_column_int64(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_FLOAT:
                    *(float*)field_ptr = (float)sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_DOUBLE:
                    *(double*)field_ptr = sqlite3_column_double(stmt, col_idx);
                    break;
                    
                case FIELD_TYPE_STRING: {
                    const char* text = (const char*)sqlite3_column_text(stmt, col_idx);
                    if (text) {
                        size_t len = strlen(text);
                        char* str = corm_alloc_fn(db, len + 1);
                        if (str) {
                            memcpy(str, text, len + 1);
                            *(char**)field_ptr = str;
                        }
                    }
                    break;
                }
                
                case FIELD_TYPE_BLOB: {
                    const void* blob_data = sqlite3_column_blob(stmt, col_idx);
                    int blob_size = sqlite3_column_bytes(stmt, col_idx);
                    if (blob_data && blob_size > 0) {
                        void* data = corm_alloc_fn(db, blob_size);
                        if (data) {
                            memcpy(data, blob_data, blob_size);
                            ((blob_t*)field_ptr)->data = data;
                            ((blob_t*)field_ptr)->size = blob_size;
                        }
                    }
                    break;
                }
                
                default:
                    log_warn("Unsupported field type for field '%s'", rel_field->name);
                    break;
            }
        }
        
        idx++;
    }
    
    sqlite3_finalize(stmt);
    arena_end_temp(tmp);
    
    // Store the array pointer in the relationship field
    void* relation_ptr = (char*)instance + field->offset;
    *(void**)relation_ptr = instances;
    
    // Store the count
    void* count_ptr = (char*)instance + field->count_offset;
    *(int*)count_ptr = count;
    
    return true;
}
bool corm_load_relation(corm_db_t* db, model_meta_t* meta, void* instance, const char* field_name) {
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

void corm_free(corm_db_t* db, model_meta_t* meta, void* instance) {
    if (!instance) return;
    
    for (u64 i = 0; i < meta->field_count; i++) {
        field_info_t* field = &meta->fields[i];
        void* field_ptr = (char*)instance + field->offset;
        
        switch (field->type) {
            case FIELD_TYPE_STRING: {
                char** str_ptr = (char**)field_ptr;
                if (*str_ptr) {
                    corm_free_fn(db, *str_ptr);
                }
                break;
            }
            case FIELD_TYPE_BLOB: {
                blob_t* blob = (blob_t*)field_ptr;
                if (blob->data) {
                    corm_free_fn(db, blob->data);
                }
                break;
            }
            case FIELD_TYPE_BELONGS_TO: {
                // Free the single related instance
                void** related_ptr = (void**)field_ptr;
                if (*related_ptr && field->related_model) {
                    corm_free(db, field->related_model, *related_ptr);
                }
                break;
            }
            case FIELD_TYPE_HAS_MANY: {
                // Free the array of related instances
                void** related_array = (void**)field_ptr;
                if (*related_array && field->related_model) {
                    // Get the count
                    void* count_ptr = (char*)instance + field->count_offset;
                    int count = *(int*)count_ptr;
                    
                    // Free each instance in the array
                    for (int j = 0; j < count; j++) {
                        void* inst = (char*)(*related_array) + (j * field->related_model->struct_size);
                        corm_free(db, field->related_model, inst);
                    }
                    
                    // Free the array itself
                    corm_free_fn(db, *related_array);
                }
                break;
            }
            default:
                break;
        }
    }
    
    corm_free_fn(db, instance);
}
