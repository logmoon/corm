#include "thirdparty/sqlite/sqlite3.h"
#include "log.utils/lu_arena.h"
#include "log.utils/lu_darray.h"
#include "log.utils/lu_string.h"

#include "corm.h"

// =============================================================================
// MODEL DEFINITIONS
// =============================================================================

bool validate_password(void* value, const char** error_msg) {
    char* pwd = *(char**)value;
    if (!pwd || strlen(pwd) < 8) {
        *error_msg = "Password must be at least 8 characters";
        return false;
    }
    return true;
}

 typedef struct {
    int id;
    int mat;
    char* name;
    char* pwd;
    blob_t profile_picture;
} User;


DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_INT(User, mat),
    F_STRING(User, name),
    F_STRING_LEN(User, pwd, 256, NOT_NULL, validate_password),
    F_BLOB(User, profile_picture)
);


typedef struct {
    int id;
    char* bio;
    int user_id;
    User* user;
} Profile;


DEFINE_MODEL(Profile, Profile,
    F_INT(Profile, id, PRIMARY_KEY | AUTO_INC),
    F_STRING_LEN(Profile, bio, 200),
	F_INT(Profile, user_id, UNIQUE | NOT_NULL),
    F_BELONGS_TO(Profile, user, User, user_id)
);

// =============================================================================
// MAIN
// =============================================================================

int main() {
    arena_t* arena = arena_create(MiB(10));
    corm_db_t* db = corm_init(arena, "test.db");
    
    // Register models
    if (!corm_register_model(db, &User_model)) {
		corm_deinit(arena, db);
		arena_destroy(arena);
		return 1;
	}
    if (!corm_register_model(db, &Profile_model)) {
		corm_deinit(arena, db);
		arena_destroy(arena);
		return 1;
	}

    if (!corm_sync(db, CORM_SYNC_DROP)) {
		corm_deinit(arena, db);
		arena_destroy(arena);
		return 1;
	}

	FILE* f = fopen("avatar.jpg", "rb");
	fseek(f, 0, SEEK_END);
	size_t size = ftell(f);
	fseek(f, 0, SEEK_SET);
	void* data = arena_alloc(arena, size);
	fread(data, 1, size, f);
	fclose(f);

    User user = {0};
    user.name = "John Doe";
	user.mat = 10;
	user.profile_picture = (blob_t){ .data = data, .size = size };
    user.pwd = "12345678";
    
    if (!corm_save(db, &User_model, &user)) {
		corm_deinit(arena, db);
		arena_destroy(arena);
		return 1;
	}
    log_info("User saved with ID: %d", user.id);
    
	/*
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
	*/
    
    corm_deinit(arena, db);
    arena_destroy(arena);
    return 0;
}
