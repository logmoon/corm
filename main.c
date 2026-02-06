#include "thirdparty/sqlite/sqlite3.h"
#include "log.utils/lu_arena.h"
#include "log.utils/lu_darray.h"
#include "log.utils/lu_string.h"

#include "corm.h"

// =============================================================================
// MODEL DEFINITIONS
// =============================================================================

typedef struct {
    int id;
	int mat;
    char* name;
    char* pwd_hash;
} User;

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_INT(User, mat),
    F_STRING(User, name),
    F_STRING_LEN(User, pwd_hash, 256, NOT_NULL)
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
    corm_sync(db, CORM_SYNC_DROP);
    
	/*
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
	*/
    
    corm_deinit(arena, db);
    arena_destroy(arena);
    return 0;
}
