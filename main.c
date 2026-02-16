#include <stdio.h>
#include "corm.h"

typedef struct {
	int id;
	char* username;
	char* email;
	int age;
	bool is_active;
} User;


bool verify_email(void* email, const char** error_msg) {
	return true;
}

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING_LEN(User, username, 50, NOT_NULL | UNIQUE),
    F_STRING(User, email, NOT_NULL, verify_email),
    F_INT(User, age),
    F_BOOL(User, is_active)
);

typedef struct {
    int id;
    char* title;
    char* content;
    int user_id;
    User* user;
} Post;

DEFINE_MODEL(Post, Post,
    F_INT(Post, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Post, title, NOT_NULL),
    F_STRING(Post, content),
    F_INT(Post, user_id, NOT_NULL),
    F_BELONGS_TO(Post, user, User, user_id)
);

int main() {
	corm_db_t* db = corm_init("main.db");
	if (!corm_register_model(db, &User_model) ||
		!corm_register_model(db, &Post_model)) {
		corm_close(db);
		return 1;
	}
	
    if (!corm_sync(db, CORM_SYNC_DROP)) {
		corm_close(db);
		return 1;
	}

	User first_user = {0};
	first_user.username = "Amen";
	first_user.email = "email";
	first_user.age = 23;
	first_user.is_active = true;

	if (!corm_save(db, &User_model, &first_user)) {
		corm_close(db);
		return 1;
	}
	printf("User saved\n");

	Post post = {0};
	post.title = "New Post";
	post.content = "This is my first post!";
	post.user_id = first_user.id;

	if (!corm_save(db, &Post_model, &post)) {
		corm_close(db);
		return 1;
	}
	printf("Post saved\n");

	corm_result_t* user_result = corm_load_relation(db, &Post_model, &post, "user");
	if (user_result == NULL) {
		corm_close(db);
		return 1;
	}

	printf("Loaded user relation, username: %s\n", post.user->username);
	
	corm_free_result(db, user_result);

	return 0;
}
