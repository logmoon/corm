#include <stdio.h>
#include "corm.h"

typedef struct {
    int id;
    char* username;
    char* email;
    int age;
    bool is_active;
} User;

bool verify_email(void* instance, void* email, const char** error_msg) {
	(void)instance;
    (void)email;
    (void)error_msg;
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
    if (!db) return 1;

    if (!corm_register_model(db, &User_model) ||
        !corm_register_model(db, &Post_model)) {
        corm_close(db);
        return 1;
    }

    if (!corm_sync(db, CORM_SYNC_DROP)) {
        printf("Sync error: %s\n", corm_get_last_error(db));
        corm_close(db);
        return 1;
    }

    User users[] = {
        { .username = "Amen",  .email = "amen@example.com",  .age = 23, .is_active = true  },
        { .username = "Sara",  .email = "sara@example.com",  .age = 27, .is_active = true  },
        { .username = "Ghost", .email = "ghost@example.com", .age = 19, .is_active = false },
    };

    for (int i = 0; i < 3; i++) {
        if (!corm_save(db, &User_model, &users[i])) {
            printf("Save error: %s\n", corm_get_last_error(db));
            corm_close(db);
            return 1;
        }
    }
    printf("Saved 3 users\n");

    Post post = {
        .title   = "New Post",
        .content = "This is my first post!",
        .user_id = users[0].id,
    };

    if (!corm_save(db, &Post_model, &post)) {
        printf("Save error: %s\n", corm_get_last_error(db));
        corm_close(db);
        return 1;
    }
    printf("Saved post\n");

    // load relation
    corm_result_t* user_result = corm_load_relation(db, &Post_model, &post, "user");
    if (!user_result) {
        printf("Relation error: %s\n", corm_get_last_error(db));
        corm_close(db);
        return 1;
    }
    printf("Post belongs to: %s\n", post.user->username);
    corm_free_result(db, user_result);

    // All users ordered by age
    corm_query_t* q = corm_query(db, &User_model);
    corm_query_order_by(q, "age ASC");
    corm_result_t* all_users = corm_query_exec(q);
    if (all_users) {
        printf("\nAll users (ordered by age):\n");
        User* u = (User*)all_users->data;
        for (int i = 0; i < all_users->count; i++) {
            printf("  [%d] %s, age %d, active: %d\n", u[i].id, u[i].username, u[i].age, u[i].is_active);
        }
        corm_free_result(db, all_users);
    }

    // Active users over 20, limited to 2
    int min_age = 20;
    int active  = 1;
    void* params[]       = { &min_age, &active };
    field_type_e types[] = { FIELD_TYPE_INT, FIELD_TYPE_INT };

    q = corm_query(db, &User_model);
    corm_query_where(q, "age > ? AND is_active = ?", params, types, 2);
    corm_query_order_by(q, "age DESC");
    corm_query_limit(q, 2);

    corm_result_t* filtered = corm_query_exec(q);
    if (filtered) {
        printf("\nActive users over 20 (limit 2, age DESC):\n");
        User* u = (User*)filtered->data;
        for (int i = 0; i < filtered->count; i++) {
            printf("  [%d] %s, age %d\n", u[i].id, u[i].username, u[i].age);
        }
        corm_free_result(db, filtered);
    }

    corm_close(db);
    return 0;
}
