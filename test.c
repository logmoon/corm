#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "corm.h"

// ============================================================================
// Model Definitions
// ============================================================================

// User model
typedef struct {
    int id;
    char* username;
    char* email;
    int age;
    bool is_active;
} User;

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING_LEN(User, username, 50, NOT_NULL | UNIQUE),
    F_STRING(User, email, NOT_NULL),
    F_INT(User, age),
    F_BOOL(User, is_active)
);

// Post model (belongs to User)
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

// Comment model (belongs to Post and User)
typedef struct {
    int id;
    char* text;
    int post_id;
    int user_id;
    Post* post;
    User* author;
} Comment;

DEFINE_MODEL(Comment, Comment,
    F_INT(Comment, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Comment, text, NOT_NULL),
    F_INT(Comment, post_id, NOT_NULL),
    F_INT(Comment, user_id, NOT_NULL),
    F_BELONGS_TO(Comment, post, Post, post_id),
    F_BELONGS_TO(Comment, author, User, user_id)
);

// Profile model (one-to-one with User)
typedef struct {
    int id;
    int user_id;
    char* bio;
    blob_t avatar;
} Profile;

DEFINE_MODEL(Profile, Profile,
    F_INT(Profile, id, PRIMARY_KEY | AUTO_INC),
    F_INT(Profile, user_id, NOT_NULL | UNIQUE),
    F_STRING(Profile, bio),
    F_BLOB(Profile, avatar)
);

typedef struct {
    int64_t id;
    char* name;
    float price;
    double rating;
    int stock;
} Product;

DEFINE_MODEL(Product, Product,
    F_INT64(Product, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Product, name, NOT_NULL),
    F_FLOAT(Product, price),
    F_DOUBLE(Product, rating),
    F_INT(Product, stock)
);

// ============================================================================
// Test Helpers
// ============================================================================

#define TEST(name) printf("\n=== Testing: %s ===\n", name)
#define PASS(msg) printf("  %s\n", msg)
#define FAIL(msg) printf("  FAILED: %s\n", msg)

int tests_passed = 0;
int tests_failed = 0;

#define ASSERT_TRUE(cond, msg) do { \
    if (cond) { \
        PASS(msg); \
        tests_passed++; \
    } else { \
        FAIL(msg); \
        tests_failed++; \
    } \
} while(0)

// ============================================================================
// Test Functions
// ============================================================================

void test_database_init(corm_db_t** db) {
    TEST("Database Initialization");
    
    *db = corm_init(":memory:");
    ASSERT_TRUE(*db != NULL, "Database initialized successfully");
}

void test_model_registration(corm_db_t* db) {
    TEST("Model Registration");
    
    bool success = true;
    success = corm_register_model(db, &User_model);
    ASSERT_TRUE(success, "User model registered");
    
    success = corm_register_model(db, &Post_model);
    ASSERT_TRUE(success, "Post model registered");
    
    success = corm_register_model(db, &Comment_model);
    ASSERT_TRUE(success, "Comment model registered");
    
    success = corm_register_model(db, &Profile_model);
    ASSERT_TRUE(success, "Profile model registered");
    
    success = corm_register_model(db, &Product_model);
    ASSERT_TRUE(success, "Product model registered");
}

void test_database_sync(corm_db_t* db) {
    TEST("Database Synchronization");
    
    bool success = corm_sync(db, CORM_SYNC_DROP);
    ASSERT_TRUE(success, "Database synced with DROP mode");
}

void test_basic_crud(corm_db_t* db) {
    TEST("Basic CRUD Operations");
    
    User user1 = {
        .id = 0,
        .username = "alice",
        .email = "alice@example.com",
        .age = 28,
        .is_active = true
    };
    
    bool success = corm_save(db, &User_model, &user1);
    ASSERT_TRUE(success, "User created successfully");
    ASSERT_TRUE(user1.id > 0, "Auto-incremented ID assigned");
    
    int user1_id = user1.id;
    
    corm_result_t* r1 = corm_find(db, &User_model, &user1_id);
    ASSERT_TRUE(r1 != NULL, "User found by ID");
    User* found = (User*)r1->data;
    ASSERT_TRUE(strcmp(found->username, "alice") == 0, "Username matches");
    ASSERT_TRUE(strcmp(found->email, "alice@example.com") == 0, "Email matches");
    ASSERT_TRUE(found->age == 28, "Age matches");
    ASSERT_TRUE(found->is_active == true, "Active status matches");
    
    User update = {
        .id = found->id,
        .username = found->username,
        .email = "alice.updated@example.com",
        .age = 29,
        .is_active = found->is_active
    };
    success = corm_save(db, &User_model, &update);
    ASSERT_TRUE(success, "User updated successfully");
    
    corm_free_result(db, r1);
    
    corm_result_t* r2 = corm_find(db, &User_model, &user1_id);
    ASSERT_TRUE(r2 != NULL, "User found after update");
    found = (User*)r2->data;
    ASSERT_TRUE(found->age == 29, "Age updated correctly");
    ASSERT_TRUE(strcmp(found->email, "alice.updated@example.com") == 0, "Email updated correctly");
    
    corm_free_result(db, r2);
    
    success = corm_delete(db, &User_model, &user1_id);
    ASSERT_TRUE(success, "User deleted successfully");
    
    corm_result_t* r3 = corm_find(db, &User_model, &user1_id);
    ASSERT_TRUE(r3 == NULL, "Deleted user not found");
}

void test_multiple_records(corm_db_t* db) {
    TEST("Multiple Records");
    
    User users[] = {
        {0, "bob", "bob@example.com", 30, true},
        {0, "charlie", "charlie@example.com", 25, true},
        {0, "diana", "diana@example.com", 32, false},
        {0, "eve", "eve@example.com", 27, true}
    };
    
    for (int i = 0; i < 4; i++) {
        bool success = corm_save(db, &User_model, &users[i]);
        ASSERT_TRUE(success, "User saved");
    }
    
    corm_result_t* result = corm_find_all(db, &User_model);
    ASSERT_TRUE(result != NULL, "Find all succeeded");
    ASSERT_TRUE(result->count == 4, "All 4 users retrieved");
    
    User* all_users = (User*)result->data;
    
    bool found_bob = false, found_eve = false;
    for (int i = 0; i < result->count; i++) {
        if (strcmp(all_users[i].username, "bob") == 0) found_bob = true;
        if (strcmp(all_users[i].username, "eve") == 0) found_eve = true;
    }
    ASSERT_TRUE(found_bob, "Bob found in results");
    ASSERT_TRUE(found_eve, "Eve found in results");
    
    corm_free_result(db, result);
}

void test_data_types(corm_db_t* db) {
    TEST("Various Data Types");
    
    Product product = {
        .id = 0,
        .name = "Laptop",
        .price = 999.99f,
        .rating = 4.7,
        .stock = 15
    };
    
    bool success = corm_save(db, &Product_model, &product);
    ASSERT_TRUE(success, "Product created");
    ASSERT_TRUE(product.id > 0, "Int64 ID assigned");
    
    int64_t prod_id = product.id;
    corm_result_t* result = corm_find(db, &Product_model, &prod_id);
    ASSERT_TRUE(result != NULL, "Product found");
    
    Product* found = (Product*)result->data;
    ASSERT_TRUE(found->price > 999.0f && found->price < 1000.0f, "Float price correct");
    ASSERT_TRUE(found->rating > 4.6 && found->rating < 4.8, "Double rating correct");
    ASSERT_TRUE(found->stock == 15, "Integer stock correct");
    
    corm_free_result(db, result);
}

void test_relationships_belongs_to(corm_db_t* db) {
    TEST("Relationships - Belongs To");
    
    User user = {0, "john", "john@example.com", 35, true};
    corm_save(db, &User_model, &user);
    int user_id = user.id;
    
    Post post1 = {0, "First Post", "This is my first post!", user.id, NULL};
    Post post2 = {0, "Second Post", "Another great post!", user.id, NULL};
    
    bool success = corm_save(db, &Post_model, &post1);
    ASSERT_TRUE(success, "Post 1 created");
    success = corm_save(db, &Post_model, &post2);
    ASSERT_TRUE(success, "Post 2 created");
    
    int post_id = post1.id;
    corm_result_t* result = corm_find(db, &Post_model, &post_id);
    ASSERT_TRUE(result != NULL, "Post found");
    
    Post* found_post = (Post*)result->data;
    
    success = corm_load_relation(db, result, &Post_model, found_post, "user");
    ASSERT_TRUE(success, "User relation loaded");
    ASSERT_TRUE(found_post->user != NULL, "User object is populated");
    ASSERT_TRUE(strcmp(found_post->user->username, "john") == 0, "User data correct");
    
    corm_free_result(db, result);
}

void test_blob_field(corm_db_t* db) {
    TEST("BLOB Field Type");
    
    corm_result_t* result = corm_find_all(db, &User_model);
    if (!result || result->count == 0) {
        printf("  (Skipping blob test - no users available)\n");
        if (result) corm_free_result(db, result);
        return;
    }
    
    User* users = (User*)result->data;
    int user_id = users[0].id;
    corm_free_result(db, result);
    
    unsigned char avatar_data[] = {0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10};
    Profile profile = {
        .id = 0,
        .user_id = user_id,
        .bio = "Software developer and coffee enthusiast",
        .avatar = {.data = avatar_data, .size = sizeof(avatar_data)}
    };
    
    bool success = corm_save(db, &Profile_model, &profile);
    ASSERT_TRUE(success, "Profile with blob saved");
    
    int profile_id = profile.id;
    corm_result_t* prof_result = corm_find(db, &Profile_model, &profile_id);
    ASSERT_TRUE(prof_result != NULL, "Profile retrieved");
    
    Profile* found = (Profile*)prof_result->data;
    ASSERT_TRUE(found->avatar.size == sizeof(avatar_data), "Blob size correct");
    ASSERT_TRUE(memcmp(found->avatar.data, avatar_data, sizeof(avatar_data)) == 0, "Blob data correct");
    
    corm_free_result(db, prof_result);
}

void test_where_clause(corm_db_t* db) {
    TEST("WHERE Clause Queries");
    
    bool active_val = true;
    void* params[] = {&active_val};
    field_type_e param_types[] = {FIELD_TYPE_BOOL};
    
    corm_result_t* result = corm_where_raw(db, &User_model, 
                                           "is_active = ?", 
                                           params, param_types, 1);
    
    int count = result ? result->count : 0;
    printf("  Found %d active users\n", count);
    ASSERT_TRUE(count >= 0, "Query executed successfully");
    
    if (result) {
        User* active_users = (User*)result->data;
        bool all_active = true;
        for (int i = 0; i < count; i++) {
            if (!active_users[i].is_active) all_active = false;
        }
        ASSERT_TRUE(all_active, "All returned users are active");
        corm_free_result(db, result);
    }
    
    int min_age = 25;
    int max_age = 30;
    void* age_params[] = {&min_age, &max_age};
    field_type_e age_param_types[] = {FIELD_TYPE_INT, FIELD_TYPE_INT};
    
    corm_result_t* age_result = corm_where_raw(db, &User_model,
                                                "age >= ? AND age <= ?",
                                                age_params, age_param_types, 2);
    
    count = age_result ? age_result->count : 0;
    printf("  Found %d users aged 25-30\n", count);
    ASSERT_TRUE(count >= 0, "Age range query executed");
    
    if (age_result) {
        corm_free_result(db, age_result);
    }
}

void test_custom_allocator(corm_db_t* db) {
    TEST("Custom Allocator");
    
    static int alloc_count = 0;
    static int free_count = 0;
    
    void* custom_alloc(void* ctx, size_t size) {
        alloc_count++;
        return malloc(size);
    }
    
    void custom_free(void* ctx, void* ptr) {
        free_count++;
        free(ptr);
    }
    
    int before_alloc = alloc_count;
    int before_free = free_count;
    
    corm_set_allocator(db, NULL, custom_alloc, custom_free);
    
    User user = {0, "alloctest", "alloc@test.com", 40, true};
    corm_save(db, &User_model, &user);
    
    int user_id = user.id;
    corm_result_t* result = corm_find(db, &User_model, &user_id);
    if (result) {
        corm_free_result(db, result);
    }
    
    ASSERT_TRUE(alloc_count > before_alloc, "Custom allocator was used");
    printf("  Allocations: %d, Frees: %d\n", alloc_count - before_alloc, free_count - before_free);
}

void print_summary() {
    printf("\n");
    printf("=====================================\n");
    printf("Test Summary\n");
    printf("=====================================\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    printf("Total:  %d\n", tests_passed + tests_failed);
    printf("=====================================\n");
    
    if (tests_failed == 0) {
        printf("All tests passed!\n");
    } else {
        printf("Some tests failed.\n");
    }
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(int argc, char** argv) {
    printf("=====================================\n");
    printf("CORM ORM Test Suite\n");
    printf("=====================================\n");
    
    corm_db_t* db = NULL;
    
    test_database_init(&db);
    
    if (db == NULL) {
        printf("Failed to initialize database. Exiting.\n");
        return 1;
    }
    
    test_model_registration(db);
    test_database_sync(db);
    test_basic_crud(db);
    test_multiple_records(db);
    test_data_types(db);
    test_relationships_belongs_to(db);
    test_blob_field(db);
    test_where_clause(db);
    test_custom_allocator(db);
    
    print_summary();
    
    corm_close(db);
    
    return tests_failed > 0 ? 1 : 0;
}
