# corm

An SQL ORM in C to piss off the java devs.

No dependencies beyond the C standard library and whatever sql backend you wanna use. Define your models with macros, get CRUD and basic relation loading without writing SQL by hand.

## Building

Compile `corm.c` alongside your project. Ships with a SQLite backend by default, so link against it if that's what you're using:

```sh
gcc main.c corm.c -lsqlite3 -o myapp
```

## Usage

Define a struct, then describe it with `DEFINE_MODEL`:

```c
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
```

Open a database, register your models, sync the schema:

```c
corm_db_t* db = corm_init("app.db");
corm_register_model(db, &User_model);
corm_sync(db, CORM_SYNC_DROP); // drops and recreates tables
```

Save a record:

```c
User u = {0};
u.username = "alice";
u.email = "alice@example.com";
u.age = 30;
u.is_active = true;
corm_save(db, &User_model, &u);
// u.id is now set from last insert id
```

Query:

```c
corm_result_t* res = corm_find(db, &User_model, &u.id);
User* found = (User*)res->data;

corm_result_t* all = corm_find_all(db, &User_model);
User* users = (User*)all->data;
for (int i = 0; i < all->count; i++) { ... }

corm_free_result(db, res);
```

Raw where clause:

```c
void* params[] = { &age };
field_type_e types[] = { FIELD_TYPE_INT };
corm_result_t* res = corm_where_raw(db, &User_model, "age > ?", params, types, 1);
```

Relations - belongs_to and has_many:

```c
// belongs_to: Post owns a pointer to its User
typedef struct {
    int id;
    char* title;
    int user_id;
    User* user;
} Post;

DEFINE_MODEL(Post, Post,
    F_INT(Post, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Post, title, NOT_NULL),
    F_INT(Post, user_id, NOT_NULL),
    F_BELONGS_TO(Post, user, User, user_id)
);

// has_many: Author has a dynamic array of Posts
// MANY(posts) expands to: int posts_count; void* posts;
typedef struct {
    int id;
    char* name;
    MANY(posts);
} Author;

DEFINE_MODEL(Author, Author,
    F_INT(Author, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Author, name, NOT_NULL),
    F_HAS_MANY(Author, posts, Post, author_id)
);

// loading
corm_result_t* r = corm_load_relation(db, &Post_model, &post, "user");
// post.user is now populated
corm_free_result(db, r);
```

Delete:

```c
corm_delete(db, &User_model, &u.id);
```

Close:

```c
corm_close(db);
```

## Field Macros

| Macro | Description |
|---|---|
| `F_INT` | int field |
| `F_INT64` | int64_t field |
| `F_FLOAT` | float field |
| `F_DOUBLE` | double field |
| `F_STRING` | char* field |
| `F_STRING_LEN` | char* field with max length constraint |
| `F_BOOL` | bool field |
| `F_BLOB` | blob_t field |
| `F_BELONGS_TO` | pointer to related struct, loaded on demand |
| `F_HAS_MANY` | array of related structs, loaded on demand |

Field flags: `PRIMARY_KEY`, `NOT_NULL`, `UNIQUE`, `AUTO_INC`. Combine with `|`.

Validators are optional function pointers with signature `bool fn(void* value, const char** error_msg)`.

## Sync Modes

- `CORM_SYNC_SAFE` - creates tables if they don't exist, does nothing otherwise
- `CORM_SYNC_DROP` - drops and recreates all tables
- `CORM_SYNC_MIGRATE` - adds missing columns (does not drop existing ones)

## Custom Allocator

```c
corm_db_t* db = corm_init_with_allocator("app.db", ctx, my_alloc, my_free);
```

Or swap it out after init:

```c
corm_set_allocator(db, ctx, my_alloc, my_free);
```

## Custom Backend

The backend interface is in `corm_backend.h`. Implement `corm_backend_ops_t` and pass it in:

```c
corm_db_t* db = corm_init_with_backend(&my_backend_ops, "connection_string");
```

SQLite is the only built-in backend. The abstraction is there if you want to add postgres or whatever.

## Error Handling

```c
if (!corm_save(db, &User_model, &u)) {
    fprintf(stderr, "%s\n", corm_get_last_error(db));
}
```

## Limits

`CORM_MAX_MODELS` defaults to 128. Override before including the header:

```c
#define CORM_MAX_MODELS 256
#include "corm.h"
```

Same for `CORM_MALLOC` and `CORM_FREE` if you want to swap the allocator globally at compile time.
