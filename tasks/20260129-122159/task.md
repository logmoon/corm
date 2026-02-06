---
title: Real work begins
state: in_progress
priority: 0
created: 20260129-122159
---
# Real work begins

## Rewrite base functions and add in new ones
I've removed all the code from the functions, because I didn't write most of it, and I found myself getting lost in it. I've opted to rewrite it and add to it in one go.
### Notes
- `corm_save` - This either does an `UPDATE` or `INSERT` operation based on what's passed. We'd also like to add validation at the very start of the function to make sure what's passed is good; string max lengths, required fields, etc.
## Add custom validators
One cool thing is that we can add custom user defined validators to each field.
Something like:
```c
typedef bool (*validator_fn)(void* value, const char** error_msg);

typedef struct {
    // ...
    validator_fn validator;
} field_info_t;
```
We'd of course need to make sure to call these when we check for validation.
## Relationships
### One-To-One
> has one that belongs to for one-to-one
Example structure:
```c
// USER:
typedef struct {
    int id;
    char* name;
    Profile* profile;
} User;
DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(User, name, NOT_NULL),
    F_HAS_ONE(User, profile, Profile, user_id)
);

// PROFILE:
typedef struct {
    int id;
    int user_id;
    char* bio;
} Profile;

DEFINE_MODEL(Profile, Profile,
    F_INT(Profile, id, PRIMARY_KEY | AUTO_INC),
    F_INT(Profile, user_id, NOT_NULL | UNIQUE),  // UNIQUE to enforce one-to-one - Can be added automatically by the `BELONGS_TO` down bellow?
    F_BELONGS_TO(Profile, user, User, user_id),
    F_STRING(Profile, bio, NOT_NULL)
);
```
### One-To-Many and Many-To-One
> has many that belongs to for one-to-many and many-to-one (just flip the direction)
```c
// USER:
typedef struct {
    int id;
    char* name;
    Post** posts;
    int post_count;
} User;

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(User, name, NOT_NULL),
    F_HAS_MANY(User, posts, Post, user_id)
);

// POST:
typedef struct {
    int id;
    int user_id;  // Foreign key
    char* title;
} Post;

DEFINE_MODEL(Post, Post,
    F_INT(Post, id, PRIMARY_KEY | AUTO_INC),
    F_INT(Post, user_id, NOT_NULL),
    F_BELONGS_TO(Post, user, User, user_id),
    F_STRING(Post, title, NOT_NULL)
);
```

### Many-To-Many
Creation of a join table, automatically, or maybe the user can create one on their own and pass it, I'd like that I think.
Example structure:
```c
// USER:
typedef struct {
    int id;
    char* name;
    Tag** tags;  // Array of tag pointers
    int tag_count;
} User;

DEFINE_MODEL(User, User,
    F_INT(User, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(User, name, NOT_NULL),
    F_MANY_TO_MANY(User, tags, Tag, "user_tags")
);

// TAG:
typedef struct {
    int id;
    char* name;
    User** users;  // Array of user pointers
    int user_count;
} Tag;


DEFINE_MODEL(Tag, Tag,
    F_INT(Tag, id, PRIMARY_KEY | AUTO_INC),
    F_STRING(Tag, name, NOT_NULL),
    F_MANY_TO_MANY(Tag, users, User, "user_tags")
);
```
For supplying user defined join tables, we could do:
```c
typedef struct {
    int user_id;  // FK to User
    int tag_id;   // FK to Tag
    // ... Maybe other shit
} UserTag; // Custom join table

DEFINE_MODEL(UserTag, UserTag,
    F_INT(UserTag, user_id, NOT_NULL),
    F_INT(UserTag, tag_id, NOT_NULL),
    // Add additional constraints or indexes as needed
);

// ...
// In Many-To-Many relationship definition, sum like:
F_MANY_TO_MANY(User, tags, Tag, "UserTag") // Explicitly refer to user-defined join table

// ...
{
    void corm_register_join_table(corm_db_t* db, const char* join_table_name, model_meta_t* meta);
}
```

### Loading
Default to lazy loading (but a lil more manual cuz I don't got getters on here), when we fetch anything from the db into memory, we initialize any relationship fields (pretty sure these are always pointers) to `NULL`. When a user needs that relationship they'd call something like:
```c
void corm_load_relation(corm_db_t* db, void* instance, const char* relation_name);
```
