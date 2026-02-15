#include <stdio.h>

#include "corm.h"

typedef struct {
	int id;
	char* name;
} User;

DEFINE_MODEL(User, User,
	F_INT(User, id, PRIMARY_KEY | AUTO_INC),
	F_STRING(User, name),
);

int main() {
	corm_db_t* db = corm_init("test.db");

	corm_register_model(db, &User_model);
	corm_sync(db, CORM_SYNC_SAFE);

	User user = {0};
	user.name = "Amen";

	corm_save(db, &User_model, &user);

	User* found = corm_find(db, &User_model, &user.id);
	if (found) {
		printf("Found user: %s with id: %d\n", found->name, found->id);
	}
	corm_free(db, &User_model, found);

	corm_close(db);
	return 0;
}
