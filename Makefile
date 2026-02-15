CC = gcc
CFLAGS = -Iinclude -I. -Wall -Wextra
LIBS = -lm -lpthread

TEST_OBJ = test.o
MAIN_OBJ = main.o
CORE_OBJ = src/corm.o
BACKEND_OBJ = backends/sqlite/corm_backend_sqlite.o
SQLITE_OBJ = thirdparty/sqlite/sqlite3.o

OBJS = $(CORE_OBJ) $(BACKEND_OBJ) $(SQLITE_OBJ)

main: $(MAIN_OBJ) $(OBJS)
	$(CC) $(CFLAGS) -o corm $(MAIN_OBJ) $(OBJS) $(LIBS)

test: $(TEST_OBJ) $(OBJS)
	$(CC) $(CFLAGS) -o corm $(TEST_OBJ) $(OBJS) $(LIBS)

main.o: main.c include/corm.h
	$(CC) $(CFLAGS) -c main.c -o main.o

test.o: test.c include/corm.h
	$(CC) $(CFLAGS) -c test.c -o test.o

src/corm.o: src/corm.c include/corm.h include/corm_backend.h
	$(CC) $(CFLAGS) -c src/corm.c -o src/corm.o

backends/sqlite/corm_backend_sqlite.o: backends/sqlite/corm_backend_sqlite.c include/corm_backend.h include/corm.h
	$(CC) $(CFLAGS) -c backends/sqlite/corm_backend_sqlite.c -o backends/sqlite/corm_backend_sqlite.o

thirdparty/sqlite/sqlite3.o: thirdparty/sqlite/sqlite3.c thirdparty/sqlite/sqlite3.h
	$(CC) -c thirdparty/sqlite/sqlite3.c -o thirdparty/sqlite/sqlite3.o

clean:
	rm -f corm.exe corm *.db $(TEST_OBJ) $(MAIN_OBJ) $(OBJS)

.PHONY: clean
