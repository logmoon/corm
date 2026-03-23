CC      = gcc
AR      = ar
CFLAGS  = -Iinclude -I. -Wall -Wextra
LIBS    = -lm -lpthread

CORE_OBJ    = src/corm.o
BACKEND_OBJ = backends/sqlite/corm_backend_sqlite.o
SQLITE_OBJ  = thirdparty/sqlite/sqlite3.o

LIB_OBJS = $(CORE_OBJ) $(BACKEND_OBJ) $(SQLITE_OBJ)
LIB_OUT  = lib/libcorm.a

DEMO_SRC = main.c
DEMO_OUT = demo

.PHONY: all lib demo clean

all: lib demo

lib: $(LIB_OUT)

$(LIB_OUT): $(LIB_OBJS)
	mkdir -p lib
	$(AR) rcs $@ $^

demo: $(DEMO_SRC) $(LIB_OUT)
	$(CC) $(CFLAGS) -o $(DEMO_OUT) $(DEMO_SRC) -Llib -lcorm $(LIBS)

src/corm.o: src/corm.c include/corm.h include/corm_backend.h
	$(CC) $(CFLAGS) -c $< -o $@

backends/sqlite/corm_backend_sqlite.o: backends/sqlite/corm_backend_sqlite.c include/corm_backend.h include/corm.h
	$(CC) $(CFLAGS) -c $< -o $@

thirdparty/sqlite/sqlite3.o: thirdparty/sqlite/sqlite3.c thirdparty/sqlite/sqlite3.h
	$(CC) -c $< -o $@

clean:
	rm -f $(LIB_OUT) $(DEMO_OUT) *.db $(LIB_OBJS)
