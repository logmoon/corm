CC = gcc
CFLAGS = -I. -Wall -Wextra
LIBS = -lm

OBJS = main.o thirdparty/sqlite/sqlite3.o

crom: $(OBJS)
	$(CC) $(CFLAGS) -o corm $(OBJS) $(LIBS)

main.o: main.c
	$(CC) $(CFLAGS) -c main.c -o main.o

thirdparty/sqlite/sqlite3.o: thirdparty/sqlite/sqlite3.c thirdparty/sqlite/sqlite3.h
	$(CC) -c thirdparty/sqlite/sqlite3.c -o thirdparty/sqlite/sqlite3.o

clean:
	rm -f corm *.db $(OBJS)
