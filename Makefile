CC = gcc
CFLAGS = -I. -Wall -Wextra
LIBS = -lm -lpthread
OBJS = main.o corm.o thirdparty/sqlite/sqlite3.o

corm: $(OBJS)
	$(CC) $(CFLAGS) -o corm $(OBJS) $(LIBS)

main.o: main.c corm.h
	$(CC) $(CFLAGS) -c main.c -o main.o

corm.o: corm.c corm.h
	$(CC) $(CFLAGS) -c corm.c -o corm.o

thirdparty/sqlite/sqlite3.o: thirdparty/sqlite/sqlite3.c thirdparty/sqlite/sqlite3.h
	$(CC) -c thirdparty/sqlite/sqlite3.c -o thirdparty/sqlite/sqlite3.o

clean:
	rm -f corm.exe corm *.db $(OBJS)
