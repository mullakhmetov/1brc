CC = clang
CFLAGS = -Wall -Wextra -Werror -O2 -std=c99 -pedantic -g

TARGET = main

all: bin/ $(TARGET) analyze

bin/:
	mkdir -p bin/

analyze:
	bin/$(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) -o bin/$(TARGET) $(TARGET).c $(LFLAGS)
