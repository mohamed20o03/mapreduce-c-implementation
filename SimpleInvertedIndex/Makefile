CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
TARGET = inverted_index

all: $(TARGET)

$(TARGET): inverted_index.c
	$(CC) $(CFLAGS) -o $(TARGET) inverted_index.c
	@echo "Build complete: $(TARGET)"

clean:
	rm -f $(TARGET) simple_output.txt
	@echo "Clean complete"

run: $(TARGET)
	./$(TARGET) ../input/*.txt

.PHONY: all clean run
