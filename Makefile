# Makefile for MapReduce Project
# Organizes object files and executables into separate directories

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -pthread -O2 -g
LDFLAGS = -pthread

# Directories
SRC_DIR = src
BUILD_DIR = build
OBJ_DIR = $(BUILD_DIR)/obj
BIN_DIR = $(BUILD_DIR)/bin

# Target executable
TARGET = $(BIN_DIR)/mapreduce

# Source files
SRCS = main.c mapreduce.c mapper.c reduce.c partition.c buffer.c sorting.c reader_queue.c metrics.c

# Object files (placed in obj directory)
OBJS = $(SRCS:%.c=$(OBJ_DIR)/%.o)

# Default target
all: directories $(TARGET)

# Create necessary directories
directories:
	@mkdir -p $(OBJ_DIR)
	@mkdir -p $(BIN_DIR)

# Link object files to create executable
$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^
	@echo "Build complete: $(TARGET)"

# Compile source files to object files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -rf output
	@echo "Clean complete"

# Clean and rebuild
rebuild: clean all

# Run with default parameters
run: $(TARGET)
	@mkdir -p output
	$(TARGET) test_input.txt

# Run with custom parameters
run-custom: $(TARGET)
	@mkdir -p output
	$(TARGET) -i 5 -m 5 -r 10 $(ARGS)

# Debug build with sanitizers
debug: CFLAGS += -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
debug: LDFLAGS += -fsanitize=address -fsanitize=undefined
debug: clean all

# Memory leak check with valgrind
valgrind: $(TARGET)
	@mkdir -p output
	valgrind --leak-check=full --show-leak-kinds=all $(TARGET) $(ARGS)

# Display help
help:
	@echo "MapReduce Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build the project (default)"
	@echo "  clean        - Remove build artifacts"
	@echo "  rebuild      - Clean and rebuild"
	@echo "  run          - Run with default test file"
	@echo "  run-custom   - Run with custom args: make run-custom ARGS='file1.txt file2.txt'"
	@echo "  debug        - Build with AddressSanitizer and UBSanitizer"
	@echo "  valgrind     - Run with valgrind memory checker"
	@echo "  help         - Display this help message"
	@echo ""
	@echo "Build structure:"
	@echo "  $(SRC_DIR)/   - Source files (.c, .h)"
	@echo "  $(OBJ_DIR)/   - Object files (.o)"
	@echo "  $(BIN_DIR)/   - Executable binary"

.PHONY: all clean rebuild run run-custom debug valgrind help directories
