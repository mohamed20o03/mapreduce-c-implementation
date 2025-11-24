/**
 * @file mapreduce.h
 * @brief Public API for MapReduce Framework with Reader Queue Architecture
 *
 * This header defines the public interface for a high-performance MapReduce
 * implementation featuring:
 *   - Chunk-level parallel I/O with atomic work assignment
 *   - Producer-consumer pattern with bounded work queue
 *   - Thread-local buffering for reduced lock contention
 *   - Fine-grained partition locking for scalability
 *
 */

#ifndef __mapreduce_h__
#define __mapreduce_h__

/* ========================================================================== */
/*                         INTERMEDIATE VALUE NODES                           */
/* ========================================================================== */

/**
 * @struct value_node
 * @brief Linked-list node that stores intermediate values for a key
 *
 * Combiner implementations can walk and mutate this list in-place to shrink
 * the amount of data that reaches reducers (e.g., pre-aggregate within a
 * mapper). Strings are heap-allocated and owned by the framework unless the
 * combiner frees them while removing nodes.
 */
typedef struct value_node {
    char *value_name;                  /**< Heap-allocated value string */
    struct value_node *next_value_node; /**< Next value in list, or NULL */
} value_node_t;

/* ========================================================================== */
/*                          FUNCTION POINTER TYPES                            */
/* ========================================================================== */

/**
 * @typedef Getter
 * @brief Function pointer type for retrieving values during reduce phase
 *
 * This function is provided to the user's Reducer function to iterate through
 * all values associated with a particular key. It maintains internal state to
 * track iteration progress.
 *
 * @param key The key whose values are being retrieved
 * @param partition_number The partition index containing this key
 * @return Pointer to the next value string, or NULL when no more values exist
 *
 * @note The returned string should not be freed by the caller
 * @note This function is thread-safe when called from different partitions
 *
 * Usage Example:
 * @code
 * void MyReduce(char* key, Getter get_next, int partition_number) {
 *     char* value;
 *     while ((value = get_next(key, partition_number)) != NULL) {
 *         // Process value
 *     }
 * }
 * @endcode
 */
typedef char *(*Getter)(char *key, int partition_number);

/**
 * @typedef Mapper
 * @brief Function pointer type for processing data chunks in map phase
 *
 * This function is called by mapper worker threads to process chunks of data
 * that have been read from input files and placed in the work queue. The
 * mapper should parse the chunk and emit key-value pairs using MR_Emit().
 *
 * @param data Pointer to the chunk data buffer
 * @param size Size of the chunk in bytes
 *
 * @note The data buffer is owned by the caller and will be freed after return
 * @note This function must be thread-safe as it's called by multiple mappers
 * @note Use MR_Emit() to output key-value pairs
 *
 * Usage Example:
 * @code
 * void MyMap(char* data, int size) {
 *     // Parse chunk and extract words
 *     for (each word in chunk) {
 *         MR_Emit(word, "1");
 *     }
 * }
 * @endcode
 */
typedef void (*Mapper)(char *data, int size);

/**
 * @typedef Reducer
 * @brief Function pointer type for processing keys and their values
 *
 * This function is called once per unique key in a partition. It receives a
 * Getter function to iterate through all values associated with the key.
 * The reducer aggregates values and typically writes results to an output file.
 *
 * @param key The key being processed (unique within partition)
 * @param get_func Function to call repeatedly to get next value for this key
 * @param partition_number The partition index (used for get_func calls)
 *
 * @note Keys are provided in sorted order within each partition
 * @note Each reducer processes one partition in a separate thread
 * @note Write output to partition-specific files to avoid conflicts
 *
 * Usage Example:
 * @code
 * void MyReduce(char* key, Getter get_next, int partition_number) {
 *     int count = 0;
 *     char* value;
 *     while ((value = get_next(key, partition_number)) != NULL) {
 *         count += atoi(value);
 *     }
 *     printf("%s: %d\n", key, count);
 * }
 * @endcode
 */
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);

/**
 * @typedef Combiner
 * @brief Optional map-side combiner invoked before flushing mapper buffers
 *
 * Called once per key inside each mapper's local buffer. The combiner can
 * mutate or prune the linked list of values to reduce shuffle volume. The
 * value_head pointer may be updated by the combiner (e.g., if removing the
 * first node) and any nodes it deletes must also free their value strings.
 *
 * @param key The key whose values are being combined
 * @param value_head Pointer to the head pointer for this key's value list
 */
typedef void (*Combiner)(char *key, value_node_t **value_head);

/**
 * @typedef Partitioner
 * @brief Function pointer type for determining key-to-partition assignment
 *
 * This function determines which partition (and thus which reducer) will
 * process a given key. A good partitioner distributes keys evenly across
 * partitions for balanced reducer workload.
 *
 * @param key The key to be partitioned
 * @param num_partitions Total number of partitions available
 * @return Partition index in range [0, num_partitions-1]
 *
 * @note Must be deterministic: same key always maps to same partition
 * @note Should distribute keys uniformly for load balancing
 * @note Use MR_DefaultHashPartition() if no custom logic needed
 *
 * Usage Example:
 * @code
 * unsigned long MyPartitioner(char* key, int num_partitions) {
 *     // Custom logic: partition by first letter
 *     return (key[0] - 'a') % num_partitions;
 * }
 * @endcode
 */
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

/* ========================================================================== */
/*                              PUBLIC API FUNCTIONS                          */
/* ========================================================================== */

/**
 * @brief Emit a key-value pair from the map function
 *
 * This function is called by the user's ChunkMapper to output intermediate
 * key-value pairs. It uses thread-local buffering to minimize lock contention,
 * batching up to 50,000 values before flushing to partitions.
 *
 * @param key The key string (will be copied internally)
 * @param value The value string (will be copied internally)
 *
 * @note Thread-safe: Can be called concurrently from multiple mapper threads
 * @note Strings are copied, so caller retains ownership of original buffers
 * @note Automatically flushes buffer when full (50,000 values)
 * @note Must be called from within a ChunkMapper function
 *
 * Performance Characteristics:
 *   - Buffered: Only acquires partition lock once per 2,500 values (on average)
 *   - Lock-free hash computation
 *   - Zero-copy flush to partitions
 *
 * @warning Do not call this function outside of a ChunkMapper context
 */
void MR_Emit(char *key, char *value);

/**
 * @brief Retrieve the filename currently being processed by the mapper
 *
 * Mapper implementations can call this helper to learn which input file the
 * active chunk originated from. The returned pointer refers to the argv entry
 * provided when MR_Run_With_ReaderQueue was invoked and remains valid for the
 * duration of the Map function call.
 *
 * @return Pointer to the current filename string, or empty string if unknown
 */
const char *MR_CurrentFile(void);

/**
 * @brief Register an optional map-side combiner function
 *
 * Applications can install a combiner to shrink mapper output prior to the
 * shuffle/sort phase. Pass NULL to disable combiner logic.
 */
void MR_SetCombiner(Combiner combiner);

/**
 * @brief Default hash-based partitioning function
 *
 * Provides a high-quality hash function (MurmurHash-based) that distributes
 * keys uniformly across partitions. This is suitable for most use cases.
 *
 * @param key The key to partition
 * @param num_partitions Total number of partitions
 * @return Partition index in range [0, num_partitions-1]
 *
 * @note Deterministic: same key always produces same partition
 * @note Uses MurmurHash algorithm for uniform distribution
 * @note Thread-safe and lock-free
 *
 * Usage:
 * @code
 * MR_Run_With_ReaderQueue(argc, argv, MyMap, 3, 10, MyReduce, 20,
 *                         MR_DefaultHashPartition);
 * @endcode
 */
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

/**
 * @brief Execute MapReduce job with parallel reader queue architecture
 *
 * This is the main entry point for running a MapReduce job with advanced
 * features including chunk-level parallel I/O, bounded work queue, and
 * optimized thread coordination.
 *
 * Architecture:
 *   1. Pre-calculate all chunks (1MB each) across all input files
 *   2. Launch reader threads that atomically grab chunks and read files
 *   3. Readers push chunks to bounded work queue (backpressure when full)
 *   4. Mapper workers pop chunks, process them, emit to thread-local buffers
 *   5. After mapping completes, sort all partitions by key
 *   6. Launch reducer threads (one per partition) to process results
 *   7. Cleanup all resources and report performance metrics
 *
 * @param argc Number of command-line arguments (including program name)
 * @param argv Array of argument strings (argv[1..n] are input file paths)
 * @param chunk_map User's map function to process data chunks
 * @param num_readers Number of parallel reader threads (recommended: 1-3)
 * @param num_mappers Number of mapper worker threads (recommended: 8-12)
 * @param reduce User's reduce function to aggregate values per key
 * @param num_reducers Number of reducer threads (= number of partitions)
 * @param partition Partitioner function (use MR_DefaultHashPartition or custom)
 *
 * @note Requires linking with reader_queue.c for work queue implementation
 * @note Output files: output/part-00000.txt through output/part-NNNNN.txt
 * @note Creates output directory if it doesn't exist
 * @note Prints performance statistics (wall time, CPU time) upon completion
 *
 * Performance Tuning:
 *   - Readers: 1-3 optimal (I/O bound, more threads cause contention)
 *   - Mappers: ~1.5x CPU cores (CPU bound, balance utilization)
 *   - Reducers: 10-30 (affects partition granularity and parallelism)
 *
 * Example Usage:
 * @code
 * int main(int argc, char* argv[]) {
 *     MR_Run_With_ReaderQueue(argc, argv,
 *                             MyChunkMap,   // map function
 *                             3,            // 3 reader threads
 *                             10,           // 10 mapper threads
 *                             MyReduce,     // reduce function
 *                             20,           // 20 partitions/reducers
 *                             MR_DefaultHashPartition);
 *     return 0;
 * }
 * @endcode
 *
 * @warning Input files must be readable and accessible
 * @warning Ensure sufficient memory for all threads and data structures
 */
void MR_Run_With_ReaderQueue(int argc, char *argv[], 
                              Mapper map, int num_readers, int num_mappers, 
                              Reducer reduce, int num_reducers, 
                              Partitioner partition);

#endif // __mapreduce_h__