/**
 * @file mapreduce_internal.h
 * @brief Internal structures and constants for MapReduce implementation
 *
 * This header defines the internal data structures, constants, and function
 * declarations used by the MapReduce implementation. These details are NOT
 * exposed to user applications - only implementation files include this header.
 *
 * Architecture Overview:
 *   - Fine-grained locking: 10,000 buckets per partition × 20 partitions = 200,000 locks
 *   - Thread-local buffering: Batch 50,000 values before flushing to partitions
 *   - Zero-copy transfers: Move entire bucket chains during flush operations
 *   - Memory efficiency: Linked lists for values, hash tables for keys
 *
 * Performance Optimizations:
 *   1. Thread-local buffers reduce lock acquisitions by 50,000×
 *   2. Fine-grained bucket locks enable high parallelism
 *   3. Separate chaining handles hash collisions efficiently
 *   4. Direct memory transfer on flush (no copying)
 *
 * @note For public API, see mapreduce.h
 * @note For queue structures, see reader_queue.h
 *
 */

#ifndef __mapreduce_internal_h__
#define __mapreduce_internal_h__

#include "mapreduce.h"
#include "reader_queue.h"
#include "metrics.h"
#include <pthread.h>

/* ========================================================================== */
/*                          CONFIGURATION CONSTANTS                           */
/* ========================================================================== */

/**
 * @def BUCKETS_PER_PARTITION
 * @brief Number of hash buckets per partition for fine-grained locking
 *
 * Each partition has its own hash table with this many buckets. Each bucket
 * has its own mutex lock, enabling high parallelism during concurrent inserts.
 *
 * Trade-offs:
 *   - Larger: Better parallelism, more memory for locks
 *   - Smaller: Less memory, more lock contention
 *
 * With 20 partitions: 10,000 × 20 = 200,000 total buckets/locks
 *
 * @note Must be power-of-2 for optimal hash distribution (though not enforced)
 */
#define BUCKETS_PER_PARTITION 10000

/**
 * @def LOCAL_BUFFER_BUCKETS
 * @brief Number of hash buckets in each thread-local buffer
 *
 * Thread-local buffers use smaller hash tables since they only accumulate
 * values from a single mapper thread. This reduces memory overhead while
 * maintaining fast lookups.
 *
 * @note No locking needed for thread-local buffers (single-threaded access)
 */
#define LOCAL_BUFFER_BUCKETS 4096

/**
 * @def FLUSH_THRESHOLD
 * @brief Maximum number of values to buffer before automatic flush
 *
 * Mapper threads accumulate up to this many key-value pairs in their
 * thread-local buffer before automatically flushing to global partitions.
 *
 * Performance Impact:
 *   - Larger buffers: Fewer flushes, less lock contention, more memory
 *   - Smaller buffers: More flushes, more lock contention, less memory
 *
 * With 50,000 value capacity:
 *   - Average ~2,500 values per partition (20 partitions)
 *   - Single lock acquisition per partition per flush
 *   - 50,000× reduction in lock operations vs. no buffering
 *
 * @note Flush also occurs automatically at end of mapper thread
 */
#define FLUSH_THRESHOLD  50000

/* ========================================================================== */
/*                          INTERNAL DATA STRUCTURES                          */
/* ========================================================================== */

/**
 * @struct keyNode_t
 * @brief Node representing a unique key and all its associated values
 *
 * Each unique key gets one keyNode_t, which maintains a linked list of all
 * values emitted for that key. Multiple keyNode_t structs in the same bucket
 * are chained together to handle hash collisions.
 *
 * Structure:
 *   - key_name: The unique key string
 *   - value_head/tail: Linked list of values (O(1) append)
 *   - next_key: Collision chain (separate chaining for hash table)
 *
 * @note Both value list and collision chain use linked lists
 */
typedef struct key_node {
    char *key_name;                  /**< Heap-allocated key string (unique in bucket) */
    value_node_t *value_head;        /**< First value in list (for iteration) */
    value_node_t *value_tail;        /**< Last value in list (for O(1) append) */
    struct key_node *next_key;       /**< Next key in bucket (collision chain) */
} key_node_t;

/**
 * @struct bucket_t
 * @brief A single hash table bucket with fine-grained locking
 *
 * Each bucket is the head of a linked list of keyNode_t structs (for handling
 * hash collisions via separate chaining). Each bucket has its own mutex lock,
 * enabling fine-grained parallelism.
 *
 * Synchronization:
 *   - lock: Protects the bucket's key list during concurrent inserts
 *   - Multiple threads can safely modify different buckets simultaneously
 *   - Only threads accessing the SAME bucket will contend for the lock
 *
 * Performance:
 *   - 10,000 buckets per partition = very low collision rate
 *   - Fine-grained locks = high parallelism (10 mappers rarely collide)
 *
 * @note With 200,000 total buckets, lock contention is minimal
 */
typedef struct {
    key_node_t *head;       /**< Head of linked list of keys (collision chain) */
    pthread_mutex_t lock;   /**< Protects this bucket during modifications */
} bucket_t;

/**
 * @struct partition_t
 * @brief A partition containing a hash table of keys and values
 *
 * The MapReduce output is divided into multiple partitions based on key hash.
 * Each partition is a complete hash table with HASH_TABLE_SIZE buckets.
 * During reduce phase, each partition is processed by one reducer thread.
 *
 * Layout:
 *   - buckets: Array of HASH_TABLE_SIZE bucket_t structs
 *
 * Partitioning Benefits:
 *   1. Parallel reduce: Each partition processed independently
 *   2. Fine-grained locking: Locks within partition, not global
 *   3. Better cache locality: Reducer works on contiguous data
 *
 * Example with 20 partitions:
 *   - Key "apple" → hash % 20 = partition 7
 *   - Partition 7 → hash table with 10,000 buckets
 *   - Reducer thread 7 processes only partition 7
 *
 * @note Total memory: 20 partitions × 10,000 buckets × sizeof(bucket_t)
 */
typedef struct {
    bucket_t *buckets;      /**< Array of hash buckets (size = num_buckets) */
} partition_t;

/**
 * @struct local_bucket_t
 * @brief A bucket in a thread-local buffer's hash table
 *
 * Thread-local buffers use a similar hash table structure to partitions, but
 * without locks (since only one thread accesses). This allows fast local
 * accumulation before bulk transfer to global partitions.
 *
 * Differences from bucket_t:
 *   - No mutex lock (thread-local, no concurrency)
 *
 * During Flush:
 *   - Entire value chain (value_head) is transferred to partition
 *   - Zero-copy: Just update pointers, no data copying
 *   - Original buffer bucket is reset to NULL
 *
 * @note Used only in thread_buffer_t, not in global partitions
 */
typedef struct local_bucket {
    key_node_t *head;       /**< Head of linked list of keys (collision chain) */
} local_bucket_t;

/**
 * @struct local_buffer_t
 * @brief Thread-local buffer for batching MR_Emit calls before flushing
 *
 * Each mapper thread has its own private buffer (thread-local storage) where
 * it accumulates key-value pairs. When the buffer fills (BUFFER_SIZE values)
 * or the mapper finishes, the buffer is flushed to global partitions.
 *
 * Purpose: Minimize Lock Contention
 *   - Without buffering: Lock partition on every MR_Emit (50,000× locks)
 *   - With buffering: Lock partition once per flush (20× locks)
 *   - Reduction: 2,500× fewer lock operations per mapper thread
 *
 * Implementation:
 *   - buckets: Small hash table (4096 buckets) for fast local lookups
 *   - total_values: Counter to trigger automatic flush at BUFFER_SIZE
 *
 * Flush Operation:
 *   1. Walk through all buffer buckets
 *   2. For each key, find target partition and bucket
 *   3. Acquire partition bucket lock
 *   4. Transfer entire value chain (zero-copy)
 *   5. Release lock, continue
 *   6. Reset buffer to empty state
 *
 * @note One instance per mapper thread (thread-local storage)
 * @see BUFFER_SIZE for flush threshold
 */
typedef struct {
    local_bucket_t **buckets;  /**< Hash table array of bucket heads (size = num_buckets); each bucket is a linked list of buffer_bucket_t nodes */
    int total_values;          /**< Total values across all buckets (for flush trigger) */
} local_buffer_t;

/**
 * @struct sort_args_t
 * @brief Arguments passed to each sorting thread
 *
 * Each sorting thread processes one partition independently. This structure
 * contains the partition number to sort.
 *
 * @param partition_num Index of partition to sort (0-based)
 *
 * @note Each sorting thread gets a unique partition_num (0 to num_partitions-1)
 */
typedef struct {
    int partition_num;         /**< Index of partition to sort (0-based) */
} sort_args_t;


/**
 * @struct mapper_worker_args_t
 * @brief Arguments passed to each mapper worker thread
 *
 * Mapper threads pop chunks from the work queue and process them using the
 * user's Mapper function. This structure contains all the context needed.
 *
 * @param queue Work queue to pop chunks from
 * @param map User's map function to process each chunk
 *
 * @note Shared across all mapper threads (read-only after initialization)
 */
typedef struct {
    work_queue_t *queue;    /**< Shared work queue (thread-safe pop operations) */
    Mapper map;             /**< User's map function (thread-safe if user implemented correctly) */
} mapper_worker_args_t;

/**
 * @struct reducer_args_t
 * @brief Arguments passed to each reducer thread
 *
 * Each reducer processes one partition independently. This structure contains
 * the user's reduce function and the partition number to process.
 *
 * @param reduce_func User's reduce function
 * @param partition_number Which partition this reducer should process
 *
 * @note Each reducer gets a unique partition_number (0 to num_reducers-1)
 */
typedef struct {
    Reducer reduce_func;    /**< User's reduce function (called once per unique key) */
    int partition_number;   /**< Index of partition to process (0-based) */
} reducer_args_t;

/**
 * @struct get_states_t
 * @brief State for iterating through values during reduce phase
 *
 * Each reducer thread maintains iteration state to track progress through
 * the values of the current key. This enables the get_next_value() function
 * to return values sequentially without re-scanning.
 *
 * @param next_value Pointer to next value node to return
 * @param curr_key Pointer to current key node being processed
 *
 * @note One instance per reducer thread (array indexed by partition number)
 */
typedef struct {
    value_node_t *next_value;  /**< Next value to return */
    key_node_t *curr_key;      /**< Current key being processed */
} get_states_t;


/* ========================================================================== */
/*                          GLOBAL STATE VARIABLES                            */
/* ========================================================================== */

/**
 * @var partitions
 * @brief Global array of all partitions
 *
 * This array is allocated once at startup and contains all partitions.
 * Each partition is processed by one reducer thread during reduce phase.
 *
 * @note Allocated by init_partitions(), freed by cleanup_partitions()
 * @note Size: num_partitions elements
 */
extern partition_t *partitions;

/**
 * @var get_states
 * @brief Global array of reducer iteration states
 *
 * This array stores iteration state for each reducer thread. Each element
 * tracks which value should be returned next for the current key being processed.
 *
 * @note Allocated by init_partitions(), freed by cleanup_partitions()
 * @note Size: num_partitions elements (one per reducer thread)
 */
extern get_states_t *get_states;

/**
 * @var num_partitions
 * @brief Total number of partitions (equals number of reducer threads)
 *
 * This value is set at startup and determines:
 *   - Size of partitions array
 *   - Number of reducer threads to create
 *   - Range of values from partitioner function [0, num_partitions-1]
 *
 * @note Typically equals num_reducers parameter to MR_Run_With_ReaderQueue()
 */
extern int num_partitions;

/**
 * @var partitioner_func
 * @brief Function pointer to current partitioner24 items, totalling 805.9 MB
 *
 * Points to the partitioner function being used (either user-provided or
 * MR_DefaultHashPartition). This function determines which partition a key
 * is assigned to.
 *
 * @note Set during MR_Run_With_ReaderQueue() initialization
 * @note Must return values in range [0, num_partitions-1]
 */
extern Partitioner partitioner_func;
extern Combiner combiner_func;

/**
 * @var my_buffer
 * @brief Thread-local pointer to each mapper's private buffer
 *
 * Each mapper thread gets its own thread_buffer_t instance. This is declared
 * with __thread storage class, so each thread has a private copy of the pointer.
 *
 * Thread-Local Storage:
 *   - __thread: Each thread gets its own variable instance
 *   - Initialized to NULL, allocated on first MR_Emit call (lazy init)
 *   - No locking needed (thread-private data)
 *
 * @note Allocated by init_thread_buffer() on first use
 * @note Automatically deallocated when thread exits
 */
extern __thread local_buffer_t *local_buffer;
extern __thread const char *current_input_filename;

/* ========================================================================== */
/*                          FUNCTION DECLARATIONS                             */
/* ========================================================================== */

/*
 * Partition Management (partition.c)
 * ----------------------------------
 */

/**
 * @brief Initialize all partitions with hash tables
 *
 * Allocates the global partitions array and initializes each partition's
 * hash table with HASH_TABLE_SIZE buckets. Each bucket gets its own mutex lock.
 *
 * @param num Number of partitions to create
 *
 * @note Called once at startup before mapper threads are created
 * @note Total buckets created: num × HASH_TABLE_SIZE (e.g., 20 × 10,000 = 200,000)
 * @note Must call cleanup_partitions() to free memory when done
 */
void init_partitions(int num);

/**
 * @brief Hash function for strings (MurmurHash-based)
 *
 * Computes a hash value for a given string. Uses a variant of MurmurHash
 * which provides good distribution and performance.
 *
 * @param str String to hash
 * @return Hash value (unsigned long)
 *
 * @note Thread-safe and lock-free
 * @note Used by MR_DefaultHashPartition()
 */
unsigned long default_hash(char *str);

/*
 * Buffer Management (buffer.c)
 * ----------------------------
 */

/**
 * @brief Initialize thread-local buffer for current mapper thread
 *
 * Allocates a new thread_buffer_t and initializes its hash table with
 * BUFFER_HASH_SIZE buckets. This is called lazily on the first MR_Emit
 * call from each mapper thread.
 *
 * @note Sets my_buffer (thread-local variable) to point to new buffer
 * @note Only called once per mapper thread (lazy initialization)
 * @note Memory freed automatically when thread exits or on explicit flush
 */
void init_thread_buffer(void);

/**
 * @brief Flush thread-local buffer to global partitions
 *
 * Transfers all accumulated key-value pairs from the thread's private buffer
 * to the global partition hash tables. Uses zero-copy technique: moves entire
 * value chains instead of copying individual values.
 *
 * Algorithm:
 *   1. Walk through all buffer buckets
 *   2. For each key, compute target partition and bucket
 *   3. Lock the target partition bucket
 *   4. Find or create keyNode_t for the key
 *   5. Append buffer's value chain to key's value list (zero-copy)
 *   6. Unlock bucket
 *   7. Clear buffer and reset counters
 *
 * @note Called automatically when buffer reaches BUFFER_SIZE values
 * @note Called explicitly at end of mapper thread
 * @note Acquires partition locks (one per unique partition accessed)
 */
void flush_buffer_to_partitions(void);

/*
 * Mapper Operations (mapper.c)
 * ----------------------------
 */

/**
 * @brief Mapper worker thread entry point
 *
 * This function runs in each mapper thread. It continuously pops chunks from
 * the work queue and processes them using the user's ChunkMapper function.
 *
 * Thread Flow:
 *   1. Initialize thread-local buffer (lazy, on first MR_Emit)
 *   2. Loop:
 *      a. Pop chunk from queue (blocks if empty)
 *      b. If NULL returned, queue is finished → break
 *      c. Call user's chunk_map(chunk.data, chunk.size)
 *      d. Free chunk.data memory
 *   3. Flush remaining buffered data
 *   4. Return NULL
 *
 * @param arg Pointer to mapper_worker_args_t structure
 * @return NULL (ignored)
 *
 * @note Multiple instances run concurrently (num_mappers threads)
 * @note Thread-safe: Uses thread-local buffers and queue_pop() synchronization
 */
void *mapper_worker_thread(void *arg);

/*
 * Reducer Operations (reducer.c)
 * ------------------------------
 */

/**
 * @brief Reducer thread entry point
 *
 * This function runs in each reducer thread. It processes one partition by
 * iterating through all unique keys and calling the user's reduce function
 * for each key.
 *
 * Thread Flow:
 *   1. Sort partition (convert hash table to sorted array)
 *   2. For each unique key in sorted order:
 *      a. Call user's reduce(key, get_next_value, partition_number)
 *      b. User calls get_next_value() repeatedly to iterate values
 *   3. Write output to partition-specific file
 *   4. Return NULL
 *
 * @param arg Pointer to reducer_args_t structure
 * @return NULL (ignored)
 *
 * @note Each reducer processes exactly one partition independently
 * @note Output file: output/part-XXXXX.txt where XXXXX is partition number
 */
void *reducer_thread(void *arg);

/**
 * @brief Iterator function to get next value for a key
 *
 * This function is provided to the user's Reducer function as the Getter
 * parameter. It maintains thread-local state to iterate through the values
 * of the current key.
 *
 * Implementation:
 *   - Uses thread-local variables to track iteration state
 *   - First call: Returns first value, advances internal pointer
 *   - Subsequent calls: Returns next value, advances pointer
 *   - When exhausted: Returns NULL
 *
 * @param key The key whose values are being iterated (for verification)
 * @param partition_number Which partition we're querying (for context)
 * @return Next value string, or NULL when no more values
 *
 * @note Thread-safe: Each reducer has its own thread-local iteration state
 * @note Called by user's reduce function in a loop
 * @note Returned strings should NOT be freed by caller
 *
 * Example Usage (in user's reduce function):
 * @code
 * void MyReduce(char* key, Getter get_next, int partition_number) {
 *     char* value;
 *     while ((value = get_next(key, partition_number)) != NULL) {
 *         // Process value
 *     }
 * }
 * @endcode
 */
char *get_next_value(char *key, int partition_number);

/*
 * Sorting Operations (sorting.c)
 * ------------------------------
 */

/**
 * @brief Sort all partitions by key
 *
 * Prepares all partitions for the reduce phase by sorting keys within each
 * partition. MapReduce guarantees that keys are processed in sorted order
 * during reduce.
 *
 * Algorithm (for each partition):
 *   1. Count total keys in hash table
 *   2. Allocate array to hold keyNode_t pointers
 *   3. Walk hash table, collect all keyNode_t into array
 *   4. qsort() array using strcmp comparator
 *   5. Clear original hash table buckets
 *   6. Rebuild as sorted linked list (preserves order)
 *
 * @note Called after all mapper threads complete
 * @note Before reducer threads are created
 * @note Each partition sorted independently (could be parallelized)
 */
void sort_all_partitions(void);

/**
 * @brief Comparator function for qsort (compares key strings)
 *
 * Compares two keyNode_t pointers by their key strings using strcmp.
 * Used by qsort() during partition sorting.
 *
 * @param a Pointer to first keyNode_t* (void* cast)
 * @param b Pointer to second keyNode_t* (void* cast)
 * @return Negative if a < b, 0 if a == b, positive if a > b
 *
 * @note Standard qsort comparator signature
 * @note Uses strcmp for lexicographic ordering
 */
int compare_keys(const void *a, const void *b);

#endif // __mapreduce_internal_h__
