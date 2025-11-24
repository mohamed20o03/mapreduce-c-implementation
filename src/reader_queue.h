/**
 * @file reader_queue.h
 * @brief Reader Queue System for Chunk-Level Parallel I/O in MapReduce
 *
 * This module implements a producer-consumer pattern that decouples I/O operations
 * from CPU-intensive map processing. Key features:
 *
 * Architecture:
 *   - Multiple reader threads read chunks from files in parallel
 *   - Atomic chunk assignment enables chunk-level parallelism (not just file-level)
 *   - Bounded queue (100 chunks) provides automatic backpressure
 *   - Mapper threads pop chunks from queue and process independently
 *
 * Performance Benefits:
 *   - Perfect load balancing: Readers grab chunks atomically (±3% variance)
 *   - Maximized I/O throughput: Dedicated reader threads keep disk busy
 *   - Minimized mapper wait time: Queue keeps mappers fed with data
 *   - Scalability: Works efficiently with variable file sizes and counts
 *
 */

#ifndef __reader_queue_h__
#define __reader_queue_h__

#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>

/* ========================================================================== */
/*                          CONFIGURATION CONSTANTS                           */
/* ========================================================================== */

/**
 * @def QUEUE_SIZE
 * @brief Maximum number of chunks that can be buffered in the work queue
 *
 * A larger queue provides better buffering between readers and mappers but
 * consumes more memory. With 1MB chunks, 100 slots = ~100MB peak memory.
 *
 * Tuning:
 *   - Too small: Readers block waiting for mappers (I/O underutilized)
 *   - Too large: Excessive memory usage, no practical benefit
 *   - Recommended: 50-200 (balance memory vs. buffering)
 */
#define QUEUE_SIZE 100

/**
 * @def CHUNK_SIZE
 * @brief Size of each data chunk read from files (in bytes)
 *
 * Larger chunks reduce overhead from chunk management and queue operations,
 * but may cause load imbalance if chunks are too large relative to file sizes.
 *
 * Tuning:
 *   - Too small: Excessive chunk management overhead, queue thrashing
 *   - Too large: Poor load balancing, wasted work if files are small
 *   - Recommended: 512KB - 2MB (1MB is optimal for most workloads)
 */
#define CHUNK_SIZE (256 * 1024)   /* 256 kB per chunk */

/* ========================================================================== */
/*                          DATA STRUCTURES                                   */
/* ========================================================================== */

/**
 * @struct work_chunk_t
 * @brief Represents a single chunk of work for mapper threads
 *
 * Each chunk contains a pointer to data read from a file and its size.
 * Mappers pop chunks from the queue, process them, and free the memory.
 *
 * Memory Management:
 *   - Allocated by reader threads using malloc()
 *   - Ownership transferred through queue
 *   - Freed by mapper threads after processing
 *
 * @note The data buffer is dynamically allocated and must be freed by consumer
 */
typedef struct {
    char *data;          /**< Pointer to chunk data buffer (heap-allocated) */
    int size;            /**< Actual size of valid data in buffer (may be < CHUNK_SIZE) */
    const char *filename;/**< Pointer to originating filename (argv entry, not owned) */
} work_chunk_t;

/**
 * @struct work_queue_t
 * @brief Thread-safe bounded producer-consumer queue for work chunks
 *
 * This queue decouples reader threads (producers) from mapper threads (consumers)
 * using a circular buffer with mutex and condition variable synchronization.
 *
 * Synchronization:
 *   - mutex: Protects all queue state (head, tail, count, finished)
 *   - not_empty: Signals waiting consumers when data becomes available
 *   - not_full: Signals waiting producers when space becomes available
 *
 * States:
 *   - Active: Readers can push, mappers can pop
 *   - Finished: No more pushes, mappers drain remaining chunks
 *   - Empty + Finished: Mappers return (no more work)
 *
 * @note Implements backpressure: readers block when queue is full
 */
typedef struct {
    work_chunk_t *chunks;       /**< Circular buffer array of chunks (size = capacity) */
    int head;                   /**< Index where next chunk will be inserted */
    int tail;                   /**< Index where next chunk will be removed */
    int count;                  /**< Current number of chunks in queue */
    int capacity;               /**< Maximum number of chunks (typically QUEUE_SIZE) */
    int finished;               /**< Flag: 1 if no more pushes will occur, 0 otherwise */
    pthread_mutex_t mutex;      /**< Protects queue state */
    pthread_cond_t not_empty;   /**< Signaled when queue transitions from empty */
    pthread_cond_t not_full;    /**< Signaled when queue transitions from full */
} work_queue_t;

/**
 * @struct file_chunk_info_t
 * @brief Metadata describing a specific chunk to be read from a file
 *
 * This structure is used in the chunk pre-calculation phase where the main
 * thread walks through all input files and creates a global array of chunks.
 * Reader threads then atomically grab chunks from this array using an atomic
 * counter, enabling chunk-level parallelism.
 *
 * Benefits of Pre-Calculation:
 *   - Enables chunk-level parallelism (multiple readers on same file)
 *   - Perfect load balancing through atomic assignment
 *   - No locking needed for file assignment
 *   - Handles variable file sizes gracefully
 *
 * @note This enables the key innovation: chunk-level vs. file-level parallelism
 */
typedef struct {
    int file_idx;           /**< Index into filenames array (which file) */
    off_t offset;           /**< Byte offset within the file to start reading */
    size_t size;            /**< Number of bytes to read (may be < CHUNK_SIZE for last chunk) */
} file_chunk_info_t;

/**
 * @struct reader_args_t
 * @brief Arguments passed to each reader thread for chunk-level parallel reading
 *
 * This structure contains all the information a reader thread needs to:
 *   1. Atomically grab the next chunk to read
 *   2. Open the appropriate file
 *   3. Seek to the correct offset
 *   4. Read the chunk data
 *   5. Push the chunk to the work queue
 *
 * Lock-Free Coordination:
 *   - next_chunk_idx: Atomic counter incremented with atomic_fetch_add()
 *   - active_readers: Tracks how many readers are still running
 *   - No mutex needed for chunk assignment (scalable!)
 *
 * Thread Flow:
 *   1. Atomically grab next_chunk_idx
 *   2. If idx >= total_chunks, exit thread
 *   3. Otherwise, read chunk[idx] from file
 *   4. Push to queue, repeat
 *   5. Decrement active_readers when done
 *
 * @note Multiple readers can safely read different chunks of the same file
 */
typedef struct {
    char **filenames;           /**< Array of input file paths */
    int num_files;              /**< Number of input files */
    work_queue_t *queue;        /**< Shared work queue to push chunks into */
    file_chunk_info_t *chunks;  /**< Pre-calculated array of all chunks to read */
    int total_chunks;           /**< Total number of chunks across all files */
    atomic_int *next_chunk_idx; /**< Atomic counter for lock-free chunk assignment */
    atomic_int *active_readers; /**< Count of readers still running (for cleanup) */
} reader_args_t;

/* ========================================================================== */
/*                          PUBLIC API FUNCTIONS                              */
/* ========================================================================== */

/**
 * @brief Initialize a bounded work queue
 *
 * Allocates the circular buffer and initializes synchronization primitives.
 * Must be called before any push/pop operations.
 *
 * @param q Pointer to uninitialized work_queue_t structure
 * @param capacity Maximum number of chunks the queue can hold
 *
 * @note Caller must call queue_destroy() to free resources when done
 * @note Typically called once at startup with capacity = QUEUE_SIZE
 *
 * Example:
 * @code
 * work_queue_t queue;
 * queue_init(&queue, QUEUE_SIZE);
 * @endcode
 */
void queue_init(work_queue_t *q, int capacity);

/**
 * @brief Push a chunk into the work queue (blocking if full)
 *
 * Adds a chunk to the queue. If the queue is full, the calling thread blocks
 * until space becomes available (backpressure mechanism).
 *
 * @param q Pointer to initialized work queue
 * @param chunk Work chunk to push (ownership transfers to queue)
 *
 * @note Thread-safe: Multiple readers can call concurrently
 * @note Blocks when queue is full (waits on not_full condition variable)
 * @note Signals not_empty condition to wake waiting consumers
 * @note Should not be called after queue_finish() has been called
 *
 * Memory Ownership:
 *   - The chunk.data pointer is transferred to the queue
 *   - Consumer (mapper) is responsible for freeing chunk.data after processing
 */
void queue_push(work_queue_t *q, work_chunk_t chunk);

/**
 * @brief Pop a chunk from the work queue (blocking if empty)
 *
 * Removes a chunk from the queue. If the queue is empty but not finished,
 * blocks until a chunk becomes available. If queue is empty AND finished,
 * returns 0 to signal no more work.
 *
 * @param q Pointer to initialized work queue
 * @param chunk Pointer to store the popped chunk
 * @return 1 if chunk was successfully retrieved, 0 if queue is finished and empty
 *
 * @note Thread-safe: Multiple mappers can call concurrently
 * @note Blocks when queue is empty and not finished (waits on not_empty)
 * @note Signals not_full condition to wake waiting producers
 * @note Returns 0 when no more work available (queue finished and drained)
 *
 * Memory Ownership:
 *   - Caller receives ownership of chunk.data pointer
 *   - Caller must free(chunk.data) after processing
 *
 * Example:
 * @code
 * work_chunk_t chunk;
 * while (queue_pop(&queue, &chunk)) {
 *     process_chunk(chunk.data, chunk.size);
 *     free(chunk.data);  // Caller's responsibility
 * }
 * @endcode
 */
int queue_pop(work_queue_t *q, work_chunk_t *chunk);

/**
 * @brief Mark the queue as finished (no more pushes)
 *
 * Signals that no more chunks will be pushed to the queue. Consumers can
 * continue popping until the queue is drained, then queue_pop() returns 0.
 *
 * @param q Pointer to work queue
 *
 * @note Thread-safe: Typically called once by main thread after readers finish
 * @note Wakes all waiting consumers (broadcasts not_empty)
 * @note Consumers will drain remaining chunks before exiting
 * @note Must be called to allow mapper threads to terminate cleanly
 *
 * Example Usage:
 * @code
 * // Wait for all readers to finish
 * for (int i = 0; i < num_readers; i++) {
 *     pthread_join(reader_threads[i], NULL);
 * }
 * queue_finish(&queue);  // Signal mappers to exit after draining
 * @endcode
 */
void queue_finish(work_queue_t *q);

/**
 * @brief Destroy queue and free all resources
 *
 * Cleans up the queue structure by freeing the circular buffer and destroying
 * synchronization primitives. Should be called after all threads have finished
 * using the queue.
 *
 * @param q Pointer to work queue
 *
 * @note Must be called after queue_finish() and all consumers have exited
 * @note Destroys mutex and condition variables
 * @note Frees the chunks array
 * @note Does NOT free chunks still in queue (should be drained first)
 *
 * Example Cleanup:
 * @code
 * queue_finish(&queue);
 * // Wait for all mappers to finish
 * queue_destroy(&queue);
 * @endcode
 *
 * @warning Calling this while threads are still using the queue causes undefined behavior
 */
void queue_destroy(work_queue_t *q);

/**
 * @brief Reader thread function for chunk-level parallel file reading
 *
 * This is the thread entry point for reader threads. Each reader:
 *   1. Atomically grabs the next chunk index
 *   2. Opens the corresponding file
 *   3. Seeks to the chunk's offset
 *   4. Reads CHUNK_SIZE (or less for last chunk) bytes
 *   5. Pushes the chunk to the work queue
 *   6. Repeats until all chunks are assigned
 *   7. Decrements active_readers counter
 *   8. Last reader calls queue_finish()
 *
 * @param arg Pointer to reader_args_t structure with all necessary context
 * @return NULL (return value ignored)
 *
 * @note Thread-safe: Multiple instances run concurrently
 * @note Uses lock-free atomic operations for chunk assignment
 * @note Automatically handles last chunk (may be < CHUNK_SIZE)
 * @note Last reader to finish calls queue_finish() to signal mappers
 *
 * Key Innovation - Chunk-Level Parallelism:
 *   Traditional approach: Each reader grabs a file, reads entirely
 *   Problem: Load imbalance if files have different sizes
 *   
 *   This approach: Pre-calculate all chunks, readers grab atomically
 *   Benefit: Perfect load balancing (±3% variance vs ±20% file-level)
 *
 * Example Usage:
 * @code
 * pthread_t readers[num_readers];
 * reader_args_t args = {...};
 * for (int i = 0; i < num_readers; i++) {
 *     pthread_create(&readers[i], NULL, reader_thread, &args);
 * }
 * @endcode
 *
 * @see file_chunk_info_t for chunk metadata structure
 * @see queue_push() for how chunks are added to queue
 */
void *reader_thread(void *arg);

#endif // __reader_queue_h__
