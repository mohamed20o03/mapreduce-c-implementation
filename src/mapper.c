/**
 * mapper.c - Mapper Thread Implementation
 *
 * PURPOSE:
 * Mapper worker threads pop chunks from the reader queue, run the user
 * Map function on each chunk, and buffer emissions locally to reduce
 * contention before flushing to global partitions.
 *
 * THREADING MODEL:
 * - Multiple mapper threads run concurrently
 * - Each thread maintains a thread-local buffer
 * - Buffer is flushed once at the end of the thread (and when threshold hits)
 *
 * KEY STEPS:
 * 1. Pop a chunk from the queue (blocks if empty until finished)
 * 2. Set `current_input_filename` for the Map function
 * 3. Call user Map on the chunk
 * 4. Free chunk data
 * 5. Flush buffer at the end and clean up
 */

#include "mapreduce_internal.h"
#include <stdlib.h>

__thread const char *current_input_filename = NULL;

/* ========================================================================== */
/*                          HELPER FUNCTIONS                                  */
/* ========================================================================== */

/**
 * process_chunk - Run user Map on a single chunk and record metrics
 * @args: Mapper args (contains queue and map function)
 * @chunk: Work chunk to process
 *
 * Sets the thread-local `current_input_filename` for the duration of the map
 * call, then records mapper metrics and frees the chunk buffer.
 */
static inline void process_chunk(mapper_worker_args_t *args, work_chunk_t *chunk) {
    current_input_filename = chunk->filename;
    args->map(chunk->data, chunk->size);
    current_input_filename = NULL;
    metrics_record_mapper_chunk();
    free(chunk->data);
}

/**
 * cleanup_thread_local_buffer - Free mapper-local buffer structures
 *
 * Called after flushing to partitions. Frees bucket arrays and resets
 * the thread-local buffer pointer to NULL.
 */
static void cleanup_thread_local_buffer(void) {
    if (!local_buffer) return;
    for (int i = 0; i < LOCAL_BUFFER_BUCKETS; i++) {
        if (local_buffer->buckets[i]) {
            free(local_buffer->buckets[i]);
        }
    }
    free(local_buffer->buckets);
    free(local_buffer);
    local_buffer = NULL;
}


/**
 * mapper_worker_thread - Thread function for chunk-based mapper
 * @arg: Pointer to mapper_worker_args_t
 * Returns: NULL
 * 
 * Continuously pops chunks from queue and processes them with user's
 * ChunkMapper function. Exits when queue is finished and empty.
 * Flushes any remaining buffered data before exiting.
 */
void *mapper_worker_thread(void *arg) {
    mapper_worker_args_t *args = (mapper_worker_args_t *)arg;

    /* Process chunks until queue is empty and finished */
    while (1) {
        work_chunk_t chunk;
        if (!queue_pop(args->queue, &chunk)) {
            break; /* Queue finished and empty - no more work */
        }
        process_chunk(args, &chunk);
    }

    /* Flush any remaining buffered emissions to partitions */
    flush_buffer_to_partitions();

    /* Cleanup thread-local buffer */
    cleanup_thread_local_buffer();

    free(args);
    return NULL;
}