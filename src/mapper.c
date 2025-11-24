/**
 * mapper.c - Mapper Thread Implementation
 * 
 * Handles mapper worker threads that pop chunks from queue and process them.
 */

#include "mapreduce_internal.h"
#include <stdlib.h>

__thread const char *current_input_filename = NULL;


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
    while(1) {
        work_chunk_t chunk;

        if(!queue_pop(args->queue, &chunk)) {
            break; /* Queue finished and empty - no more work */
        }

        /* Process chunk with user's ChunkMapper function */
        current_input_filename = chunk.filename;
        args->map(chunk.data, chunk.size);
        current_input_filename = NULL;
        metrics_record_mapper_chunk();

        /* Free chunk data (allocated by reader thread) */
        free(chunk.data);
    }
    /* Flush any remaining buffered emissions to partitions */
    flush_buffer_to_partitions();

    /* Cleanup thread-local buffer */
    if (local_buffer) {
        /* Free all allocated buckets */
        for (int i = 0; i < LOCAL_BUFFER_BUCKETS; i++) {
            if (local_buffer->buckets[i]) {
                free(local_buffer->buckets[i]);
            }
        }
        free(local_buffer->buckets);
        free(local_buffer);
        local_buffer = NULL;
    }

    free(args);
    return NULL;
}