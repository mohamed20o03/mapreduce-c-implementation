#define _GNU_SOURCE
/**
 * reader_queue.c - Reader Queue System for MapReduce I/O Optimization
 * 
 * This module decouples I/O from processing using a producer-consumer pattern:
 * - Multiple reader threads: read chunks in parallel (chunk-level parallelism)
 * - Atomic chunk assignment: multiple readers can read from same file
 * - Mapper threads: pop chunks from queue and process
 * - Bounded queue provides automatic backpressure
 * 
 * Benefits:
 * - True parallel I/O: Multiple readers on same large file
 * - Perfect load balancing: Dynamic chunk assignment prevents idle threads
 * - Maximizes disk throughput: All readers stay busy until completion
 * - Works great with variable-sized files: No early termination
 * - Better scalability: #readers can exceed #files
 */

#include "reader_queue.h"
#include "metrics.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>

static int64_t diff_timespec_ns(const struct timespec *start, const struct timespec *end) {
    int64_t sec = (int64_t)(end->tv_sec - start->tv_sec);
    int64_t nsec = (int64_t)(end->tv_nsec - start->tv_nsec);
    return sec * 1000000000LL + nsec;
}

/* ========================================================================== */
/*                          QUEUE IMPLEMENTATION                              */
/* ========================================================================== */

/**
 * queue_init - Initialize bounded work queue
 * @q: Queue to initialize
 * @capacity: Maximum number of chunks in queue
 * 
 * Allocates circular buffer and initializes synchronization primitives
 */
void queue_init(work_queue_t *q, int capacity) {
    q->chunks = malloc(capacity * sizeof(work_chunk_t));
    q->capacity = capacity;
    q->head = q->tail = q->count = 0;
    q->finished = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

/**
 * queue_push - Push chunk to queue (blocks if full)
 * @q: Work queue
 * @chunk: Chunk to push
 * 
 * Waits if queue is full (backpressure), then adds chunk and signals consumers
 */
void queue_push(work_queue_t *q, work_chunk_t chunk) {
    pthread_mutex_lock(&q->mutex);
    int64_t wait_ns = 0;

    /* Wait while queue is full */
    while(q->count == q->capacity) {
        struct timespec wait_start, wait_end;
        clock_gettime(CLOCK_MONOTONIC, &wait_start);
        pthread_cond_wait(&q->not_full, &q->mutex);
        clock_gettime(CLOCK_MONOTONIC, &wait_end);
        wait_ns += diff_timespec_ns(&wait_start, &wait_end);
    }
    
    /* Add chunk to circular buffer */
    q->chunks[q->tail] = chunk;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    int depth_snapshot = q->count;

    /* Signal waiting consumers */
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);

    /* Record producer perspective: instantaneous depth + wait cost */
    metrics_record_queue_depth(depth_snapshot);
    metrics_record_queue_push_wait(wait_ns);
}

/**
 * queue_pop - Pop chunk from queue (blocks if empty)
 * @q: Work queue
 * @chunk: Pointer to store popped chunk
 * Returns: 1 if chunk retrieved, 0 if queue finished and empty
 * 
 * Waits if queue is empty and not finished, returns 0 when no more work
 */
int queue_pop(work_queue_t *q, work_chunk_t *chunk) {
    pthread_mutex_lock(&q->mutex);
    int64_t wait_ns = 0;

    /* Wait while queue is empty and not finished */
    while (q->count == 0 && !q->finished) {
        struct timespec wait_start, wait_end;
        clock_gettime(CLOCK_MONOTONIC, &wait_start);
        pthread_cond_wait(&q->not_empty, &q->mutex);
        clock_gettime(CLOCK_MONOTONIC, &wait_end);
        wait_ns += diff_timespec_ns(&wait_start, &wait_end);
    }
    
    /* If queue is empty and finished, no more work */
    if (q->count == 0 && q->finished) {
        pthread_mutex_unlock(&q->mutex);
        metrics_record_queue_pop_wait(wait_ns);
        return 0;
    }
    
    /* Pop chunk from circular buffer */
    *chunk = q->chunks[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    int depth_snapshot = q->count;
    
    /* Signal waiting producer */
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    /* Record consumer perspective for queue diagnostics */
    metrics_record_queue_depth(depth_snapshot);
    metrics_record_queue_pop_wait(wait_ns);
    return 1;
}

/**
 * queue_finish - Mark queue as finished (no more pushes)
 * @q: Work queue
 * 
 * Broadcasts to all waiting consumers so they can exit
 */
void queue_finish(work_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->finished = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/**
 * queue_destroy - Clean up queue resources
 * @q: Queue to destroy
 */
void queue_destroy(work_queue_t *q) {
    free(q->chunks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

/* ========================================================================== */
/*                          READER THREAD FUNCTION                            */
/* ========================================================================== */

/**
 * reader_thread - Thread function that reads file chunks and produces work
 * @arg: Pointer to reader_args_t
 * Returns: NULL
 * 
 * CHUNK-LEVEL PARALLELISM:
 * Multiple readers work in parallel, atomically grabbing next CHUNK to process.
 * This allows multiple threads to read different parts of the SAME file,
 * achieving true parallel I/O and perfect load balancing.
 * 
 * Example: File is 20MB, 4 readers, 1MB chunks
 *   Reader1: chunks 0, 4, 8, 12, 16
 *   Reader2: chunks 1, 5, 9, 13, 17
 *   Reader3: chunks 2, 6, 10, 14, 18
 *   Reader4: chunks 3, 7, 11, 15, 19
 * All readers stay busy, no idle time!
 */
void *reader_thread(void *arg) {
    reader_args_t *args = (reader_args_t *)arg;
    int chunks_read = 0; /* Counter for successfully read chunks */

    /* Keep grabbing chunks until all chunks are processed */
    while (1) {
        /* Atomically fetch the next chunk index to process */
        /* This ensures no two threads take the same chunk */
        int chunk_idx = atomic_fetch_add(args->next_chunk_idx, 1);

        /* If all chunks have been assigned, exit the loop */
        if (chunk_idx >= args->total_chunks) {
            break;
        }

        /* Get information about the chunk (file index, offset, size) */
        file_chunk_info_t *chunk_info = &args->chunks[chunk_idx];
        const char *filename = args->filenames[chunk_info->file_idx];

        /* Open the file for reading (each thread opens independently for thread-safety) */
        FILE *fp = fopen(filename, "r");
        if (!fp) {
            fprintf(stderr, "Reader: failed to open %s\n", filename);
            continue; /* Skip this chunk and try next */
        }

        /* Adjust starting offset to avoid beginning inside a word */
        off_t start_offset = chunk_info->offset;
        size_t prefix_skip = 0;
        if (chunk_info->offset > 0) {
            /* Peek at the character just before this chunk */
            if (fseeko(fp, chunk_info->offset - 1, SEEK_SET) == 0) {
                int prev = fgetc(fp);
                if (prev != EOF && isalnum((unsigned char)prev)) {
                    /* We are in the middle of a word; skip ahead until delimiter */
                    if (fseeko(fp, chunk_info->offset, SEEK_SET) == 0) {
                        int c;
                        while ((c = fgetc(fp)) != EOF) {
                            start_offset++;
                            prefix_skip++;
                            if (!isalnum((unsigned char)c)) {
                                break;
                            }
                        }
                    }
                }
            } else {
                fprintf(stderr, "Reader: failed to seek in %s\n", filename);
                fclose(fp);
                continue;
            }
        }

        /* Move the file pointer to the adjusted starting offset */
        if (fseeko(fp, start_offset, SEEK_SET) != 0) {
            fprintf(stderr, "Reader: failed to seek in %s\n", filename);
            fclose(fp);
            continue; /* Skip this chunk if seeking fails */
        }

        size_t request_size = chunk_info->size;
        if (prefix_skip >= request_size) {
            request_size = 0;
        } else {
            request_size -= prefix_skip;
        }

        if (request_size == 0) {
            fclose(fp);
            continue; /* Entire chunk was part of previous word */
        }

        /* Allocate memory for the chunk */
        char *buffer = malloc(request_size);
        if (!buffer) {
            fprintf(stderr, "Reader: malloc failed for chunk %d\n", chunk_idx);
            fclose(fp);
            continue; /* Skip this chunk if memory allocation fails */
        }

        /* Read the chunk from file into buffer */
        size_t bytes_read = fread(buffer, 1, request_size, fp);
        
        /* Handle word boundaries: read extra bytes until we hit a non-alphanumeric character
         * This prevents splitting words across chunks (e.g., "acade" + "my" instead of "academy")
         */
        size_t actual_size = bytes_read;
        if (bytes_read == request_size && bytes_read > 0) {
            /* Only extend if we read a full chunk (not the last chunk) */
            /* Check if last character is alphanumeric */
            if (isalnum((unsigned char)buffer[bytes_read - 1])) {
                int done = 0;
                while (!done) {
                    char extra[256];
                    size_t extra_read = fread(extra, 1, sizeof(extra), fp);
                    if (extra_read == 0) {
                        break;
                    }

                    size_t extend_len = 0;
                    for (; extend_len < extra_read; extend_len++) {
                        if (!isalnum((unsigned char)extra[extend_len])) {
                            extend_len++; /* include delimiter */
                            done = 1;
                            break;
                        }
                    }

                    if (extend_len == 0) {
                        extend_len = extra_read; /* whole block is part of the word */
                    }

                    char *new_buffer = realloc(buffer, actual_size + extend_len);
                    if (!new_buffer) {
                        done = 1;
                        break;
                    }
                    buffer = new_buffer;
                    memcpy(buffer + actual_size, extra, extend_len);
                    actual_size += extend_len;

                    if (done) {
                        break;
                    }
                }
            }
        }
        
        fclose(fp); /* Close the file as soon as reading is done */
        
        /* If nothing was read, free buffer and skip */
        if (actual_size == 0) {
            free(buffer);
            continue;
        }

        /* Wrap the buffer into a work_chunk structure */
        work_chunk_t chunk = {
            .data = buffer,
            .size = actual_size,
            .filename = filename
        };

        /* Push the chunk into the shared work queue (thread-safe) */
        queue_push(args->queue, chunk);
        chunks_read++; /* Increment successfully read chunks */
        metrics_record_chunk_read(actual_size); /* Track reader throughput */
    }

    /* Decrement the number of active readers atomically */
    /* The last reader signals the queue that no more chunks will be produced */
    int remaining = atomic_fetch_sub(args->active_readers, 1);
    if (remaining == 1) {
        queue_finish(args->queue); /* Notify mappers that production is finished */
    }

    return NULL;
}
