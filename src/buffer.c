#define _GNU_SOURCE
/**
 * buffer.c - Thread-Local Buffer Management
 * 
 * Implements thread-local buffering to reduce lock contention during map phase.
 * Each mapper thread buffers emissions locally before flushing to partitions.
 */

#include "mapreduce_internal.h"
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int64_t diff_timespec_ns(const struct timespec *start, const struct timespec *end) {
    int64_t sec = (int64_t)(end->tv_sec - start->tv_sec);
    int64_t nsec = (int64_t)(end->tv_nsec - start->tv_nsec);
    return sec * 1000000000LL + nsec;
}

static value_node_t *recompute_tail(value_node_t *head) {
    if (!head) return NULL;
    value_node_t *curr = head;
    while (curr->next_value_node) {
        curr = curr->next_value_node;
    }
    return curr;
}

/* Thread-local buffer (one per mapper thread) */
__thread local_buffer_t *local_buffer = NULL;

/**
 * init_thread_buffer - Initialize thread-local buffer for current thread
 * 
 * Called once per mapper thread before processing chunks.
 * Allocates hash table for buffering key-value pairs.
 */
void init_thread_buffer(void) {
    local_buffer = malloc(sizeof(local_buffer_t));
    local_buffer->buckets = calloc(LOCAL_BUFFER_BUCKETS ,sizeof(local_bucket_t *));
    local_buffer->total_values = 0;
}

/**
 * find_key_in_list - Search for a key in a linked list
 * @head: Head of the key list
 * @key: Key to search for
 * Returns: Pointer to key node if found, NULL otherwise
 */
key_node_t *find_key_in_list(key_node_t *head, char* key_name) {
    for (key_node_t *curr = head; curr != NULL; curr = curr->next_key) {
        if (strcmp(curr->key_name, key_name) == 0) {
            return curr;
        }
    }
    return NULL;
}

/**
 * create_and_insert_key - Create a new key node and insert at list head
 * @head: Pointer to head of list
 * @key_string: Key string (ownership transferred to new node)
 * Returns: Pointer to newly created key node
 * 
 * Note: Takes ownership of key_string pointer - caller should not free it
 */
key_node_t *create_and_insert_key(key_node_t **head, char *key_name) {
    key_node_t *new_node = malloc(sizeof(key_node_t));
    new_node->key_name = key_name;  /* Ownership transferred */
    new_node->value_head = NULL;
    new_node->value_tail = NULL;

    new_node->next_key = *head;
    *head = new_node;

    return new_node;
}

/**
 * flush_buffer_to_partitions - Flush thread-local buffer to global partitions
 * 
 * Transfers all buffered key-value pairs to appropriate partitions.
 * Called when buffer is full or when mapper finishes.
 * Uses zero-copy transfer (transfers ownership of strings).
 */
void flush_buffer_to_partitions(void) {
    if (!local_buffer || local_buffer->total_values == 0)
        return;

    int values_before = local_buffer->total_values;
    struct timespec flush_start, flush_end;
    clock_gettime(CLOCK_MONOTONIC, &flush_start);

    /* Iterate through all buffer buckets */
    for(int i = 0; i < LOCAL_BUFFER_BUCKETS; i++) {

        if (local_buffer->buckets[i] == NULL)   /* avoid uninitialize buckets */
            continue;

        key_node_t *curr_key_node = local_buffer->buckets[i]->head;

        while(curr_key_node) {
            key_node_t *next = curr_key_node->next_key;

            if (combiner_func && curr_key_node->value_head) {
                combiner_func(curr_key_node->key_name, &curr_key_node->value_head);
                curr_key_node->value_tail = recompute_tail(curr_key_node->value_head);
                if (!curr_key_node->value_head) {
                    free(curr_key_node->key_name);
                    free(curr_key_node);
                    curr_key_node = next;
                    continue;
                }
            }

            /* Determine target partition */
            int partition_num = partitioner_func(curr_key_node->key_name, num_partitions);
            unsigned long bucket_idx = default_hash(curr_key_node->key_name) % BUCKETS_PER_PARTITION;
            bucket_t *target = &partitions[partition_num].buckets[bucket_idx];

            /* Lock target bucket with contention tracking */
            int contended = 0;
            int64_t wait_ns = 0;
            int try_rc = pthread_mutex_trylock(&target->lock);
            if (try_rc != 0) {
                struct timespec wait_start, wait_end;
                clock_gettime(CLOCK_MONOTONIC, &wait_start);
                pthread_mutex_lock(&target->lock);
                clock_gettime(CLOCK_MONOTONIC, &wait_end);
                wait_ns = diff_timespec_ns(&wait_start, &wait_end);
                contended = 1;
            }

            metrics_record_partition_lock_wait(partition_num, wait_ns, contended);

            /* Search for existing key in partition */
            key_node_t *key_node = find_key_in_list(target->head, curr_key_node->key_name);

            /* If key doesn't exist in partition, create it and take ownership */
            if(!key_node) {
                key_node = create_and_insert_key(&target->head, curr_key_node->key_name);
                curr_key_node->key_name = NULL; /* Mark as transferred */
                /* TRANSFER VALUE LIST (BUGFIX): previously values were dropped causing zero counts */
                key_node->value_head = curr_key_node->value_head;
                key_node->value_tail = curr_key_node->value_tail;
            } else {
                /* Transfer buffer's value list to partition o(1) */
                if (key_node->value_head == NULL) {
                    /* Destination is empty - just take source list */
                    key_node->value_head = curr_key_node->value_head;
                    key_node->value_tail = curr_key_node->value_tail;
                } else {
                    /* Append source list to destination - O(1) using tail pointers */
                    key_node->value_tail->next_value_node = curr_key_node->value_head;
                    key_node->value_tail = curr_key_node->value_tail;
                }
                free(curr_key_node->key_name);
            }

            pthread_mutex_unlock(&target->lock);

            /* Move to next key and cleanup */
            free(curr_key_node);
            curr_key_node = next;
        }

        local_buffer->buckets[i]->head = NULL; // dangling pointer
    }
    local_buffer->total_values = 0;

    clock_gettime(CLOCK_MONOTONIC, &flush_end);
    /* Persist flush batch statistics for later tuning */
    metrics_record_flush(values_before, diff_timespec_ns(&flush_start, &flush_end));
}

/**
 * MR_Emit - Emit a key-value pair from mapper
 * @key: Key string
 * @value: Value string
 * 
 * Buffers emission in thread-local buffer. Flushes when buffer is full.
 * This is the main API called by user's map function.
 */

void MR_Emit(char *key, char *value) {
    /* Initialize buffer on first call */
    if(!local_buffer) {
        init_thread_buffer();
    }

    metrics_record_emit(); /* Count logical key/value pairs */

    /* Create value node */
    value_node_t *value_node = malloc(sizeof(value_node_t));
    value_node->value_name = strdup(value);
    value_node->next_value_node = NULL;

    /* Hash key to buffer bucket */
    unsigned long bucket_idx = default_hash(key) % LOCAL_BUFFER_BUCKETS;

    /* Search for existing key in buffer */
    local_bucket_t *curr_bucket = local_buffer->buckets[bucket_idx];

    if(!curr_bucket) {
        curr_bucket = malloc(sizeof(local_bucket_t));
        curr_bucket->head = NULL;
        local_buffer->buckets[bucket_idx] = curr_bucket;
    }

    key_node_t *curr_key_node = find_key_in_list(curr_bucket->head, key);

    if(curr_key_node) {
        /* Key exists in bucket: append value */
        curr_key_node->value_tail->next_value_node = value_node;
        curr_key_node->value_tail = value_node;
    } else {
        /* Key doesn't exist: create new key_node and append it*/
        key_node_t *new_key = malloc(sizeof(key_node_t));
        new_key->key_name = strdup(key);
        new_key->value_head = value_node;
        new_key->value_tail = value_node;
        
        new_key->next_key = curr_bucket->head;
        curr_bucket->head = new_key;
    }

    local_buffer->total_values++;

    /* Flush if buffer is full */
    if (local_buffer->total_values >= FLUSH_THRESHOLD) {
        flush_buffer_to_partitions();
    }
}
