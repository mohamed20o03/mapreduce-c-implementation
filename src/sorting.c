#define _GNU_SOURCE
/**
 * sorting.c - Sorting Operations
 * 
 * Sorts keys within each partition for ordered processing by reducers.
 */

#include "mapreduce_internal.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Helper to convert clock snapshots to nanoseconds */
static int64_t diff_timespec_ns(const struct timespec *start, const struct timespec *end) {
    int64_t sec = (int64_t)(end->tv_sec - start->tv_sec);
    int64_t nsec = (int64_t)(end->tv_nsec - start->tv_nsec);
    return sec * 1000000000LL + nsec;
}

/**
 * compare_keys - Comparison function for qsort
 * @a: Pointer to first keyNode_t pointer
 * @b: Pointer to second keyNode_t pointer
 * Returns: strcmp result
 */
int compare_keys(const void *a, const void *b) {
    key_node_t *key_a = *(key_node_t **)a;
    key_node_t *key_b = *(key_node_t **)b;
    return strcmp(key_a->key_name, key_b->key_name);
}

/**
 * sort_partition - Sort keys in a single partition
 * @partition: Partition to sort
 * 
 * Converts linked lists in hash table to sorted array for efficient traversal.
 */

int sort_partition(partition_t *partition) {
    if (!partition) return 0;

    /* Count total keys across all buckets */
    int total_keys = 0;
    for (int i = 0; i < BUCKETS_PER_PARTITION; i++) {
        key_node_t *curr = partition->buckets[i].head;
        while (curr) {
            total_keys++;
            curr = curr->next_key;
        }
    }

    if (total_keys == 0)
        return 0;

    /* Collect all key pointers into array */
    key_node_t **key_array = malloc(total_keys * sizeof(key_node_t *));
    
    int idx = 0;

    for (int i = 0; i < BUCKETS_PER_PARTITION; i++) {
        key_node_t *node = partition->buckets[i].head;
        while (node) {
            /* Store pointer to node */
            key_array[idx++] = node;
            node = node->next_key;
        }
    }

    /* Sort array of pointers */
    qsort(key_array, total_keys, sizeof(key_node_t *), compare_keys);
    
    /* Rebuild partition as a single sorted linked list in bucket 0 */
    partition->buckets[0].head = key_array[0];
    for (int i = 0; i < total_keys - 1; i++) {
        key_array[i]->next_key = key_array[i + 1];
    }
    key_array[total_keys - 1]->next_key = NULL;

    /* Clear all other buckets (they are now empty) */
    for (int i = 1; i < BUCKETS_PER_PARTITION; i++) {
        partition->buckets[i].head = NULL;
    }
    
    /* Free the pointer array (nodes are still alive) */
    free(key_array);
    return total_keys;
}

/**
 * sort_partition_thread - Thread function to sort a single partition
 * @arg: Pointer to sort_args_t
 * Returns: NULL
 * 
 * Merges all buckets and sorts them globally into one sorted list.
 * No need to sort individual buckets first since we're sorting globally anyway.
 */
void *sort_partition_thread(void *arg) {
    sort_args_t *args = (sort_args_t *)arg;
    partition_t *partition = &partitions[args->partition_num];

    /* Merge all buckets and sort globally in one step */
    struct timespec sort_start, sort_end;
    clock_gettime(CLOCK_MONOTONIC, &sort_start);
    int keys = sort_partition(partition);
    clock_gettime(CLOCK_MONOTONIC, &sort_end);
    metrics_record_sort_stats(args->partition_num, keys, diff_timespec_ns(&sort_start, &sort_end));

    free(args);
    return NULL;
}

/**
 * sort_all_partitions - Sort keys in all partitions
 * 
 * Called after map phase and before reduce phase.
 * Ensures keys are processed in lexicographic order.
 */
void sort_all_partitions(void)
{
    pthread_t *sort_threads = malloc(num_partitions * sizeof(pthread_t));

    /* Create a sorting thread for each partition */
    for (int i = 0; i < num_partitions; i++) {
        sort_args_t *args = malloc(sizeof(sort_args_t));
        args->partition_num = i;
        pthread_create(&sort_threads[i], NULL, sort_partition_thread, args);
    }
    
    /* Wait for all sorting threads to complete */
    for (int i = 0; i < num_partitions; i++) {
        pthread_join(sort_threads[i], NULL);
    }
    
    free(sort_threads);
}