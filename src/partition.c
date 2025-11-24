/**
 * partition.c - Partition Management
 * 
 * Handles partition initialization, hash table creation, and hash functions.
 */

#include "mapreduce_internal.h"
#include <stdlib.h>
#include <string.h>

/* Global partition array */
partition_t *partitions = NULL;
get_states_t *get_states = NULL;
int num_partitions = 0;
Partitioner partitioner_func = NULL;

/**
 * default_hash - Default hash function for strings
 * @str: String to hash
 * Returns: Hash value
 * 
 * Uses a simple multiplicative hash algorithm.
 */
unsigned long default_hash (char *str) {
    unsigned long hash = 5381;
    int c;
    while((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c*/
    return hash;
}

/**
 * MR_DefaultHashPartition - Default partitioning function
 * @key: Key to partition
 * @num_partitions: Total number of partitions
 * Returns: Partition number (0 to num_partitions-1)
 */
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    return default_hash(key) % num_partitions;
}


/**
 * init_partitions - Initialize partition data structures
 * @num: Number of partitions to create
 * 
 * Creates partition array and initializes hash table buckets with locks.
 */
void init_partitions(int num) {
    num_partitions = num;
    metrics_set_partition_count(num_partitions); /* Pre-size per-partition stats */
    partitions = malloc(num_partitions * sizeof(partition_t));
    get_states = malloc(num_partitions * sizeof(get_states_t));

    for (int i = 0; i < num_partitions; i++) {
        partitions[i].buckets = malloc(BUCKETS_PER_PARTITION * sizeof(bucket_t));
        
        /* Initialize each bucket */
        for (int j = 0; j < BUCKETS_PER_PARTITION; j++) {
            partitions[i].buckets[j].head = NULL;
            pthread_mutex_init(&partitions[i].buckets[j].lock, NULL);
        }
        
        /* Initialize reducer state */
        get_states[i].curr_key = NULL;
        get_states[i].next_value = NULL;
    }
}