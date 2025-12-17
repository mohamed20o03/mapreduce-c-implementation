/**
 * reduce.c - Reducer Thread Implementation
 *
 * PURPOSE:
 * Reducer threads iterate over sorted keys within their assigned partition
 * and call the user Reduce function. The iterator protocol is implemented
 * via get_next_value(), which advances through the value list for the
 * current key.
 */

#include "mapreduce_internal.h"
#include <stdlib.h>
#include <string.h>

/**
 * get_next_value - Iterator function to get next value for a key
 * @key: Key to get values for
 * @partition_num: Which partition to query
 * Returns: Next value string, or NULL if no more values
 *
 * Called by user's Reduce function to iterate through all values for a key.
 * Maintains iterator state per-partition using global get_states.
 */

char *get_next_value(char *key, int partition_num) {
    get_states_t *state = &get_states[partition_num];
    
    /* Validate current key exists */
    if (state->curr_key == NULL) {
        return NULL;
    }

    /* Ensure we're still on the same key */
    if (strcmp(key, state->curr_key->key_name) != 0) {
        return NULL;
    }

    /* Check if more values exist */
    if (state->next_value == NULL) {
        return NULL;
    }

    /* Return current value and advance iterator */
    char *value = state->next_value->value_name;
    state->next_value = state->next_value->next_value_node;
    return value;
}

/* Initialize iterator state for a key */
static inline void init_state_for_key(int partition_num, key_node_t *key_node) {
    get_states_t *state = &get_states[partition_num];
    state->curr_key = key_node;
    state->next_value = key_node->value_head;
}

/**
 * reducer_thread - Thread function for reducer
 * @arg: Pointer to reducer_args_t
 * Returns: NULL
 *
 * Iterates over keys in bucket 0 (already sorted) and calls the user Reduce
 * function for each key, using get_next_value() to iterate values.
 */
void *reducer_thread(void *arg) {
    reducer_args_t *args = (reducer_args_t *)arg;
    partition_t *part = &partitions[args->partition_number];
    
    /* Iterate through sorted keys in bucket 0 (all keys after sorting) */
    key_node_t *key_node = part->buckets[0].head;
    
    while (key_node) {
        /* Initialize iterator state for this key */
        init_state_for_key(args->partition_number, key_node);
        
        metrics_record_reduce_key(args->partition_number); /* Capture per-partition load */

        /* Call user's reduce function for this key */
        args->reduce_func(key_node->key_name, get_next_value, args->partition_number);
        key_node = key_node->next_key;
    }
    
    free(args);
    return NULL;
}