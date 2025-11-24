/**
 * reducer.c - Reducer Thread Implementation
 * 
 * Handles reducer threads that process sorted partitions.
 */

#include "mapreduce_internal.h"
#include <stdlib.h>
#include <string.h>

/**
 * get_next_value - Iterator function to get next value for a key
 * @key: Key to get values for
 * @partition_number: Which partition to query
 * Returns: Next value string, or NULL if no more values
 * 
* Called by user's Reduce function to iterate through all values for a key
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

/**
 * reducer_thread - Thread function for reducer
 * @arg: Pointer to reducer_args_t
 * Returns: NULL
 * 
 * Processes one partition by iterating through sorted keys
 * and calling user's reduce function for each key.
 */
void *reducer_thread(void *arg) {
    reducer_args_t *args = (reducer_args_t *)arg;
    partition_t *part = &partitions[args->partition_number];
    
    /* Iterate through sorted keys in bucket 0 (all keys after sorting) */
    key_node_t *key_node = part->buckets[0].head;
    
    while (key_node) {
        /* Initialize get_states for this key */
        get_states_t *state = &get_states[args->partition_number];
        state->curr_key = key_node;
        state->next_value = key_node->value_head;
        
        metrics_record_reduce_key(args->partition_number); /* Capture per-partition load */

        /* Call user's reduce function for this key */
        args->reduce_func(key_node->key_name, get_next_value, args->partition_number);
        key_node = key_node->next_key;
    }
    
    free(args);
    return NULL;
}