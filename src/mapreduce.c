/**
 * mapreduce.c - Main MapReduce Coordination
 * 
 * Implements the main MR_Run_With_ReaderQueue function that coordinates
 * the entire MapReduce job: readers, mappers, sorting, and reducers.
 */

#include "mapreduce_internal.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>

Combiner combiner_func = NULL;

void MR_SetCombiner(Combiner combiner) {
    combiner_func = combiner;
}

/**
 * cleanup_partitions - Free all partition data structures
 * 
 * Called after reduce phase completes.
 * Frees all keys, values, and data structures.
 */
static void cleanup_partitions(void) {
    for (int i = 0; i < num_partitions; i++) {
        for (int j = 0; j < BUCKETS_PER_PARTITION; j++) {
            key_node_t *curr_key = partitions[i].buckets[j].head;
            
            while (curr_key) {
                key_node_t *next_key = curr_key->next_key;
                free(curr_key->key_name);
                
                /* Free all values for this key */
                value_node_t *curr_val = curr_key->value_head;
                while (curr_val) {
                    value_node_t *next_val = curr_val->next_value_node;
                    free(curr_val->value_name);
                    free(curr_val);
                    curr_val = next_val;
                }
                
                free(curr_key);
                curr_key = next_key;
            }
            
            pthread_mutex_destroy(&partitions[i].buckets[j].lock);
        }
        
        free(partitions[i].buckets);
    }
    
    free(partitions);
}

const char *MR_CurrentFile(void) {
    return current_input_filename ? current_input_filename : "";
}

/**
 * MR_Run_With_ReaderQueue - Execute MapReduce job with chunk-level parallel reading
 * @argc: Number of arguments (program name + files)
 * @argv: Argument array (argv[0] = program, argv[1..] = input files)
 * @chunk_map: User's chunk map function
 * @num_readers: Number of reader threads for parallel I/O
 * @num_mappers: Number of mapper worker threads to create
 * @reduce: User's reduce function
 * @num_reducers: Number of reducer threads (also number of partitions)
 * @partition: Partitioning function to assign keys to partitions
 * 
 * CHUNK-LEVEL PARALLELISM:
 * Multiple readers can read different chunks of the SAME file simultaneously.
 * This provides true parallel I/O and perfect load balancing.
 */

void MR_Run_With_ReaderQueue(int argc, char *argv[], 
                              Mapper chunk_map, int num_readers, int num_mappers,
                              Reducer reduce, int num_reducers, 
                              Partitioner partition) {
    
    /* Store partitioner function for use in MR_Emit */
    partitioner_func = partition;
    
    /* Initialize partitions (one per reducer) */
    init_partitions(num_reducers);

    /* Time the combined reader + mapper portion */
    metrics_stage_begin(METRICS_STAGE_MAP);

    /* ====================================================================== */
    /* MAP PHASE WITH CHUNK-LEVEL PARALLEL READING                            */
    /* ====================================================================== */
    
    /* Create and initialize work queue */
    work_queue_t queue;
    queue_init(&queue, QUEUE_SIZE);

    /* Pre-calculate all chunks across all files */
    int num_files = argc - 1;
    int total_chunks = 0;
    off_t *file_sizes = malloc(num_files * sizeof(off_t));
    
    /* Get file sizes */
    for (int i = 0; i < num_files; i++) {
        struct stat st;
        if (stat(argv[i + 1], &st) == 0) {
            file_sizes[i] = st.st_size;
            total_chunks += (st.st_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        } else {
            fprintf(stderr, "Warning: cannot stat %s\n", argv[i + 1]);
            file_sizes[i] = 0;
        }
    }

    /* Build chunk array */
    file_chunk_info_t *chunks = malloc(total_chunks * sizeof(file_chunk_info_t));
    int chunk_idx = 0;
    
    for (int file_idx = 0; file_idx < num_files; file_idx++) {
        off_t file_size = file_sizes[file_idx];
        off_t offset = 0;
        
        while (offset < file_size) {
            size_t chunk_size = (file_size - offset > CHUNK_SIZE) ? 
                                CHUNK_SIZE : (file_size - offset);
            
            chunks[chunk_idx].file_idx = file_idx;
            chunks[chunk_idx].offset = offset;
            chunks[chunk_idx].size = chunk_size;
            
            chunk_idx++;
            offset += chunk_size;
        }
    }
    
    free(file_sizes);

    /* Initialize atomic counters for chunk-level coordination */
    atomic_int next_chunk_idx = ATOMIC_VAR_INIT(0);
    atomic_int active_readers = ATOMIC_VAR_INIT(num_readers);
    
    /* Setup reader arguments - one per all readers*/
    reader_args_t reader_args = {
        .filenames = &argv[1],
        .num_files = num_files,
        .queue = &queue,
        .chunks = chunks,
        .total_chunks = total_chunks,
        .next_chunk_idx = &next_chunk_idx,
        .active_readers = &active_readers
    };
    
    /* Start multiple reader threads */
    pthread_t *readers = malloc(num_readers * sizeof(pthread_t));
    for (int i = 0; i < num_readers; i++) {
        pthread_create(&readers[i], NULL, reader_thread, &reader_args);
    }
    
    /* Create mapper worker threads */
    pthread_t *mappers = malloc(num_mappers * sizeof(pthread_t));
    for (int i = 0; i < num_mappers; i++) {
        mapper_worker_args_t *args = malloc(sizeof(mapper_worker_args_t));
        args->queue = &queue;
        args->map = chunk_map;
        pthread_create(&mappers[i], NULL, mapper_worker_thread, args);
    }
    
    /* Wait for all readers to finish */
    for (int i = 0; i < num_readers; i++) {
        pthread_join(readers[i], NULL);
    }
    free(readers);
    
    /* Wait for all mapper workers to complete */
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mappers[i], NULL);
    }
    free(mappers);
    
    /* Cleanup */
    free(chunks);
    queue_destroy(&queue);

    metrics_stage_end(METRICS_STAGE_MAP);
    
    /* ====================================================================== */
    /* SORT PHASE                                                             */
    /* ====================================================================== */
    
    /* Capture partition sort duration independently */
    metrics_stage_begin(METRICS_STAGE_SORT);
    sort_all_partitions();
    metrics_stage_end(METRICS_STAGE_SORT);
    
    /* ====================================================================== */
    /* REDUCE PHASE                                                           */
    /* ====================================================================== */
    
    pthread_t *reducer_threads = malloc(num_reducers * sizeof(pthread_t));

    /* Reducer threads run after sorting; track their wall time too */
    metrics_stage_begin(METRICS_STAGE_REDUCE);
    
    for (int i = 0; i < num_reducers; i++) {
        reducer_args_t *args = malloc(sizeof(reducer_args_t));
        args->reduce_func = reduce;
        args->partition_number = i;
        pthread_create(&reducer_threads[i], NULL, reducer_thread, args);
    }
    
    /* Wait for all reducers to complete */
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }
    free(reducer_threads);

    metrics_stage_end(METRICS_STAGE_REDUCE);
    
    /* ====================================================================== */
    /* CLEANUP                                                                */
    /* ====================================================================== */
    
    cleanup_partitions();
}