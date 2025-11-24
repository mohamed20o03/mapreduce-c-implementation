#ifndef MAPREDUCE_METRICS_H
#define MAPREDUCE_METRICS_H

/**
 * @file metrics.h
 * @brief Internal metrics API for timing and throughput instrumentation.
 *
 * The implementation files use this interface to record phase boundaries,
 * queue contention, flush behavior, and reducer skew so the runtime can emit
 * a human-readable report after each job.
 */

#include <stddef.h>
#include <stdint.h>

typedef enum {
    METRICS_STAGE_MAP = 0,
    METRICS_STAGE_SORT,
    METRICS_STAGE_REDUCE,
    METRICS_STAGE_COUNT
} metrics_stage_t;

void metrics_init(const char *report_path);
void metrics_set_job_configuration(int num_files, int readers, int mappers, int reducers);
void metrics_set_timing(double wall_sec, double user_sec, double sys_sec);
void metrics_set_partition_count(int partitions);

void metrics_stage_begin(metrics_stage_t stage);
void metrics_stage_end(metrics_stage_t stage);

void metrics_record_chunk_read(size_t bytes);
void metrics_record_mapper_chunk(void);
void metrics_record_emit(void);
void metrics_record_flush(int values, int64_t duration_ns);
void metrics_record_queue_push_wait(int64_t wait_ns);
void metrics_record_queue_pop_wait(int64_t wait_ns);
void metrics_record_queue_depth(int depth);
void metrics_record_sort_stats(int partition, int keys, int64_t duration_ns);
void metrics_record_reduce_key(int partition);
void metrics_record_partition_lock_wait(int partition, int64_t wait_ns, int contended);

void metrics_write_report(void);
void metrics_shutdown(void);

#endif /* MAPREDUCE_METRICS_H */
