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

/**
 * metrics_stage_t - Pipeline phases for coarse timing breakdown.
 *
 * Use with `metrics_stage_begin()`/`metrics_stage_end()` to measure the wall
 * time spent in each phase. Values are indices into internal arrays.
 */
typedef enum {
    METRICS_STAGE_MAP = 0,
    METRICS_STAGE_SORT,
    METRICS_STAGE_REDUCE,
    METRICS_STAGE_COUNT
} metrics_stage_t;

/**
 * metrics_init - Initialize metrics subsystem.
 * @report_path: Optional path for the output report (defaults to metrics_report.txt).
 *
 * Must be called before any other metrics_* function. Safe to call once per job.
 */
void metrics_init(const char *report_path);

/**
 * metrics_set_job_configuration - Record job-level configuration for reporting.
 * @num_files: Number of input files
 * @readers: Reader threads
 * @mappers: Mapper threads
 * @reducers: Reducer threads (and partitions)
 */
void metrics_set_job_configuration(int num_files, int readers, int mappers, int reducers);

/**
 * metrics_set_timing - Record overall job timing.
 * @wall_sec: Wall-clock seconds
 * @user_sec: User CPU seconds
 * @sys_sec: System CPU seconds
 */
void metrics_set_timing(double wall_sec, double user_sec, double sys_sec);

/**
 * metrics_set_partition_count - Allocate per-partition stats arrays.
 * @partitions: Number of partitions (reducers)
 *
 * Call after job configuration is known and before sort/reduce stages.
 */
void metrics_set_partition_count(int partitions);

/** Begin/end coarse phase timing. */
void metrics_stage_begin(metrics_stage_t stage);
void metrics_stage_end(metrics_stage_t stage);

/** Reader: throughput counters (chunks/bytes). */
void metrics_record_chunk_read(size_t bytes);

/** Mapper: progress in processed chunks. */
void metrics_record_mapper_chunk(void);

/** MR_Emit calls: total invocation count. */
void metrics_record_emit(void);

/** Buffer flush: batch size and latency (nanoseconds). */
void metrics_record_flush(int values, int64_t duration_ns);

/** Work queue waits: producer (push) and consumer (pop). */
void metrics_record_queue_push_wait(int64_t wait_ns);
void metrics_record_queue_pop_wait(int64_t wait_ns);

/** Work queue depth sampling: instantaneous size and max. */
void metrics_record_queue_depth(int depth);

/** Sort stats per partition: total keys and duration (ns). */
void metrics_record_sort_stats(int partition, int keys, int64_t duration_ns);

/** Reduce: count keys processed per partition and total. */
void metrics_record_reduce_key(int partition);

/** Partition locks: acquire, contention, and wait time (ns). */
void metrics_record_partition_lock_wait(int partition, int64_t wait_ns, int contended);

/**
 * metrics_write_report - Emit a human-readable plaintext report.
 *
 * Safe to call once after the job completes. Respects the path set by init.
 */
void metrics_write_report(void);

/**
 * metrics_shutdown - Free internal buffers and reset state.
 *
 * Call once per job to release resources. Subsequent metrics calls will no-op
 * until `metrics_init()` is invoked again.
 */
void metrics_shutdown(void);

#endif /* MAPREDUCE_METRICS_H */
