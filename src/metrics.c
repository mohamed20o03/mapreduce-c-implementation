#define _GNU_SOURCE
/**
 * metrics.c - Runtime metrics recorder
 *
 * OVERVIEW:
 * Collects coarse (phase timings) and fine-grained stats (queue waits,
 * buffer flushes, lock contention, per-partition counts) during a MapReduce
 * job. All `metrics_*` APIs are thread-safe and cheap; they use atomics to
 * avoid locks on hot paths. If `metrics_init()` was not called, every API
 * silently no-ops, keeping metrics optional.
 *
 * TYPICAL USAGE FLOW:
 *   1) metrics_init(path)
 *   2) metrics_set_job_configuration(...)
 *   3) metrics_set_partition_count(reducers)
 *   4) metrics_stage_begin(MAP) ... metrics_stage_end(MAP)
 *      metrics_stage_begin(SORT) ... metrics_stage_end(SORT)
 *      metrics_stage_begin(REDUCE) ... metrics_stage_end(REDUCE)
 *   5) metrics_set_timing(wall,user,sys)
 *   6) metrics_write_report(); metrics_shutdown();
 *
 * Thread safety: hot-path recorders use atomics; arrays are protected by
 * `partition_lock`; stage times use `stage_lock`.
 */
#include "metrics.h"

#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef METRICS_PATH_MAX
#define METRICS_PATH_MAX 512
#endif

/**
 * metrics_state_t - Accumulates shared metrics for current job.
 *
 * All counters use atomics so producer threads can record without locks.
 * Mutexes guard arrays that require allocation (per-partition stats).
 */
typedef struct {
    char report_path[METRICS_PATH_MAX];
    int job_files;
    int job_readers;
    int job_mappers;
    int job_reducers;

    double wall_time_sec;
    double user_cpu_sec;
    double sys_cpu_sec;

    struct timespec stage_start[METRICS_STAGE_COUNT];
    double stage_durations_ms[METRICS_STAGE_COUNT];

    /* Hot-path counters (atomics keep recording lock-free) */
    atomic_long reader_chunks;
    atomic_long reader_bytes;
    atomic_long mapper_chunks;
    atomic_long emit_calls;
    atomic_long flush_calls;
    atomic_long flush_values;
    atomic_long flush_time_ns;
    atomic_long reducer_keys;
    atomic_long partition_lock_acquires;
    atomic_long partition_lock_contention;
    atomic_long partition_lock_wait_ns;

    atomic_long queue_push_wait_ns;
    atomic_long queue_pop_wait_ns;
    atomic_long queue_push_wait_events;
    atomic_long queue_pop_wait_events;

    atomic_long queue_depth_total;
    atomic_long queue_depth_samples;
    atomic_int max_queue_depth;

    /* Per-partition stats (allocated once reducers known) */
    int num_partitions;
    double *partition_sort_ms;
    long *partition_sort_keys;
    atomic_long *partition_reduced_keys;
    atomic_long *partition_lock_wait_ns_arr;
    atomic_long *partition_lock_acquires_arr;
    atomic_long *partition_lock_contention_arr;

    pthread_mutex_t stage_lock;
    pthread_mutex_t partition_lock;
    int initialized;
} metrics_state_t;

static metrics_state_t g_metrics;

static double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double sec = (double)(end->tv_sec - start->tv_sec);
    double nsec = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (sec + nsec) * 1000.0;
}

/**
 * metrics_init - Prepare metrics subsystem and target output file.
 * @report_path: Optional custom path for final report (default file.txt).
 */
void metrics_init(const char *report_path) {
    memset(&g_metrics, 0, sizeof(g_metrics));
    pthread_mutex_init(&g_metrics.stage_lock, NULL);
    pthread_mutex_init(&g_metrics.partition_lock, NULL);

    if (report_path && *report_path) {
        strncpy(g_metrics.report_path, report_path, METRICS_PATH_MAX - 1);
        g_metrics.report_path[METRICS_PATH_MAX - 1] = '\0';
    } else {
        strncpy(g_metrics.report_path, "metrics_report.txt", METRICS_PATH_MAX - 1);
        g_metrics.report_path[METRICS_PATH_MAX - 1] = '\0';
    }

    g_metrics.initialized = 1;
}

void metrics_set_job_configuration(int num_files, int readers, int mappers, int reducers) {
    if (!g_metrics.initialized) return;
    g_metrics.job_files = num_files;
    g_metrics.job_readers = readers;
    g_metrics.job_mappers = mappers;
    g_metrics.job_reducers = reducers;
}

/**
 * metrics_set_timing - Record overall job timing (wall, user, sys CPU).
 */
void metrics_set_timing(double wall_sec, double user_sec, double sys_sec) {
    if (!g_metrics.initialized) return;
    g_metrics.wall_time_sec = wall_sec;
    g_metrics.user_cpu_sec = user_sec;
    g_metrics.sys_cpu_sec = sys_sec;
}

/**
 * metrics_set_partition_count - Allocate per-partition buffers when reducers known.
 */
void metrics_set_partition_count(int partitions) {
    if (!g_metrics.initialized) return;

    pthread_mutex_lock(&g_metrics.partition_lock);

    /* Reallocate fresh arrays if called multiple times in tests */
    if (g_metrics.partition_sort_ms) {
        free(g_metrics.partition_sort_ms);
        g_metrics.partition_sort_ms = NULL;
    }
    if (g_metrics.partition_sort_keys) {
        free(g_metrics.partition_sort_keys);
        g_metrics.partition_sort_keys = NULL;
    }
    if (g_metrics.partition_reduced_keys) {
        free(g_metrics.partition_reduced_keys);
        g_metrics.partition_reduced_keys = NULL;
    }
    if (g_metrics.partition_lock_wait_ns_arr) {
        free(g_metrics.partition_lock_wait_ns_arr);
        g_metrics.partition_lock_wait_ns_arr = NULL;
    }
    if (g_metrics.partition_lock_acquires_arr) {
        free(g_metrics.partition_lock_acquires_arr);
        g_metrics.partition_lock_acquires_arr = NULL;
    }
    if (g_metrics.partition_lock_contention_arr) {
        free(g_metrics.partition_lock_contention_arr);
        g_metrics.partition_lock_contention_arr = NULL;
    }

    g_metrics.num_partitions = partitions;
    if (partitions > 0) {
        g_metrics.partition_sort_ms = calloc(partitions, sizeof(double));
        g_metrics.partition_sort_keys = calloc(partitions, sizeof(long));
        g_metrics.partition_reduced_keys = calloc(partitions, sizeof(atomic_long));
        g_metrics.partition_lock_wait_ns_arr = calloc(partitions, sizeof(atomic_long));
        g_metrics.partition_lock_acquires_arr = calloc(partitions, sizeof(atomic_long));
        g_metrics.partition_lock_contention_arr = calloc(partitions, sizeof(atomic_long));
        for (int i = 0; i < partitions; i++) {
            atomic_init(&g_metrics.partition_reduced_keys[i], 0);
            atomic_init(&g_metrics.partition_lock_wait_ns_arr[i], 0);
            atomic_init(&g_metrics.partition_lock_acquires_arr[i], 0);
            atomic_init(&g_metrics.partition_lock_contention_arr[i], 0);
        }
    }

    pthread_mutex_unlock(&g_metrics.partition_lock);
}

/**
 * metrics_stage_begin - Snapshot wall-clock before a pipeline phase.
 */
void metrics_stage_begin(metrics_stage_t stage) {
    if (!g_metrics.initialized || stage >= METRICS_STAGE_COUNT) return;
    pthread_mutex_lock(&g_metrics.stage_lock);
    clock_gettime(CLOCK_MONOTONIC, &g_metrics.stage_start[stage]);
    pthread_mutex_unlock(&g_metrics.stage_lock);
}

/**
 * metrics_stage_end - Accumulate elapsed milliseconds for a phase.
 */
void metrics_stage_end(metrics_stage_t stage) {
    if (!g_metrics.initialized || stage >= METRICS_STAGE_COUNT) return;
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    pthread_mutex_lock(&g_metrics.stage_lock);
    g_metrics.stage_durations_ms[stage] = timespec_diff_ms(&g_metrics.stage_start[stage], &end);
    pthread_mutex_unlock(&g_metrics.stage_lock);
}

/**
 * metrics_record_chunk_read - Count reader throughput in chunks/bytes.
 */
void metrics_record_chunk_read(size_t bytes) {
    if (!g_metrics.initialized) return;
    atomic_fetch_add_explicit(&g_metrics.reader_chunks, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.reader_bytes, (long)bytes, memory_order_relaxed);
}

/**
 * metrics_record_mapper_chunk - Track mapper progress in chunk units.
 */
void metrics_record_mapper_chunk(void) {
    if (!g_metrics.initialized) return;
    atomic_fetch_add_explicit(&g_metrics.mapper_chunks, 1, memory_order_relaxed);
}

/**
 * metrics_record_emit - Increment MR_Emit invocation count.
 */
void metrics_record_emit(void) {
    if (!g_metrics.initialized) return;
    atomic_fetch_add_explicit(&g_metrics.emit_calls, 1, memory_order_relaxed);
}

/**
 * metrics_record_flush - Capture buffer flush batch size and latency.
 */
void metrics_record_flush(int values, int64_t duration_ns) {
    if (!g_metrics.initialized || values <= 0) return;
    atomic_fetch_add_explicit(&g_metrics.flush_calls, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.flush_values, values, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.flush_time_ns, duration_ns, memory_order_relaxed);
}

/**
 * metrics_record_queue_push_wait - Aggregate time producers waited for space.
 */
void metrics_record_queue_push_wait(int64_t wait_ns) {
    if (!g_metrics.initialized || wait_ns <= 0) return;
    atomic_fetch_add_explicit(&g_metrics.queue_push_wait_ns, wait_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.queue_push_wait_events, 1, memory_order_relaxed);
}

/**
 * metrics_record_queue_pop_wait - Aggregate consumer wait time for work.
 */
void metrics_record_queue_pop_wait(int64_t wait_ns) {
    if (!g_metrics.initialized || wait_ns <= 0) return;
    atomic_fetch_add_explicit(&g_metrics.queue_pop_wait_ns, wait_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.queue_pop_wait_events, 1, memory_order_relaxed);
}

/**
 * metrics_record_queue_depth - Sample instantaneous queue occupancy.
 */
void metrics_record_queue_depth(int depth) {
    if (!g_metrics.initialized || depth < 0) return;
    /* Sample average depth and track a max with CAS */
    atomic_fetch_add_explicit(&g_metrics.queue_depth_total, depth, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.queue_depth_samples, 1, memory_order_relaxed);

    int prev = atomic_load_explicit(&g_metrics.max_queue_depth, memory_order_relaxed);
    while (depth > prev &&
           !atomic_compare_exchange_weak_explicit(&g_metrics.max_queue_depth,
                                                  &prev,
                                                  depth,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
        /* prev is updated with latest value by compare_exchange */
    }
}

/**
 * metrics_record_sort_stats - Store per-partition sort counts/timings.
 */
void metrics_record_sort_stats(int partition, int keys, int64_t duration_ns) {
    if (!g_metrics.initialized || partition < 0 || partition >= g_metrics.num_partitions) return;
    pthread_mutex_lock(&g_metrics.partition_lock);
    if (g_metrics.partition_sort_ms && g_metrics.partition_sort_keys) {
        g_metrics.partition_sort_ms[partition] = (double)duration_ns / 1e6;
        g_metrics.partition_sort_keys[partition] = keys;
    }
    pthread_mutex_unlock(&g_metrics.partition_lock);
}

/**
 * metrics_record_reduce_key - Count keys handled by each reducer thread.
 */
void metrics_record_reduce_key(int partition) {
    if (!g_metrics.initialized || partition < 0 || partition >= g_metrics.num_partitions) return;
    atomic_fetch_add_explicit(&g_metrics.reducer_keys, 1, memory_order_relaxed);
    if (g_metrics.partition_reduced_keys) {
        atomic_fetch_add_explicit(&g_metrics.partition_reduced_keys[partition], 1, memory_order_relaxed);
    }
}

/**
 * metrics_record_partition_lock_wait - Track partition bucket lock contention.
 * @partition: Partition index associated with the lock acquisition.
 * @wait_ns: Nanoseconds spent waiting (0 if acquired immediately).
 * @contended: Non-zero when trylock failed and blocking lock was needed.
 */
void metrics_record_partition_lock_wait(int partition, int64_t wait_ns, int contended) {
    if (!g_metrics.initialized || partition < 0 || partition >= g_metrics.num_partitions)
        return;

    atomic_fetch_add_explicit(&g_metrics.partition_lock_acquires, 1, memory_order_relaxed);
    if (g_metrics.partition_lock_acquires_arr) {
        atomic_fetch_add_explicit(&g_metrics.partition_lock_acquires_arr[partition], 1, memory_order_relaxed);
    }

    if (wait_ns > 0) {
        atomic_fetch_add_explicit(&g_metrics.partition_lock_wait_ns, wait_ns, memory_order_relaxed);
    }

    if (contended) {
        atomic_fetch_add_explicit(&g_metrics.partition_lock_contention, 1, memory_order_relaxed);
        if (g_metrics.partition_lock_contention_arr && g_metrics.partition_lock_wait_ns_arr) {
            atomic_fetch_add_explicit(&g_metrics.partition_lock_contention_arr[partition], 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.partition_lock_wait_ns_arr[partition], wait_ns, memory_order_relaxed);
        }
    }
}

static int partition_cmp_desc(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    long ka = atomic_load_explicit(&g_metrics.partition_reduced_keys[ia], memory_order_relaxed);
    long kb = atomic_load_explicit(&g_metrics.partition_reduced_keys[ib], memory_order_relaxed);
    if (ka == kb) return ia - ib;
    return (kb > ka) ? 1 : -1;
}

static int partition_lock_wait_cmp(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    long wa = g_metrics.partition_lock_wait_ns_arr ?
        atomic_load_explicit(&g_metrics.partition_lock_wait_ns_arr[ia], memory_order_relaxed) : 0;
    long wb = g_metrics.partition_lock_wait_ns_arr ?
        atomic_load_explicit(&g_metrics.partition_lock_wait_ns_arr[ib], memory_order_relaxed) : 0;
    if (wa == wb) return ia - ib;
    return (wb > wa) ? 1 : -1;
}

/**
 * write_partition_summary - Highlight heaviest partitions in report footer.
 */
static void write_partition_summary(FILE *fp) {
    /* Highlight top partitions by reduced key count */
    if (!g_metrics.partition_reduced_keys || g_metrics.num_partitions <= 0) return;

    int n = g_metrics.num_partitions;
    int *indices = malloc(sizeof(int) * n);
    if (!indices) return;
    for (int i = 0; i < n; i++) indices[i] = i;

    qsort(indices, n, sizeof(int), partition_cmp_desc);

    int limit = n < 10 ? n : 10;
    fprintf(fp, "Top %d partitions by key count:\n", limit);
    fprintf(fp, "  Partition  Keys  Sort(ms)  KeysDuringSort\n");
    for (int i = 0; i < limit; i++) {
        int idx = indices[i];
        long reduced = atomic_load_explicit(&g_metrics.partition_reduced_keys[idx], memory_order_relaxed);
        double sort_ms = 0.0;
        long sort_keys = 0;
        if (g_metrics.partition_sort_ms && g_metrics.partition_sort_keys) {
            sort_ms = g_metrics.partition_sort_ms[idx];
            sort_keys = g_metrics.partition_sort_keys[idx];
        }
        fprintf(fp, "  %9d  %4ld  %8.3f  %13ld\n", idx, reduced, sort_ms, sort_keys);
    }

    free(indices);
}

static void write_partition_lock_summary(FILE *fp) {
    /* Highlight partitions with the heaviest lock waits */
    if (!g_metrics.partition_lock_wait_ns_arr || g_metrics.num_partitions <= 0) return;

    int n = g_metrics.num_partitions;
    int *indices = malloc(sizeof(int) * n);
    if (!indices) return;
    for (int i = 0; i < n; i++) indices[i] = i;

    qsort(indices, n, sizeof(int), partition_lock_wait_cmp);

    int limit = n < 10 ? n : 10;
    fprintf(fp, "Top %d partitions by lock wait:\n", limit);
    fprintf(fp, "  Partition  Acquires  Contended  AvgWait(ms)\n");
    for (int i = 0; i < limit; i++) {
        int idx = indices[i];
        long acquires = g_metrics.partition_lock_acquires_arr ?
            atomic_load_explicit(&g_metrics.partition_lock_acquires_arr[idx], memory_order_relaxed) : 0;
        long contended = g_metrics.partition_lock_contention_arr ?
            atomic_load_explicit(&g_metrics.partition_lock_contention_arr[idx], memory_order_relaxed) : 0;
        long wait_ns = g_metrics.partition_lock_wait_ns_arr ?
            atomic_load_explicit(&g_metrics.partition_lock_wait_ns_arr[idx], memory_order_relaxed) : 0;
        double avg_wait_ms = (contended > 0) ? ((double)wait_ns / 1e6) / (double)contended : 0.0;
        fprintf(fp, "  %9d  %8ld  %9ld  %10.3f\n", idx, acquires, contended, avg_wait_ms);
    }

    free(indices);
}

/**
 * metrics_write_report - Emit the collected metrics in a readable plaintext format.
 */
void metrics_write_report(void) {
    if (!g_metrics.initialized) return;

    FILE *fp = fopen(g_metrics.report_path, "w");
    if (!fp) {
        fprintf(stderr, "Metrics: failed to open %s for writing\n", g_metrics.report_path);
        return;
    }

    /* Pull consistent snapshots up front (best-effort; relaxed order is fine for reporting) */
    long reader_chunks = atomic_load_explicit(&g_metrics.reader_chunks, memory_order_relaxed);
    long reader_bytes = atomic_load_explicit(&g_metrics.reader_bytes, memory_order_relaxed);
    long mapper_chunks = atomic_load_explicit(&g_metrics.mapper_chunks, memory_order_relaxed);
    long emit_calls = atomic_load_explicit(&g_metrics.emit_calls, memory_order_relaxed);
    long flush_calls = atomic_load_explicit(&g_metrics.flush_calls, memory_order_relaxed);
    long flush_values = atomic_load_explicit(&g_metrics.flush_values, memory_order_relaxed);
    long flush_time_ns = atomic_load_explicit(&g_metrics.flush_time_ns, memory_order_relaxed);
    long reducer_keys = atomic_load_explicit(&g_metrics.reducer_keys, memory_order_relaxed);
    long lock_acquires = atomic_load_explicit(&g_metrics.partition_lock_acquires, memory_order_relaxed);
    long lock_contention = atomic_load_explicit(&g_metrics.partition_lock_contention, memory_order_relaxed);
    long lock_wait_ns = atomic_load_explicit(&g_metrics.partition_lock_wait_ns, memory_order_relaxed);
    long push_wait_ns = atomic_load_explicit(&g_metrics.queue_push_wait_ns, memory_order_relaxed);
    long pop_wait_ns = atomic_load_explicit(&g_metrics.queue_pop_wait_ns, memory_order_relaxed);
    long push_wait_events = atomic_load_explicit(&g_metrics.queue_push_wait_events, memory_order_relaxed);
    long pop_wait_events = atomic_load_explicit(&g_metrics.queue_pop_wait_events, memory_order_relaxed);
    long depth_total = atomic_load_explicit(&g_metrics.queue_depth_total, memory_order_relaxed);
    long depth_samples = atomic_load_explicit(&g_metrics.queue_depth_samples, memory_order_relaxed);
    int max_depth = atomic_load_explicit(&g_metrics.max_queue_depth, memory_order_relaxed);

    double avg_chunk_size = reader_chunks ? (double)reader_bytes / (double)reader_chunks : 0.0;
    double avg_flush_size = flush_calls ? (double)flush_values / (double)flush_calls : 0.0;
    double avg_flush_ms = (flush_calls && flush_time_ns) ? ((double)flush_time_ns / 1e6) / (double)flush_calls : 0.0;
    double avg_depth = depth_samples ? (double)depth_total / (double)depth_samples : 0.0;
    double avg_lock_wait_ms = (lock_contention && lock_wait_ns) ? ((double)lock_wait_ns / 1e6) / (double)lock_contention : 0.0;
    double lock_contention_pct = lock_acquires ? (100.0 * (double)lock_contention / (double)lock_acquires) : 0.0;

    fprintf(fp, "========================================\n");
    fprintf(fp, "  MapReduce Performance Metrics Report\n");
    fprintf(fp, "========================================\n\n");

    fprintf(fp, "Job Configuration:\n");
    fprintf(fp, "  Input files      : %d\n", g_metrics.job_files);
    fprintf(fp, "  Readers          : %d\n", g_metrics.job_readers);
    fprintf(fp, "  Mappers          : %d\n", g_metrics.job_mappers);
    fprintf(fp, "  Reducers         : %d\n", g_metrics.job_reducers);
    fprintf(fp, "  Partitions       : %d\n\n", g_metrics.num_partitions);

    fprintf(fp, "Overall Timing:\n");
    fprintf(fp, "  Wall time        : %.3f sec\n", g_metrics.wall_time_sec);
    fprintf(fp, "  User CPU time    : %.3f sec\n", g_metrics.user_cpu_sec);
    fprintf(fp, "  System CPU time  : %.3f sec\n", g_metrics.sys_cpu_sec);
    fprintf(fp, "  Total CPU time   : %.3f sec\n\n", g_metrics.user_cpu_sec + g_metrics.sys_cpu_sec);

    fprintf(fp, "Stage Durations:\n");
    fprintf(fp, "  Map phase        : %.3f ms\n", g_metrics.stage_durations_ms[METRICS_STAGE_MAP]);
    fprintf(fp, "  Sort phase       : %.3f ms\n", g_metrics.stage_durations_ms[METRICS_STAGE_SORT]);
    fprintf(fp, "  Reduce phase     : %.3f ms\n\n", g_metrics.stage_durations_ms[METRICS_STAGE_REDUCE]);

    fprintf(fp, "Reader Statistics:\n");
    fprintf(fp, "  Chunks read      : %ld\n", reader_chunks);
    fprintf(fp, "  Bytes read       : %.2f MB\n", reader_bytes / (1024.0 * 1024.0));
    fprintf(fp, "  Avg chunk size   : %.1f bytes\n\n", avg_chunk_size);

    fprintf(fp, "Mapper Statistics:\n");
    fprintf(fp, "  Chunks mapped    : %ld\n", mapper_chunks);
    fprintf(fp, "  MR_Emit calls    : %ld\n\n", emit_calls);

    fprintf(fp, "Buffer Flush Statistics:\n");
    fprintf(fp, "  Flush calls      : %ld\n", flush_calls);
    fprintf(fp, "  Total values     : %ld\n", flush_values);
    fprintf(fp, "  Avg batch size   : %.1f values\n", avg_flush_size);
    fprintf(fp, "  Avg flush time   : %.3f ms\n\n", avg_flush_ms);

    fprintf(fp, "Work Queue Statistics:\n");
    fprintf(fp, "  Max queue depth  : %d\n", max_depth);
    fprintf(fp, "  Avg queue depth  : %.2f\n", avg_depth);
    fprintf(fp, "  Push wait events : %ld\n", push_wait_events);
    fprintf(fp, "  Push wait time   : %.3f ms\n", (double)push_wait_ns / 1e6);
    fprintf(fp, "  Pop wait events  : %ld\n", pop_wait_events);
    fprintf(fp, "  Pop wait time    : %.3f ms\n\n", (double)pop_wait_ns / 1e6);

    fprintf(fp, "Partition Lock Statistics:\n");
    fprintf(fp, "  Lock acquires    : %ld\n", lock_acquires);
    fprintf(fp, "  Contended        : %ld (%.2f%%)\n", lock_contention, lock_contention_pct);
    fprintf(fp, "  Avg wait time    : %.3f ms\n\n", avg_lock_wait_ms);

    fprintf(fp, "Reducer Statistics:\n");
    fprintf(fp, "  Total keys       : %ld\n", reducer_keys);
    double avg_keys_partition = g_metrics.num_partitions ?
        (double)reducer_keys / (double)g_metrics.num_partitions : 0.0;
    fprintf(fp, "  Avg keys/part    : %.1f\n\n", avg_keys_partition);

    write_partition_summary(fp);
    fprintf(fp, "\n");
    write_partition_lock_summary(fp);

    fprintf(fp, "\n========================================\n");

    fclose(fp);
    fprintf(stderr, "Metrics written to %s\n", g_metrics.report_path);
}

/**
 * metrics_shutdown - Release dynamic buffers guarding against reuse.
 */
void metrics_shutdown(void) {
    if (!g_metrics.initialized) return;
    pthread_mutex_lock(&g_metrics.partition_lock);
    free(g_metrics.partition_sort_ms);
    free(g_metrics.partition_sort_keys);
    free(g_metrics.partition_reduced_keys);
    free(g_metrics.partition_lock_wait_ns_arr);
    free(g_metrics.partition_lock_acquires_arr);
    free(g_metrics.partition_lock_contention_arr);
    g_metrics.partition_sort_ms = NULL;
    g_metrics.partition_sort_keys = NULL;
    g_metrics.partition_reduced_keys = NULL;
    g_metrics.partition_lock_wait_ns_arr = NULL;
    g_metrics.partition_lock_acquires_arr = NULL;
    g_metrics.partition_lock_contention_arr = NULL;
    pthread_mutex_unlock(&g_metrics.partition_lock);

    pthread_mutex_destroy(&g_metrics.stage_lock);
    pthread_mutex_destroy(&g_metrics.partition_lock);
    g_metrics.initialized = 0;
}
