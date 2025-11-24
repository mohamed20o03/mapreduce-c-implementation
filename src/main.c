/**
 * main.c - MapReduce Application Entry Point
 * 
 * Implements a word-count MapReduce application with configurable
 * readers, mappers, and reducers. Demonstrates the MapReduce API.
 * 
 * Features:
 * - Command-line configuration (-i/-m/-r flags)
 * - Performance timing and resource usage tracking
 * - Parallel I/O with chunk-level reader threads
 * - Thread-local buffering for efficient map operations
 */

#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <ctype.h>

#include "mapreduce.h"
#include "metrics.h"

/* Output directory used by reducers when writing files */
const char *output_dir = "output";

/* ----- User-provided Map and Reduce functions (inverted index) ----- */
/* Map: parse chunk into words and emit (word, filename) pairs */
void Map(char *data, int size) {
    if (!data || size <= 0) return;

    /* Make a nul-terminated copy so we can walk safely */
    char *buf = malloc(size + 1);
    if (!buf) return;
    memcpy(buf, data, size);
    buf[size] = '\0';

    char word[256];
    int wlen = 0;
    for (int i = 0; buf[i] != '\0'; i++) {
        unsigned char c = (unsigned char)buf[i];
        if (isalnum(c)) {
            if (wlen < (int)sizeof(word) - 1) {
                word[wlen++] = tolower(c);
            }
        } else {
            if (wlen > 0) {
                word[wlen] = '\0';
                MR_Emit(word, (char *)MR_CurrentFile());
                wlen = 0;
            }
        }
    }
    if (wlen > 0) {
        word[wlen] = '\0';
        MR_Emit(word, (char *)MR_CurrentFile());
    }

    free(buf);
}

/* Combiner: drop duplicate filenames per key inside mapper buffer */
static void DedupCombiner(char *key, value_node_t **head_ref) {
    (void)key;
    if (!head_ref || !*head_ref) return;

    for (value_node_t *outer = *head_ref; outer; outer = outer->next_value_node) {
        value_node_t *prev = outer;
        value_node_t *curr = outer->next_value_node;
        while (curr) {
            if (strcmp(curr->value_name, outer->value_name) == 0) {
                prev->next_value_node = curr->next_value_node;
                free(curr->value_name);
                free(curr);
                curr = prev->next_value_node;
            } else {
                prev = curr;
                curr = curr->next_value_node;
            }
        }
    }
}

static int compare_doc_names(const void *a, const void *b) {
    const char *const *sa = (const char *const *)a;
    const char *const *sb = (const char *const *)b;
    return strcmp(*sa, *sb);
}

/* Reduce: aggregate document names per key and append postings list */
void Reduce(char *key, Getter get_next, int partition_number) {
    /* Use thread-local FILE* so each reducer opens its file once */
    static __thread FILE *out = NULL;
    if (!out) {
        char path[256];
        snprintf(path, sizeof(path), "%s/part-%05d.txt", output_dir, partition_number);
        out = fopen(path, "a");
        if (!out) return; /* best-effort: skip on failure */
    }

    size_t capacity = 64;
    size_t count = 0;
    char **docs = malloc(capacity * sizeof(char *));
    if (!docs) return;

    char *val;
    while ((val = get_next(key, partition_number)) != NULL) {
        if (count == capacity) {
            capacity *= 2;
            char **tmp = realloc(docs, capacity * sizeof(char *));
            if (!tmp) {
                free(docs);
                return;
            }
            docs = tmp;
        }
        docs[count++] = val;
    }

    if (count == 0) {
        free(docs);
        return;
    }

    qsort(docs, count, sizeof(char *), compare_doc_names);

    size_t unique = 0;
    for (size_t i = 0; i < count; i++) {
        if (unique == 0 || strcmp(docs[i], docs[unique - 1]) != 0) {
            docs[unique++] = docs[i];
        }
    }

    fprintf(out, "%s -> [", key);
    for (size_t i = 0; i < unique; i++) {
        fprintf(out, "%s%s", docs[i], (i + 1 < unique) ? ", " : "");
    }
    fprintf(out, "]\n");
    fflush(out);
    free(docs);
}

int main(int argc, char *argv[]) {
    int num_readers = 3, num_mappers = 5, num_reducers = 5;

    /* Parse command-line flags */
    int i = 1;
    while (i < argc && argv[i][0] == '-') {
        if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) 
            num_readers = atoi(argv[++i]);
        else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
            num_mappers = atoi(argv[++i]);
        else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc)
            num_reducers = atoi(argv[++i]);
        i++;
    }

    /* If no input files are provided, print usage error and exit */
    if (i >= argc) {
        fprintf(stderr, "Usage: %s [-i num_readers] [-m num_mappers] [-r num_reducers] <file1> [file2 ...]\n", argv[0]);
        return 1;
    }

    int num_files = argc - i;
    char **files = &argv[i];

    struct stat st = {0};

    /* Check if output directory exists; clear it if so, create otherwise */
    if (stat("output", &st) == 0 && S_ISDIR(st.st_mode)) {
        /* Directory exists - clear previous output files */
        if (system("rm -rf output/*") != 0) {
            fprintf(stderr, "Warning: failed to clear output directory\n");
        }
    } else {
        /* Directory does not exist - create it */
        mkdir("output", 0755);
    }

    /* Initialize metrics collection so each run emits metrics_report.txt diagnostics */
    metrics_init("metrics_report.txt");
    metrics_set_job_configuration(num_files, num_readers, num_mappers, num_reducers);

    /* Start timing and resource measurement */
    struct timespec start, end;
    struct rusage usage_start, usage_end;

    /* Record starting resource usage (CPU time, memory, I/O, etc.) */
    getrusage(RUSAGE_SELF, &usage_start);

    /* Record starting wall-clock time using monotonic clock */
    clock_gettime(CLOCK_MONOTONIC, &start);

    /* Build argv for MR_Run_With_ReaderQueue */
    char **mr_argv = malloc((num_files + 1) * sizeof(char *));
    mr_argv[0] = argv[0];
    for (int j = 0; j < num_files; j++) 
        mr_argv[j+1] = files[j];

     /* Run MapReduce with multi-reader queue pattern */
    const char *disable_combiner = getenv("MR_DISABLE_COMBINER");
    if (!disable_combiner || strcmp(disable_combiner, "1") != 0) {
        MR_SetCombiner(DedupCombiner);
    } else {
        MR_SetCombiner(NULL);
    }
    MR_Run_With_ReaderQueue(num_files + 1, mr_argv, Map, num_readers, num_mappers, Reduce, num_reducers, MR_DefaultHashPartition);

    free(mr_argv);

    /* Calculate elapsed time and resource usage */
    clock_gettime(CLOCK_MONOTONIC, &end);
    getrusage(RUSAGE_SELF, &usage_end);

    double wall_time = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1e9;

    double user_cpu = (usage_end.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) +
                      (usage_end.ru_utime.tv_usec - usage_start.ru_utime.tv_usec) / 1e6;

    double sys_cpu = (usage_end.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) +
                     (usage_end.ru_stime.tv_usec - usage_start.ru_stime.tv_usec) / 1e6;

    /* Store timing in metrics system before writing */
    metrics_set_timing(wall_time, user_cpu, sys_cpu);

    /* Print performance metrics */
    fprintf(stderr, "\n===== MapReduce Performance =====\n");
    fprintf(stderr, "Readers     : %d\n", num_readers);
    fprintf(stderr, "Mappers     : %d\n", num_mappers);
    fprintf(stderr, "Reducers    : %d\n", num_reducers);
    fprintf(stderr, "Files       : %d\n", num_files);
    fprintf(stderr, "Wall time   : %.3f sec\n", wall_time);
    fprintf(stderr, "User CPU    : %.3f sec\n", user_cpu);
    fprintf(stderr, "System CPU  : %.3f sec\n", sys_cpu);
    fprintf(stderr, "Total CPU   : %.3f sec\n", user_cpu + sys_cpu);
    fprintf(stderr, "\nOutput: %s/part-*.txt (%d files)\n", output_dir, num_reducers);
    fprintf(stderr, "====================================\n");

    /* Emit the instrumentation summary after console stats */
    metrics_write_report();
    metrics_shutdown();
    return 0;
}