/**
 * job.c - MapReduce Job Framework Implementation
 * 
 * Implements the high-level MR_Run() API that hides all complexity from users.
 * Handles CLI parsing, metrics, output management, and thread orchestration.
 */

#define _GNU_SOURCE
#include "job.h"
#include "metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/resource.h>

/* ========================================================================== */
/*                          DEFAULT CONFIGURATION                             */
/* ========================================================================== */

MR_Job MR_DefaultJob(void) {
    MR_Job job = {
        /* User functions - must be set by user */
        .map = NULL,
        .reduce = NULL,
        .combiner = NULL,
        .partitioner = MR_DefaultHashPartition,
        
        /* Threading defaults - optimized for typical workload */
        .num_readers = 1,      /* I/O bound - more doesn't help */
        .num_mappers = 20,     /* CPU bound - scale with cores */
        .num_reducers = 40,    /* Balance granularity vs overhead */
        
        /* Output configuration */
        .output_dir = "output",
        .clear_output = 1,     /* Clean slate by default */
        
        /* Metrics and debugging */
        .metrics_file = "metrics_report.txt",
        .verbose = 0,
        
        /* CLI overrides enabled by default */
        .allow_cli_override = 1,
    };
    return job;
}

/* ========================================================================== */
/*                          VALIDATION                                        */
/* ========================================================================== */

int MR_ValidateJob(const MR_Job *job) {
    if (!job) {
        fprintf(stderr, "Error: NULL job pointer\n");
        return -1;
    }
    
    if (!job->map) {
        fprintf(stderr, "Error: Map function is required (job.map = NULL)\n");
        fprintf(stderr, "Example: job.map = MyMap;\n");
        return -1;
    }
    
    if (!job->reduce) {
        fprintf(stderr, "Error: Reduce function is required (job.reduce = NULL)\n");
        fprintf(stderr, "Example: job.reduce = MyReduce;\n");
        return -1;
    }
    
    if (job->num_readers < 1 || job->num_readers > 10) {
        fprintf(stderr, "Error: num_readers must be 1-10 (got %d)\n", job->num_readers);
        return -1;
    }
    
    if (job->num_mappers < 1 || job->num_mappers > 1000) {
        fprintf(stderr, "Error: num_mappers must be 1-1000 (got %d)\n", job->num_mappers);
        return -1;
    }
    
    if (job->num_reducers < 1 || job->num_reducers > 1000) {
        fprintf(stderr, "Error: num_reducers must be 1-1000 (got %d)\n", job->num_reducers);
        return -1;
    }
    
    if (!job->partitioner) {
        fprintf(stderr, "Error: Partitioner function is required\n");
        return -1;
    }
    
    return 0;
}

/* ========================================================================== */
/*                          CLI PARSING                                       */
/* ========================================================================== */

/**
 * Parse command-line flags and override job configuration
 * Supports: -i (readers), -m (mappers), -r (reducers)
 * Returns index of first non-flag argument (input files)
 */
static int parse_cli_args(MR_Job *job, int argc, char **argv) {
    int i = 1;
    while (i < argc && argv[i][0] == '-') {
        if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            job->num_readers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            job->num_mappers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) {
            job->num_reducers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            job->verbose = 1;
        } else {
            fprintf(stderr, "Warning: Unknown flag '%s' (ignored)\n", argv[i]);
        }
        i++;
    }
    return i;
}

/* ========================================================================== */
/*                          OUTPUT DIRECTORY MANAGEMENT                       */
/* ========================================================================== */

/**
 * Setup output directory: create if missing, clear if requested
 */
static int setup_output_directory(const MR_Job *job) {
    struct stat st = {0};
    
    if (stat(job->output_dir, &st) == 0 && S_ISDIR(st.st_mode)) {
        /* Directory exists */
        if (job->clear_output) {
            char cmd[512];
            snprintf(cmd, sizeof(cmd), "rm -rf %s/*", job->output_dir);
            if (system(cmd) != 0) {
                fprintf(stderr, "Warning: Failed to clear output directory\n");
            }
        }
    } else {
        /* Directory doesn't exist - create it */
        if (mkdir(job->output_dir, 0755) != 0) {
            fprintf(stderr, "Error: Failed to create output directory '%s'\n", 
                    job->output_dir);
            return -1;
        }
    }
    
    return 0;
}

/* ========================================================================== */
/*                          MAIN RUN FUNCTION                                 */
/* ========================================================================== */

int MR_Run(MR_Job *job, int argc, char **argv) {
    /* Step 1: Validate job configuration */
    if (MR_ValidateJob(job) != 0) {
        return 1;
    }
    
    /* Step 2: Parse command-line arguments */
    int first_file_idx = 1;
    if (job->allow_cli_override) {
        first_file_idx = parse_cli_args(job, argc, argv);
    }
    
    /* Step 3: Check for input files */
    if (first_file_idx >= argc) {
        fprintf(stderr, "Usage: %s [-i readers] [-m mappers] [-r reducers] <file1> [file2 ...]\n", 
                argv[0]);
        fprintf(stderr, "\nOptions:\n");
        fprintf(stderr, "  -i N    Number of reader threads (default: %d)\n", job->num_readers);
        fprintf(stderr, "  -m N    Number of mapper threads (default: %d)\n", job->num_mappers);
        fprintf(stderr, "  -r N    Number of reducer threads (default: %d)\n", job->num_reducers);
        fprintf(stderr, "  -v      Enable verbose output\n");
        fprintf(stderr, "\nEnvironment:\n");
        fprintf(stderr, "  MR_DISABLE_COMBINER=1    Disable combiner optimization\n");
        return 1;
    }
    
    int num_files = argc - first_file_idx;
    char **files = &argv[first_file_idx];
    
    if (job->verbose) {
        fprintf(stderr, "MapReduce Job Configuration:\n");
        fprintf(stderr, "  Readers:   %d\n", job->num_readers);
        fprintf(stderr, "  Mappers:   %d\n", job->num_mappers);
        fprintf(stderr, "  Reducers:  %d\n", job->num_reducers);
        fprintf(stderr, "  Files:     %d\n", num_files);
        fprintf(stderr, "  Output:    %s/\n", job->output_dir);
        fprintf(stderr, "  Combiner:  %s\n", job->combiner ? "Enabled" : "Disabled");
    }
    
    /* Step 4: Setup output directory */
    if (setup_output_directory(job) != 0) {
        return 1;
    }
    
    /* Step 5: Initialize metrics (if enabled) */
    if (job->metrics_file) {
        metrics_init(job->metrics_file);
        metrics_set_job_configuration(num_files, job->num_readers, 
                                      job->num_mappers, job->num_reducers);
    }
    
    /* Step 6: Setup combiner (respect environment variable override) */
    const char *disable_combiner = getenv("MR_DISABLE_COMBINER");
    if (disable_combiner && strcmp(disable_combiner, "1") == 0) {
        MR_SetCombiner(NULL);
        if (job->verbose) {
            fprintf(stderr, "  Note: Combiner disabled via MR_DISABLE_COMBINER=1\n");
        }
    } else {
        MR_SetCombiner(job->combiner);
    }
    
    /* Step 7: Start timing */
    struct timespec start, end;
    struct rusage usage_start, usage_end;
    getrusage(RUSAGE_SELF, &usage_start);
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    /* Step 8: Build argv for MR_Run_With_ReaderQueue */
    char **mr_argv = malloc((num_files + 1) * sizeof(char *));
    if (!mr_argv) {
        fprintf(stderr, "Error: Memory allocation failed\n");
        return 1;
    }
    mr_argv[0] = argv[0];
    for (int j = 0; j < num_files; j++) {
        mr_argv[j + 1] = files[j];
    }
    
    /* Step 9: RUN THE MAPREDUCE JOB */
    if (job->verbose) {
        fprintf(stderr, "\nStarting MapReduce execution...\n");
    }
    
    MR_Run_With_ReaderQueue(
        num_files + 1,
        mr_argv,
        job->map,
        job->num_readers,
        job->num_mappers,
        job->reduce,
        job->num_reducers,
        job->partitioner
    );
    
    free(mr_argv);
    
    /* Step 10: Calculate elapsed time */
    clock_gettime(CLOCK_MONOTONIC, &end);
    getrusage(RUSAGE_SELF, &usage_end);
    
    double wall_time = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1e9;
    
    double user_cpu = (usage_end.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) +
                      (usage_end.ru_utime.tv_usec - usage_start.ru_utime.tv_usec) / 1e6;
    
    double sys_cpu = (usage_end.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) +
                     (usage_end.ru_stime.tv_usec - usage_start.ru_stime.tv_usec) / 1e6;
    
    /* Step 11: Store timing in metrics */
    if (job->metrics_file) {
        metrics_set_timing(wall_time, user_cpu, sys_cpu);
    }
    
    /* Step 12: Print performance summary */
    fprintf(stderr, "\n===== MapReduce Performance =====\n");
    fprintf(stderr, "Readers     : %d\n", job->num_readers);
    fprintf(stderr, "Mappers     : %d\n", job->num_mappers);
    fprintf(stderr, "Reducers    : %d\n", job->num_reducers);
    fprintf(stderr, "Files       : %d\n", num_files);
    fprintf(stderr, "Wall time   : %.3f sec\n", wall_time);
    fprintf(stderr, "User CPU    : %.3f sec\n", user_cpu);
    fprintf(stderr, "System CPU  : %.3f sec\n", sys_cpu);
    fprintf(stderr, "Total CPU   : %.3f sec\n", user_cpu + sys_cpu);
    fprintf(stderr, "\nOutput: %s/part-*.txt (%d files)\n", 
            job->output_dir, job->num_reducers);
    fprintf(stderr, "====================================\n");
    
    /* Step 13: Write metrics report and cleanup */
    if (job->metrics_file) {
        metrics_write_report();
        metrics_shutdown();
    }
    
    return 0;
}
