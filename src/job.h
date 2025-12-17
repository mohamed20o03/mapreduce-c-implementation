/**
 * job.h - MapReduce Job Configuration API
 * 
 * This file defines a user-friendly API for MapReduce jobs.
 * Users only need to create a job struct, set their Map/Reduce/Combiner
 * functions, and call MR_Run(). All complexity is hidden.
 * 
 * Example Usage:
 * 
 *   int main(int argc, char **argv) {
 *       MR_Job job = MR_DefaultJob();
 *       job.map = MyMap;
 *       job.reduce = MyReduce;
 *       job.combiner = MyCombiner;  // Optional
 *       return MR_Run(&job, argc, argv);
 *   }
 */

#ifndef JOB_H
#define JOB_H

#include "mapreduce.h"

/* ========================================================================== */
/*                            JOB CONFIGURATION                               */
/* ========================================================================== */

/**
 * @struct MR_Job
 * @brief Complete MapReduce job configuration
 * 
 * This struct encapsulates all job settings including user functions,
 * thread counts, and optional configurations. Users typically create this
 * using MR_DefaultJob() and then override specific fields.
 */
typedef struct {
    /* ============ User-Provided Functions (REQUIRED) ============ */
    
    /**
     * @brief Map function that processes input chunks
     * User must implement this to emit key-value pairs
     */
    Mapper map;
    
    /**
     * @brief Reduce function that aggregates values per key
     * User must implement this to produce final output
     */
    Reducer reduce;
    
    /**
     * @brief Optional combiner for map-side pre-aggregation
     * Set to NULL to disable (default: NULL)
     */
    Combiner combiner;
    
    /**
     * @brief Partitioner function to assign keys to reducers
     * Default: MR_DefaultHashPartition (uniform hash distribution)
     */
    Partitioner partitioner;
    
    /* ============ Threading Configuration ============ */
    
    /**
     * @brief Number of reader threads (I/O workers)
     * Default: 1 (recommended: 1-3 for most workloads)
     * Readers fetch file chunks and populate the work queue
     */
    int num_readers;
    
    /**
     * @brief Number of mapper threads (processing workers)
     * Default: 20 (recommended: 1.5x CPU cores)
     * Mappers consume chunks and execute the map function
     */
    int num_mappers;
    
    /**
     * @brief Number of reducer threads (= number of partitions)
     * Default: 40 (recommended: 20-80 for good load balancing)
     * Reducers process sorted partitions and write output files
     */
    int num_reducers;
    
    /* ============ Output Configuration ============ */
    
    /**
     * @brief Output directory for result files
     * Default: "output"
     * Framework will create this directory and write part-XXXXX.txt files
     */
    const char *output_dir;
    
    /**
     * @brief Whether to clear output directory before running
     * Default: 1 (true) - removes old output files
     * Set to 0 to append to existing output
     */
    int clear_output;
    
    /* ============ Metrics & Debugging ============ */
    
    /**
     * @brief Path to metrics report file
     * Default: "metrics_report.txt"
     * Set to NULL to disable metrics collection
     */
    const char *metrics_file;
    
    /**
     * @brief Enable verbose logging
     * Default: 0 (false)
     * Set to 1 to print detailed progress information
     */
    int verbose;
    
    /* ============ CLI Override Flags ============ */
    
    /**
     * @brief Allow command-line arguments to override config
     * Default: 1 (true)
     * Users can pass -i/-m/-r flags to override thread counts
     */
    int allow_cli_override;
    
} MR_Job;

/* ========================================================================== */
/*                              PUBLIC API                                    */
/* ========================================================================== */

/**
 * @brief Create a job with sensible default settings
 * 
 * Returns a pre-configured job with:
 * - 1 reader, 20 mappers, 40 reducers
 * - Default hash partitioner
 * - No combiner (NULL)
 * - Output to "output/" directory
 * - Metrics enabled ("metrics_report.txt")
 * - CLI overrides enabled
 * 
 * Users should override the map and reduce functions:
 * 
 * @code
 * MR_Job job = MR_DefaultJob();
 * job.map = MyMap;
 * job.reduce = MyReduce;
 * @endcode
 * 
 * @return Initialized job struct with default values
 */
MR_Job MR_DefaultJob(void);

/**
 * @brief Run a MapReduce job with the given configuration
 * 
 * This function handles all framework complexity:
 * - Parse command-line arguments (if allow_cli_override enabled)
 * - Initialize metrics and timing instrumentation
 * - Create/clear output directory
 * - Launch reader, mapper, and reducer threads
 * - Track performance and write metrics report
 * - Clean up all resources
 * 
 * The user never needs to worry about:
 * - Thread management
 * - Memory allocation
 * - Metrics collection
 * - Output directory handling
 * - Resource cleanup
 * 
 * @param job Pointer to configured job struct (must have map and reduce set)
 * @param argc Number of command-line arguments (from main)
 * @param argv Command-line argument array (from main)
 * 
 * @return 0 on success, non-zero on error
 * 
 * @note Validates that job.map and job.reduce are not NULL
 * @note Prints usage message if no input files provided
 * @note Supports -i/-m/-r flags to override thread counts (if allow_cli_override)
 * 
 * Example:
 * @code
 * int main(int argc, char **argv) {
 *     MR_Job job = MR_DefaultJob();
 *     job.map = WordCountMap;
 *     job.reduce = WordCountReduce;
 *     return MR_Run(&job, argc, argv);
 * }
 * @endcode
 */
int MR_Run(MR_Job *job, int argc, char **argv);

/**
 * @brief Validate job configuration
 * 
 * Checks that required fields are set and values are sensible.
 * Called automatically by MR_Run(), but can be called manually.
 * 
 * @param job Pointer to job struct to validate
 * @return 0 if valid, non-zero with error message if invalid
 */
int MR_ValidateJob(const MR_Job *job);

#endif /* JOB_H */
