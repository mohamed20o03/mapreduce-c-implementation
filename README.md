# High-Performance MapReduce

A production-ready MapReduce engine in C featuring chunk-level parallel I/O, thread-local buffering, fine-grained locking, a pluggable combiner, and detailed instrumentation. Originally built for the OSTEP coursework, it now serves as a general-purpose playground for high-throughput data processing.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [API Reference](#api-reference)
5. [Performance Benchmarks](#performance-benchmarks)
6. [Project Layout](#project-layout)
7. [Development & Diagnostics](#development--diagnostics)
8. [Requirements & License](#requirements--license)

## Overview

### Highlights

- **Chunk planner + bounded queue** keep disks saturated while bounding RAM usage.
- **Mapper-local buffering** batches tens of thousands of `MR_Emit()` calls per lock acquisition.
- **10K-bucket partitions** keep lock contention under 1% even with 20+ mappers.
- **Optional map-side combiner** collapses duplicate values before the shuffle; disable with `MR_DISABLE_COMBINER=1` for A/B testing.
- **Always-on metrics** (`metrics_report.txt`) expose stage timing, queue waits, flush cost, and lock statistics every run.

### Default workload

The bundled application builds an inverted index: each token maps to the sorted list of files that contain it (for example `search -> [guardian_articles1.txt, guardian_articles3.txt]`). Benchmark numbers referenced in this document come from a private news/article corpus totaling **~708 MB** that lives outside the repository; raw inputs and reducer outputs are intentionally excluded from version control.

## Architecture

### Data flow

1. **Chunk planner** slices every input file into 256 KB regions, recording `{filename, offset, length}` descriptors.
2. **Reader threads** atomically claim the next descriptor, read the slice, and push `(filename, buffer, size)` records into the bounded queue.
3. **Mapper threads** pop chunks, set their thread-local `MR_CurrentFile()` pointer, parse text, and stage `(key, value)` pairs inside private hash tables (4,096 buckets, 50K values).
4. **Combiner (optional)** runs per key just before flushing, letting you dedup or pre-aggregate while the data is still thread-local.
5. **Flush** hashes each key once, locks a single bucket in the destination partition, splices the linked list directly, then unlocks.
6. **Sorters** run `qsort()` across partitions so reducers see a lexicographically ordered stream.
7. **Reducer threads** iterate their partition, pull values via `Getter`, execute user aggregation, and append to thread-local `FILE*` handles (`output/part-%05d.txt`).

```
┌──────────────┐    ┌─────────────────────┐       ┌──────────────────────┐
│ Chunk planner│──► │ Bounded reader queue│──────►│ Mapper threads (TLS) │
└──────────────┘    └─────────────────────┘       └─────────────┬────────┘
                                                                │ flush batches
                                                                ▼
                                                  ┌──────────────────────┐
                                                  │ Partitions (locks +  │
                                                  │ hash buckets)        │
                                                  └─────────────┬────────┘
                                                                │ sorted spans
                                                                ▼
                                                  ┌──────────────────────┐
                                                  │ Reducers (per part)  │
                                                  └──────────────────────┘
```

### Core building blocks

| Component               | Purpose                                                                                             |
| ----------------------- | --------------------------------------------------------------------------------------------------- |
| `reader_queue.{c,h}`    | Bounded circular queue with condition variables plus chunk-descriptor planner.                      |
| `mapper.c`              | Worker loop that pops queue entries, runs the user map function, and triggers flush.                |
| `buffer.c`              | Thread-local hash tables + zero-copy flush into partitions; records flush timing and size.          |
| `partition.c`           | Initializes per-partition hash buckets (10K) and exposes the default hash partitioner.              |
| `sorting.c`, `reduce.c` | Parallel `qsort` and reducer orchestration (one reducer thread per partition).                      |
| `metrics.{c,h}`         | Tracks stage durations, queue waits, flush stats, and lock contention; writes `metrics_report.txt`. |

## Usage

### Build

```bash
make    # optimized build (-O2, pthreads)
```

### Run

```bash
./build/bin/mapreduce -i <readers> -m <mappers> -r <reducers> <your-data-glob>
```

Recommended configuration for the sample dataset:

```bash
./build/bin/mapreduce -i 1 -m 20 -r 40 <your-data-glob>
```

| Option                  | Description                  | Guidance                                                                     |
| ----------------------- | ---------------------------- | ---------------------------------------------------------------------------- |
| `-i N`                  | Reader threads               | 1–3 is enough; extra readers fight over the same disk.                       |
| `-m N`                  | Mapper threads               | Aim for ≥1.5× physical cores for CPU-heavy parsing.                          |
| `-r N`                  | Reducer threads (partitions) | 10–40 typical. More reducers halve per-partition keys but create more files. |
| `MR_DISABLE_COMBINER=1` | Environment variable         | Set before invoking the binary to disable the map-side combiner.             |

Each run emits:

- `output/part-00000.txt` … `output/part-XXXXX.txt` — reducer output files (generated locally, not committed).
- `metrics_report.txt` — performance snapshot (overwritten each run; copy aside to keep history).

### Sequential baseline

```bash
make -C SimpleInvertedIndex
./SimpleInvertedIndex/inverted_index <your-data-glob>
```

The reference program produces `simple_output.txt` and prints its own wall/user/sys timing for comparison.

## API Reference

### Function pointer types

```c
typedef void (*Mapper)(char *data, int size);
typedef void (*Reducer)(char *key, Getter get_next, int partition_number);
typedef char *(*Getter)(char *key, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);
typedef void (*Combiner)(char *key, value_node_t **head);
```

### Runtime helpers

| Helper                                     | Description                                                |
| ------------------------------------------ | ---------------------------------------------------------- |
| `MR_Run_With_ReaderQueue(...)`             | Launches readers, mappers, sorters, reducers, and metrics. |
| `MR_Emit(key, value)`                      | Thread-safe emit with automatic buffering and flushing.    |
| `MR_CurrentFile()`                         | Returns the filename for the chunk currently being mapped. |
| `MR_DefaultHashPartition(key, partitions)` | Murmur-inspired hash with even distribution.               |
| `MR_SetCombiner(fn)`                       | Registers (or disables) the optional combiner.             |

### Example: inverted index

```c
static void Map(char *data, int size) {
    char word[256];
    int len = 0;
    const char *doc = MR_CurrentFile();
    for (int i = 0; i < size; i++) {
        unsigned char c = (unsigned char)data[i];
        if (isalnum(c) && len < (int)sizeof(word) - 1) {
            word[len++] = tolower(c);
        } else if (len > 0) {
            word[len] = '\0';
            MR_Emit(word, (char *)doc);
            len = 0;
        }
    }
    if (len > 0) {
        word[len] = '\0';
        MR_Emit(word, (char *)doc);
    }
}

static void Reduce(char *key, Getter next, int partition) {
    static __thread FILE *out = NULL;
    if (!out) {
        char path[256];
        snprintf(path, sizeof(path), "output/part-%05d.txt", partition);
        out = fopen(path, "a");
        if (!out) return;
    }
    size_t cap = 64, n = 0;
    char **docs = malloc(cap * sizeof(char *));
    char *value;
    while ((value = next(key, partition)) != NULL) {
        if (n == cap) {
            cap *= 2;
            docs = realloc(docs, cap * sizeof(char *));
        }
        docs[n++] = value;
    }
    qsort(docs, n, sizeof(char *), (int (*)(const void *, const void *))strcmp);
    size_t unique = 0;
    for (size_t i = 0; i < n; i++) {
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

static void DedupCombiner(char *key, value_node_t **head) {
    (void)key;
    for (value_node_t *outer = *head; outer; outer = outer->next_value_node) {
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

int main(int argc, char **argv) {
    // parse -i/-m/-r ...
    MR_SetCombiner(getenv("MR_DISABLE_COMBINER") ? NULL : DedupCombiner);
    MR_Run_With_ReaderQueue(argc, argv,
                            Map, readers,
                            mappers, Reduce,
                            reducers, MR_DefaultHashPartition);
}
```

## Performance Benchmarks

Hardware: Dell G15 5520 (i7-12700H, 24 GB RAM, NVMe SSD)
Dataset: seven news/article files stored locally (≈708 MB, ~2,800 × 256 KB chunks)

### Scaling with combiner enabled

| Config (-i/-m/-r) | Wall (s)   | Map stage (ms) | Reduce stage (ms) | Queue push wait (ms) | Queue pop wait (ms) | Notes                                                             |
| ----------------- | ---------- | -------------- | ----------------- | -------------------- | ------------------- | ----------------------------------------------------------------- |
| 1 / 1 / 1         | 185.511    | 173,656.538    | 5,913.487         | 164,139.708          | 0.141               | Single-thread baseline; queue stays full waiting on mapper.       |
| 1 / 5 / 20        | 35.742     | 30,525.088     | 459.719           | 28,692.466           | 6.020               | Reader-bound even with many reducers.                             |
| 1 / 10 / 20       | 24.782     | 18,652.841     | 406.997           | 17,307.105           | 33.573              | Sweet spot; both queue waits shrinking.                           |
| 1 / 20 / 20       | 19.653     | 13,431.797     | 522.148           | 12,119.825           | 455.869             | Mappers saturate CPU; reducers now the bottleneck.                |
| 1 / 20 / 40       | **19.118** | 13,273.416     | **358.090**       | 11,914.567           | 351.799             | More reducers halve per-partition keys → 31% faster reduce stage. |

### Combiner impact (1 / 20 / 40)

| Variant                                    | Wall (s)   | Reduce stage (ms) | Observation                                             |
| ------------------------------------------ | ---------- | ----------------- | ------------------------------------------------------- |
| With combiner (default)                    | **19.118** | **358.090**       | Map-side dedup keeps reducer work low.                  |
| Without combiner (`MR_DISABLE_COMBINER=1`) | 36.782     | 5,054.680         | Reducers redo all dedup work; wall time almost doubles. |

### Sequential baseline

```
./SimpleInvertedIndex/inverted_index <your-data-glob>
# -> 31.684 s wall / 31.230 s user / 0.410 s sys
```

Parallel MapReduce overtakes the sequential job once mapper parallelism ≥10 and reaches ~1.7× speedup at 1/20/40 when the combiner is enabled.

## Metrics Report Format

Every run produces a plain-text `metrics_report*.txt` file (kept locally). The sections always appear in the same order so you can diff reports between runs:

| Section                       | Key fields                             | How to use it                                                                                          |
| ----------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **Job Configuration**         | input count, thread counts, partitions | Confirms the exact CLI knobs tied to this report.                                                      |
| **Overall Timing**            | wall, user, system, total CPU          | Quick check for end-to-end regressions.                                                                |
| **Stage Durations**           | Map / Sort / Reduce milliseconds       | Highlights whether map-side parsing, sorting, or reducers are currently dominant.                      |
| **Reader Statistics**         | chunks read, bytes, avg chunk          | Validates that chunking still matches `CHUNK_SIZE` expectations.                                       |
| **Mapper Statistics**         | chunks mapped, `MR_Emit` calls         | Useful for estimating combiner savings; spikes usually mean more unique tokens.                        |
| **Buffer Flush Statistics**   | flush count, avg batch size/time       | Indicates batching efficiency; if `Avg batch size` shrinks, lower `FLUSH_THRESHOLD` or tune workloads. |
| **Work Queue Statistics**     | queue depth, push/pop waits            | Push waits measure reader bottlenecks; pop waits expose mapper CPU saturation.                         |
| **Partition Lock Statistics** | acquires, contention %, avg wait       | Helps decide whether more partitions or a new hash are needed.                                         |
| **Reducer Statistics**        | total keys, avg per partition          | Provides immediate load balance hints when tuning `-r`.                                                |
| **Top-10 tables**             | per-partition keys / lock wait         | Pinpoints hotspots that deserve targeted experiments.                                                  |

Example excerpt:

```
Partition Lock Statistics:
    Lock acquires    : 21237810
    Contended        : 205420 (0.97%)
    Avg wait time    : 0.049 ms

Top 10 partitions by lock wait:
    Partition  Acquires  Contended  AvgWait(ms)
                 12    572393       5741       0.053
```

Here, contention stays below 1%, but partitions 12/24/30 are slightly hotter—ideal targets for experiments.

## Project Layout

```
├── src/
│   ├── main.c                 # Sample inverted-index app
│   ├── mapreduce.c            # Coordinator (readers, mappers, reducers)
│   ├── mapreduce.h            # Public API
│   ├── mapreduce_internal.h   # Internal structs: buffers, partitions, queue
│   ├── reader_queue.{c,h}     # Bounded queue + chunk planner
│   ├── mapper.c               # Mapper worker threads
│   ├── buffer.c               # Thread-local buffers + flush logic
│   ├── partition.c            # Partition init + hash helpers
│   ├── sorting.c              # Parallel sort per partition
│   ├── reduce.c               # Reducer worker threads
│   └── metrics.{c,h}          # Instrumentation & report writer
├── SimpleInvertedIndex/       # Sequential correctness/perf reference
├── build/                     # Binary + objects (generated)
└── metrics_report*.txt        # Saved metrics for experiments
```

> **Data notice:** Input corpora (~708 MB total) and reducer outputs are large and contain proprietary text, so they are stored outside the repository. Update your glob arguments accordingly when reproducing the benchmarks.

## Future Work

- **SIMD-friendly hash partitioner:** Current metrics show ~0.97% lock contention with 20 mappers/40 reducers. A next optimization pass is to prototype an AVX2/NEON-friendly hashing stage (e.g., parallel Murmur or HighwayHash) so mapper flushes spread keys more evenly with fewer CPU cycles per `MR_Emit`. The same work can also pave the way for SIMD-accelerated token parsing.

## Development & Diagnostics

- **Capture metrics:** copy `metrics_report.txt` after each run (for example `cp metrics_report.txt metrics_report_i1_m20_r40.txt`) to compare stages over time.
- **Interpret queue waits:** long push waits → I/O bound (readers slow). Long pop waits → CPU bound (mappers slow).
- **Lock contention tuning:** stays <1% with defaults; raise `BUCKETS_PER_PARTITION` or lower `FLUSH_THRESHOLD` if it climbs.
- **Debug build:** `make debug` (sanitizers + `-O0 -g`).
- **Valgrind:** `make valgrind ARGS="<your-data-glob>"` for leak/race checks.
- **Clean rebuild:** `make clean && make`.

## Requirements & License

- **Compiler:** GCC ≥ 4.9 or Clang ≥ 3.5 with C11 support.
- **Platform:** Linux or any POSIX-compatible system with pthreads.
- **Runtime footprint:** ~100 MB baseline + ≈1.5× dataset size for buffers.
- **License:** Derived from the OSTEP MapReduce assignment; provided as-is for educational experimentation.
