<div align="center">

# High-Performance MapReduce Implementation in C

**A production-grade, multi-threaded MapReduce engine featuring intelligent I/O scheduling, lock-free algorithms, and comprehensive performance instrumentation**

[![Language](https://img.shields.io/badge/language-C11-blue.svg?style=flat-square)](<https://en.wikipedia.org/wiki/C_(programming_language)>)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20POSIX-lightgrey.svg?style=flat-square)](https://www.linux.org/)
[![License](https://img.shields.io/badge/license-Educational-green.svg?style=flat-square)](#contributing)
[![Threads](https://img.shields.io/badge/threading-pthreads-orange.svg?style=flat-square)](https://en.wikipedia.org/wiki/POSIX_Threads)

---

</div>

## Highlights

| Feature | Benefit |
|---------|---------|
| **Chunk-level parallel I/O** | Lock-free reader scheduling keeps disks and CPUs saturated |
| **Mapper-local buffering** | Batches 50K+ emits before touching shared memory |
| **Fine-grained locks** | 10K buckets per partition → <1% contention with 20+ mappers |
| **Always-on metrics** | Every run reveals queue pressure, flush efficiency, hotspots |
| **Optional combiner** | Map-side dedup cuts shuffle traffic by 83% |

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Code Structure](#code-structure)
- [Optimization Techniques](#optimization-techniques)
- [API Reference](#api-reference)
- [Quick Start](#quick-start)
- [Performance Analysis](#performance-analysis)
- [Metrics Guide](#metrics-guide)
- [Contributing](#contributing)

---

## Pipeline Overview

```mermaid
graph LR
    A[Input Files] --> B[Chunk Planner]
    B --> C[Bounded Queue]
    C --> D[Mapper Threads]
    D --> E[Thread-Local Buffers]
    E --> F[Flush to Partitions]
    F --> G[Parallel Sort]
    G --> H[Reducer Threads]
    H --> I[Output Files]
```

### Stage-by-Stage Execution

<table>
<thead>
<tr>
<th>Stage</th>
<th>What Happens</th>
<th>Key Technique</th>
<th>Why It Matters</th>
</tr>
</thead>
<tbody>
<tr>
<td><b>1. Plan</b></td>
<td>Slice input files into 256 KB chunks with <code>{filename, offset, length}</code> metadata. No actual I/O yet—just build a work manifest.</td>
<td>Static planning eliminates runtime decisions</td>
<td>Readers can atomically claim chunks without coordination overhead. Chunk size balances disk I/O efficiency vs. load distribution.</td>
</tr>
<tr>
<td><b>2. Read</b></td>
<td>Reader threads loop: atomically claim next chunk index, <code>fseek + fread</code> into buffer, push to bounded queue (blocking if full).</td>
<td>Lock-free indexing + bounded queue backpressure</td>
<td>No lock contention between readers. Queue depth limits RAM usage and signals when mappers are bottlenecked.</td>
</tr>
<tr>
<td><b>3. Map</b></td>
<td>Mappers pop chunks from queue, call user's <code>Map()</code> function. Each <code>MR_Emit(key, value)</code> inserts into thread-local 4K-bucket hash table.</td>
<td>Zero shared-memory writes during emit</td>
<td>No synchronization until flush. Mapper threads never block each other—pure CPU parallelism.</td>
</tr>
<tr>
<td><b>4. Combine</b></td>
<td>When buffer threshold (50K pairs) is hit, run optional <code>Combiner()</code> on each bucket's linked list to dedup/aggregate values <i>before</i> flush.</td>
<td>Map-side aggregation while data is thread-local</td>
<td>Reduces shuffle traffic by ~83% for dedup workloads. Combiner sees thread-local data—no locks needed.</td>
</tr>
<tr>
<td><b>5. Flush</b></td>
<td>For each key in buffer, hash to partition and bucket (10K buckets per partition), lock <i>single</i> bucket, splice thread-local list into global partition.</td>
<td>Fine-grained locking (10K buckets) + hash-once design</td>
<td>Lock contention <1% with 20+ mappers. Each flush touches only ~4K distinct buckets, so collision probability is low.</td>
</tr>
<tr>
<td><b>6. Sort</b></td>
<td>Spawn one thread per partition. Each thread walks its 10K buckets, flattens into array, <code>qsort()</code> by key, rebuilds sorted linked lists.</td>
<td>Parallel sorting across partitions</td>
<td>No cross-partition dependencies—perfect parallelism. Sorting is CPU-bound, so benefits from multicore.</td>
</tr>
<tr>
<td><b>7. Reduce</b></td>
<td>Reducer threads walk sorted keys in their assigned partition. For each key, iterate all values via <code>get_next()</code> and call user's <code>Reduce()</code>. Write aggregated result to partition-specific output file.</td>
<td>Iterator protocol + partition isolation</td>
<td>Reducers never conflict—each owns disjoint partitions. Per-partition output files eliminate I/O serialization.</td>
</tr>
</tbody>
</table>

**Result**: Overlapped I/O, compute, and reduce stages keep all CPU cores and disks saturated. The bounded queue acts as a valve: backpressure when mappers are slow, forward pressure when readers are slow.

## Code Structure

| Component | Files | Responsibility |
|-----------|-------|----------------|
| **Orchestrator** | [mapreduce.c](src/mapreduce.c) | Main coordinator: readers → mappers → sorters → reducers |
| **Public API** | [mapreduce.h](src/mapreduce.h), [job.h](src/job.h) | User-facing types, callbacks, and job configuration |
| **Mapper Pipeline** | [mapper.c](src/mapper.c), [buffer.c](src/buffer.c) | Worker loop + thread-local buffering & flush |
| **Partitioning** | [partition.c](src/partition.c) | Partition init, bucket management, default hash (djb2) |
| **I/O Queue** | [reader_queue.c/h](src/reader_queue.c) | Bounded queue + chunk planner + reader threads |
| **Sorting** | [sorting.c](src/sorting.c) | Parallel per-partition `qsort` and list rebuild |
| **Reducer** | [reduce.c](src/reduce.c) | Value iterator protocol + reducer worker threads |
| **Metrics** | [metrics.c/h](src/metrics.c) | Lock-free instrumentation + report generation |
| **Sample App** | [main.c](src/main.c) | Inverted-index example with custom Map/Reduce/Combiner |

## Optimization Techniques

<table>
<tr>
<td width="50%">

### Lock-Free & Low-Contention

- **Atomic chunk assignment**: Readers claim work via `atomic_int` index—zero lock overhead
- **Thread-local buffers**: 4K buckets × 50K threshold = batched flushing
- **10K partition buckets**: <1% contention measured with 20+ concurrent mappers
- **Per-reducer output handles**: No cross-thread I/O serialization

</td>
<td width="50%">

### Parallelism & Overlap

- **Bounded queue design**: Readers/mappers overlap I/O and compute without unbounded RAM
- **Parallel sorting**: One thread per partition sorts independently
- **Optional combiner**: Map-side aggregation cuts shuffle by 83%
- **Stage pipelining**: All phases run concurrently where data dependencies allow

</td>
</tr>
<tr>
<td colspan="2">

### Instrumentation

Lock-free atomic counters track queue waits, flush batches, lock contention, and per-partition load. Every run generates `metrics_report.txt` for bottleneck diagnosis.

</td>
</tr>
</table>

## API Reference

### Quick Job Setup

```c
#include "job.h"

MR_Job job = MR_DefaultJob();
job.map = Map;               // required
job.reduce = Reduce;         // required  
job.combiner = DedupCombiner; // optional (NULL to disable)
return MR_Run(&job, argc, argv);
```

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-i <N>` | `1` | Reader threads (1-3 recommended) |
| `-m <N>` | `20` | Mapper threads (≈1.5× cores for CPU-bound) |
| `-r <N>` | `40` | Reducer threads = partitions |
| `-o <dir>` | `output` | Output directory path |
| `-p <path>` | `metrics_report.txt` | Metrics report destination |

**Environment**: `MR_DISABLE_COMBINER=1` to bypass combiner even if configured.

### Core Callbacks

<details>
<summary><b>Mapper</b> — Process chunks and emit key-value pairs</summary>

```c
void Mapper(char *data, int size) {
    // Parse 'data' buffer and emit pairs
    // Use MR_CurrentFile() to get filename
    MR_Emit("key", "value");
}
```
</details>

<details>
<summary><b>Reducer</b> — Aggregate values for each key</summary>

```c
void Reducer(char *key, Getter get_next, int partition) {
    // Iterate all values for 'key'
    char *value;
    while ((value = get_next(key, partition)) != NULL) {
        // Aggregate...
    }
}
```
</details>

<details>
<summary><b>Combiner</b> (optional) — Map-side pre-aggregation</summary>

```c
void Combiner(char *key, value_node_t **head) {
    // Dedup/aggregate values in-place before shuffle
    // Operates on linked list while data is thread-local
}
```
</details>

<details>
<summary><b>Partitioner</b> — Control key distribution</summary>

```c
unsigned long Partitioner(char *key, int num_partitions) {
    return custom_hash(key) % num_partitions;
}
```
*Default: `MR_DefaultHashPartition` (djb2-based)*
</details>

### Runtime Helpers

```c
const char* MR_CurrentFile(void);           // Get filename being mapped
void MR_Emit(char *key, char *value);       // Emit key-value pair (buffered)
void MR_SetCombiner(Combiner fn);           // Register combiner (low-level API)
```

## Quick Start

### Build

```bash
make                    # Optimized build (-O2, pthreads)
make clean && make      # Clean rebuild
```

### Run

```bash
# Recommended configuration for 708 MB dataset
./build/bin/mapreduce -i 1 -m 20 -r 40 input/*.txt
```

### Tuning Guide

<table>
<tr>
<th>Parameter</th>
<th>Range</th>
<th>Tuning Advice</th>
</tr>
<tr>
<td><b>Readers (-i)</b></td>
<td>1-3</td>
<td>More than 3 usually causes disk contention. Start with 1.</td>
</tr>
<tr>
<td><b>Mappers (-m)</b></td>
<td>≈1.5× cores</td>
<td>CPU-bound parsing benefits from slight oversubscription.</td>
</tr>
<tr>
<td><b>Reducers (-r)</b></td>
<td>10-40</td>
<td>More reducers = better balance but more output files.</td>
</tr>
<tr>
<td><b>Combiner</b></td>
<td>on/off</td>
<td>`MR_DISABLE_COMBINER=1` to measure worst-case load.</td>
</tr>
</table>

### Output

```
output/part-00000.txt ... part-XXXXX.txt   # Reducer outputs
metrics_report.txt                         # Performance snapshot
```

> **Tip**: Copy metrics between runs for comparison: `cp metrics_report.txt metrics_i1_m20_r40.txt`

## Performance Analysis

### Benchmark Methodology

| Component | Specification |
|-----------|---------------|
| **Hardware** | Dell G15 5520: i7-12700H (20 cores), 16 GB RAM, NVMe SSD |
| **Dataset** | Seven news articles (≈708 MB, ~2,800 × 256 KB chunks) |
| **Workload** | Inverted index with map-side deduplication combiner |

---

### Combiner Impact

<div align="center">

| Configuration | Wall Time | Reduce Stage | Total Emits | Post-Combiner | Shuffle Reduction |
|---------------|-----------|--------------|-------------|---------------|-------------------|
| **With Combiner** | **19.1 s** | **358 ms** | 126,770,743 | 21,237,810 | **83%** ↓ |
| **Without** | 36.8 s | 5,055 ms | 126,770,743 | 126,770,743 | — |

**Speedup: 1.92× overall** | **14.1× reduce phase**

</div>

> **Key Insight**: Map-side dedup eliminates 105M redundant pairs before shuffle, roughly halving end-to-end time.

---

### Mapper Thread Scaling (1 reader, 40 reducers)

| Mappers | Wall (s) | Map (ms) | Queue Wait | Speedup | Efficiency | Notes |
|---------|----------|----------|------------|---------|------------|-------|
| 1 | 185.5 | 173,657 | 0.1 ms | 1.00× | 100% | Baseline |
| 5 | 35.7 | 30,525 | 6.0 ms | 5.19× | 104% | Super-linear (cache) |
| 10 | 24.8 | 18,653 | 33.6 ms | 7.48× | 75% | Near-linear |
| 20 | **19.1** | 13,273 | 351.8 ms | **9.70×** | 49% | Queue bottleneck |

**Analysis**: Linear scaling through 10 mappers (CPU-bound). Beyond 10, queue contention emerges as bottleneck.

---

### Reducer Thread Scaling (1 reader, 20 mappers)

| Reducers | Wall (s) | Reduce (ms) | Keys/Partition | Load Balance (σ) | Improvement |
|----------|----------|-------------|----------------|------------------|-------------|
| 20 | 19.7 | 522 | 26,439 | ±412 | Baseline |
| 40 | **19.1** | **358** | 13,220 | ±189 | **31% faster** |

**Analysis**: Doubling reducers halves keys per partition → 31% faster reduce phase with better load balance.

---

## Project Layout

```
mapreduceV1/
├── src/                     # Core engine + sample app
│   ├── mapreduce.c            # Main orchestrator
│   ├── mapreduce.h            # Public API
│   ├── job.{c,h}              # Job configuration
│   ├── mapper.c, buffer.c     # Mapper pipeline
│   ├── partition.c            # Partitioning & hashing
│   ├── reader_queue.{c,h}     # I/O queue
│   ├── sorting.c, reduce.c    # Sort & reduce
│   ├── metrics.{c,h}          # Instrumentation
│   └── main.c                 # Inverted-index example
├── build/                   # Compiled artifacts (generated)
│   ├── bin/mapreduce          # Executable
│   └── obj/*.o                # Object files
├── input/                   # Test corpora (local)
├── output/                  # Reducer outputs (generated)
│   └── part-*.txt             
├── metrics_report*.txt      # Performance snapshots (generated)
└── Makefile                 # Build configuration
```

---

## Design Philosophy

### How This Architecture Maximizes Throughput

<table>
<tr>
<td>

#### Overlapped Execution
Readers, mappers, sorters, and reducers run concurrently. Bounded queue prevents RAM exhaustion while keeping all stages fed.

</td>
<td>

#### Minimal Synchronization
Lock-free reader scheduling + thread-local buffers eliminate most shared-memory writes in hot paths.

</td>
</tr>
<tr>
<td>

#### Batched Flushes
50K-value batches amortize lock costs. Per-bucket locks (10K/partition) keep contention <1%.

</td>
<td>

#### Data-Parallel Scaling
Independent sorting per partition + optional map-side combiner keep later stages scalable.

</td>
</tr>
</table>

## Metrics Guide

Every run generates `metrics_report.txt` (override with `-p`). Sections appear in consistent order for easy diffing:

<details>
<summary><b>Report Sections</b></summary>

| Section | Key Metrics | What It Reveals |
|---------|-------------|-----------------|
| **Job Configuration** | Files, threads, partitions | Run parameters for reproducibility |
| **Overall Timing** | Wall/user/system CPU | End-to-end regression check |
| **Stage Durations** | Map/Sort/Reduce ms | Identifies dominant phase |
| **Reader Stats** | Chunks, bytes, avg size | I/O health & chunking validation |
| **Mapper Stats** | Chunks, `MR_Emit` calls | Token diversity & emit volume |
| **Buffer Flush** | Count, avg batch/time | Flush efficiency & batching |
| **Work Queue** | Depth, push/pop waits | I/O-bound vs CPU-bound detection |
| **Partition Locks** | Acquires, contention %, wait | Lock tuning guidance |
| **Reducer Stats** | Total keys, avg/partition | Load balance across reducers |
| **Top-10 Tables** | Hottest partitions | Pinpoint skew & hotspots |

</details>

### Diagnostic Patterns

```
High push waits → Reader-bound (slow I/O)
High pop waits  → Mapper-bound (slow CPU)
>1% contention  → Consider more buckets or partitions
Uneven keys/part → Improve hash distribution
```

> **Tip**: `cp metrics_report.txt metrics_i1_m20_r40.txt` to archive snapshots between experiments.

---

## Contributing

We welcome improvements to algorithms, documentation, and instrumentation! Please:

- **Discuss first**: Open an issue for API changes or significant refactors
- **Test thoroughly**: Verify `make && ./build/bin/mapreduce` completes without regression
- **Match style**: Follow existing comment patterns in [src/](src)
- **Include metrics**: Attach before/after reports when claiming performance gains
- **Update docs**: Keep README and code comments synchronized

### Enhancement Ideas

- **SIMD hashing**: AVX2/NEON-accelerated partition function
- **Adaptive flush**: Dynamic threshold based on contention

---

<div align="center">


⭐ **Found this useful? Star it!** | 📚 **Questions? Open an issue!**

</div>
