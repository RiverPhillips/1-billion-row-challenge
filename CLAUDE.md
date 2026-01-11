# 1 Billion Row Challenge - Go Implementation

## Quick Start

```bash
# Build (without PGO)
go build -o 1brc

# Build with Profile-Guided Optimization
./1brc -cpuprofile=cpu.pgo weather_stations.csv > /dev/null
go build -pgo=cpu.pgo -o 1brc

# Run
./1brc weather_stations.csv

# Benchmark
hyperfine --warmup 1 --runs 5 './1brc weather_stations.csv'
```

## Performance

- **Current**: 3.604s ± 0.049s (with PGO)
- **Baseline**: 3.768s ± 0.040s
- **Improvement**: 4.4% faster

## Core Strategy

1. **Memory-mapped I/O**: Use `mmap` with `MAP_POPULATE` to pre-fault pages and `MADV_SEQUENTIAL` for optimal prefetching
2. **Parallel processing**: Split file into chunks, one per CPU core (12 workers)
3. **Per-worker hash tables**: Each worker maintains its own hash table to avoid contention
4. **Fixed-point arithmetic**: Store temperatures as int32 (multiplied by 10) to avoid floating-point overhead
5. **Merge phase**: Combine per-worker results into final hash table, then sort and output

## Key Optimizations

### Hash Table Sizing (4.4% improvement)
- Per-worker tables: 2^14 = 16,384 buckets (~2.0 load factor for ~34k stations)
- Merge table: 2^18 = 262,144 buckets (~1.6 load factor for ~413k unique stations)
- Reduced collision chains from 400x load factor to ~2x
- **Impact**: Fewer random memory accesses, better cache locality

### What Didn't Work

Multiple optimization attempts that failed or regressed performance:

1. **Modulo → Bitmasking** (-1.5%): Doubled cache misses
2. **Unsafe temperature parsing** (-6.2%): Added branches, hurt pipeline
3. **Combined delimiter scans** (-10%): Disrupted branch patterns
4. **Length-based optimizations** (-5% to -7%): Added overhead
5. **SIMD delimiter scanning** (-2.8%): Function call overhead, limited benefit for short strings

**Pattern**: CPU prefetcher, branch predictor, and cache hierarchy are already highly optimized for simple sequential patterns. "Clever" optimizations often disrupt these patterns.

## Architecture

### Data Structures

```go
type stats struct {
    min   int32   // Minimum temperature (fixed-point)
    max   int32   // Maximum temperature (fixed-point)
    sum   int32   // Sum of temperatures (fixed-point)
    count uint64  // Number of measurements
}

type item struct {
    hash  fnvHash // FNV-1a hash of station name
    key   []byte  // Station name (slice into mmap'd data)
    value *stats  // Aggregated statistics
}

type hashtable struct {
    items []item  // Linear probing hash table
    size  uint64  // Number of occupied slots
}
```

### Hash Function

Uses FNV-1a (Fowler-Noll-Vo) hash with:
- Large offset: 14695981039346656037
- Prime multiplier: 1099511628211

Critical for good distribution across hash table buckets.

## Bottleneck Analysis

From extensive perf profiling:

- **IPC**: 2.08 instructions/cycle (excellent)
- **Branch prediction**: 98.5% accuracy (excellent)
- **L1 cache hit rate**: 96.95% (excellent)
- **L2/L3 cache hit rate**: 98.4% (excellent)
- **Memory bandwidth**: Only 13% utilized (not bottleneck)

**Cycle distribution**:
- 80.69% - Main parsing loop (delimiter scanning, hashing)
- 11.22% - Temperature parsing
- 7.48% - String comparison (hash table lookups)

**Conclusion**: Bottlenecked on fundamental limit of sequential text processing. Near theoretical optimal for this workload without SIMD/assembly.

## Build & Benchmark Commands

```bash
# Profile with perf
perf record -g -o perf.data ./1brc weather_stations.csv
perf report -i perf.data

# Detailed CPU metrics
perf stat -d -d -d ./1brc weather_stations.csv

# Branch prediction analysis
perf stat -e branches,branch-misses ./1brc weather_stations.csv

# Cache analysis
perf stat -e cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses ./1brc weather_stations.csv
```

## Git History

All optimization attempts are documented in git history with full performance data, including:
- Baseline measurements
- Changed code
- Benchmark results
- Analysis of why it worked or failed

See commit messages for detailed performance analysis of each optimization.
