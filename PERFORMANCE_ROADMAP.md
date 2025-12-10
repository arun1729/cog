# CogDB Performance Roadmap

> Weekly release cadence - tracking performance issues and optimizations

## 🔴 High Priority

### Star Graph / High-Degree Vertex Performance Degradation
**Discovered:** 2024-12-10 via benchmark  
**Issue:** When inserting many edges from/to the same vertex, performance degrades severely.

| Edges | Speed | Degradation |
|-------|-------|-------------|
| 100 | 569 edges/s | baseline |
| 500 | 633 edges/s | - |
| 1,000 | 338 edges/s | 47% slower |
| 5,000 | 83 edges/s | **87% slower** |

**Root Cause:** `put_set()` in `database.py` traverses linked lists to check for duplicates. This is O(n) per insert, making high-degree vertices O(n²) overall.

**Location:** `database.py:241-277` (put_set method)

**Potential Fix:**  
1. Use hash-based set for duplicate checking instead of linked list traversal
2. Consider bloom filter for faster "definitely not present" checks
3. Or maintain an in-memory index of vertex adjacencies

---

## 🟡 Medium Priority

### Unbounded Cache Growth
**Issue:** Cache in `cache.py` grows unboundedly - no eviction policy.  
**Fix:** Implement LRU cache with `collections.OrderedDict`  
**Effort:** Low

### Redundant Table Switches in put_node
**Issue:** `put_node()` calls `use_table()` 5 times per edge insert.  
**Fix:** Cache table references within the method  
**Effort:** Low

---

## 🟢 Low Priority / Nice to Have

### Efficient Serialization
**Issue:** Record.marshal() uses string concatenation with `+`  
**Fix:** Use bytearray for efficient concatenation  
**Effort:** Low, ~5% improvement

### Configurable Auto-Flush
**Issue:** Currently binary (batch mode on/off)  
**Fix:** Add config option for flush frequency (every N records)  
**Effort:** Low

---

## ✅ Completed

### v3.1.0 (2024-12-10)
- [x] Batch flush mode - defer flush() during bulk inserts
- [x] `Graph.put_batch()` method for efficient bulk loading
- [x] Comprehensive benchmark suite (`test/benchmark.py`)
- [x] ~1.6x speedup for large batch inserts

---

## Benchmark Baselines (v3.1.0)

```
Chain graph (batch, 5000 edges): 4,377 edges/s
Social graph (12,492 edges): 3,233 edges/s  
Dense graph (985 edges): 2,585 edges/s
Read performance: 20,000+ ops/s
```

Run benchmarks: `python3 test/benchmark.py`
