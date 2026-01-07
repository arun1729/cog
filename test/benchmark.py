#!/usr/bin/env python3
"""
CogDB Performance Benchmark Suite

Run with: python3 test/benchmark.py

This benchmark compares:
1. Individual puts vs batch puts
2. Various graph sizes (small, medium, large)
3. Read performance after inserts
4. Different edge densities

Options:
  --fast-flush       Use flush_interval=100 for faster writes (auto-enables async)
  --flush-interval N Custom flush interval (>1 auto-enables async)
"""
import os
import shutil
import timeit
import argparse
from dataclasses import dataclass
from typing import List, Tuple

# Setup path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cog.torque import Graph
from cog import config

BENCHMARK_DIR = "/tmp/CogBenchmark"

# Global flush settings (set by args)
FLUSH_INTERVAL = 1


@dataclass
class BenchmarkResult:
    name: str
    num_edges: int
    time_seconds: float
    edges_per_second: float
    
    def __str__(self):
        return f"{self.name:40} | {self.num_edges:8} edges | {self.time_seconds:8.3f}s | {self.edges_per_second:10.0f} edges/s"


def setup():
    """Clean up and prepare benchmark directory"""
    if os.path.exists(BENCHMARK_DIR):
        shutil.rmtree(BENCHMARK_DIR)
    os.makedirs(BENCHMARK_DIR)
    config.CUSTOM_COG_DB_PATH = BENCHMARK_DIR


def cleanup():
    """Clean up benchmark data"""
    if os.path.exists(BENCHMARK_DIR):
        shutil.rmtree(BENCHMARK_DIR)


def generate_chain_graph(num_edges: int) -> List[Tuple[str, str, str]]:
    """Generate a linear chain graph: A->B->C->D->..."""
    return [(f"node_{i}", "next", f"node_{i+1}") for i in range(num_edges)]


def generate_star_graph(center: str, num_edges: int) -> List[Tuple[str, str, str]]:
    """Generate a star graph: center connected to all others"""
    return [(center, "connects", f"node_{i}") for i in range(num_edges)]


def generate_dense_graph(num_nodes: int) -> List[Tuple[str, str, str]]:
    """Generate a dense graph where each node connects to next 5 nodes"""
    triples = []
    for i in range(num_nodes):
        for j in range(i + 1, min(i + 6, num_nodes)):
            triples.append((f"node_{i}", "connects", f"node_{j}"))
    return triples


def generate_social_graph(num_users: int, avg_connections: int = 10) -> List[Tuple[str, str, str]]:
    """Generate a social graph with users following each other"""
    import random
    random.seed(42)  # Reproducible
    triples = []
    for i in range(num_users):
        # Each user follows ~avg_connections other users
        num_follows = random.randint(avg_connections // 2, avg_connections * 2)
        targets = random.sample(range(num_users), min(num_follows, num_users - 1))
        for target in targets:
            if target != i:
                triples.append((f"user_{i}", "follows", f"user_{target}"))
    return triples


def create_graph(graph_name: str) -> Graph:
    """Create a graph with the configured flush settings"""
    return Graph(graph_name, flush_interval=FLUSH_INTERVAL)


def benchmark_individual_puts(graph_name: str, triples: List[Tuple[str, str, str]]) -> BenchmarkResult:
    """Benchmark inserting edges one at a time"""
    g = create_graph(graph_name)
    
    start = timeit.default_timer()
    for v1, pred, v2 in triples:
        g.put(v1, pred, v2)
    g.sync()  # Ensure all data is flushed
    elapsed = timeit.default_timer() - start
    
    g.close()
    
    return BenchmarkResult(
        name=f"Individual puts ({graph_name})",
        num_edges=len(triples),
        time_seconds=elapsed,
        edges_per_second=len(triples) / elapsed if elapsed > 0 else 0
    )


def benchmark_batch_puts(graph_name: str, triples: List[Tuple[str, str, str]]) -> BenchmarkResult:
    """Benchmark inserting edges using put_batch"""
    g = create_graph(graph_name)
    
    start = timeit.default_timer()
    g.put_batch(triples)
    g.sync()  # Ensure all data is flushed
    elapsed = timeit.default_timer() - start
    
    g.close()
    
    return BenchmarkResult(
        name=f"Batch puts ({graph_name})",
        num_edges=len(triples),
        time_seconds=elapsed,
        edges_per_second=len(triples) / elapsed if elapsed > 0 else 0
    )


def benchmark_reads(graph_name: str, sample_vertices: List[str]) -> BenchmarkResult:
    """Benchmark reading/traversing from vertices"""
    g = create_graph(graph_name)
    
    start = timeit.default_timer()
    for vertex in sample_vertices:
        result = g.v(vertex).out().all()
    elapsed = timeit.default_timer() - start
    
    g.close()
    
    return BenchmarkResult(
        name=f"Reads ({graph_name})",
        num_edges=len(sample_vertices),
        time_seconds=elapsed,
        edges_per_second=len(sample_vertices) / elapsed if elapsed > 0 else 0
    )


def run_benchmarks(sizes: List[int] = None, skip_individual: bool = False):
    """Run all benchmarks"""
    if sizes is None:
        sizes = [100, 500, 1000, 5000]
    
    results = []
    
    print("\n" + "=" * 85)
    print("CogDB Performance Benchmark")
    if FLUSH_INTERVAL != 1:
        print(f"  flush_interval={FLUSH_INTERVAL} (async enabled)")
    print("=" * 85)
    
    # Benchmark 1: Chain graphs at various sizes
    print("\n--- Chain Graph Benchmarks ---")
    for size in sizes:
        setup()
        triples = generate_chain_graph(size)
        
        if not skip_individual:
            result = benchmark_individual_puts(f"chain_ind_{size}", triples)
            results.append(result)
            print(result)
        
        setup()
        result = benchmark_batch_puts(f"chain_batch_{size}", triples)
        results.append(result)
        print(result)
        
        # Read benchmark
        g = create_graph(f"chain_batch_{size}")
        sample = [f"node_{i}" for i in range(0, min(size, 100), 10)]
        result = benchmark_reads(f"chain_batch_{size}", sample)
        results.append(result)
        print(result)
        
        cleanup()
    
    # Benchmark 2: Star graphs
    print("\n--- Star Graph Benchmarks ---")
    for size in sizes:
        setup()
        triples = generate_star_graph("hub", size)
        
        result = benchmark_batch_puts(f"star_{size}", triples)
        results.append(result)
        print(result)
        cleanup()
    
    # Benchmark 3: Dense graphs
    print("\n--- Dense Graph Benchmarks ---")
    for num_nodes in [50, 100, 200]:
        setup()
        triples = generate_dense_graph(num_nodes)
        
        result = benchmark_batch_puts(f"dense_{num_nodes}", triples)
        results.append(result)
        print(result)
        cleanup()
    
    # Benchmark 4: Social graph simulation
    print("\n--- Social Graph Benchmarks ---")
    for num_users in [100, 500, 1000]:
        setup()
        triples = generate_social_graph(num_users, avg_connections=10)
        
        result = benchmark_batch_puts(f"social_{num_users}", triples)
        results.append(result)
        print(result)
        cleanup()
    
    # Summary
    print("\n" + "=" * 85)
    print("Summary")
    print("=" * 85)
    
    batch_results = [r for r in results if "Batch" in r.name]
    if batch_results:
        avg_speed = sum(r.edges_per_second for r in batch_results) / len(batch_results)
        max_speed = max(r.edges_per_second for r in batch_results)
        print(f"Average batch insert speed: {avg_speed:,.0f} edges/second")
        print(f"Peak batch insert speed: {max_speed:,.0f} edges/second")
    
    # Compare individual vs batch if we ran individual benchmarks
    if not skip_individual:
        print("\n--- Individual vs Batch Comparison ---")
        for size in sizes:
            ind_results = [r for r in results if f"chain_ind_{size}" in r.name]
            batch_results = [r for r in results if f"chain_batch_{size}" in r.name]
            if ind_results and batch_results:
                speedup = ind_results[0].time_seconds / batch_results[0].time_seconds
                print(f"Size {size:5}: Batch is {speedup:.2f}x faster than individual puts")
    
    return results


def main():
    global FLUSH_INTERVAL
    
    parser = argparse.ArgumentParser(description="CogDB Performance Benchmark")
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 500, 1000, 5000],
                        help="Edge counts to benchmark (default: 100 500 1000 5000)")
    parser.add_argument("--skip-individual", action="store_true",
                        help="Skip individual put benchmarks (faster)")
    parser.add_argument("--quick", action="store_true",
                        help="Quick benchmark with smaller sizes")
    parser.add_argument("--fast-flush", action="store_true",
                        help="Use flush_interval=100 for faster writes (auto-enables async)")
    parser.add_argument("--flush-interval", type=int, default=1,
                        help="Custom flush interval (default: 1, >1 auto-enables async)")
    args = parser.parse_args()
    
    # Set global flush settings
    if args.fast_flush:
        FLUSH_INTERVAL = 100
    elif args.flush_interval != 1:
        FLUSH_INTERVAL = args.flush_interval
    
    sizes = [50, 100, 200] if args.quick else args.sizes
    
    try:
        run_benchmarks(sizes=sizes, skip_individual=args.skip_individual)
    finally:
        cleanup()
    
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()

