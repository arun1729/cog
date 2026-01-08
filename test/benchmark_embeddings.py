#!/usr/bin/env python3
"""
CogDB Embedding Performance Benchmark

Run with: python3 test/benchmark_embeddings.py

This benchmark tests:
1. put_embedding / get_embedding performance
2. put_embeddings_batch performance
3. k_nearest similarity search
4. sim() filtering performance
"""
import os
import shutil
import timeit
import random
from dataclasses import dataclass
from typing import List, Tuple

# Setup path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cog.torque import Graph
from cog import config

BENCHMARK_DIR = "/tmp/CogEmbeddingBenchmark"


@dataclass
class BenchmarkResult:
    name: str
    num_operations: int
    time_seconds: float
    ops_per_second: float
    
    def __str__(self):
        return f"{self.name:45} | {self.num_operations:8} ops | {self.time_seconds:8.3f}s | {self.ops_per_second:10.0f} ops/s"


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


def generate_random_embedding(dimensions: int = 50) -> List[float]:
    """Generate a random embedding vector"""
    return [random.uniform(-1.0, 1.0) for _ in range(dimensions)]


def generate_clustered_embeddings(num_embeddings: int, 
                                   num_clusters: int = 5, 
                                   dimensions: int = 50) -> List[Tuple[str, List[float]]]:
    """
    Generate embeddings in clusters so similarity search is meaningful.
    Words in the same cluster will be similar to each other.
    """
    embeddings = []
    words_per_cluster = num_embeddings // num_clusters
    
    for cluster_id in range(num_clusters):
        # Generate cluster centroid
        centroid = generate_random_embedding(dimensions)
        
        for i in range(words_per_cluster):
            word = f"word_{cluster_id}_{i}"
            # Add small noise to centroid to create similar words
            embedding = [c + random.uniform(-0.1, 0.1) for c in centroid]
            embeddings.append((word, embedding))
    
    return embeddings


def benchmark_individual_puts(g: Graph, embeddings: List[Tuple[str, List[float]]]) -> BenchmarkResult:
    """Benchmark inserting embeddings one at a time"""
    start = timeit.default_timer()
    for word, embedding in embeddings:
        g.put(word, "type", "word")  # Add to graph
        g.put_embedding(word, embedding)
    g.sync()
    elapsed = timeit.default_timer() - start
    
    return BenchmarkResult(
        name="Individual put_embedding",
        num_operations=len(embeddings),
        time_seconds=elapsed,
        ops_per_second=len(embeddings) / elapsed if elapsed > 0 else 0
    )


def benchmark_batch_puts(g: Graph, embeddings: List[Tuple[str, List[float]]]) -> BenchmarkResult:
    """Benchmark inserting embeddings using batch mode"""
    start = timeit.default_timer()
    
    # Add words to graph
    triples = [(word, "type", "word") for word, _ in embeddings]
    g.put_batch(triples)
    
    # Batch insert embeddings
    g.put_embeddings_batch(embeddings)
    g.sync()
    elapsed = timeit.default_timer() - start
    
    return BenchmarkResult(
        name="Batch put_embeddings_batch",
        num_operations=len(embeddings),
        time_seconds=elapsed,
        ops_per_second=len(embeddings) / elapsed if elapsed > 0 else 0
    )


def benchmark_get_embedding(g: Graph, words: List[str]) -> BenchmarkResult:
    """Benchmark retrieving embeddings"""
    start = timeit.default_timer()
    for word in words:
        _ = g.get_embedding(word)
    elapsed = timeit.default_timer() - start
    
    return BenchmarkResult(
        name="get_embedding",
        num_operations=len(words),
        time_seconds=elapsed,
        ops_per_second=len(words) / elapsed if elapsed > 0 else 0
    )


def benchmark_k_nearest(g: Graph, query_words: List[str], k: int = 5) -> BenchmarkResult:
    """Benchmark k-nearest neighbor search"""
    start = timeit.default_timer()
    for word in query_words:
        _ = g.v().k_nearest(word, k=k).all()
    elapsed = timeit.default_timer() - start
    
    return BenchmarkResult(
        name=f"k_nearest (k={k})",
        num_operations=len(query_words),
        time_seconds=elapsed,
        ops_per_second=len(query_words) / elapsed if elapsed > 0 else 0
    )


def benchmark_sim_filter(g: Graph, query_words: List[str], threshold: float = 0.8) -> BenchmarkResult:
    """Benchmark similarity filtering"""
    start = timeit.default_timer()
    for word in query_words:
        _ = g.v().sim(word, ">", threshold).all()
    elapsed = timeit.default_timer() - start
    
    return BenchmarkResult(
        name=f"sim() filter (threshold={threshold})",
        num_operations=len(query_words),
        time_seconds=elapsed,
        ops_per_second=len(query_words) / elapsed if elapsed > 0 else 0
    )


def run_benchmarks(sizes: List[int] = None, dimensions: int = 50):
    """Run all embedding benchmarks"""
    if sizes is None:
        sizes = [100, 500, 1000]
    
    results = []
    
    print("\n" + "=" * 90)
    print("CogDB Embedding Performance Benchmark")
    print(f"Embedding dimensions: {dimensions}")
    print("=" * 90)
    
    for size in sizes:
        print(f"\n--- Benchmark with {size} embeddings ---")
        
        # Generate test data
        random.seed(42)  # Reproducible
        embeddings = generate_clustered_embeddings(size, num_clusters=5, dimensions=dimensions)
        words = [word for word, _ in embeddings]
        sample_words = random.sample(words, min(10, len(words)))
        
        # Benchmark 1: Individual puts
        setup()
        g = Graph("embed_individual", cog_home="EmbedBench")
        result = benchmark_individual_puts(g, embeddings)
        results.append(result)
        print(result)
        g.close()
        cleanup()
        
        # Benchmark 2: Batch puts
        setup()
        g = Graph("embed_batch", cog_home="EmbedBench")
        result = benchmark_batch_puts(g, embeddings)
        results.append(result)
        print(result)
        
        # Benchmark 3: Get embeddings (reusing the graph from batch)
        result = benchmark_get_embedding(g, words[:100])
        results.append(result)
        print(result)
        
        # Benchmark 4: k_nearest search
        result = benchmark_k_nearest(g, sample_words, k=5)
        results.append(result)
        print(result)
        
        # Benchmark 5: sim() filtering
        result = benchmark_sim_filter(g, sample_words, threshold=0.9)
        results.append(result)
        print(result)
        
        g.close()
        cleanup()
    
    # Summary
    print("\n" + "=" * 90)
    print("Summary")
    print("=" * 90)
    
    # Compare individual vs batch
    print("\n--- Write Performance ---")
    for size in sizes:
        ind_results = [r for r in results if "Individual" in r.name and r.num_operations == size]
        batch_results = [r for r in results if "Batch" in r.name and r.num_operations == size]
        if ind_results and batch_results:
            speedup = ind_results[0].time_seconds / batch_results[0].time_seconds
            print(f"Size {size:5}: Batch is {speedup:.2f}x faster than individual puts")
    
    print("\n--- Read/Search Performance ---")
    get_results = [r for r in results if "get_embedding" in r.name]
    knn_results = [r for r in results if "k_nearest" in r.name]
    sim_results = [r for r in results if "sim()" in r.name]
    
    if get_results:
        avg_speed = sum(r.ops_per_second for r in get_results) / len(get_results)
        print(f"Average get_embedding speed: {avg_speed:,.0f} ops/second")
    if knn_results:
        avg_speed = sum(r.ops_per_second for r in knn_results) / len(knn_results)
        print(f"Average k_nearest speed: {avg_speed:,.0f} queries/second")
    if sim_results:
        avg_speed = sum(r.ops_per_second for r in sim_results) / len(sim_results)
        print(f"Average sim() filter speed: {avg_speed:,.0f} queries/second")
    
    return results


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="CogDB Embedding Performance Benchmark")
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 500, 1000],
                        help="Number of embeddings to benchmark (default: 100 500 1000)")
    parser.add_argument("--dimensions", type=int, default=50,
                        help="Embedding dimensions (default: 50)")
    parser.add_argument("--quick", action="store_true",
                        help="Quick benchmark with smaller sizes")
    args = parser.parse_args()
    
    sizes = [50, 100, 200] if args.quick else args.sizes
    
    try:
        run_benchmarks(sizes=sizes, dimensions=args.dimensions)
    finally:
        cleanup()
    
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
