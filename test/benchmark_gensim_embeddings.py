#!/usr/bin/env python3
"""
CogDB + Gensim Word Embeddings Benchmark

This benchmark uses real word vectors from Gensim to test CogDB's embedding features.
Gensim caches downloads in ~/gensim-data, so subsequent runs won't re-download.

This is NOT a unit test - it requires gensim to be installed and downloads data.
Run manually like benchmark.py:

    python3 test/benchmark_gensim_embeddings.py
    python3 test/benchmark_gensim_embeddings.py --limit 10000
    python3 test/benchmark_gensim_embeddings.py --full

Prerequisites:
    pip install gensim
"""
import os
import sys
import shutil
import time
import argparse

# Setup path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gensim.downloader as api
from cog.torque import Graph

TEST_DIR = "/tmp/CogGensimBenchmark"


def setup():
    """Clean up and prepare test directory"""
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    os.makedirs(TEST_DIR)


def cleanup():
    """Clean up test data"""
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def main():
    parser = argparse.ArgumentParser(description="CogDB + Gensim Embedding Benchmark")
    parser.add_argument("--limit", type=int, default=5000,
                        help="Number of embeddings to load (default: 5000)")
    parser.add_argument("--full", action="store_true",
                        help="Load all embeddings (1.2M words, slow!)")
    parser.add_argument("--skip-search", action="store_true",
                        help="Skip k_nearest/sim tests (just test loading)")
    args = parser.parse_args()
    
    limit = None if args.full else args.limit
    
    setup()
    timings = {}
    
    print("=" * 70)
    print("CogDB + Gensim Word Embeddings Benchmark")
    print("Loading: {} embeddings".format('ALL' if limit is None else limit))
    print("=" * 70)
    
    # ============================================================
    # Load word vectors (cached after first download)
    # ============================================================
    print("\nLoading Gensim vectors (cached in ~/gensim-data)...")
    start = time.time()
    vectors = api.load("glove-twitter-25")  # 25-dim vectors, ~1.2M words
    timings['gensim_load'] = time.time() - start
    print("Gensim load: {:.2f}s".format(timings['gensim_load']))
    print("Loaded {} word vectors (25 dimensions)".format(len(vectors)))
    
    # ============================================================
    # Create CogDB graph and load embeddings
    # ============================================================
    g = Graph("word_demo", cog_home="GensimBenchmark", cog_path_prefix=TEST_DIR)
    
    print("\nLoading {} words into CogDB...".format(limit if limit else 'ALL'))
    start = time.time()
    count = g.load_gensim(vectors, limit=limit)
    g.sync()  # Ensure data is flushed
    timings['cogdb_load'] = time.time() - start
    print("CogDB load: {:.2f}s".format(timings['cogdb_load']))
    print("Loaded {} embeddings ({:.0f} embeddings/s)".format(
        count, count / timings['cogdb_load']))
    
    # Get stats
    start = time.time()
    stats = g.embedding_stats()
    timings['stats'] = time.time() - start
    print("Stats: {}".format(stats))
    print("embedding_stats(): {:.2f}s".format(timings['stats']))
    
    if not args.skip_search:
        # ============================================================
        # Test k_nearest (similarity search)
        # ============================================================
        print("\n" + "-" * 70)
        print("k_nearest similarity search")
        print("-" * 70)
        
        test_words = ["happy", "love", "good", "the", "is"]
        
        for word in test_words:
            if g.get_embedding(word):
                start = time.time()
                result = g.v().k_nearest(word, k=5).all()
                elapsed = time.time() - start
                similar = [item['id'] for item in result['result']]
                print("\n'{}' -> {}".format(word, similar))
                print("k_nearest: {:.3f}s".format(elapsed))
            else:
                print("\n'{}' -> (not in first {} words)".format(word, limit))
        
        # ============================================================
        # Test sim() filtering
        # ============================================================
        print("\n" + "-" * 70)
        print("sim() threshold filtering")
        print("-" * 70)
        
        for word in ["love", "happy"]:
            if g.get_embedding(word):
                start = time.time()
                result = g.v().sim(word, ">", 0.8).limit(5).all()
                elapsed = time.time() - start
                similar = [item['id'] for item in result['result']]
                print("\nWords with sim > 0.8 to '{}': {}".format(word, similar))
                print("sim() filter: {:.3f}s".format(elapsed))
    
    # ============================================================
    # Test get_embedding performance
    # ============================================================
    print("\n" + "-" * 70)
    print("get_embedding performance")
    print("-" * 70)
    
    # Get 100 random words and time lookups
    sample_words = list(vectors.index_to_key[:100])
    start = time.time()
    for word in sample_words:
        _ = g.get_embedding(word)
    timings['get_embedding_100'] = time.time() - start
    print("100 get_embedding calls: {:.3f}s".format(timings['get_embedding_100']))
    print("({:.0f} lookups/s)".format(100 / timings['get_embedding_100']))
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "=" * 70)
    print("TIMING SUMMARY")
    print("=" * 70)
    print("  Gensim load (from cache):  {:>8.2f}s".format(timings['gensim_load']))
    print("  CogDB load ({} words): {:>8.2f}s  ({:.0f}/s)".format(
        count, timings['cogdb_load'], count / timings['cogdb_load']))
    print("  embedding_stats():         {:>8.2f}s".format(timings['stats']))
    print("  get_embedding (100x):      {:>8.3f}s".format(timings['get_embedding_100']))
    
    if count > 0:
        print("\n  Load rate: {:.0f} embeddings/second".format(
            count / timings['cogdb_load']))
    
    # ============================================================
    # Cleanup
    # ============================================================
    g.close()
    cleanup()
    
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
