![](https://static.pepy.tech/badge/cogdb) [![PyPI version](https://badge.fury.io/py/cogdb.svg)](https://badge.fury.io/py/cogdb) ![Python 3.8](https://img.shields.io/badge/python-3.8+-blue.svg)
 [![Build Status](https://travis-ci.org/arun1729/cog.svg?branch=master)](https://travis-ci.org/arun1729/cog) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![codecov](https://codecov.io/gh/arun1729/cog/branch/master/graph/badge.svg)](https://codecov.io/gh/arun1729/cog) [![Discord](https://img.shields.io/badge/Discord-Join%20Server-7289da?logo=discord&logoColor=white)](https://discord.gg/nqNpNGfjts)

<img src="cog-logo.png" alt="CogDB Logo" width="350">

# CogDB - Micro Graph Database for Python Applications
> Documents and examples at [cogdb.io](https://cogdb.io)

> New release: 3.4.0
> - **52x performance boost** for hub-heavy graphs (star graphs)
> - `flush_interval` parameter for tunable write performance
> - `sync()` method for manual flush control

![ScreenShot](notes/ex2.png)

## Installing Cog
```
pip install cogdb
```
CogDB is a persistent, embedded graph database library implemented purely in Python.  Torque is CogDB's graph query language, it is implemented as a Python API. CogDB is an ideal choice if you need a database that is easy to use and that has no setup overhead. All you need to do is to import it into your Python application. CogDB can be used interactively in an IPython environment like Jupyter notebooks.

CogDB is a triple store; it models data as `vertex edge vertex` or in other words `subject predicate object`. Triples are a serialization format for RDF. See [Wikipedia](https://en.wikipedia.org/wiki/N-Triples), [W3C](https://www.w3.org/TR/n-triples/) for details. 
and generally graph databases that model graphs this way are known as RDF databases. CogDB is inspired by RDF databases, but it does not follow a strict RDF format.

### Creating a graph

#### Using `put` to insert triples

```python
from cog.torque import Graph
g = Graph("people")
g.put("alice","follows","bob")
g.put("bob","follows","fred")
g.put("bob","status","cool_person")
g.put("charlie","follows","bob")
g.put("charlie","follows","dani")
g.put("dani","follows","bob")
g.put("dani","follows","greg")
g.put("dani","status","cool_person")
g.put("emily","follows","fred")
g.put("fred","follows","greg")
g.put("greg","status","cool_person")
g.put("bob","score","5")
g.put("greg","score","10")
g.put("alice","score","7")
g.put("dani","score","100")
```

#### Using `put_batch` for bulk inserts (faster)

```python
from cog.torque import Graph
g = Graph("people")

# Insert multiple triples at once - significantly faster for large graphs
g.put_batch([
    ("alice", "follows", "bob"),
    ("bob", "follows", "charlie"),
    ("charlie", "follows", "alice"),
    ("alice", "likes", "pizza"),
    ("bob", "likes", "tacos"),
])
```

### Performance Tuning

Control flush behavior for faster bulk inserts:

```python
# Default: flush every write (safest)
g = Graph("mydb")

# Fast mode: flush every 100 writes (auto-enables async)
g = Graph("mydb", flush_interval=100)

# Manual flush only (fastest for bulk loads)
g = Graph("mydb", flush_interval=0)
g.put_batch(large_dataset)
g.sync()  # Flush when done
```

| `flush_interval` | Behavior | Use Case |
|------------------|----------|----------|
| `1` (default) | Flush every write | Interactive, safe |
| `> 1` | Async flush every N writes | Bulk inserts |
| `0` | Manual only (`sync()`) | Maximum speed |

#### Drop Edge ###

```python
g.drop("bob", "follows", "fred")
```

#### Using `putj` to insert JSONs

```python
f = Graph("followers")
f.putj('{"name" : "bob", "status" : "cool_person", "follows" : ["fred", "dani"]}')
f.putj('{"_id":  "1", "name" : "fred", "status" : "cool_person", "follows" : ["alice", "greg"]}')
```

#### Using `updatej` to update JSONs
```python
g.updatej('{"_id" : "1", "status" : "not_cool"}')
```

### Torque query examples

#### Scan vertices
```python
g.scan(3)
```

> {'result': [{'id': 'bob'}, {'id': 'emily'}, {'id': 'charlie'}]}

#### Scan edges
```python
g.scan(3, 'e')
```
>{'result': [{'id': 'status'}, {'id': 'follows'}]}

#### Starting from a vertex, follow all outgoing edges and list all vertices
```python
g.v("bob").out().all()
```
> {'result': [{'id': '5'}, {'id': 'fred'}, {'id': 'cool_person'}]}

#### Everyone with status 'cool_person'
```python
g.v().has("status", 'cool_person').all()
```

> {'result': [{'id': 'bob'}, {'id': 'dani'}, {'id': 'greg'}]}

#### Include edges in the results
```python
g.v().has("follows", "fred").inc().all('e')
```
> {'result': [{'id': 'dani', 'edges': ['follows']}, {'id': 'charlie', 'edges': ['follows']}, {'id': 'alice', 'edges': ['follows']}]}

#### starting from a vertex, follow all outgoing edges and count vertices
```python
g.v("bob").out().count()
```
> '3'

#### See who is following who and create a view of that network
#### Note: `render()` is supported only in IPython environment like Jupyter notebook otherwise use view(..).url.
By tagging the vertices 'from' and 'to', the resulting graph can be visualized.
```python
g.v().tag("from").out("follows").tag("to").view("follows").render()

```

# ![ScreenShot](notes/ex1.png)

```python
g.v().tag("from").out("follows").tag("to").view("follows").url

```
> file:///Path/to/your/cog_home/views/follows.html

#### List all views 
```
g.lsv()
```
> ['follows']

#### Load existing visualization
```
g.getv('follows').render()
```

#### starting from a vertex, follow all out going edges and tag them

```python
g.v("bob").out().tag("from").out().tag("to").all()
```
> {'result': [{'from': 'fred', 'id': 'greg', 'to': 'greg'}]}
> 

#### starting from a vertex, follow all incoming edges and list all vertices
```python
g.v("bob").inc().all()
```
> {'result': [{'id': 'alice'}, {'id': 'charlie'}, {'id': 'dani'}]}

#### Filtering

```python
g.v().filter(func=lambda x: x.startswith("d")).all()
```
> {'result': [{'id': 'dani'}]}


```python
g.v().out("score").filter(func=lambda x: int(x) > 5).inc().all()
```
> {'result': [{'id': 'alice'}, {'id': 'dani'}, {'id': 'greg'}]}

```python
g.v("emily").out("follows").filter(func=lambda x: x.startswith("f")).all()
```
> {'result': [{'id': 'fred'}]}

#### Bidirectional Traversal

Follow edges in both directions (outgoing and incoming):
```python
g.v("bob").both("follows").all()
```
> {'result': [{'id': 'fred'}, {'id': 'alice'}, {'id': 'charlie'}, {'id': 'dani'}]}

#### Filter to Specific Nodes

Filter results to only include specific vertices:
```python
g.v("alice").out("follows").is_("bob", "dani").all()
```
> {'result': [{'id': 'bob'}, {'id': 'dani'}]}

#### Remove Duplicates

Remove duplicate vertices from results:
```python
g.v().out("follows").unique().all()
```
> {'result': [{'id': 'bob'}, {'id': 'fred'}, {'id': 'greg'}, {'id': 'dani'}]}

#### Pagination with Limit and Skip

Limit results to first N vertices:
```python
g.v().limit(3).all()
```
> {'result': [{'id': 'alice'}, {'id': 'bob'}, {'id': 'charlie'}]}

Skip first N vertices:
```python
g.v().skip(2).limit(2).all()
```
> {'result': [{'id': 'charlie'}, {'id': 'dani'}]}

#### Navigate Back to Tagged Vertex

Return to a previously tagged position while preserving the traversal path:
```python
g.v("alice").tag("start").out("follows").out("follows").back("start").all()
```
> {'result': [{'start': 'alice', 'id': 'alice'}]}


#### json example

```python
#### Using `putj` to insert JSONs
f = Graph("followers")
f.putj('{"name" : "bob", "status" : "cool_person", "follows" : ["fred", "dani"]}')
f.putj('{"name" : "fred", "status" : "cool_person", "follows" : ["alice", "greg"]}')
```

```python 
f.v().has('name','bob').out('follows').all()
```

> {'result': [{'id': 'dani'}, {'id': 'fred'}]}

```python
f.v().has('name','fred').out('follows').all()
```

> {'result': [{'id': 'greg'}, {'id': 'alice'}]}

In a json, CogDB treats `_id` property as a unique identifier for each object. If `_id` is not provided, a randomly generated `_id` is created for each object with in a JSON object.
`_id` field is used to update a JSON object, see example below.

## Using word embeddings

CogDB supports word embeddings with SIMD-optimized similarity search powered by [SimSIMD](https://github.com/ashvardanian/SimSIMD). Word embeddings are useful for semantic search, recommendations, and NLP tasks.

#### Load pre-trained embeddings (GloVe):

```python
# Load GloVe embeddings (one-liner!)
count = g.load_glove("glove.6B.100d.txt", limit=50000)
print(f"Loaded {count} embeddings")
```

#### Load from Gensim model:

```python
from gensim.models import Word2Vec
model = Word2Vec(sentences)
count = g.load_gensim(model)
```

#### Add embeddings manually:

```python
g.put_embedding("orange", [0.1, 0.2, 0.3, 0.4, 0.5])

# Bulk insert for better performance
g.put_embeddings_batch([
    ("apple", [0.1, 0.2, ...]),
    ("banana", [0.3, 0.4, ...]),
])
```

#### Find k-nearest neighbors:

```python
# Find 5 most similar vertices to "machine_learning"
g.v().k_nearest("machine_learning", k=5).all()
```
> {'result': [{'id': 'deep_learning'}, {'id': 'neural_network'}, ...]}

#### Filter by similarity threshold:

```python 
g.v().sim('orange', '>', 0.35).all()
```
> {'result': [{'id': 'clementines'}, {'id': 'tangerine'}, {'id': 'orange'}]}

```python
g.v().sim('orange', 'in', [0.25, 0.35]).all()
```
> {'result': [{'id': 'banana'}, {'id': 'apple'}]}

#### Get embedding stats:

```python
g.embedding_stats()
```
> {'count': 50000, 'dimensions': 100}

The `sim` method filters vertices based on cosine similarity. The `k_nearest` method returns the top-k most similar vertices.

## Loading data from a file

### Create a graph from CSV file

```python
from cog.torque import Graph
g = Graph("books")
g.load_csv('test/test-data/books.csv', "book_id")
```
#### Get the names of the books that have an average rating greater than 4.0
```python
g.v().out("average_rating", func=lambda x: float(x) > 4.0).inc().out("title").all()
```

#### Triples file

CogDB can load a graph stored as N-Triples, a serialization format for RDF. See [Wikipedia](https://en.wikipedia.org/wiki/N-Triples), [W3C](https://www.w3.org/TR/n-triples/) for details. 

In short, an N-Triple is sequence of subject, predicate and object in a single line that defines a connection between two vertices:

  ```vertex <predicate> vertex```

[Learn more about RDF triples](https://www.w3.org/TR/rdf-concepts/#:~:text=An%20RDF%20triple%20contains%20three,literal%20or%20a%20blank%20node)

```python
from cog.torque import Graph
g = Graph(graph_name="people")
g.load_triples("/path/to/triples.nt", "people")
```

#### Edgelist file
```python
from cog.torque import Graph
g = Graph(graph_name="people")
g.load_edgelist("/path/to/edgelist", "people")
```

## Config

If no config is provided when creating a Cog instance, it will use the defaults:

```
COG_PATH_PREFIX = "/tmp"
COG_HOME = "cog-test"
```

### Example updating config

```python
from cog import config

config.COG_HOME = "app1_home"
data = ('user_data:id=1', '{"firstname":"Hari","lastname":"seldon"}')
cog = Cog(config)
cog.create_or_load_namespace("test")
cog.create_table("db_test", "test")
cog.put(data)
scanner = cog.scanner()
for r in scanner:
 print
 r

```

## Benchmark

![Put Perf](notes/bench.png)

### Performance Results

Run benchmarks with: `python3 test/benchmark.py`

| Graph Type | Edges | Speed (edges/s) |
|------------|-------|----------------|
| Chain (batch) | 5,000 | 4,377 |
| Social network | 12,492 | 3,233 |
| Dense graph | 985 | 2,585 |
| Chain (individual) | 5,000 | 2,712 |

**Batch vs Individual Insert:**
- 1.6x faster at 5,000 edges
- Read performance: 20,000+ ops/second
