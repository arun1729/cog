![](https://static.pepy.tech/badge/cogdb) [![PyPI version](https://badge.fury.io/py/cogdb.svg)](https://badge.fury.io/py/cogdb) ![Python 3.8](https://img.shields.io/badge/python-3.8+-blue.svg)
 [![Build Status](https://travis-ci.org/arun1729/cog.svg?branch=master)](https://travis-ci.org/arun1729/cog) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![codecov](https://codecov.io/gh/arun1729/cog/branch/master/graph/badge.svg)](https://codecov.io/gh/arun1729/cog)

# Cog - Embedded Graph Database for Python
# ![ScreenShot](/cog-logo.png)
> [cogdb.io](https://cogdb.io)

> New release!: 3.0.0
>
> - New and improved indexing and storage.
> - Introduced caching.
> - Query performance improvements.
> - Bug fixes.
> - Note: Data stored in CogDB 2.xx is not compatible with 3.xx

![ScreenShot](notes/ex2.png)

## Installing Cog
```
pip install cogdb
```
Cog is a persistent embedded graph database implemented purely in Python. Torque is Cog's graph query language. Cog also provides a low level API to its fast persistent key-value store.

Cog is ideal for python applications that does not require a full featured database. Cog can easily be used as a library from within a Python application. Cog be used interactively in an IPython environment like Jupyter notebooks.

Cog can load a graph stored as N-Triples, a serialization format for RDF. See [Wikipedia](https://en.wikipedia.org/wiki/N-Triples), [W3C](https://www.w3.org/TR/n-triples/) for details. 

In short, an N-Triple is sequence of subject, predicate and object in a single line that defines a connection between two vertices:

  ```vertex <predicate> vertex```

[Learn more about RDF triples](https://www.w3.org/TR/rdf-concepts/#:~:text=An%20RDF%20triple%20contains%20three,literal%20or%20a%20blank%20node)


### Creating a graph

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
```

### Create a graph from CSV file

```python
from cog.torque import Graph
g = Graph("books")
g.load_csv('test/test-data/books.csv', "isbn")
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
> {'result': [{'id': 'cool_person'}, {'id': 'fred'}]}

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
> '2'

### See who is following who and create a view of that network
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


## Loading data from a file

#### Triples file
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

## Low level key-value store API:
Every record inserted into Cog's key-value store is directly persisted on to disk. It stores and retrieves data based 
on hash values of the keys, it can perform fast look ups (O(1) avg) and fast (O(1) avg) inserts.

```python

from cog.database import Cog

cogdb = Cog('path/to/dbdir')

# create a namespace
cogdb.create_or_load_namespace("my_namespace")

# create new table
cogdb.create_table("new_db", "my_namespace")

# put some data
cogdb.put(('key', 'val'))

# retrieve data 
cogdb.get('key')

# put some more data
cogdb.put(('key2', 'val2'))

# scan
scanner = cogdb.scanner()
for r in scanner:
 print
 r

# delete data
cogdb.delete('key1')

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

# ![Put Perf](notes/bench.png)
