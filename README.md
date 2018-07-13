[![PyPI version](https://badge.fury.io/py/cogdb.svg)](https://badge.fury.io/py/cogdb) [![Build Status](https://travis-ci.org/arun1729/cog.svg?branch=master)](https://travis-ci.org/arun1729/cog)
# Cog - A persistent hashtable implemented purely in Python.
# ![ScreenShot](/cog-logo.png)


## Installing Cog
```
pip install cogdb
```

Cog is a simple key value store based on persistent hashmap. Every record inserted into Cog is directly persisted on to disk. Cog stores and retrieves data based on hash values of keys, therefore it can perform fast look ups (O(1) avg). Cog also provides fast (O(1) avg) inserts. It is written purely in Python so it has no complex dependencies.


## Cog is easy:
```python

from cog.database import Cog

cogdb = Cog('path/to/dbdir')

# create a namespace
cogdb.create_namespace("my_namespace")

# create new table
cogdb.create_table("new_db", "my_namespace")

# put some data
cogdb.put(('key','val'))

# retrieve data 
cogdb.get('key')

# put some more data
cogdb.put(('key2','val2'))

# scan
scanner = cogdb.scanner()
for r in scanner:
    print r
    
# delete data
cogdb.delete('key1')

```

## Config

If no config is provided while creating a Cog instance, it will use the defaults:

```
COG_PATH_PREFIX = "/tmp"
COG_HOME = "cog-test"
```

### Example updating config

```python
from cog import config
config.COG_HOME = "app1_home"
data = ('user_data:id=1','{"firstname":"Hari","lastname":"seldon"}')
cog = Cog(config)
cog.create_namespace("test")
cog.create_table("db_test", "test")
cog.put(data)
scanner = cog.scanner()
for r in scanner:
    print r

```

### Advance config

```
INDEX_BLOCK_LEN=10
INDEX_CAPACITY = 2000
INDEX_LOAD_FACTOR = 80
```

Default index capacity of 2000 is on the lower end, it is intend for light usage of Cog such as using it as a hash-table data structure.
For larger indexing use case, INDEX_CAPACITY should be set to larger number otherwise it could lead to too many open index files.

## Performance

Put and Get calls performance:

> ops/s: 15685.4110073

The perf test script is included with the tests: insert_bench.py

INDEX_LOAD_FACTOR on an index determines when a new index file is created, Cog uses linear probing to resolve index collisions.
Higher INDEX_LOAD_FACTOR leads slightly lower performance on operations on index files that have reached the target load.

#### Put performance profile

![Put Perf](put_perf.png)