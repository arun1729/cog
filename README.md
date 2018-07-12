[![PyPI version](https://badge.fury.io/py/cogdb.svg)](https://badge.fury.io/py/cogdb)
# Cog - A persistent hashtable implemented purely in Python.
# ![ScreenShot](/cog-logo.png)


## Installing Cog
```
pip install cogdb
```

Cog is a simple key value store based on persistent hashmap. Every record inserted into Cog is directly persisted on to disk. Cog stores and retrieves data based on hash values of keys, therefore it can perform fast look ups (O(1) avg). Cog also provides fast (O(1) avg) inserts. It is written purely in Python so it has no complex dependencies.


## Cog is easy:
```

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

