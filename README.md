# ![ScreenShot](/cog-logo.png)


## Installing Cog
```
pip install cogdb

```

Cog is a simple key value store based on persistent hashmap. Every record inserted into Cog is directly persisted on to disk. Cog stores and retrieves data based on hash values of keys, therefore it can perform fast look ups (O(1) Average case). Cog also provides O(1) (Average case) inserts. It is written purely in Python so it has no complex dependencies.


## Cog is easy to use
```

from cog.database import Cog

cogdb = Cog('path/to/dbdir')

cogdb.create_namespace("my_namespace")

cogdb.create_table("new_db", "my_namespace")

cogdb.put(('key','val'))

cogdb.get('key')

cogdb.put(('key2','val2'))

scanner = cogdb.scanner()
for r in scanner:
    print r

cogdb.delete('key1')

```

