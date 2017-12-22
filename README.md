# ![ScreenShot](/cog-logo.png)

Cog is a persistent key/value micro database written in Python. Cog is simple, easy and fast. 
Can uses works with raw text, JSONs and images. Cog can be used as a fast persistant key/value store. JSONs stored in Cog can be queried as columns using SQL.

## Cog is easy to use
```

cog = Cog('path/to/dbdir')

cog.create_table('table_name')

cog.put(record)

cog.get(key)

cog.delete(key)

```

## Installing Cog
```
pip install cogdb
```

Every record inserted into Cog is directly persisted on to disk. Cog stores and retreives data based on hash values of keys, therfore it can perform fast look ups (O(1) Average case). Cog also provides O(1) (Average case) inserts. It is written purely in Python so it has no dependencies outside. 

## Prefomance
