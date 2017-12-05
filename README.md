![ScreenShot](/cog-logo.png)

#
Cog is a persistent key/value micro database written in Python. Cog is simple, easy and fast. 

## API
```

cog = Cog('path/to/dbdir')

cog.create_table('table_name')

put(record)

get(key)

delete()

```


Every record inserted into Cog is directly persisted on to disk. Cog stores and retreives data based on hash values of keys, therfore it can perform fast look ups (O(1) Average case). Cog also provides O(1) (Average case) inserts. It is written purely in Python so it has no dependencies outside. 
