from core import Database
import config

cogdb = Database(config)
cogdb.put("test",'{"name":"test"}2')
print cogdb.get("test")
