from core import Database
import config

cogdb = Database(config)
cogdb.set("test","test")
print cogdb.get("test")
