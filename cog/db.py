from core2 import Indexer
from core2 import Store
from core2 import Table
import config
import logging

from logging.config import dictConfig

# cogdb = Database(config)
# cogdb.put("test",'{"name":"test"}2')
# print cogdb.get("test")
dictConfig(config.logging_config)
logger = logging.getLogger()

data = ("new super data","super new old stuff")

table = Table("testdb","test_table","test_xcvzdfsadx")

store = Store(table,config,logger)
indexer = Indexer(table,config,logger)


position=store.save(data)
print "stored"
   
indexer.index(data[0],position,store)
print "indexed"

indexer.delete(data[0],store)

data=indexer.get(data[0], store)
print "retrieved data: "+str(data)
