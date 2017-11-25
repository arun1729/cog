from core import Index
from core import Store
from core import Table
from database import Cog
import config
import logging

from logging.config import dictConfig

data = ("new super data","super new old stuff")
cogdb = Cog(config)
cogdb.create_table("data_table")
cogdb.put(data[0])
print cogdb.get(data[0])

# dictConfig(config.logging_config)
# logger = logging.getLogger()
# 
# data = ("new super data","super new old stuff")
# 
# table = Table("testdb","test_table","test_xcvzdfsadx")
# 
# store = Store(table,config,logger)
# index = Index(table,config,logger)
# 
# 
# position=store.save(data)
# print "stored"
#    
# index.put(data[0],position,store)
# print "indexed"
# 
# index.delete(data[0],store)
# 
# data=index.get(data[0], store)
# print "retrieved data: "+str(data)
