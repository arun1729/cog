from core import Index
from core import Store
from core import Table
from database import Cog
import config
import logging
import os

from logging.config import dictConfig

if (not os.path.exists("/tmp/cog-test/test_table/")):
    os.mkdir("/tmp/cog-test/test_table/")
    
dictConfig(config.logging_config)
logger = logging.getLogger()
 
data = ("new super data","super new old stuff")
 
table = Table("testdb","test_table","test_xcvzdfsadx")
 
store = Store(table,config,logger)
index = Index(table,config,logger)
 
 
position=store.save(data)
print "stored"
    
index.put(data[0],position,store)
print "indexed"
 

data=index.get(data[0], store)
print "retrieved data: "+str(data)

index.delete(data[0],store)
 
