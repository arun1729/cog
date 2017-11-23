from core2 import Indexer
from core2 import Table
import config
import logging

from logging.config import dictConfig

# cogdb = Database(config)
# cogdb.put("test",'{"name":"test"}2')
# print cogdb.get("test")
dictConfig(config.logging_config)
logger = logging.getLogger()

table = Table("testdb","test_table","test_xcvzdfsadx")
indexer = Indexer(table,config,logger)
indexer.index("test")