from context import cog
from cog.core import Index
from cog.core import Store
from cog.core import Table
from cog.database import Cog
from cog import config
import logging
import os
from logging.config import dictConfig

import unittest

class TestDB(unittest.TestCase):

    def test_db(self):
        data = ("new super data","super new old stuff")
        cogdb = Cog(config)
        cogdb.create_namespace("testspace")
        cogdb.create_table("data_table", "testspace")
        cogdb.put(data)
        print cogdb.get(data[0])
        cogdb.delete(data[0])
        print cogdb.get(data[0])


        cogdb.create_namespace("newtestspace")
        cogdb.create_table("data_table", "newtestspace")
        cogdb.put(data)
        print cogdb.get(data[0])
        cogdb.delete(data[0])
        print cogdb.get(data[0])

if __name__ == '__main__':
    unittest.main()
    
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
