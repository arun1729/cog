from context import cog
from cog.core import Index
from cog.core import Store
from cog.core import Table
from cog.database import Cog
from cog import config
import logging
import os
import json
from logging.config import dictConfig

import unittest

def filter(jsn):
    d=json.loads(jsn[1])
    return d["firstname"]

class TestDB(unittest.TestCase):

    def test_db(self):
        data = ('user100','{"firstname":"Hari","lastname":"seldon"}')
        cogdb = Cog(config)
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        scanner = cogdb.scanner(filter)
        for r in scanner:
            print r


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
