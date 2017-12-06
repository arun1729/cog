from context import cog
from cog.core import Index
from cog.core import Store
from cog.core import Table
from cog.core import Indexer
from cog import database
from cog import config
import logging
import os
from logging.config import dictConfig
import string
import random

import unittest

class TestIndexerPerf(unittest.TestCase):
    def test_indexer(self):
        if (not os.path.exists("/tmp/cog-test/perf_ns/")):
            os.mkdir("/tmp/cog-test/perf_ns/")

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("perf_table","perf_ns","instance_1")

        store = Store(table,config,logger)
        indexer = Indexer(table,config,logger)

        max_range=100000
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)

            position=store.save(expected_data)
            indexer.put(expected_data[0],position,store)
            print "Test progress: "+str(i*100.0/max_range)


if __name__ == '__main__':
    unittest.main()
