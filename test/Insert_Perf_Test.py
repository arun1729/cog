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
import timeit
import unittest
import matplotlib.pyplot as plt

#!!! clean namespace before running test.
#need OPS/s
class TestIndexerPerf(unittest.TestCase):
    def test_indexer(self):
        if (not os.path.exists("/tmp/cog-test/perf_ns/")):
            os.mkdir("/tmp/cog-test/perf_ns/")

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("perf_table","perf_ns","instance_1")

        store = Store(table,config,logger)
        indexer = Indexer(table,config,logger)

        max_range=1000000

        insert_perf=[]
        overall_start_time = timeit.default_timer()
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)

            start_time = timeit.default_timer()
            position=store.save(expected_data)
            indexer.put(expected_data[0],position,store)
            elapsed = timeit.default_timer() - start_time
            insert_perf.append(elapsed*1000.0)
            #print "Test progress: "+str(i*100.0/max_range)
        plt.xlim([-1,max_range])
        plt.ylim([0,2])
        plt.xlabel("put call")
        plt.ylabel("ms")
        plt.plot(insert_perf)
        plt.savefig("test.png")

if __name__ == '__main__':
    unittest.main()
