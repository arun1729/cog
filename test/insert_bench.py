from cog.core import Store
from cog.core import Table
from cog.core import Indexer
from cog import config
import logging
import os
import shutil
from logging.config import dictConfig
import string
import random
import timeit
import unittest
import matplotlib.pyplot as plt


#!!! clean namespace before running test.
#need OPS/s
#read, ops/s: 16784.0337416

DIR_NAME = "TestIndexerPerf"


class TestIndexerPerf(unittest.TestCase):

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/perf_ns/")

        config.COG_HOME = DIR_NAME
        config.INDEX_CAPACITY = 100000

    def test_indexer(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("perf_table","perf_ns","instance_1")

        store = Store(table,config,logger)
        indexer = Indexer(table,config,logger)

        max_range=1000000

        insert_perf=[]
        overall_start_time = timeit.default_timer()
        total_seconds=0.0
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)

            start_time = timeit.default_timer()
            position=store.save(expected_data)
            indexer.put(expected_data[0],position,store)
            elapsed = timeit.default_timer() - start_time
            insert_perf.append(elapsed*1000.0) #to ms
            total_seconds += elapsed
            #print "Test progress: "+str(i*100.0/max_range)
        plt.xlim([-1,max_range])
        plt.ylim([0,2])
        plt.xlabel("put call")
        plt.ylabel("ms")
        plt.plot(insert_perf)
        plt.savefig("test.png")
        print "ops/s: "+str(max_range/total_seconds)

    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()
