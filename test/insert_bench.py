import logging
import os
import random
import shutil
import string
import timeit
import unittest
from logging.config import dictConfig

import matplotlib.pyplot as plt

from cog import config
from cog.core import Table

#!!! clean namespace before running test.
#need OPS/s
#read, ops/s: 16784.0337416

DIR_NAME = "TestIndexerPerf"


class TestIndexerPerf(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = "/tmp/"+DIR_NAME+"/test_table/"
        if not os.path.exists(path):
            os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/"+DIR_NAME
        print("*** " + config.CUSTOM_COG_DB_PATH + "\n")

    def test_indexer(self):

        dictConfig(config.logging_config)
        config.INDEX_CAPACITY=1000000
        logger = logging.getLogger()
        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)
        store = table.store
        indexer = table.indexer
        max_range=100000

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
            print("Test progress: "+str(i*100.0/max_range) + "%", end="\r")
        plt.xlim([-1,max_range])
        plt.ylim([0,2])
        plt.xlabel("put call")
        plt.ylabel("ms")
        plt.plot(insert_perf)
        plt.savefig("insert_bench.png")
        print("\n total index files: " + str(len(indexer.index_list)))
        print("\n ops/s: "+str(max_range/total_seconds))
        table.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
