from cog.core import Table, Record
from cog.core import Table
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
import subprocess
from pathlib import Path

#!!! clean namespace before running test.
#need OPS/s
#read, ops/s: 16784.0337416

DIR_NAME = "TestIndexerPerf"
ver_path = "../setup.py"
if not os.path.exists(ver_path):
    ver_path = "setup.py"
out = str(subprocess.check_output(['grep "version" {}'.format(ver_path)], shell=True))
COG_VERSION = out.split("'")[1]


class TestIndexerPerf(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = "/tmp/"+DIR_NAME+"/test_table/"
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/"+DIR_NAME
        print("*** " + config.CUSTOM_COG_DB_PATH + "\n")

    def test_indexer(self):

        dictConfig(config.logging_config)

        logger = logging.getLogger()
        config.INDEX_CAPACITY = 10003
        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)
        store = table.store
        indexer = table.indexer
        max_range = 100000

        plt.title("CogDB v"+COG_VERSION + " BENCHMARK Total records:" + str(max_range), fontsize=12)

        put_perf=[]

        key_list = []
        total_seconds_put = 0.0
        annotation = " index size: {}\n index_block_len: {}\n store read buffer: {}\n".format(config.INDEX_CAPACITY, config.INDEX_BLOCK_LEN, config.STORE_READ_BUFFER_SIZE)
        print(annotation)
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = Record(key,value)
            key_list.append(key)
            start_time = timeit.default_timer()
            position=store.save(expected_data)
            indexer.put(expected_data.key,position,store)
            elapsed = timeit.default_timer() - start_time
            put_perf.append(elapsed * 1000.0)  # to ms
            total_seconds_put += elapsed
            print("Loading data progress: " + str(i * 100.0 / max_range) + "%", end="\r")

        plt.xlim([-1, max_range])
        plt.ylim([0, 10])
        plt.plot(put_perf, '-r', label="put")
        annotation +="\n put ops/s: " + str(max_range / total_seconds_put)

        total_index_file_size = 0

        for i in table.indexer.index_list:
            total_index_file_size += Path(i.name).stat().st_size

        total_index_file_size = total_index_file_size >> 20
        store_file_size = Path(table.store.store).stat().st_size >> 20
        annotation += "\n index size: {}Mb \n store size: {}Mb ".format(total_index_file_size, store_file_size)

        get_perf = []
        total_seconds_get = 0.0
        i = 0

        for key in key_list:
            start_time = timeit.default_timer()
            indexer.get(key, store)
            elapsed = timeit.default_timer() - start_time
            get_perf.append(elapsed*1000.0) #to ms
            total_seconds_get += elapsed
            print("get test progress: " + str(i * 100.0 / max_range) + "%", end="\r")
            i += 1

        plt.xlim([-1,max_range])
        plt.ylim([0,10])
        plt.ylabel("ms")
        plt.plot(get_perf, '-b', label='get')
        plt.legend(loc="upper right")
        annotation +="\n get ops/s: "+str(max_range/total_seconds_get)
        annotation +='\n num index files: '+str(len(table.indexer.index_list))
        plt.annotate(annotation, xy=(0.05, .5), xycoords='axes fraction')

        notes_path = "../notes/"
        if not os.path.exists(notes_path):
            notes_path = "notes/"
        plt.savefig("{}bench_{}.png".format(notes_path, max_range))
        table.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
