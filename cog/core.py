import logging
from logging.config import dictConfig
import marshal
import mmap
from operator import pos
from os import mkdir
import os
import os.path
import pickle
import socket
import struct
import sys
import traceback
import uuid


class Table:

    def __init__(self, name, db_name, db_instance_id, column_mode=False):
        self.column_mode=column_mode
        self.name = name
        self.db_name = db_name
        self.db_instance_id = db_instance_id


class Index:

    def __init__(self, table, config, logger, index_id=0):
        self.logger = logger
        self.table = table
        self.config = config
        self.name = self.config.cog_index(table.db_name, table.name, table.db_instance_id, index_id)
        self.empty_block = '-1'.zfill(self.config.INDEX_BLOCK_LEN)
        if not os.path.exists(self.name):
            self.logger.info("creating index...")
            f = open(self.name, 'wb+')
            i = 0
            e_blocks = []
            while(i < config.INDEX_CAPACITY):
                e_blocks.append(self.empty_block)
                i += 1
            f.write(b''.join(e_blocks))
            self.file_limit = f.tell()
            f.close()
            self.logger.info("new index with capacity" + str(config.INDEX_CAPACITY) + "created: " + self.name)
        else:
            logger.info("Index: "+self.name+" already exists.")

        self.db = open(self.name, 'r+b')
        self.db_mem = mmap.mmap(self.db.fileno(), 0)

        self.load = 0
        self.db_mem.seek(0)
        current_block = self.db_mem.read(self.config.INDEX_BLOCK_LEN)
        #computes current load on index file.
        while(current_block != ''):
            if(current_block != self.empty_block):
                self.load += 1
            current_block = self.db_mem.read(self.config.INDEX_BLOCK_LEN)

        # self.db_mem.seek(0)

    def get_load(self):
        return self.load

    def put(self, key, store_position, store):
        orig_position = self.get_index(key)
        probe_position = orig_position
        data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN].strip()
        self.logger.debug("PUT: probe position: "+str(probe_position)+" value = "+data_at_prob_position)
        looped_back=False
        while(data_at_prob_position != self.empty_block):
            if(looped_back):# Terminating condition
                if(probe_position >= orig_position or data_at_prob_position == ''):
                    self.logger.warn("Unable to index data. Index capacity reached!: "+self.name)
                    return None
            if(data_at_prob_position == ''):#check if EOF reached.
                    probe_position=0
                    data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN].strip()
                    looped_back=True
                    self.logger.debug("PUT: LOOP BACK to position: "+str(probe_position)+" value = "+data_at_prob_position)
                    continue

            record = store.read(int(data_at_prob_position))
#             print "put store record check: "+str(record)
            if(record[1][0] == key):
                self.logger.debug("PUT: Updating index: " + self.name)
                break
            else:
                probe_position += self.config.INDEX_BLOCK_LEN
                data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                self.logger.debug("PUT: probing next position: "+str(probe_position)+" value = "+data_at_prob_position)
        # if an free index block is found, then write key to at that position.
        self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN] = str(store_position).rjust(self.config.INDEX_BLOCK_LEN)
        self.logger.debug("indexed " + key + " @: " + str(probe_position) + " : store position: " + str(store_position))
        self.load += 1
        return probe_position

    def get_index(self, key):
        num = hash(key) % ((sys.maxsize + 1) * 2)
        logging.debug("hash for: " + key + " : " + str(num))
        # there may be diff when using mem slice vs write (+1 needed)
        index = (self.config.INDEX_BLOCK_LEN *
                 (max((num % self.config.INDEX_CAPACITY) - 1, 0)))
        logging.debug("offset : " + key + " : " + str(index))
        return index

    def get(self, key, store):
        self.logger.debug("GET: Reading index: " + self.name)
        orig_position = self.get_index(key)
        probe_position = orig_position
        record = None
        looped_back=False

        while(True):
            data_at_probe_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
            self.logger.debug("GET: probe position @1: "+str(probe_position)+" value = "+data_at_probe_position)

            if(data_at_probe_position == ''):#EOF index
                if(not looped_back):
                    probe_position = 0
                    data_at_probe_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                    self.logger.debug("GET: LOOP BACK: "+str(probe_position)+" value = "+data_at_probe_position)
                    looped_back = True
                else:
                    self.logger.info("Index EOF reached! Key not found.")
                    return None

            if(data_at_probe_position == self.empty_block):
                probe_position += self.config.INDEX_BLOCK_LEN
                self.logger.debug("GET: skipping empty block")
                continue

            record = store.read(int(data_at_probe_position))

            if(record == ''):#EOF store
                self.logger.error("Store EOF reached! Indexed record not found.")
                return None

            if(record !=None and key == record[1][0]):# found record!
                return record

            probe_position += self.config.INDEX_BLOCK_LEN

    '''
        Iterates through record in itr_store.
    '''
    def scanner(self,store):
        scan_cursor = 0
        while(True):
            data_at_position = self.db_mem[scan_cursor:scan_cursor + self.config.INDEX_BLOCK_LEN]
            if(data_at_position == ''):#EOF index
                self.logger.info("Index EOF reached! Scan terminated.")
                raise StopIteration
            if(data_at_position == self.empty_block):
                scan_cursor += self.config.INDEX_BLOCK_LEN
                self.logger.debug("GET: skipping empty block during iteration.")
                continue
            record = store.read(int(data_at_position))
            if(record == ''):#EOF store
                self.logger.error("Store EOF reached! Iteration terminated.")
                raise StopIteration
            yield record
            scan_cursor += self.config.INDEX_BLOCK_LEN

    def delete(self, key, store):
        index_position = self.get_index(key)
        current_store_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
        if(current_store_position == self.empty_block):
            return False
        record = store.read(int(current_store_position))
        if(record == None):
            self.logger.info("Store EOF reached! Record not found.")
            return False
        while(key != record[1][0]):
            index_position += self.config.INDEX_BLOCK_LEN
            current_store_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
            if(current_store_position == ''):
                self.logger.info("Index EOF reached! Key not found.")
                return False
            record = store.read(int(current_store_position))
            if(record == ''):
                self.logger.info("Store EOF reached! Record not found.")
                return False

        self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN] = self.empty_block
        self.load -= 1
        return True

    def flush(self):
        self.db_mem.flush()


class Store:

    def __init__(self, table, config, logger):
        self.logger = logger
        self.table = table
        self.config = config
        self.store = self.config.cog_store(
            table.db_name, table.name, table.db_instance_id)
        temp = open(self.store, 'a')  # create if not exist
        temp.close()
        self.store_file = open(self.store, 'rb+')
        logger.info("Store for file init: " + self.store)

    def save(self, kv):
        """Store data"""
        self.store_file.seek(0, 2)
        store_position = self.store_file.tell()
        record = marshal.dumps(kv)
        length = str(len(record))
        self.store_file.seek(0, 2)
        self.store_file.write("0")  # delete bit
        self.store_file.write(length)
        self.store_file.write('\x1F')  # unit seperator
        self.store_file.write(record)
        self.store_file.flush()
        return store_position

    def read(self, position):
        self.store_file.seek(position)
        tombstone = self.store_file.read(1)
        c = self.store_file.read(1)
        data = [c]
        while(c != '\x1F'):
            data.append(c)
            c = self.store_file.read(1)
            if(c == ''):
                self.logger.debug("EOF store file! Data read error.")
                return None

        length = int("".join(data))
        record = marshal.loads(self.store_file.read(length))
        return (tombstone, record)


class Indexer:
    '''
    Manages indexes. Creates new index when an index is full.
    Searches all indexes for get requests.
    Provides same get/put/del method as single index but over multuple files.
    '''

    def __init__(self, table, config, logger):
        self.table = table
        self.config = config
        self.logger = logger
        self.index_list = []
        self.index_id = 0
        self.load_indexes()
        #if no index currenlty exist, create new live index.
        if(len(self.index_list) == 0 ):
            self.index_list.append(Index(table, config, logger, self.index_id))
            self.live_index = self.index_list[self.index_id]

    def load_indexes(self):
        for f in os.listdir(self.config.cog_data_dir(self.table.db_name)):
            if(self.config.INDEX in f):
                self.logger.info("Loading index "+f)
                id = self.config.index_id(f)
                index = Index(self.table, self.config, self.logger, id)
                self.index_list.append(index)
                #make the latest index the live index.
                if(id >= self.index_id):
                    self.index_id = id
                    self.live_index = index

    def put(self, key, store_position, store):

        while(True):
            if(self.live_index.get_load() * 100.0 / self.config.INDEX_CAPACITY > self.config.INDEX_LOAD_FACTOR):
                self.live_index.flush()
                self.index_id += 1
                self.logger.info("Index load reached, creating new index file: "+str(self.index_id))
                self.index_list.append(Index(self.table, self.config, self.logger, self.index_id))
                self.live_index = self.index_list[self.index_id]
                self.live_index_usage = self.live_index.get_load()

            resp = self.live_index.put(key, store_position, store)

            if(resp != None):
                self.logger.debug("Key: "+key+" indexed in: "+self.live_index.name)
                break

    def get(self, key, store):
        record = None
        for idx in self.index_list:
            self.logger.info("GET: looking in index: "+idx.name)
            record=idx.get(key, store)
            if(record):
                return record

        self.logger.warn("Key: "+key+ " not found in any index!")
        return None

    def scanner(self, store):
        for idx in self.index_list:
            self.logger.info("SCAN: index: "+idx.name)
            for r in idx.scanner(store):
                yield r


    def delete(self, key, store):
        for idx in self.index_list:
            isDeleted=idx.delete(key, store)
            if(isDeleted): return True
