import marshal
import mmap
import sys,traceback
import os.path
import os
from os import mkdir
import uuid
import socket
import pickle
from operator import pos
import struct
from block import Block
from block import BLOCK_LEN
import logging
from logging.config import dictConfig

class Table:
    
    def __init__(self, name, db_name, db_instance_id):
        self.name = name
        self.db_name = db_name
        self.db_instance_id = db_instance_id


class Index:
    
    def __init__(self,table,config, logger):
        self.logger = logger
        self.table = table
        self.config = config
        self.name=self.config.cog_index(table.db_name,table.name,table.db_instance_id)
        self.empty_block = '-1'.zfill(self.config.INDEX_BLOCK_LEN)
        if not os.path.exists(self.name):
            print "creating index..."
            f=open(self.name,'wb+')
            i=0
            e_blocks=[]
            while(i<config.INDEX_CAPACITY):
                e_blocks.append(self.empty_block)
                i+=1
            f.write(b''.join(e_blocks))   
            self.file_limit=f.tell()
            f.close()
            self.logger.info("new index with capacity"+str(config.INDEX_CAPACITY)+"created: "+self.name)
            print "Done."

        self.db=open(self.name,'r+b')
        self.db_mem=mmap.mmap(self.db.fileno(), 0)
        
    def put(self, key, store_position, store):
        index_position=self.get_index(key)
        current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN].strip()
        
        while(current_block != self.empty_block):
            record = store.read(int(current_block))
            if(record[1][0]==key):
                self.logger.debug("Updating index: "+self.name)
                break
            else:
                index_position += self.config.INDEX_BLOCK_LEN
                current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        #if an free index block is found, then write key to at that position.
        self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]=str(store_position).rjust(self.config.INDEX_BLOCK_LEN)
        self.logger.debug("indexed "+key+" @: "+str(index_position) + " : store position: "+str(store_position))
        #need to handle capacity overflow condition
        return index_position
    
    def get_index(self,key):
        num=hash(key) % ((sys.maxsize + 1) * 2)
        logging.debug("hash for: "+key+" : "+str(num))
        index=(self.config.INDEX_BLOCK_LEN*(max( (num%self.config.INDEX_CAPACITY)-1,0) )) #there may be diff when using mem slice vs write (+1 needed)
        logging.debug("offset : "+key+" : "+str(index))
        return index
    
    def get(self, key, store):
        self.logger.debug("Reading index: "+self.name)
        index_position=self.get_index(key)
        current_store_position=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        if(current_store_position == self.empty_block):
            return None
        record = store.read(int(current_store_position))
        if(record == None):
            self.logger.info("Store EOF reached! Record not found.")
            return
        while(key != record[1][0]):
            index_position += self.config.INDEX_BLOCK_LEN
            current_store_position=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
            if(current_store_position == ''):
                self.logger.info("Index EOF reached! Key not found.")
                return None
            record = store.read(int(current_store_position))
            if(record == ''):
                self.logger.info("Store EOF reached! Record not found.")
                return None
        return record
    
    def delete(self, key, store):
        index_position=self.get_index(key)
        current_store_position=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        if(current_store_position == self.empty_block):
            return None
        record = store.read(int(current_store_position))
        if(record == None):
            self.logger.info("Store EOF reached! Record not found.")
            return
        while(key != record[1][0]):
            index_position += self.config.INDEX_BLOCK_LEN
            current_store_position=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
            if(current_store_position == ''):
                self.logger.info("Index EOF reached! Key not found.")
                return
            record = store.read(int(current_store_position))
            if(record == ''):
                self.logger.info("Store EOF reached! Record not found.")
                return
        
        self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]=self.empty_block
    
class Store:
    
    def __init__(self,table,config,logger):
        self.logger = logger
        self.table = table
        self.config = config
        self.store=self.config.cog_store(table.db_name,table.name,table.db_instance_id)
        temp=open(self.store,'a')# create if not exist
        temp.close()
        self.store_file=open(self.store,'rb+')
        logger.info("Store for file init: "+self.store)
    
    def save(self,kv):
        """Store data"""
        self.store_file.seek(0, 2)
        store_position=self.store_file.tell()
        record=marshal.dumps(kv)
        length=str(len(record))
        self.store_file.seek(0, 2)
        self.store_file.write("0")#delete bit
        self.store_file.write(length)
        self.store_file.write('\x1F')#unit seperator
        self.store_file.write(record)
        self.store_file.flush()
        return store_position
    
    def read(self, position):
        self.store_file.seek(position)
        tombstone=self.store_file.read(1)
        c=self.store_file.read(1)
        data=[c]
        while(c !='\x1F'):
            data.append(c)
            c = self.store_file.read(1)
            if(c==''):
                self.logger.debug("EOF store file! Data read error.")
                return None
            
        length=int("".join(data))
        record=marshal.loads(self.store_file.read(length))
        return (tombstone,record)
        
        
class Indexer:
    '''
    Manager indexes. Creates new index when an index is full.
    Searches all indexes for ger requests.
    Provides same get/put/del method as single index but over multuple files. 
    '''

