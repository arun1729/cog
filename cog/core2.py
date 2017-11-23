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

class Database:
    
    def __init__(self,config,db_name="DEFAULT", table_name="DEFAULT"):
        dictConfig(config.logging_config)
        self.logger = logging.getLogger()
        self.config=config
        self.logger.info("Cog init.")
        
        if os.path.exists(self.config.cog_instance_sys_file()):
            f=open(self.config.cog_instance_sys_file(),"rb")
            self.m_info=pickle.load(f)
            self.instance_id=self.m_info["m_instance_id"]
            f.close()
        else:
            self.instance_id=self.init_instance(db_name)
        
        self.create_db(db_name)
        self.table = Table(db_name,table_name,self.instance_id)
        self.indexer = Indexer(self.table,config)
        self.store = Store(self.table,config)
    
    def init_instance(self, db_name):
        """ initiates cog instance - called the 'c instance' for the first time"""
#
        instance_id=str(uuid.uuid4())
        if not os.path.exists(self.config.cog_instance_sys_dir()): os.makedirs(self.config.cog_instance_sys_dir())

        m_file=dict()
        m_file["m_instance_id"]=self.instance_id
        m_file["host_name"]=socket.gethostname()
        m_file["host_ip"]=socket.gethostname()

        f=open(self.config.cog_instance_sys_file(),'wb')
        pickle.dump(m_file,f)
        f.close()
        self.logger.info("Cog sys file created.")
        os.mkdir(self.config.cog_data_dir(db_name))
        self.logger.info("Database created: "+db_name)
        self.logger.info("done.")
        return instance_id;

    def create_db(self,db_name):
        if not os.path.exists(self.config.cog_data_dir(db_name)):
            os.mkdir(self.config.cog_data_dir(db_name))
                
             
    def put(self,key,value):
        assert type(key) is str, "Only string type is supported is currenlty supported."
        assert type(value) is str, "Only string type is supported is currenlty supported."
        
        self.indexer.index((key,value))
        self.store.save((key,value));
    

class Table:
    
    def __init__(self, name, db_name, db_instance_id):
        self.name = name
        self.db_name = db_name
        self.db_instance_id = db_instance_id


class Indexer:
    
    def __init__(self,table,config):
        self.table = table
        self.config = config
        self.name=self.config.cog_index(table.db_name,table.name,table.db_instance_id)
        if not os.path.exists(self.name):
            print "creating index..."
            f=open(self.name,'wb+')
            i=0
            e_blocks=[]
            empty_block = '0'.zfill(config.INDEX_BLOCK_LEN)
            while(i<config.index_capacity):
                e_blocks.append(empty_block)
                i+=1
            f.write(b''.join(e_blocks))   
            self.file_limit=f.tell()
            f.close()
            self.logger.info("new index with capacity"+str(config.index_capacity)+"created: "+self.name)
            print "Done."
        else:
            self.file_limit=self.getLen(self.name)

        self.logger.debug("Index size: "+str(self.file_limit))
        self.db=open(self.index,'r+b')
        self.db_mem=mmap.mmap(self.db.fileno(), 0)
        
    def index(self, key):
        index_position=self.get_index(key)
        current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        empty_block = '0'.zfill(self.config.INDEX_BLOCK_LEN)
        while(current_block != empty_block):
            if(current_block == key):
                print "updating existing record"
            else:
                index_position += self.config.INDEX_BLOCK_LEN
                current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        #if an free index block is found, then write key to at that position.
        self.db_mem[index_position]=key
        #need to handle capacity overflow condition
        return index_position
    
    def get_index(self,key):
        num=hash(key) % ((sys.maxsize + 1) * 2)
        logging.debug("hash for: "+key+" : "+str(num))
        index=(self.config.INDEX_BLOCK_LEN*(max( (num%self.config.capacity)-1,0) )) #there may be diff when using mem slice vs write (+1 needed)
        logging.debug("offset : "+key+" : "+str(index))
        return index
    
    def get(self, key):
        index_position=self.get_index(key)
        current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        while(current_block != key):
            index_position += self.config.INDEX_BLOCK_LEN
            current_block=self.db_mem[index_position:index_position+self.config.INDEX_BLOCK_LEN]
        
        return current_block
                
class Store:
    
    def __init__(self,table,config):
        self.table = table
        self.config = config
        self.store=self.config.cog_store(table.db_name,table.name,table.db_instance_id)
        self.store_file=open(self.store,'ab+')
    
    def save(self,kv):
        """Store data"""
        self.store_file.seek(0, 2)
        record=marshal.dumps(kv)
        length=str(len(record))
        self.store_file.seek(0, 2)
        self.store_file.write(0)
        self.store_file.write(length)
        self.store_file.write('1F')#unit seperator
        self.store_file.write(record)

          


        
        
#         pos = self.store_file.tell()
#         logging.debug("Store position for: "+key+" = "+str(pos))
#         """Store index """
#         self.db_mem[index]=
# 
#         self.db_mem[pos]=0 #not deleted flag by default
#         self.db_mem[pos+1]=length # second position for 2 bytes is the length of the content to be written.
#         self.db_mem[pos+3]=record # record is written at the third position            

