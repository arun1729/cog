'''
Created on Nov 25, 2017

@author: arun
'''

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
import sys, traceback
import uuid
from core import Table
from core import Index
from core import Store
from block import BLOCK_LEN
from block import Block


class Compaction:
    '''
    Read index file, reacd records stored in 'store' and write out new store file. Update index with position in store. 
    '''

class Cog:
    
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
        self.db_name=db_name
        self.table = Table(db_name,table_name,self.instance_id)
        self.index = Index(self.table,self.config,self.logger)
        self.store = Store(self.table,self.config,self.logger)
    
    def init_instance(self, db_name):
        """ initiates cog instance - called the 'c instance' for the first time"""
#
        instance_id=str(uuid.uuid4())
        if not os.path.exists(self.config.cog_instance_sys_dir()): os.makedirs(self.config.cog_instance_sys_dir())

        m_file=dict()
        m_file["m_instance_id"]=instance_id
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
                
    def create_table(self, name, db_name=None):
        if(not db_name):
            db_name=self.db_name
        self.table = Table(name,self.db_name,self.instance_id)
        self.store = Store(self.table,self.config,self.logger)
        self.index = Index(self.table,self.config,self.logger)
             
    def put(self,data):
        assert type(data[0]) is str, "Only string type is supported is currently supported."
        assert type(data[1]) is str, "Only string type is supported is currently supported."
        position=self.store.save(data)
        self.index.put(data[0],position,self.store)
        
    def get(self,key):
        return self.index.get(key, self.store)
    
    def delete(self, key):
        self.index.delete(key,self.store)
        
        