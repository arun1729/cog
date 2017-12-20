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
    Read index file, record records stored in 'store' and write out new store file. Update index with position in store.
    '''

class Cog:

    def __init__(self,config):
        dictConfig(config.logging_config)
        self.logger = logging.getLogger()
        self.config=config
        self.logger.info("Cog init.")
        self.namespaces = {}
        '''creates Cog instance files.'''
        if os.path.exists(self.config.cog_instance_sys_file()):
            f=open(self.config.cog_instance_sys_file(),"rb")
            self.m_info=pickle.load(f)
            self.instance_id=self.m_info["m_instance_id"]
            f.close()
        else:
            self.instance_id=self.init_instance("default")

        '''Create default database and table.'''
        self.create_namespace("default")
        self.create_table("default", "default")

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

    def load_namespaces(self):
        '''load existing name spaces'''

    def create_namespace(self,namespace):
        if not os.path.exists(self.config.cog_data_dir(namespace)):
            os.mkdir(self.config.cog_data_dir(namespace))
            '''add namespace to dict'''
            self.namespaces[namespace] = {}
        self.current_namespace = namespace

    def create_table(self, name, namespace):
        table = Table(name,namespace,self.instance_id)
        store = Store(table,self.config,self.logger)
        index = Index(table,self.config,self.logger)
        self.namespaces[namespace] = {}
        self.namespaces[namespace][table] = (index,store)
        self.current_namespace = namespace
        self.current_table = table

    def put(self,data):
        assert type(data[0]) is str, "Only string type is supported is currently supported."
        assert type(data[1]) is str, "Only string type is supported is currently supported."
        ts = self.namespaces[self.current_namespace][self.current_table]
        position=ts[1].save(data)
        ts[0].put(data[0],position,ts[1])

    def get(self,key):
        ts = self.namespaces[self.current_namespace][self.current_table]
        return ts[0].get(key, ts[1])

    # def scan(columns=None):
    #     if(columns):
    #         for c in columns:
    #             print c
    #     else:
    #         print "not implemented."

    def delete(self, key):
        ts = self.namespaces[self.current_namespace][self.current_table]
        ts[0].delete(key,ts[1])
