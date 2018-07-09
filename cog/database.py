'''
Created on Nov 25, 2017

@author: arun
'''

import logging
from logging.config import dictConfig
import os
import os.path
import pickle
import socket
import uuid
from core import Table
from core import Indexer
from core import Store
import config as cfg


# '''
# Read index file, record records stored in 'store' and write out new store file. Update index with position in store.
# '''
# class Compaction:


class Cog:

    def __init__(self, db_path=None, config=cfg):
        if not db_path:
            config.COG_PATH_PREFIX = db_path
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

    ''' initiates cog instance - called the 'c instance' for the first time '''
    def init_instance(self, db_name):

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

    '''load existing name spaces'''
    # def load_namespaces(self):

    def create_namespace(self,namespace):
        if not os.path.exists(self.config.cog_data_dir(namespace)):
            os.mkdir(self.config.cog_data_dir(namespace))
            self.logger.info("Created new namespace: "+self.config.cog_data_dir(namespace))
            '''add namespace to dict'''
            self.namespaces[namespace] = {}
        else:
            self.logger.info("Using existing namespace: "+self.config.cog_data_dir(namespace))
        self.current_namespace = namespace

    def create_table(self, name, namespace):
        table = Table(name,namespace,self.instance_id)
        store = Store(table,self.config,self.logger)
        indexer = Indexer(table,self.config,self.logger)
        self.namespaces[namespace] = {}
        self.namespaces[namespace][table] = (indexer,store)
        self.current_namespace = namespace
        self.current_table = table
        self.current_indexer = indexer
        self.current_store = store

    def put(self,data):
        assert type(data[0]) is str, "Only string type is supported is currently supported."
        assert type(data[1]) is str, "Only string type is supported is currently supported."
        position=self.current_store.save(data)
        self.current_indexer.put(data[0],position,self.current_store)

    def get(self,key):
        return self.current_indexer.get(key, self.current_store)

    def scanner(self, sfilter=None):
        scanner = self.current_indexer.scanner(self.current_store)
        for r in scanner:
            if sfilter:
                yield sfilter.process(r[1][1])
            else:
                yield r[1]

    def delete(self, key):
        self.current_indexer.delete(key, self.current_store)
