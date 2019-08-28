'''
Created on Nov 25, 2017

@author: arun
'''

import logging
from logging.config import dictConfig
import os
import os.path
from os import listdir
from os.path import isfile
from os.path import join
import ast
import pickle
import socket
import uuid
from core import Table
import config as cfg


# '''
# Read index file, record records stored in 'store' and write out new store file. Update index with position in store.
# '''
# class Compaction:

def out_nodes(v):
    return (v + "__:out:__")

def in_nodes(v):
    return (v + "__:in:__")


class Cog:

    def __init__(self, db_path=None, config=cfg):
        if db_path is not None:
            db_path = db_path + cfg.COG_ROOT if db_path.endswith("/") else db_path + "/" + cfg.COG_ROOT
            try:
                os.makedirs(db_path)
            except OSError:
                if not os.path.isdir(db_path):
                    raise
            config.CUSTOM_COG_DB_PATH = db_path
        dictConfig(config.logging_config)
        self.logger = logging.getLogger()
        self.config = config
        self.logger.info("Cog init.")
        self.namespaces = {}
        '''creates Cog instance files.'''
        if os.path.exists(self.config.cog_instance_sys_file()):
            f=open(self.config.cog_instance_sys_file(),"rb")
            self.m_info=pickle.load(f)
            self.instance_id=self.m_info["m_instance_id"]
            f.close()
        else:
            # only single database is currently supported.
            self.instance_id = self.init_instance(config.COG_DEFAULT_NAMESPACE)

        '''Create default namespace and table.'''
        self.create_namespace(self.config.COG_DEFAULT_NAMESPACE)
        self.create_or_load_table("default", self.config.COG_DEFAULT_NAMESPACE)

        '''Load all table names but lazy load actual tables on request.'''
        self.namespaces.update(dict.fromkeys(self.list_tables(), None))


    def init_instance(self, namespace):
        '''
        Initiates cog instance - called the 'c instance' for the first time
        :param namespace:
        :return:
        '''

        instance_id=str(uuid.uuid4())
        if not os.path.exists(self.config.cog_instance_sys_dir()): os.makedirs(self.config.cog_instance_sys_dir())

        m_file=dict()
        m_file["m_instance_id"] = instance_id
        m_file["host_name"] = socket.gethostname()
        m_file["host_ip"] = socket.gethostname()

        f=open(self.config.cog_instance_sys_file(),'wb')
        pickle.dump(m_file,f)
        f.close()
        self.logger.info("Cog sys file created.")
        os.mkdir(self.config.cog_data_dir(namespace))
        self.logger.info("Database created: " + namespace)
        self.logger.info("done.")
        return instance_id

    def create_namespace(self,namespace):
        if not os.path.exists(self.config.cog_data_dir(namespace)):
            os.mkdir(self.config.cog_data_dir(namespace))
            self.logger.info("Created new namespace: "+self.config.cog_data_dir(namespace))
            '''add namespace to dict'''
            self.namespaces[namespace] = {}
        else:
            self.logger.info("Using existing namespace: "+self.config.cog_data_dir(namespace))
        self.current_namespace = namespace

    def create_or_load_table(self, name, namespace):
        table = Table(name, namespace, self.instance_id, self.config, self.logger)
        self.current_namespace = namespace
        self.current_table = table

        if namespace not in self.namespaces:
            self.namespaces[namespace] = {}

        self.namespaces[namespace][table] = table

    def list_tables(self):
        p = set(())
        path = self.config.cog_data_dir(self.current_namespace)
        print path
        if not os.path.exists(path):
            return p
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for f in files:
            p.add(f.split("-")[0])
        return list(p)

    def use_table(self, name, namespace = "default"):
        '''
        :param name:
        :param namespace:
        :return:
        '''
        namespace = self.current_namespace if not namespace else namespace
        if name not in self.namespaces[namespace] or self.namespaces[namespace][name]:
            self.create_or_load_table(name, namespace)

        return self

    def put(self, data):
        assert type(data[0]) is str, "Only string type is supported supported."
        assert type(data[1]) is str, "Only string type is supported supported."
        position = self.current_table.store.save(data)
        self.current_table.indexer.put(data[0], position, self.current_table.store)

    def get(self, key):
        return self.current_table.indexer.get(key, self.current_table.store)

    def scanner(self, sfilter=None):
        scanner = self.current_table.indexer.scanner(self.current_table.store)
        for r in scanner:
            if sfilter:
                yield sfilter.process(r[1][1])
            else:
                yield r[1]

    def delete(self, key):
        self.table.indexer.delete(key, self.current_table.store)


    def put_node(self, vertex1, predicate, vertex2):
        # out vertices
        out_ng_vertices = []
        record = self.get(out_nodes(vertex1))
        if record is not None: out_ng_vertices = ast.literal_eval(record[1][1])
        out_ng_vertices.append(vertex2)
        vertex = (out_nodes(vertex1), str(out_ng_vertices))
        self.put(vertex)

        # in vertices
        in_ng_vertices = []
        record = self.get(in_nodes(vertex2))
        if record is not None: in_ng_vertices = ast.literal_eval(record[1][1])
        in_ng_vertices.append(vertex1)
        vertex = (in_nodes(vertex2), str(in_ng_vertices))
        self.put(vertex)

    def load_triples(self, graph_data_path, graph_name):
        self.create_namespace(graph_name)
        with open(graph_data_path) as f:
            for line in f:
                tokens = line.split()
                this_vertex = tokens[0].strip()
                predicate = tokens[1].strip()
                other_vertex = tokens[2].strip()
                self.create_or_load_table(predicate, graph_name)  # it wont create if it exists.
                self.put_node(this_vertex, predicate, other_vertex)

    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        self.create_namespace(graph_name)
        with open(edgelist_file_path) as f:
            for line in f:
                tokens = line.split()
                v1 = tokens[0].strip()
                v2 = tokens[1].strip()
                self.create_or_load_table(predicate, graph_name)
                self.put_node(v1, predicate, v2)

