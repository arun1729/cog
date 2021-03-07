'''
Created on Nov 25, 2017

@author: arun
'''

from cog.core import Record
import logging
import os
import os.path
from os import listdir
from os.path import isfile
from os.path import join
import ast
import pickle
import socket
import uuid
from .core import Table
from . import config as cfg
import xxhash

# class Compaction:

def out_nodes(v):
    return (v + "__:out:__")

def in_nodes(v):
    return (v + "__:in:__")

def hash_predicate(predicate):
    return str(xxhash.xxh32(predicate,seed=2).intdigest())
    #return str(hash(predicate) % ((sys.maxsize + 1) * 2))

class Cog:
    """
        Read index file, record records stored in 'store' and write out new store file. Update index with position in store.
    """

    def __init__(self, db_path=None, config=cfg):
        if db_path:
            db_path = db_path + cfg.COG_ROOT if db_path.endswith("/") else db_path + "/" + cfg.COG_ROOT
            try:
                os.makedirs(db_path)
            except OSError:
                if not os.path.isdir(db_path):
                    raise
            config.CUSTOM_COG_DB_PATH = db_path
        self.logger = logging.getLogger('database')
        self.config = config
        self.logger.info("Cog init.")
        self.namespaces = {}
        self.current_table = None
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
        #self.create_or_load_table("default", self.config.COG_DEFAULT_NAMESPACE)

        '''Load all table names but lazy load actual tables on request.'''
        for name in self.list_tables():
            if name not in self.namespaces:
                self.namespaces[name] = None


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
            self.load_namespace(namespace)

        self.current_namespace = namespace

    def create_table(self, table_name, namespace):
        table = Table(table_name, namespace, self.instance_id, self.config, self.logger)
        self.current_namespace = namespace
        self.current_table = table
        self.namespaces[namespace][table_name] = table


    def load_namespace(self, namespace):
        for index_file_name in os.listdir(self.config.cog_data_dir(namespace)):
            table_names = set()
            if self.config.INDEX in index_file_name:
                id = self.config.index_id(index_file_name)
                table_name = cfg.get_table_name(index_file_name)
                if table_name not in table_names:
                    table_names.add(table_name)
                    self.logger.debug("loading index: id: {}, table name: {}".format(id, table_name))
                    self.load_table(table_name, namespace)
        self.current_namespace = namespace


    def load_table(self, name, namespace):
        if not namespace in self.namespaces:
            self.namespaces[namespace] = {}
        self.logger.debug("loading table: "+name)
        if name not in self.namespaces[namespace]:
            self.namespaces[namespace][name] = Table(name, namespace, self.instance_id, self.config, self.logger)
            self.logger.debug("created new table: " + name)

        self.current_table = self.namespaces[namespace][name]
        self.logger.debug("SET table {} in namespace {}. ".format(name, namespace))

    def close(self):
        for name, space in self.namespaces.items():
            if space is None:
                continue
            for name, table in space.items():
                self.logger.info("closing.. : "+table.table_meta.name)
                table.close()

    def list_tables(self):
        p = set(())
        self.logger.debug("LIST TABLES, current namespace: "+str(self.current_namespace))
        path = self.config.cog_data_dir(self.current_namespace)
        if not os.path.exists(path):
            return p
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for f in files:
            p.add(f.split("-")[0])
        return list(p)

    def use_namespace(self, namespace):
        self.current_namespace = namespace
        return self

    def use_table(self, name):
        '''
        :param name:
        :param namespace:
        :return:
        '''

        if name not in self.namespaces[self.current_namespace] or self.namespaces[self.current_namespace][name]:
            self.load_table(name, self.current_namespace)
        else:
            self.current_table=self.namespaces[self.current_namespace][name]

        return self

    def put(self, data):
        assert type(data.key) is str, "Only string type is supported."
        assert type(data.value) is str, "Only string type is supported."
        position = self.current_table.store.save(data)
        self.current_table.indexer.put(data.key, position, self.current_table.store)

    def put_list(self, data):
        '''
        Creates or appends to a lits. If the key does now exist a new list is created, else it appends.
        :param data:
        :return:
        '''
        assert type(data.key) is str, "Only string type is supported."
        assert type(data.value) is str, "Only string type is supported."
        record = self.current_table.indexer.get(data.key, self.current_table.store)
        position = self.current_table.store.save(data, record.store_position, 'l')
        self.current_table.indexer.put(data.key, position, self.current_table.store)

    def get(self, key):
        return self.current_table.indexer.get(key, self.current_table.store)

    def scanner(self, sfilter=None):
        scan_itr = self.current_table.indexer.scanner(self.current_table.store)
        for r in scan_itr:
            if sfilter:
                yield sfilter.process(r[1][1])
            else:
                yield r

    def delete(self, key):
        self.table.indexer.delete(key, self.current_table.store)


    def put_node(self, vertex1, predicate, vertex2):
        """
         Graph method
        :param vertex1: string
        :param predicate:
        :param vertex2:
        :return:

        A - B
        A - C
        B - C
        B - D
        C - D
        ======
        A => [B,C]
        B => [A,D]
        C => [A,B,D]
        D => [B]
        """
        # add to node set
        #print "-> v1 " + str(vertex1) + " predicate: "+ self.config.GRAPH_NODE_SET_TABLE_NAME + " v2 " + str(vertex2)
        predicate = hash_predicate(predicate)
        self.use_table(self.config.GRAPH_NODE_SET_TABLE_NAME).put(Record(vertex1, ""))
        self.use_table(self.config.GRAPH_NODE_SET_TABLE_NAME).put(Record(vertex2, ""))
        self.use_table(predicate).put_list(Record(out_nodes(vertex1), vertex2))
        self.use_table(predicate).put_list(Record(in_nodes(vertex2), vertex1))

    def load_triples(self, graph_data_path, graph_name, delimiter=None):
       """
       :param graph_data_path: 
       :param graph_name: 
       :param delim: 
       :return: 
       """
       self.create_namespace(graph_name)
       self.load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, graph_name)
       with open(graph_data_path) as f:
            for line in f:
                tokens = line.split(delimiter) if delimiter is not None else line.split()
                subject = tokens[0].strip()
                predicate = tokens[1].strip()
                object = tokens[2].strip()

                if len(tokens) > 3: #nQuad
                    context = tokens[3].strip() #not used currently

                self.load_table(hash_predicate(predicate), graph_name)
                self.put_node(subject, predicate, object)

    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        """
        Graph method
        :param edgelist_file_path:
        :param graph_name:
        :param predicate:
        :return:
        """
        self.create_namespace(graph_name)
        self.create_or_load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, graph_name)
        with open(edgelist_file_path) as f:
            for line in f:
                tokens = line.split()
                v1 = tokens[0].strip()
                v2 = tokens[1].strip()
                self.create_or_load_table(predicate, graph_name)
                self.put_node(v1, predicate, v2)

