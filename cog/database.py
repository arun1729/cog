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
import pickle
import socket
import uuid
from .core import Table
from . import config
import xxhash
import csv
import shlex

# functions

def out_nodes(v):
    return (v + "__:out:__")


def in_nodes(v):
    return (v + "__:in:__")

# https://www.w3.org/TR/n-triples/#sec-n-triples-language
def hash_predicate(predicate):
    return str(xxhash.xxh32(predicate, seed=2).intdigest())


def parse_tripple(tripple):
    tokens = shlex.split(tripple)
    subject = tokens[0].strip()
    predicate = tokens[1].strip()
    object = tokens[2].strip()
    context = None

    if len(tokens) > 3:  # nQuad
        context = tokens[3].strip()

    return subject, predicate, object, context


class Cog:
    """
        Read index file, record records stored in 'store' and write out new store file. Update index with position in store.
    """

    def __init__(self, shared_cache=None):
        self.logger = logging.getLogger('database')
        self.config = config
        self.logger.info("Cog init.")
        self.namespaces = {}
        self.current_table = None
        self.shared_cache = shared_cache
        '''creates Cog instance files.'''
        if os.path.exists(self.config.cog_instance_sys_file()):
            f=open(self.config.cog_instance_sys_file(),"rb")
            self.m_info=pickle.load(f)
            self.instance_id=self.m_info["m_instance_id"]
            f.close()
        else:
            self.instance_id = self.init_instance(config.COG_DEFAULT_NAMESPACE)

        '''Create default namespace and table.'''
        self.create_or_load_namespace(self.config.COG_DEFAULT_NAMESPACE)

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

    def create_or_load_namespace(self, namespace):
        if not os.path.exists(self.config.cog_data_dir(namespace)):
            os.mkdir(self.config.cog_data_dir(namespace))
            self.logger.info("Created new namespace: "+self.config.cog_data_dir(namespace))
            '''add namespace to dict'''
            self.namespaces[namespace] = {}
        else:
            self.logger.info("Using existing namespace: "+self.config.cog_data_dir(namespace))
            self.load_namespace(namespace)

        self.current_namespace = namespace

    def is_namespace(self, namespace):
        return os.path.exists(self.config.cog_data_dir(namespace))

    def create_table(self, table_name, namespace):
        table = Table(table_name, namespace, self.instance_id, self.config, shared_cache=self.shared_cache)
        self.current_namespace = namespace
        self.current_table = table
        self.namespaces[namespace][table_name] = table

    def load_namespace(self, namespace):
        for index_file_name in os.listdir(self.config.cog_data_dir(namespace)):
            table_names = set()
            if self.config.INDEX in index_file_name:
                id = self.config.index_id(index_file_name)
                table_name = config.get_table_name(index_file_name)
                if table_name not in table_names:
                    table_names.add(table_name)
                    self.logger.debug("loading index: id: {}, table name: {}".format(id, table_name))
                    self.load_table(table_name, namespace)
                    self.refresh_cache(table_name, namespace)
        self.current_namespace = namespace

    def load_table(self, name, namespace):
        # this method should not refresh cache since it's used in many places, this is basically "context switch" method.
        if namespace not in self.namespaces:
            self.namespaces[namespace] = {}
        self.logger.debug("loading table: "+name)

        if name not in self.namespaces[namespace]:
            self.namespaces[namespace][name] = Table(name, namespace, self.instance_id, self.config, shared_cache=self.shared_cache)
            self.logger.debug("created new table: " + name)

        self.current_table = self.namespaces[namespace][name]
        self.logger.debug("SET table {} in namespace {}. ".format(name, namespace))

    def refresh_cache(self, name, namespace):
        self.current_table = self.namespaces[namespace][name]
        # scan table to refresh cache
        for r in self.scanner():
            pass

    def refresh_all(self):
        for ns in self.namespaces:
            table = self.namespaces[ns]

        # scan table to refresh cache
        for r in self.scanner():
            pass

    def print_cache_info(self):
        print("::: cache info ::: {}, {}, {}".format(self.current_namespace, self.current_table.table_meta.name, str(self.current_table.store.store_cache.size_list())))

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
        Creates or appends to a list. If the key does now exist a new list is created, else it appends.
        :param data:
        :return:
        '''
        assert type(data.key) is str, "Only string type is supported."
        assert type(data.value) is str, "Only string type is supported."
        record = self.current_table.indexer.get(data.key, self.current_table.store)
        new_record = Record(data.key, data.value, value_type='l')
        if record is not None:
            new_record.set_value_link(record.store_position)
        position = self.current_table.store.save(new_record)
        self.current_table.indexer.put(new_record.key, position, self.current_table.store)

    def put_set(self, data):
        '''
        Creates or appends to a set. If the key does now exist a new list is created, else it adds to the set.
        :param data:
        :return:
        '''
        assert type(data.key) is str, "Only string type is supported."
        assert type(data.value) is str, "Only string type is supported."
        record = self.current_table.indexer.get(data.key, self.current_table.store)
        # skip insert into store-list if value already present.
        new_record = Record(data.key, data.value, value_type='l')
        if record is not None:
            if data.value not in record.value: # record value is a linked list.
                new_record.set_value_link(record.store_position)
                position = self.current_table.store.save(new_record)
                self.current_table.indexer.put(new_record.key, position, self.current_table.store)
        else:
            # insert new record
            position = self.current_table.store.save(new_record)
            self.current_table.indexer.put(new_record.key, position, self.current_table.store)

    def get(self, key):
        return self.current_table.indexer.get(key, self.current_table.store)

    def scanner(self, table=None, scan_filter=None):
        scan_itr = self.current_table.indexer.scanner(self.current_table.store) if not table else table.indexer.scanner(table.store)
        for r in scan_itr:
            if scan_filter:
                yield scan_filter.process(r.key)
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
        B - Dput_node
        C - D
        ======
        A => [B,C]
        B => [A,D]
        C => [A,B,D]
        D => [B]
        """
        # add to node set
        predicate_hashed = hash_predicate(predicate)
        self.use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).put(Record(str(predicate_hashed), predicate))
        self.use_table(self.config.GRAPH_NODE_SET_TABLE_NAME).put(Record(vertex1, ""))
        self.use_table(self.config.GRAPH_NODE_SET_TABLE_NAME).put(Record(vertex2, ""))
        self.use_table(predicate_hashed).put_set(Record(out_nodes(vertex1), vertex2))
        self.use_table(predicate_hashed).put_set(Record(in_nodes(vertex2), vertex1))

    def load_triples(self, graph_data_path, graph_name):
       """
       :param graph_data_path: 
       :param graph_name: 
       :param delimiter:
       :return: 
       """
       self.create_or_load_namespace(graph_name)
       self.load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, graph_name)
       with open(graph_data_path) as f:
           for line in f:
                subject, predicate, object, context = parse_tripple(line)
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
        self.create_or_load_namespace(graph_name)
        self.load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, graph_name)
        with open(edgelist_file_path) as f:
            for line in f:
                tokens = line.split()
                v1 = tokens[0].strip()
                v2 = tokens[1].strip()
                self.create_or_load_table(predicate, graph_name)
                self.put_node(v1, predicate, v2)

    def load_csv(self, file_name, id_column_name, graph_name):
        """
        Load CSV into in graph, you must select on of the columns as ID.
        :param file_name:
        :param id_column_name:
        :param graph_name
        :return:
        """
        self.create_or_load_namespace(graph_name)
        self.load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, graph_name)
        with open(file_name) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                for k in row:
                    subject = row[id_column_name]
                    predicate = k
                    obj = row[k]
                    self.put_node(subject, predicate, obj)
                    self.logger.info("""loaded: __:{0} {1} {2} .""".format(subject, predicate, obj))


