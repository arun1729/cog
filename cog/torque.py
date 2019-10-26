from cog.database import Cog
from cog.database import in_nodes, out_nodes
import config as cfg
import json
import ast
from os import listdir
from os.path import isfile, join
import os

NOTAG="NOTAG"

class Vertex(object):

    def __init__(self, _id):
        self.id = _id
        self.tags = {}

    def __str__(self):
        return json.dumps(self.__dict__)


class Graph:
    """
        https://www.w3.org/TR/WD-rdf-syntax-971002/
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
    """

    def __init__(self, graph_name, cog_dir):
        '''
        :param graph_name:
        :param cog_dir:
        list of
        '''
        self.config = cfg
        self.cog = Cog(db_path=cog_dir, config=cfg)
        self.graph_name = graph_name
        self.cog_dir = cog_dir
        self.all_predicates = self.cog.list_tables()
        self.last_visited_vertices = None
        self.cog.create_namespace(self.graph_name)
        self.cog.create_or_load_table(self.config.GRAPH_NODE_SET_TABLE_NAME, self.graph_name)

    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        self.cog.load_edgelist(edgelist_file_path, graph_name, predicate)
        self.all_predicates = self.cog.list_tables()

    def load_triples(self, graph_data_path, graph_name):
        '''
        Loads a list of triples
        :param graph_data_path:
        :param graph_name:
        :return:
        '''
        self.cog.load_triples(graph_data_path, graph_name)
        self.all_predicates = self.cog.list_tables()

    def put(self, vertex1, predicate, vertex2):
        self.cog.create_or_load_table(predicate, self.graph_name)
        self.cog.use_namespace(self.graph_name).use_table(predicate)
        self.cog.put_node(vertex1, predicate, vertex2)
        self.all_predicates = self.cog.list_tables()
        return self


    def list_predicate_tables(self, cog_dir, graph_name):
        p = set(())
        path = "/".join([cog_dir, graph_name])
        if not os.path.exists(path): return p
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for f in files:
            p.add(f.split("-")[0])
        return p

    def v(self, vertex=None):
        #TODO: need to check if node exists
        if vertex:
            self.last_visited_vertices = [Vertex(vertex)]
        else:
            if not self.last_visited_vertices:
                self.last_visited_vertices = []
                self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
                scanner = self.cog.scanner()
                for r in scanner:
                    self.last_visited_vertices.append(Vertex(r))
        return self

    def out(self, predicates=None):
        '''
        List of string predicates
        :param predicates:
        :return:
        '''
        if predicates:
            assert type(predicates) == list
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=None):
        self.__hop("in", predicates)
        return self

    def __hop(self, direction, predicates=None, tag=NOTAG):
        self.cog.use_namespace(self.graph_name)
        predicates = self.all_predicates if not predicates else predicates
        #print "hopping from vertices: " + str(map(lambda x : x.id, self.last_visited_vertices))
        #print "direction: " + str(direction) + " predicates: "+str(self.all_predicates)
        traverse_vertex = []
        for predicate in predicates:
            for v in self.last_visited_vertices:
                if direction == "out":
                    record = self.cog.use_table(predicate).get(out_nodes(v.id))
                else:
                    record = self.cog.use_table(predicate).get(in_nodes(v.id))
                #print "==? " + str(direction)+ " <> " + str(predicate) + " ::: " + str(v.id) + " ==> " + str(record)
                if record:
                    for v_adjacent in ast.literal_eval(record[1][1]):
                        v_adjacent_obj = Vertex(v_adjacent)
                        v_adjacent_obj.tags.update(v.tags)
                        traverse_vertex.append(v_adjacent_obj)
        self.last_visited_vertices = traverse_vertex

    def tag(self, tag_name):
        '''
        Saves nodes with a tag name and returned in the result set.
        Primarily used to capture nodes while navigating the graph.
        :param tag_name:
        :return:
        '''
        for v in self.last_visited_vertices:
            v.tags[tag_name] = v.id
        return self

    def count(self):
        return len(self.last_visited_vertices)

    def all(self):
        """
        returns all the nodes in the result.
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
        :return:
        """
        result = []
        for v in self.last_visited_vertices:
            #print "all:: tag: " + v + " vertex:"+ str(self.last_visited_vertices[v])
            item = {"id":v.id}
            item.update(v.tags)
            result.append(item)
        return {"result": result}

