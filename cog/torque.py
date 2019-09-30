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

    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        self.cog.load_edgelist(edgelist_file_path, graph_name, predicate)
        self.all_predicates = self.cog.list_tables()

    def load_triples(self, graph_data_path, graph_name):
        self.cog.load_triples(graph_data_path, graph_name)
        self.all_predicates = self.cog.list_tables()

    def put(self, vertex1, predicate, vertex2):
        self.cog.use_table(predicate, self.graph_name)
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
            self.vertices = {NOTAG: [Vertex(vertex)]}
        else:
            self.vertices = {NOTAG: []}
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
            scanner = self.cog.scanner()
            for r in scanner:
                self.vertices[NOTAG].append(Vertex(r))
            # scan GRAPH_SET_TABLE and populate vertices

        return self

    def out(self, predicates=None):
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=["none"]):
        self.__hop("in", predicates)
        return self

    def __hop(self, direction, predicates=None, tag=NOTAG):
        print "direction: " + str(direction) + " predicates: "+str(predicates)
        print "~~~vertices: "+ str(self.vertices[tag])
        self.cog.use_namespace(self.graph_name)
        predicates = self.all_predicates if not predicates else predicates
        for predicate in predicates:
            for v in self.vertices[tag]:
                if direction == "out":
                    record = self.cog.use_table(predicate).get(out_nodes(v.id))
                else:
                    record = self.cog.use_table(predicate).get(in_nodes(v.id))
                if record:
                    for v_adjacent in ast.literal_eval(record[1][1]):
                        print "v_adjacent:" + str(v_adjacent)
                        self.vertices[tag].append(Vertex(v_adjacent))
                        # visit_verts[v_hop].update(meta)

        # discard other vertices and keep only visited verts - why?

    def tag(self, tag_name):
        '''
        Saves nodes with a tag name and returned in the result set.
        Primarily used to capture nodes while navigating the graph.
        :param tag_name:
        :return:
        '''
        self.vertices[tag_name] = []
        for v in self.vertices:
            self.vertices[tag_name].append(v)
        return self

    def count(self):
        return len(self.vertices)

    def all(self):
        """
        returns all the nodes in the result.
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
        :return:
        """
        result = []
        for v in self.vertices:
            print "all:: tag: " + v + " vertex:"+ str(self.vertices[v])
            result.append(v)
        return {"result": result}

