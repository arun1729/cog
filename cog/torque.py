from cog.database import Cog
from cog import config as cfg
import json
import ast
from os import listdir
from os.path import isfile, join
import os

class Graph:

    def __init__(self, graph_name, cog_dir):
        '''
        :param graph_name:
        :param cog_dir:
        '''
        self.predicates = self.list_predicate_tables(cog_dir, graph_name)
        self.graph_name = graph_name
        self.cog_dir = cog_dir
        self.cogs = {}
        for predicate in self.predicates:
            cog = Cog(db_path=cog_dir)
            cog.use_table(predicate, graph_name)
            self.cogs[predicate] = cog

    def put(self, vertex1, predicate, vertex2):
        cog = Cog(db_path=self.cog_dir)
        cog.create_namespace(self.graph_name)
        cog.use_table(predicate, self.graph_name)  # it wont create if it exists.
        put_node(cog, vertex1, predicate, vertex2)
        self.cogs[predicate] = cog
        return self


    def list_predicate_tables(self, cog_dir, graph_name):
        p = set(())
        path = "/".join([cog_dir, graph_name])
        if not os.path.exists(path): return p
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for f in files:
            p.add(f.split("-")[0])
        return p


    def v(self, vertex):
        self.vertices = {vertex : {"id" : vertex }}
        return self

    def out(self, predicates=None):
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=None):
        self.__hop("in", predicates)
        return self

    def __hop(self, direction, predicates=None):
        visit_verts = {}
        #print "before hop **** "+direction
        #print self.vertices
        cogs = self.cogs.values()
        if predicates:
            cogs = []
            for p in predicates:
                if p in self.cogs:
                    cogs.append(self.cogs[p])
        for cog in cogs:
            for v in self.vertices.values():
                meta = {}
                for key in v:
                    if key != "id":
                        meta[key] = v[key]
                if(direction == "out"):
                    record = cog.get(out_nodes(v["id"]))
                else:
                    record = cog.get(in_nodes(v["id"]))
                if record:
                    for v_hop in ast.literal_eval(record[1][1]):
                        # if v_hop not in self.vertices:
                        #     self.vertices[v_hop] = {"id" : v_hop }
                        # visit_verts[v_hop] = self.vertices[v_hop]
                        visit_verts[v_hop] = {"id" : v_hop }
                        visit_verts[v_hop].update(meta)

        # discard other vertices and keep only visited verts
        self.vertices = visit_verts
        #print "after hop ****" + direction
        #print self.vertices

    def tag(self, tag_name):
        tagged_verts = {}
        for v in self.vertices.values():
            tagged_verts[v["id"]] = self.vertices[v["id"]]
            tagged_verts[v["id"]][tag_name] = v["id"]

        self.vertices = tagged_verts
        #print "*** TAG: "+tag_name
        #print self.vertices
        return self

    def count(self):
        return len(self.vertices)

    def all(self):
        result = []
        for v in self.vertices.values():
            result.append(v)
        return json.dumps({"result" : result})


def out_nodes(v):
    return (v + "__:out:__")

def in_nodes(v):
    return (v + "__:in:__")

def put_node(cog_instance, vertex1, predicate, vertex2):
    # out vertices
    out_ng_vertices = []
    record = cog_instance.get(out_nodes(vertex1))
    if record is not None: out_ng_vertices = ast.literal_eval(record[1][1])
    out_ng_vertices.append(vertex2)
    vertex = (out_nodes(vertex1), str(out_ng_vertices))
    cog_instance.put(vertex)

    # in vertices
    in_ng_vertices = []
    record = cog_instance.get(in_nodes(vertex2))
    if record is not None: in_ng_vertices = ast.literal_eval(record[1][1])
    in_ng_vertices.append(vertex1)
    vertex = (in_nodes(vertex2), str(in_ng_vertices))
    cog_instance.put(vertex)


class Loader:

    def __init__(self, db_path=None, config=cfg):
         self.cog = Cog(db_path=db_path, config=config)

    def load_triples(self, graph_data_path, graph_name):
         self.cog.create_namespace(graph_name)
         with open(graph_data_path) as f:
             for line in f:
                 tokens = line.split()
                 this_vertex = tokens[0].strip()
                 predicate = tokens[1].strip()
                 other_vertex = tokens[2].strip()
                 self.cog.create_table(predicate, graph_name) #it wont create if it exists.
                 put_node(self.cog,this_vertex, predicate, other_vertex)


    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        self.cog.create_namespace(graph_name)
        with open(edgelist_file_path) as f:
            for line in f:
                tokens = line.split()
                v1 = tokens[0].strip()
                v2 = tokens[1].strip()
                self.cog.create_table(predicate, graph_name)
                put_node(self.cog, v1, predicate, v2)


# call exec('string cog lang')

