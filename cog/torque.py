from cog.database import Cog
from cog.database import in_nodes, out_nodes
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
        list of
        '''
        self.cog = Cog(db_path=cog_dir)
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


    def v(self, vertex):
        self.vertices = {vertex : {"id" : vertex }}
        return self

    def out(self, predicates=None):
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=["none"]):
        self.__hop("in", predicates)
        return self

    def __hop(self, direction, predicates=None):
        visit_verts = {}
        predicates = self.all_predicates if not predicates else predicates
        for predicate in predicates:
            for v in self.vertices.values():
                meta = {}
                for key in v:
                    if key != "id":
                        meta[key] = v[key]
                if direction == "out":
                    record = self.cog.use_table(predicate, self.graph_name).get(out_nodes(v["id"]))
                else:
                    record = self.cog.use_table(predicate, self.graph_name).get(in_nodes(v["id"]))
                if record:
                    for v_hop in ast.literal_eval(record[1][1]):
                        # if v_hop not in self.vertices:
                        #     self.vertices[v_hop] = {"id" : v_hop }
                        # visit_verts[v_hop] = self.vertices[v_hop]
                        visit_verts[v_hop] = {"id" : v_hop }
                        visit_verts[v_hop].update(meta)

        # discard other vertices and keep only visited verts
        self.vertices = visit_verts

    def tag(self, tag_name):
        tagged_verts = {}
        for v in self.vertices.values():
            tagged_verts[v["id"]] = self.vertices[v["id"]]
            tagged_verts[v["id"]][tag_name] = v["id"]

        self.vertices = tagged_verts
        return self

    def count(self):
        return len(self.vertices)

    def all(self):
        result = []
        for v in self.vertices.values():
            result.append(v)
        return json.dumps({"result" : result})

