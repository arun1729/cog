from cog.database import Cog
from cog import config as cfg
import json
import ast
from os import listdir
from os.path import isfile, join

class Graph:

    def __init__(self, graph_name, cog_dir):
        '''
        :param graph_name:
        :param cog_dir:
        '''
        self.predicates = self.list_predicate_tables(cog_dir, graph_name)
        self.cogs = {}
        for predicate in self.predicates:
            cog = Cog(db_path=cog_dir)
            cog.use_table(predicate, graph_name)
            self.cogs[predicate] = cog

    def list_predicate_tables(self, cog_dir, graph_name):
        path = "/"+"/".join([cog_dir, graph_name])
        files = [f for f in listdir(path) if isfile(join(path, f))]
        p = set(())
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


class Loader:

    def __init__(self, graph_data_path, graph_name, db_path=None, config=cfg):

         cog = Cog(db_path=db_path, config=config)
         cog.create_namespace(graph_name)

         with open(graph_data_path) as f:
             for line in f:
                 tokens = line.split()
                 this_vertex = tokens[0].strip()
                 predicate = tokens[1].strip()
                 other_vertex = tokens[2].strip()

                 cog.create_table(predicate, graph_name) #it wont create if it exists.

                 # out vertices
                 out_ng_vertices = []
                 record = cog.get(this_vertex + "__:out:__")
                 if record is not None: out_ng_vertices = ast.literal_eval(record[1][1])
                 out_ng_vertices.append(other_vertex)
                 vertex = (this_vertex + "__:out:__", str(out_ng_vertices))
                 cog.put(vertex)

                # in vertices
                 in_ng_vertices = []
                 record = cog.get(other_vertex + "__:in:__")
                 if record is not None: in_ng_vertices = ast.literal_eval(record[1][1])
                 in_ng_vertices.append(this_vertex)
                 vertex = (other_vertex + "__:in:__", str(in_ng_vertices))
                 cog.put(vertex)

# call exec('string cog lang')

