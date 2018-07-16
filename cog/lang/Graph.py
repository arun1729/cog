from cog.database import Cog
from cog import config
import json

# g.V("<alice>").Tag("source").Out().In().Tag("target").All()
#g.V("<alice>").Out().All()

class Graph:
    """Graph object like in Gizmo"""
    def __init__(self, graph_name, dir):
        self.cog = Cog(dir)
        self.cog.create_namespace("test")

    def v(self, vertex):
        self.vertex = vertex
        return self

    def read_vertex(self, vertex):
        """read vertex from cog"""

    def out(self):
        """ """
        return self

    def in(self):
        """ """
        return self

    def all(self):
        """ """
        return self



"""
--> edge:<in/out> list strategy
table: predicate
<node1> : [node2, node3, node4 ...]

insert(alice:follows:john) =>
1. create_table_if_not_exist(follows)
2. list = get(key: alice)
3. put(key: alice, value: list += john)
** using string list (json dumps).
** deleting an item from the list will need to traverse the list. slightly slow. but most use cases for graph traversal are read only.

"""
 class Loader:

     def __init__(self, graph_data_path, graph_name, db_path=None, conf=config):
         if db_path is not None:
             config.COG_PATH_PREFIX = db_path
         cog = Cog(config)
         cog.create_namespace(graph_name)

         with open(graph_data_path) as f:
             for line in f:
                 tokens = line.split()
                 predicate = tokens[2].strip()
                 cog.create_table(predicate, graph_name) #it wont create if it exists.

                 vertex_name = tokens[0].strip()
                 out_vertex = tokens[1].strip()

                 out_ng_vertices = []
                 record = cog.get(vertex_name + "__:out:__")
                 if record is not None:
                    out_ng_vertices = json.loads(record[1][1])

                out_ng_vertices.append(out_vertex)
                vertex = (vertex_name + "__:out:__", str(out_ng_vertices))
                cog.put(vertex)

# call exec('string cog lang')

