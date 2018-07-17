from cog.database import Cog
from cog import config as cfg
import ast

# g.V("<alice>").Tag("source").Out().In().Tag("target").All()
#g.V("<alice>").Out().All()
# ('<fred>__:in:__', "['<bob>', '<emily>']")
# ('<dani>__:in:__', "['<charlie>']")
# ('<dani>__:out:__', "['<bob>', '<greg>']")
# ('<fred>__:out:__', "['<greg>']")
# ('<greg>__:in:__', "['<dani>', '<fred>']")
# ('<emily>__:out:__', "['<fred>']")
# ('<bob>__:out:__', "['<fred>']")
# ('<alice>__:out:__', "['<bob>']")

class Graph:
    """Graph object like in Gizmo"""
    def __init__(self, graph_name, cog_dir):
        self.cog = Cog(db_path=cog_dir)
        self.cog.create_namespace(graph_name)
        self.cog.create_table("<follows>", graph_name)

    def v(self, vertex):
        self.vertices = [vertex]
        """read vertex from cog"""
        self.current_vertex_in = []
        self.current_vertex_out = []

        record = self.cog.get(in_nodes(vertex))

        if record:
            self.current_vertex_in=ast.literal_eval(record[1][1])

        print out_nodes(vertex)
        record = self.cog.get(out_nodes(vertex))
        print record
        if record:
            self.current_vertex_out=ast.literal_eval(record[1][1])

        print self.current_vertex_in
        print self.current_vertex_out

        return self

    def out(self):
        self.vertices = self.current_vertex_out
        return self

    def inc(self):
        """ """
        self.vertices = self.current_vertex_in
        return self

    def count(self):
        return len(self.vertices)

    def all(self):
        """ """
        return self


def out_nodes(v):
    return (v + "__:out:__")

def in_nodes(v):
    return (v + "__:in:__")

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

