from cog.database import Cog
from cog import config as cfg
import json
import ast
from os import listdir
from os.path import isfile, join

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

    def __init__(self, graph_name, cog_dir):
        '''
        :param graph_name:
        :param cog_dir:
        '''
        self.predicates = self.list_predicate_tables(cog_dir, graph_name)
        self.cogs = []
        for predicate in self.predicates:
            cog = Cog(db_path=cog_dir)
            cog.use_table(predicate, graph_name)
            self.cogs.append(cog)

    def list_predicate_tables(self, cog_dir, graph_name):
        path = "/"+"/".join([cog_dir, graph_name])
        files = [f for f in listdir(path) if isfile(join(path, f))]
        p = set(())
        for f in files:
            p.add(f.split("-")[0])
        return p


    def v(self, vertex):
        self.vertices = [vertex]
        """read vertex from cog"""
        self.current_vertex_in = []
        self.current_vertex_out = []

        for cog in self.cogs:
            record = cog.get(in_nodes(vertex))

            if record:
                self.current_vertex_in=ast.literal_eval(record[1][1])

            record = cog.get(out_nodes(vertex))
            if record:
                self.current_vertex_out=ast.literal_eval(record[1][1])

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
        result = []
        for v in self.vertices:
            result.append({"id":v})
        return "{" + json.dumps(result) + "}"


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

{
	"result": [
		{
			"id": "<fred>"
		},
		{
			"id": "cool_person"
		}
	]
}

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

