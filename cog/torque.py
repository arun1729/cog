from cog.database import Cog
from cog.database import in_nodes, out_nodes, hash_predicate
import json
import logging
from logging.config import dictConfig
from . import config as cfg
from cog.view import graph_template, script_part1, script_part2, graph_lib_src
import os
from os import listdir

NOTAG="NOTAG"


class Vertex(object):

    def __init__(self, _id):
        self.id = _id
        self.tags = {}
        self.edge = None

    def set_edge(self, edge):
        self.edge = edge
        return self

    def get_dict(self):
        return self.__dict__

    def __str__(self):
        return json.dumps(self.__dict__)

class Graph:
    """
        https://www.w3.org/TR/WD-rdf-syntax-971002/
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
    """

    def __init__(self, graph_name, cog_home="cog_home", cog_path_prefix=None):
        '''
        :param graph_name:
        :param cog_home: Home directory name, for most use cases use the default
        :param cog_path_prefix: sets the root directory location for Cog db. Default: '/tmp' set in cog.Config. Change this to current directory when running in an IPython environment.
        '''
        self.config = cfg
        self.config.COG_HOME = cog_home
        if cog_path_prefix:
            self.config.COG_PATH_PREFIX = cog_path_prefix
        self.graph_name = graph_name

        dictConfig(self.config.logging_config)
        self.logger = logging.getLogger("torque")
        #self.logger.setLevel(logging.DEBUG)
        self.logger.debug("Torque init : graph: " + graph_name + " predicates: ")

        self.cog = Cog()
        self.cog.create_namespace(self.graph_name)
        self.all_predicates = self.cog.list_tables()
        self.views_dir = self.config.cog_views_dir()
        self.current_view = None
        self.current_result = None
        if not os.path.exists(self.views_dir):
            os.mkdir(self.views_dir)
        self.logger.debug("predicates: " + str(self.all_predicates))

        self.last_visited_vertices = None

    def load_edgelist(self, edgelist_file_path, graph_name, predicate="none"):
        self.cog.load_edgelist(edgelist_file_path, graph_name, predicate)
        self.all_predicates = self.cog.list_tables()

    def load_triples(self, graph_data_path, graph_name=None):
        '''
        Loads a list of triples
        :param graph_data_path:
        :param graph_name:
        :return:
        '''
        graph_name = self.graph_name if graph_name is None else graph_name
        self.cog.load_triples(graph_data_path, graph_name)
        self.all_predicates = self.cog.list_tables()

    def load_csv(self, csv_path, id_column_name, graph_name=None):
        """
        Loads CSV to a graph. One column must be designated as ID column
        :param csv_path:
        :param id_column_name:
        :param graph_name:
        :return:
        """
        if id_column_name is None:
            raise Exception("id_column_name must not be None")
        graph_name = self.graph_name if graph_name is None else graph_name
        self.cog.load_csv(csv_path, id_column_name, graph_name)
        self.all_predicates = self.cog.list_tables()

    def close(self):
        self.logger.info("closing graph: "+self.graph_name)
        self.cog.close()

    def put(self, vertex1, predicate, vertex2):
        self.cog.use_namespace(self.graph_name).use_table(predicate)
        self.cog.put_node(vertex1, predicate, vertex2)
        self.all_predicates = self.cog.list_tables()
        return self

    def v(self, vertex=None):
        #TODO: need to check if node exists
        if vertex is not None:
            self.last_visited_vertices = [Vertex(vertex)]
        else:
            self.last_visited_vertices = []
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
            for r in self.cog.scanner():
                self.last_visited_vertices.append(Vertex(r.key))
        return self

    def out(self, predicates=None):
        '''
        List of string predicates
        :param predicates:
        :return:
        '''
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = map(hash_predicate, predicates)
        else:
            predicates = self.all_predicates

        self.logger.debug("OUT: predicates: "+str(predicates))
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=None):
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = map(hash_predicate, predicates)
        else:
            predicates = self.all_predicates

        self.__hop("in", predicates)
        return self

    def has(self, predicate, object):
        pass


    def scan(self, limit=10, scan_type='v'):
        assert type(scan_type) is str, "Scan type must be either 'v' for vertices or 'e' for edges."
        if scan_type == 'e':
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME)
        else:
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
        result = []
        for i, r in enumerate(self.cog.scanner()):
            if i < limit:
                if scan_type == 'v':
                    v = Vertex(r.key)
                else:
                    v = Vertex(r.value)
                result.append({"id": v.id})
            else:
                break
        return {"result": result}

    def __hop(self, direction, predicates=None, tag=NOTAG):
        self.logger.debug("__hop : direction: " + str(direction) + " predicates: " + str(predicates) + " graph name: "+self.graph_name)
        self.cog.use_namespace(self.graph_name)
        self.logger.debug("hopping from vertices: " + str(map(lambda x : x.id, self.last_visited_vertices)))
        self.logger.debug("direction: " + str(direction) + " predicates: "+str(self.all_predicates))
        traverse_vertex = []
        for predicate in predicates:
            self.logger.debug("__hop predicate: "+predicate + " of "+ str(predicates))
            for v in self.last_visited_vertices:
                if direction == "out":
                    record = self.cog.use_table(predicate).get(out_nodes(v.id))
                else:
                    record = self.cog.use_table(predicate).get(in_nodes(v.id))
                if not record.is_empty():
                    for v_adjacent in record.value:
                        v_adjacent_obj = Vertex(v_adjacent).set_edge(predicate)
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
            item = {"id":v.id}
            # item['edge'] = self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(item['edge']).value
            item.update(v.tags)

            result.append(item)
        self.current_result = result
        return {"result": result}

    def view(self, view_name, js_src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"):
        """
            Returns html view of the graph
            :return:
        """
        assert view_name is not None, "a view name is required to create a view, it can be any string."
        result = self.all()
        # create a new view if one the conditions are met
        if not self.current_view or self.current_view != view_name or self.current_result != result:
            view_html = script_part1 + graph_lib_src.format(js_src=js_src) + graph_template.format(plot_data_insert=json.dumps(result['result'])) + script_part2
            view = self.views_dir+"/{view_name}.html".format(view_name=view_name)
            view = View(view, view_html)
            view.persist()
            self.current_view = view_name
        else:
            view = self.views_dir+"/{view_name}.html".format(view_name=view_name)
            with open(view, 'r') as f:
                view_html = f.read()
            view = View(view, view_html)
        return view

    def lsv(self):
        return [f.split(".")[0] for f in listdir(self.views_dir)]


class View(object):

    def __init__(self, url, html):
        self.url = url
        self.html = html

    def render(self):
        """
             This feature only works on IPython
             :return:
        """

        iframe_html = r"""  <iframe srcdoc='{0}' width="700" height="700"> </iframe> """.format(self.html)
        from IPython.core.display import display, HTML
        display(HTML(iframe_html))

    def persist(self):
        f = open(self.url, "w")
        f.write(self.html)
        f.close()

    def __str__(self):
        return self.url


