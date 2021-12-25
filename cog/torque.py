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
        self.edges = set()

    def set_edge(self, edge):
        self.edges.add(edge)
        return self

    def get_dict(self):
        return self.__dict__

    def __str__(self):
        return json.dumps(self.__dict__)

class Graph:
    """
    Creates a graph object.
    """

    def __init__(self, graph_name, cog_home="cog_home", cog_path_prefix=None):
        '''
        :param graph_name:
        :param cog_home: Home directory name, for most use cases use default.
        :param cog_path_prefix: sets the root directory location for Cog db. Default: '/tmp' set in cog.Config. Change this to current directory when running in an IPython environment.
        '''

        self.config = cfg
        self.config.COG_HOME = cog_home

        if cog_path_prefix:
            self.config.COG_PATH_PREFIX = cog_path_prefix

        self.graph_name = graph_name
        self.cache = {}
        dictConfig(self.config.logging_config)
        self.logger = logging.getLogger("torque")

        self.logger.debug("Torque init on graph: " + graph_name + " predicates: ")

        self.cog = Cog(self.cache)
        self.cog.create_or_load_namespace(self.graph_name)

        self.all_predicates = self.cog.list_tables()
        self.views_dir = self.config.cog_views_dir()

        if not os.path.exists(self.views_dir):
            os.mkdir(self.views_dir)
        self.logger.debug("predicates: " + str(self.all_predicates))

        self.last_visited_vertices = None

    def refresh(self):
        self.cog.refresh_all()

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
        return None

    def load_csv(self, csv_path, id_column_name, graph_name=None):
        """
        Loads CSV to a graph. One column must be designated as ID column.
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
        Traverse forward through edges.
        :param predicates: A string or a List of strings.
        :return:
        '''
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        self.logger.debug("OUT: predicates: "+str(predicates))
        self.__hop("out", predicates)
        return self

    def inc(self, predicates=None):
        '''
        Traverse backward through edges.
        :param predicates:
        :return:
        '''
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        self.__hop("in", predicates)
        return self

    def __adjacent_vertices(self, vertex, predicates, direction='out'):
        self.cog.use_namespace(self.graph_name)
        adjacent_vertices = []
        for predicate in predicates:
            if direction == 'out':
                out_record = self.cog.use_table(predicate).get(out_nodes(vertex.id))
                if out_record is not None:
                    for v_adj in out_record.value:
                        adjacent_vertices.append(Vertex(v_adj).set_edge(predicate))
            elif direction == 'in':
                in_record = self.cog.use_table(predicate).get(in_nodes(vertex.id))
                if not in_record is not None:
                    for v_adj in in_record.value:
                        adjacent_vertices.append(Vertex(v_adj).set_edge(predicate))

        return adjacent_vertices

    def has(self, predicates, vertex):
        """
        Filters all outgoing edges from a vertex that matches a list of predicates.
        :param predicates:
        :param vertex:
        :return:
        """

        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))

        has_vertices = []
        for lv in self.last_visited_vertices:
            adj_vertices = self.__adjacent_vertices(lv, predicates)
            for av in adj_vertices:
                if av.id == vertex:
                    has_vertices.append(lv)

        self.last_visited_vertices = has_vertices
        return self

    def hasr(self, predicates, vertex):
        """
        'Has' in reverse. Filters all incoming edges from a vertex that matches a list of predicates.
        :param predicates:
        :param vertex:
        :return:
        """

        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))

        has_vertices = []
        for lv in self.last_visited_vertices:
            adj_vertices = self.__adjacent_vertices(lv, predicates, 'in')
            # print(lv.id + " -> " + str([x.id for x in adj_vertices]))
            for av in adj_vertices:
                if av.id == vertex:
                    has_vertices.append(lv)

        self.last_visited_vertices = has_vertices
        return self


    def scan(self, limit=10, scan_type='v'):
        '''
        Scans vertices or edges in a graph.
        :param limit:
        :param scan_type:
        :return:
        '''
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
                if record is not None:
                    for v_adjacent in record.value:
                        v_adjacent_obj = Vertex(v_adjacent).set_edge(predicate)
                        v_adjacent_obj.tags.update(v.tags)
                        traverse_vertex.append(v_adjacent_obj)
        self.last_visited_vertices = traverse_vertex

    def tag(self, tag_name):
        '''
        Saves vertices with a tag name. Used to capture vertices while traversing a graph.
        :param tag_name:
        :return:
        '''
        for v in self.last_visited_vertices:
            v.tags[tag_name] = v.id
        return self

    def count(self):
        return len(self.last_visited_vertices)

    def all(self, options=None):
        """
        Returns all the vertices that are resultant of the graph query. Options 'e' would include the edges that were traversed.
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
        :return:
        """
        result = []
        show_edge = True if options is not None and 'e' in options else False
        for v in self.last_visited_vertices:
            item = {"id": v.id}
            if show_edge and v.edges:
                item['edges'] = [self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(edge).value for edge in v.edges]
            # item['edge'] = self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(item['edge']).value
            item.update(v.tags)

            result.append(item)
        res = {"result": result}
        return res

    def view(self, view_name, js_src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"):
        """
            Returns html view of the resulting graph from a query.
            :return:
        """
        assert view_name is not None, "a view name is required to create a view, it can be any string."
        result = self.all()
        view_html = script_part1 + graph_lib_src.format(js_src=js_src) + graph_template.format(plot_data_insert=json.dumps(result['result'])) + script_part2
        view = self.views_dir+"/{view_name}.html".format(view_name=view_name)
        view = View(view, view_html)
        view.persist()
        return view

    def getv(self, view_name):
        view = self.views_dir + "/{view_name}.html".format(view_name=view_name)
        assert os.path.isfile(view), "view not found, create a view by calling .view()"
        with open(view, 'r') as f:
            view_html = f.read()
        view = View(view, view_html)
        return view

    def lsv(self):
        return [f.split(".")[0] for f in listdir(self.views_dir)]

    def get_new_graph_instance(self):
        return Graph(self.graph_name, self.config.COG_HOME, self.config.COG_PATH_PREFIX)



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