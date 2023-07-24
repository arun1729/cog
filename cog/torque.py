from cog.database import Cog
from cog.database import in_nodes, out_nodes, hash_predicate
from cog.core import cog_hash, Record
import json
import logging
from logging.config import dictConfig
from . import config as cfg
from cog.view import graph_template, script_part1, script_part2, graph_lib_src
import os
from os import listdir
import time
import random
from math import isclose
import warnings

NOTAG = "NOTAG"


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


CHARS = u'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'


class BlankNode(object):
    ID_PREFIX = "_id_"

    def __init__(self, label=None):
        if not label:
            label = str(time.time_ns()) + ''.join(random.choices(CHARS, k=4))
            self.id = "_:{}".format(label)
        else:
            self.id = "_:{}{}".format(BlankNode.ID_PREFIX, label)

    def __str__(self):
        return self.id

    @classmethod
    def is_id(cls, label):
        return label.startswith("_:" + BlankNode.ID_PREFIX)


class Graph:
    """
    Creates a graph object.
    """

    def __init__(self, graph_name, cog_home="cog_home", cog_path_prefix=None, enable_caching=True):
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

        if enable_caching:
            self.cache = {}
        else:
            self.cache = None

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

    def updatej(self, json_object):
        self.put_json(json_object, True)

    def putj(self, json_object, update=False):
        """
        Shorthand for put_json
        :param update:
        :param json_object:
        :return:
        """
        self.put_json(json_object, update)

    def put_json(self, json_object, update=False):
        """
        Experimental Feature
        ====================

        Inserts a JSON object into the graph. Each object (including the root object) in this JSON object will be
        identified by a BlankNode with a unique label. For example: {"name" : "bob", "location" : { "city" :
        "Toronto", "country" : "Canada"} } will be transformed into the following triples:

        _:1654006783197959000lIxa, name, bob
        _:1654006783197959000lIxa, location, _:1654006783844002000kAgC
        _:1654006783844002000kAgC, city, toronto
        _:1654006783844002000kAgC, country, canada

        """
        if isinstance(json_object, str):
            json_object = json.loads(json_object)
        self._traverse_json(json_object, update)

    def _traverse_json(self, jsn, update=False):
        new_edge_created = set()

        def traverse(json_obj, subject, predicate=None, update_object=False, sub_list_item=False):

            if type(json_obj) is dict:
                # every object has an id
                if "_id" in json_obj:
                    if sub_list_item and update_object:
                        raise Exception("Updating a sub object or list item with an _id is not supported.")
                    child_id = str(BlankNode(json_obj["_id"]))
                else:
                    # if _id is not present generate one.
                    child_id = str(BlankNode())
                if predicate:
                    # this is to skip the first iteration where predicate is None.
                    self.put(subject, predicate, child_id, update_object)
                for a in json_obj:
                    traverse(json_obj[a], child_id, a, update_object, sub_list_item=True)

            elif type(json_obj) is list:
                # create a new blank node for each list.
                list_id = str(BlankNode())
                self.put(subject, predicate, list_id, update_object)
                # new_edge_created.add((str(subject), str(predicate)))

                # traverse the list.
                for obj in json_obj:
                    traverse(obj, list_id, predicate, update_object, sub_list_item=True)

            else:
                if (str(subject), str(predicate)) in new_edge_created:
                    self.put(subject, predicate, json_obj, update_object)
                else:
                    self.put(subject, predicate, json_obj, update_object)
                    new_edge_created.add((str(subject), str(predicate)))

        if "_id" in jsn:
            traverse(jsn, str(BlankNode(jsn["_id"])), update_object=update)
        else:
            traverse(jsn, str(BlankNode()), update_object=update)

    def load_triples(self, graph_data_path, graph_name=None):
        """
        Loads triples from a file (one triple per line) into a graph.

        :param graph_data_path:
        :param graph_name:
        :return:
        """

        graph_name = self.graph_name if graph_name is None else graph_name
        self.cog.load_triples(graph_data_path, graph_name)
        self.all_predicates = self.cog.list_tables()
        return None

    def load_csv(self, csv_path, id_column_name, graph_name=None):
        """
        Loads a CSV file to a graph. One column must be designated as ID column. This method is intended for loading
        simple CSV data, for more complex ones that require additional logic, convert the CSV to triples using custom
        logic.

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
        self.logger.info("closing graph: " + self.graph_name)
        self.cog.close()

    def put(self, vertex1, predicate, vertex2, update=False, create_new_edge=False):
        self.cog.use_namespace(self.graph_name).use_table(predicate)
        if update:
            if create_new_edge:
                self.cog.put_new_edge(vertex1, predicate, vertex2)
            else:
                self.cog.update_edge(vertex1, predicate, vertex2)
        else:
            self.cog.put_node(vertex1, predicate, vertex2)
        self.all_predicates = self.cog.list_tables()
        return self

    def drop(self, vertex1, predicate, vertex2):
        """
        Drops edge between vertex1 and vertex2 for the given predicate.
        :param vertex1:
        :param predicate:
        :param vertex2:
        :return:
        """
        self.cog.delete_edge(vertex1, predicate, vertex2)

    def update(self, vertex1, predicate, vertex2):
        self.updatej(vertex1, predicate, vertex2)
        return self

    def v(self, vertex=None, func=None):
        if func:
            warnings.warn("The use of func is deprecated, please use filter instead.", DeprecationWarning)
        if vertex is not None:
            if isinstance(vertex, list):
                self.last_visited_vertices = [Vertex(v) for v in vertex]
            else:
                self.last_visited_vertices = [Vertex(vertex)]
        else:
            self.last_visited_vertices = []
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
            for r in self.cog.scanner():
                if func is not None and not func(r.key):
                    continue
                self.last_visited_vertices.append(Vertex(r.key))
        return self

    def out(self, predicates=None, func=None):
        '''
        Traverse forward through edges.
        :param func:
        :param predicates: A string or a List of strings.
        :return:
        '''

        if func:
            warnings.warn("The use of func is deprecated, please use filter instead.", DeprecationWarning)
            assert callable(func), "func must be a lambda. Example: func = lambda d: int(d) > 5"
            assert not isinstance(predicates, list), "func cannot be used with a list of predicates"

        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        self.logger.debug("OUT: predicates: " + str(predicates))
        self.__hop("out", predicates=predicates, func=func)
        return self

    def inc(self, predicates=None, func=None):
        '''
        Traverse backward through edges.
        :param predicates:
        :return:
        '''

        if func:
            warnings.warn("The use of func is deprecated, please use filter instead.", DeprecationWarning)
            assert callable(func), "func must be a lambda. Example: func = lambda d: int(d) > 5"
            assert not isinstance(predicates, list), "func cannot be used with a list of predicates"

        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        self.__hop("in", predicates, func=func)
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

    def __hop(self, direction, predicates=None, func=None):
        self.logger.debug("__hop : direction: " + str(direction) + " predicates: " + str(
            predicates) + " graph name: " + self.graph_name)
        self.cog.use_namespace(self.graph_name)
        self.logger.debug("hopping from vertices: " + str(map(lambda x: x.id, self.last_visited_vertices)))
        self.logger.debug("direction: " + str(direction) + " predicates: " + str(self.all_predicates))
        traverse_vertex = []
        for predicate in predicates:
            self.logger.debug("__hop predicate: " + predicate + " of " + str(predicates))
            for v in self.last_visited_vertices:
                if direction == "out":
                    record = self.cog.use_table(predicate).get(out_nodes(v.id))
                else:
                    record = self.cog.use_table(predicate).get(in_nodes(v.id))
                if record is not None:
                    if record.value_type == "s":
                        v_adjacent = str(record.value)
                        if func is not None and not func(v_adjacent):
                            continue
                        v_adjacent_obj = Vertex(v_adjacent).set_edge(predicate)
                        v_adjacent_obj.tags.update(v.tags)
                        traverse_vertex.append(v_adjacent_obj)
                    elif record.value_type == "l":
                        for v_adjacent in record.value:
                            self.logger.debug("record v: " + str(record.value) + " type: " + str(record.value_type))
                            if func is not None and not func(v_adjacent):
                                continue
                            v_adjacent_obj = Vertex(v_adjacent).set_edge(predicate)
                            v_adjacent_obj.tags.update(v.tags)
                            traverse_vertex.append(v_adjacent_obj)
        self.last_visited_vertices = traverse_vertex

    def filter(self, func):
        '''
            Applies a filter function to the vertices and removes any vertices that do not pass the filter.
        '''
        for v in self.last_visited_vertices:
            if not func(v.id):
                self.last_visited_vertices.remove(v)
        return self

    def sim(self, word, operator, threshold, strict=False):
        """
            Applies cosine similarity filter to the vertices and removes any vertices that do not pass the filter.

            Parameters:
            -----------
            word: str
                The word to compare to the other vertices.
            operator: str
                The comparison operator to use. One of "==", ">", "<", ">=", "<=", or "in".
            threshold: float or list of 2 floats
                The threshold value(s) to use for the comparison. If operator is "==", ">", "<", ">=", or "<=", threshold should be a float. If operator is "in", threshold should be a list of 2 floats.
            strict: bool, optional
                If True, raises an exception if a word embedding is not found for either word. If False, assigns a similarity of 0.0 to any word embedding that is not found.

            Returns:
            --------
            self: GraphTraversal
                Returns self to allow for method chaining.

            Raises:
            -------
            ValueError:
                If operator is not a valid comparison operator or if threshold is not a valid threshold value for the given operator.
                If strict is True and a word embedding is not found for either word.
    """
        if not isinstance(threshold, (float, int, list)):
            raise ValueError("Invalid threshold value: {}".format(threshold))

        if operator == 'in':
            if not isinstance(threshold, list) or len(threshold) != 2:
                raise ValueError("Invalid threshold value: {}".format(threshold))
            if not all(isinstance(t, (float, int)) for t in threshold):
                raise ValueError("Invalid threshold value: {}".format(threshold))

        filtered_vertices = []
        for v in self.last_visited_vertices:
            similarity = self.__cosine_similarity(word, v.id)
            if not similarity:
                # similarity is None if a word embedding is not found for either word.
                if strict:
                    raise ValueError("Missing word embedding for either '{}' or '{}'".format(word, v.id))
                else:
                    # Treat vertices without word embeddings as if they have no similarity to any other vertex.
                    similarity = 0.0
            if operator == '=':
                if isclose(similarity, threshold):
                    filtered_vertices.append(v)
            elif operator == '>':
                if similarity > threshold:
                    filtered_vertices.append(v)
            elif operator == '<':
                if similarity < threshold:
                    filtered_vertices.append(v)
            elif operator == '>=':
                if similarity >= threshold:
                    filtered_vertices.append(v)
            elif operator == '<=':
                if similarity <= threshold:
                    filtered_vertices.append(v)
            elif operator == 'in':
                if not threshold[0] <= similarity <= threshold[1]:
                    continue
                filtered_vertices.append(v)
            else:
                raise ValueError("Invalid operator: {}".format(operator))
        self.last_visited_vertices = filtered_vertices
        return self

    def __cosine_similarity(self, word1, word2):
        x = self.get_embedding(word1)
        y = self.get_embedding(word2)

        if x is None or y is None:
            return None

        dot_product = 0
        x_norm = 0
        y_norm = 0
        for i in range(len(x)):
            dot_product += x[i] * y[i]
            x_norm += x[i] ** 2
            y_norm += y[i] ** 2
        x_norm = x_norm ** (1 / 2)
        y_norm = y_norm ** (1 / 2)
        return dot_product / (x_norm * y_norm)

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
                item['edges'] = [
                    self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(
                        edge).value for edge in v.edges]
            # item['edge'] = self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(item['edge']).value
            item.update(v.tags)

            result.append(item)
        res = {"result": result}
        return res

    def view(self, view_name,
             js_src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/vis-network.min.js"):
        """
            Returns html view of the resulting graph from a query.
            :return:
        """
        assert view_name is not None, "a view name is required to create a view, it can be any string."
        result = self.all()
        view_html = script_part1 + graph_lib_src.format(js_src=js_src) + graph_template.format(
            plot_data_insert=json.dumps(result['result'])) + script_part2
        view = self.views_dir + "/{view_name}.html".format(view_name=view_name)
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

    def put_embedding(self, word, embedding):
        """
        Saves a word embedding.
        """

        assert isinstance(word, str), "word must be a string"
        self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).put(Record(
            str(cog_hash(word, self.config.INDEX_CAPACITY)), embedding))

    def get_embedding(self, word):
        """
        Returns a word embedding.
        """
        assert isinstance(word, str), "word must be a string"
        record = self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).get(
            str(cog_hash(word, self.config.INDEX_CAPACITY)))
        if record is None:
            return None
        return record.value

    def delete_embedding(self, word):
        """
        Deletes a word embedding.
        """
        assert isinstance(word, str), "word must be a string"
        self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).delete(
            str(cog_hash(word, self.config.INDEX_CAPACITY)))


class View(object):

    def __init__(self, url, html):
        self.url = url
        self.html = html

    def render(self, height=700, width=700):
        '''
        :param self:
        :param height:
        :param width:
        :return:
        '''
        iframe_html = r"""  <iframe srcdoc='{0}' width="{1}" height="{2}"> </iframe> """.format(self.html, width,
                                                                                                height)
        from IPython.core.display import display, HTML
        display(HTML(iframe_html))

    def persist(self):
        f = open(self.url, "w")
        f.write(self.html)
        f.close()

    def __str__(self):
        return self.url
