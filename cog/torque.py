from cog.database import Cog
from cog.database import in_nodes, out_nodes, hash_predicate, parse_tripple
import json
import logging
from . import config as cfg
from .config import CogConfig
from cog.view import build_graph_html, View
from cog.embeddings import EmbeddingMixin
from cog.search import TraversalMixin
import os
import shutil
from os import listdir
from cog.cloud_client import CloudClient
import time
import random
import warnings

NOTAG = "NOTAG"

# Sort direction constants for order()
ASC = "asc"
DESC = "desc"


class Vertex(object):

    def __init__(self, _id):
        self.id = _id
        self.tags = {}
        self.edges = set()
        self._path = None

    def set_edge(self, edge):
        self.edges.add(edge)
        return self

    def get_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != '_path'}

    def __str__(self):
        return json.dumps(self.get_dict())


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


class Graph(EmbeddingMixin, TraversalMixin):
    """
    Creates a graph object.
    
    Args:
        graph_name: Name of the graph (default: "default")
        cog_home: Home directory name for the database
        cog_path_prefix: Root directory location for Cog db
        enable_caching: Enable in-memory caching for faster reads
        flush_interval: Number of writes before auto-flush (local and cloud).
                       1 = flush every write (safest, default)
                       0 = manual flush only (fastest, use sync())
                       N>1 = flush every N writes with async background threads
        config: Optional CogConfig instance. When provided, overrides all other
                config options (cog_home, cog_path_prefix) and prevents mutation
                of global config state. Each Graph gets its own isolated copy.
        api_key: API key for CogDB Cloud. When provided (or set via
                 COGDB_API_KEY env var), the graph operates in cloud mode —
                 all operations go over HTTP and no local files are created.
    """

    def __init__(self, graph_name="default", cog_home="cog_home", cog_path_prefix=None, enable_caching=True,
                 flush_interval=1, config=None, api_key=None):
        """
        :param graph_name: Name of the graph (default: "default")
        :param cog_home: Home directory name, for most use cases use default.
        :param cog_path_prefix: sets the root directory location for Cog db. Default: '/tmp' set in cog.Config. Change this to current directory when running in an IPython environment.
        :param flush_interval: Number of writes before auto-flush. 1 = every write (safest).
        :param config: Optional CogConfig instance. Overrides cog_home and cog_path_prefix when provided.
        :param api_key: API key for CogDB Cloud mode.
        """


        self.graph_name = graph_name
        self.logger = logging.getLogger(__name__)

        # Resolve API key: explicit param > env var > None
        resolved_key = api_key or os.environ.get("COGDB_API_KEY")

        if resolved_key:
            # Cloud mode — all operations go over HTTP
            self._cloud = True
            self._api_key = resolved_key
            self._flush_interval = flush_interval
            self._cloud_client = CloudClient(graph_name, resolved_key, flush_interval=flush_interval)
            self._cloud_chain = []  # accumulates traversal steps
            self.config = cfg
            self.last_visited_vertices = None
            self._server_port = None
            self.views_dir = None
            self._predicate_reverse_lookup_cache = {}
            self._default_provider = "cogdb"
            self._default_provider_kwargs = {}
            self._vectorize_configured = False
            self.logger.debug(f"Torque cloud mode on graph: {graph_name}")
            # No local storage initialized
            return

        # Local mode (existing behavior, unchanged)
        self._cloud = False
        self._api_key = None
        self._cloud_client = None

        if config is not None:
            self.config = config
        else:
            self.config = CogConfig(COG_HOME=cog_home)
            if cog_path_prefix:
                self.config.COG_PATH_PREFIX = cog_path_prefix

        if config is None:
            # Keep module-level globals in sync for backward compat (single-graph usage)
            cfg.COG_HOME = cog_home
            if cog_path_prefix:
                cfg.COG_PATH_PREFIX = cog_path_prefix

        if enable_caching:
            self.cache = {}
        else:
            self.cache = None

        self.logger.debug(f"Torque init on graph: {graph_name} (flush_interval={flush_interval})")

        self.cog = Cog(self.cache, flush_interval=flush_interval, config=self.config)
        self.cog.create_or_load_namespace(self.graph_name)

        self.all_predicates = self.cog.list_tables()
        self.views_dir = self.config.cog_views_dir()

        if not os.path.exists(self.views_dir):
            os.mkdir(self.views_dir)
        self.logger.debug("predicates: " + str(self.all_predicates))

        self.last_visited_vertices = None
        self._predicate_reverse_lookup_cache = {}  # hash -> human-readable predicate name
        # Hydrate predicate names from persisted edge set for reopened graphs
        try:
            self.cog.use_namespace(self.graph_name)
            for pred_hash in self.all_predicates:
                edge_record = self.cog.use_table(self.config.GRAPH_EDGE_SET_TABLE_NAME).get(pred_hash)
                if edge_record is not None:
                    self._predicate_reverse_lookup_cache[pred_hash] = edge_record.value
        except Exception:
            pass  # Edge set table may not exist yet for new graphs
        self._server_port = None  # Port this graph is being served on
        self._default_provider = "cogdb"  # Provider for auto-embed in queries
        self._default_provider_kwargs = {}  # Provider kwargs (e.g. api_key)
        self._vectorize_configured = False  # True after explicit vectorize() call

    # === Cloud Traversal Helpers ===

    def _cloud_reset_chain(self):
        """Reset the cloud traversal chain for a new query."""
        self._cloud_chain = []

    def _cloud_append(self, method, **kwargs):
        """Append a traversal step to the cloud chain."""
        step = {"method": method}
        if kwargs:
            step["args"] = {k: v for k, v in kwargs.items() if v is not None}
        self._cloud_chain.append(step)
        return self

    def _cloud_execute_chain(self, terminal_method, **kwargs):
        """Send accumulated chain + terminal method to cloud and return results."""
        chain = list(self._cloud_chain)
        step = {"method": terminal_method}
        if kwargs:
            step["args"] = {k: v for k, v in kwargs.items() if v is not None}
        chain.append(step)
        result = self._cloud_client.query_chain(chain)
        self._cloud_reset_chain()
        # Strip cloud envelope to match local response format
        result.pop("ok", None)
        return result

    # === Network Methods ===
    
    def serve(self, port=8080, host="0.0.0.0", blocking=False, writable=False, share=False):
        """
        Start HTTP server for this graph instance.
        
        Multiple graphs can be served on the same port - each graph is accessible
        at /{graph_name}/ path. The index page at / lists all available graphs.
        
        Args:
            port: HTTP port to listen on (default 8080)
            host: Bind address (default "0.0.0.0" for all interfaces)
            blocking: If True, blocks forever (for dedicated servers)
            writable: If True, allows write operations via API
            share: If True, connect to CogDB relay
        
        Returns:
            self for method chaining
        
        Example:
            # Serve single graph
            g.serve(port=8080)
            
            # Serve multiple graphs on same port
            g1.serve(port=8080)
            g2.serve(port=8080)  # Both accessible at /{graph_name}/
            
            # Allow remote writes
            g.serve(port=8080, writable=True)
            
            # Share graph publicly
            g.serve(port=8080, share=True)
        """
        if self._cloud:
            raise RuntimeError(
                "g.serve() is not available in cloud mode. "
                "The graph is already hosted on CogDB Cloud."
            )
        from cog.server import get_or_create_server
        
        if self._server_port is not None:
            raise RuntimeError(f"Graph '{self.graph_name}' already being served. Call stop() first.")
        
        # Get or create shared server on this port
        server, is_new = get_or_create_server(port, host)
        
        # Check if this graph name is already registered
        if server.has_graph(self.graph_name):
            raise RuntimeError(f"Graph '{self.graph_name}' already registered on port {port}")
        
        # Register this graph
        server.register_graph(self, writable=writable)
        self._server_port = port
        
        # Start share if requested
        if share:
            from cog.share import start_share
            share_info = start_share(port, local_host=host)
            share_url = share_info.wait()
            self._share_url = share_url
            self.logger.info(f"Share connected: {share_url}")
        
        # Start server if it's new
        if is_new:
            server.start(blocking=blocking)
            
            # If blocking mode exited, clean up
            if blocking:
                self._server_port = None
        
        return self
    
    def stop(self):
        """
        Stop serving this graph.
        
        If this is the last graph on the server, the server shuts down.
        
        Returns:
            self for method chaining
        """
        from cog.server import unregister_from_server
        
        if self._server_port is not None:
            unregister_from_server(self._server_port, self.graph_name)
            self._server_port = None
        return self
    
    def share_url(self):
        """
        Get the public share URL for this graph.
        
        Only available after calling serve(share=True).
        
        Returns:
            str: The share URL (e.g., "https://abc123.s.cogdb.io/") or None if not sharing
        
        Example:
            g.serve(port=8080, share=True)
            print(g.share_url())  # https://abc123.s.cogdb.io/
        """
        return getattr(self, '_share_url', None)
    
    @classmethod
    def connect(cls, url, timeout=30):
        """
        Connect to a remote CogDB server.
        
        Args:
            url: HTTP(S) URL including graph name path
                 (e.g., "http://localhost:8080/my_graph")
            timeout: Request timeout in seconds (default 30)
        
        Returns:
            RemoteGraph instance that can be used like a local Graph
        
        Example:
            remote = Graph.connect("http://192.168.1.5:8080/social")
            remote.v("alice").out("knows").all()
            
            # Via ngrok
            remote = Graph.connect("https://abc123.ngrok.io/my_graph")
        """
        from cog.remote import RemoteGraph
        return RemoteGraph(url, timeout=timeout)

    def sync(self):
        """
        Force flush all pending writes to disk (local) or cloud.
        Blocks until all flushes are complete.
        
        Use this when flush_interval > 1 or when you need to ensure 
        data durability at a specific point.
        """
        if self._cloud:
            self._cloud_client.sync()
            return
        self.cog.sync()

    def refresh(self):
        if self._cloud:
            return  # No-op in cloud mode
        self.cog.refresh_all()

    def ls(self):
        """
        List all graph names accessible from this connection.

        In cloud mode, queries the server for all graphs under this API key.
        In local mode, scans the cog_home directory for graph subdirectories.

        Returns:
            list[str]: Sorted list of graph names.

        Example:
            g = Graph(api_key="sk-...")
            print(g.ls())  # ['default', 'products', 'social']

            g = Graph()
            print(g.ls())  # ['default', 'my_graph']
        """
        if self._cloud:
            return self._cloud_client.list_graphs()

        # Local mode: each graph is a subdirectory under cog_db_path
        db_path = self.config.cog_db_path()
        if not os.path.exists(db_path):
            return []
        skip = {self.config.COG_SYS_DIR, self.config.VIEWS}
        return sorted([
            d for d in os.listdir(db_path)
            if os.path.isdir(os.path.join(db_path, d)) and d not in skip
        ])

    def use(self, graph_name):
        """
        Switch this instance to a different graph.

        Flushes any pending writes before switching. The graph is created
        if it does not already exist (same behavior as the constructor).

        Args:
            graph_name: Name of the graph to switch to.

        Returns:
            self for method chaining.

        Example:
            g = Graph(api_key="sk-...")
            g.ls()                           # ['default', 'social']
            g.use("social").v("alice").out("knows").all()
        """
        if self._cloud:
            self._cloud_client.sync()  # flush pending mutations
            self.graph_name = graph_name
            self._cloud_client = CloudClient(
                graph_name, self._api_key, flush_interval=self._flush_interval
            )
            self._cloud_reset_chain()
            return self

        # Local mode: switch namespace
        self.graph_name = graph_name
        self.cog.create_or_load_namespace(graph_name)
        self.cog.use_namespace(graph_name)
        self.all_predicates = self.cog.list_tables()
        # Rebuild predicate reverse lookup cache for the new graph
        self._predicate_reverse_lookup_cache = {}
        try:
            for pred_hash in self.all_predicates:
                edge_record = self.cog.use_table(
                    self.config.GRAPH_EDGE_SET_TABLE_NAME
                ).get(pred_hash)
                if edge_record is not None:
                    self._predicate_reverse_lookup_cache[pred_hash] = edge_record.value
        except Exception:
            pass  # Edge set table may not exist yet for new graphs
        return self

    def updatej(self, json_object):
        self.put_json(json_object, True)

    def putj(self, json_object, update=False):
        """
        Shorthand for put_json
        :param update:
        :param json_object:
        :return: None
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

        def traverse(json_obj, subject, predicate=None, update_object=False, in_list=False):

            if type(json_obj) is dict:
                # every object has an id
                if "_id" in json_obj:
                    if in_list and update_object:
                        raise Exception("Updating a sub object or list item with an _id is not supported.")
                    child_id = str(BlankNode(json_obj["_id"]))
                else:
                    # if _id is not present generate one.
                    child_id = str(BlankNode())
                if predicate:
                    # this is to skip the first iteration where predicate is None.
                    # For items inside a list, always add (don't replace) even during updates
                    effective_update = update_object and not in_list
                    self.put(subject, predicate, child_id, effective_update)
                for a in json_obj:
                    # Properties of a dict are NOT in a list - reset in_list to False
                    traverse(json_obj[a], child_id, a, update_object, in_list=False)

            elif type(json_obj) is list:
                # create a new blank node for each list.
                list_id = str(BlankNode())
                self.put(subject, predicate, list_id, update_object)
                # new_edge_created.add((str(subject), str(predicate)))

                # traverse the list - mark items as being in a list
                for obj in json_obj:
                    traverse(obj, list_id, predicate, update_object, in_list=True)

            else:
                # For items inside a list, always add (don't replace) even during updates
                effective_update = update_object and not in_list
                if (str(subject), str(predicate)) in new_edge_created:
                    self.put(subject, predicate, json_obj, effective_update)
                else:
                    self.put(subject, predicate, json_obj, effective_update)
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
        :return: None
        """
        if self._cloud:
            # Read triples from file and send to cloud in batches
            batch = []
            batch_size = 1000
            with open(graph_data_path) as f:
                for line in f:
                    subject, predicate, obj, _ = parse_tripple(line)
                    batch.append({"s": subject, "p": predicate, "o": obj})
                    if len(batch) >= batch_size:
                        self._cloud_client.mutate_put_batch(batch)
                        batch = []
            if batch:
                self._cloud_client.mutate_put_batch(batch)
            return None

        graph_name = self.graph_name if graph_name is None else graph_name
        self.cog.load_triples(graph_data_path, graph_name)
        self.all_predicates = self.cog.list_tables()
        # Rebuild _predicate_reverse_lookup_cache by parsing the triples file
        with open(graph_data_path) as f:
            for line in f:
                _, predicate, _, _ = parse_tripple(line)
                self._predicate_reverse_lookup_cache[hash_predicate(predicate)] = predicate
        return None

    def load_csv(self, csv_path, id_column_name, graph_name=None):
        """
        Loads a CSV file to a graph. One column must be designated as ID column. This method is intended for loading
        simple CSV data, for more complex ones that require additional logic, convert the CSV to triples using custom
        logic.

        :param csv_path:
        :param id_column_name:
        :param graph_name:
        :return: None
        """

        if id_column_name is None:
            raise Exception("id_column_name must not be None")
        if self._cloud:
            # Read CSV locally and send triples to cloud in batches
            batch = []
            batch_size = 1000
            with open(csv_path) as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    subject = row[id_column_name]
                    for col, val in row.items():
                        if col != id_column_name:
                            batch.append({"s": subject, "p": col, "o": val})
                            if len(batch) >= batch_size:
                                self._cloud_client.mutate_put_batch(batch)
                                batch = []
            if batch:
                self._cloud_client.mutate_put_batch(batch)
            return None
        graph_name = self.graph_name if graph_name is None else graph_name
        self.cog.load_csv(csv_path, id_column_name, graph_name)
        self.all_predicates = self.cog.list_tables()
        # Rebuild _predicate_reverse_lookup_cache from CSV column headers
        import csv
        with open(csv_path) as csv_file:
            reader = csv.DictReader(csv_file)
            if reader.fieldnames:
                for col in reader.fieldnames:
                    self._predicate_reverse_lookup_cache[hash_predicate(col)] = col

    def close(self):
        if self._cloud:
            self._cloud_client.sync()  # flush any pending mutations
            return
        self.logger.info("closing graph: " + self.graph_name)
        self.cog.close()

    def put(self, vertex1, predicate, vertex2, update=False, create_new_edge=False):
        if self._cloud:
            self._cloud_client.mutate_put(vertex1, predicate, vertex2,
                                          update=update, create_new_edge=create_new_edge)
            return self
        self._predicate_reverse_lookup_cache[hash_predicate(predicate)] = predicate
        self.cog.use_namespace(self.graph_name)
        if update:
            if create_new_edge:
                self.cog.put_new_edge(vertex1, predicate, vertex2)
            else:
                self.cog.update_edge(vertex1, predicate, vertex2)
        else:
            self.cog.put_node(vertex1, predicate, vertex2)
        self.all_predicates = self.cog.list_tables()
        return self

    def put_batch(self, triples):
        """
        Insert multiple triples efficiently using batch mode.
        Significantly faster than calling put() in a loop for large datasets.
        
        :param triples: List of (vertex1, predicate, vertex2) tuples
        :return: self for method chaining
        
        Example:
            g.put_batch([
                ("alice", "follows", "bob"),
                ("bob", "follows", "charlie"),
                ("charlie", "follows", "alice")
            ])
        """
        if self._cloud:
            batch = []
            for v1, pred, v2 in triples:
                batch.append({"s": str(v1), "p": str(pred), "o": str(v2)})
                if len(batch) >= 1000:
                    self._cloud_client.mutate_put_batch(batch)
                    batch = []
            if batch:
                self._cloud_client.mutate_put_batch(batch)
            return self
        self.cog.use_namespace(self.graph_name)
        self.cog.begin_batch()
        try:
            for v1, pred, v2 in triples:
                self._predicate_reverse_lookup_cache[hash_predicate(pred)] = pred
                self.cog.put_node(v1, pred, v2)
        finally:
            self.cog.end_batch()
        self.all_predicates = self.cog.list_tables()
        return self

    def delete(self, vertex1, predicate, vertex2):
        """
        Removes a specific triple/edge from the graph.
        
        :param vertex1: Source vertex
        :param predicate: Edge predicate/relationship
        :param vertex2: Target vertex
        :return: self for method chaining
        
        Example:
            g.put("alice", "knows", "bob")
            g.delete("alice", "knows", "bob")
        """
        if self._cloud:
            self._cloud_client.mutate_delete(vertex1, predicate, vertex2)
            return self
        self.cog.delete_edge(vertex1, predicate, vertex2)
        return self

    def drop(self, *args):
        """
        Deletes the entire graph and its persistent storage from disk.
        
        WARNING: This is a destructive operation that cannot be undone.
        The graph object becomes unusable after this call.
        
        :return: None
        
        Example:
            g.drop()  # Deletes entire graph from disk
        """
        if len(args) > 0:
            raise DeprecationWarning(
                "drop(s, p, o) is deprecated. Use delete(s, p, o) for edges. "
                "Use drop() with no arguments to delete the entire graph."
            )
        if self._cloud:
            self._cloud_client.mutate_drop()
            return
        
        # Clear the cache
        if self.cache is not None:
            self.cache.clear()
        
        # Unregister from server if currently served
        self.stop()
        
        # Close the graph
        self.close()
        
        # Delete the entire graph directory using the same method Cog uses
        graph_path = self.config.cog_data_dir(self.graph_name)
        if os.path.exists(graph_path):
            shutil.rmtree(graph_path)

    def truncate(self):
        """
        Wipes all triples but keeps the graph structure/directory intact.
        
        Useful for resetting state without needing to re-initialize.
        The graph remains usable after this call.
        
        :return: self for method chaining
        
        Example:
            g.put("alice", "knows", "bob")
            g.truncate()  # Graph is now empty but still usable
            g.put("new", "data", "here")  # Works fine
        """
        if self._cloud:
            self._cloud_client.mutate_truncate()
            return self
        # Get the graph directory path using the same method Cog uses
        # This correctly handles CUSTOM_COG_DB_PATH if set
        graph_path = self.config.cog_data_dir(self.graph_name)
        
        # Save flush_interval before closing (access while cog is still open)
        flush_interval = self.cog.flush_interval
        
        # Close current connections
        self.cog.close()
        
        try:
            # Delete all contents but keep the directory
            if os.path.exists(graph_path):
                for item in os.listdir(graph_path):
                    item_path = os.path.join(graph_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
            
            # Clear the cache to prevent stale data
            if self.cache is not None:
                self.cache.clear()
        finally:
            # Re-initialize the graph to ensure the object remains usable,
            # even if some files could not be deleted.
            self.cog = Cog(self.cache, flush_interval=flush_interval, config=self.config)
            self.cog.create_or_load_namespace(self.graph_name)
            self.all_predicates = self.cog.list_tables()
        
        return self


    def update(self, vertex1, predicate, vertex2):
        self.updatej(vertex1, predicate, vertex2)
        return self

    def v(self, vertex=None, func=None):
        if self._cloud:
            self._cloud_reset_chain()
            if isinstance(vertex, list):
                return self._cloud_append("v", vertex=vertex)
            return self._cloud_append("v", vertex=vertex)
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
        """
        Traverse forward through edges.
        :param func:
        :param predicates: A string or a List of strings.
        :return: self for method chaining.
        """
        if self._cloud:
            p = predicates if isinstance(predicates, list) else ([predicates] if predicates else None)
            return self._cloud_append("out", predicates=p)

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
        """
        Traverse backward through edges.
        :param predicates: List of predicates
        :return: self for method chaining.
        """
        if self._cloud:
            p = predicates if isinstance(predicates, list) else ([predicates] if predicates else None)
            return self._cloud_append("inc", predicates=p)

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
                if in_record is not None:
                    for v_adj in in_record.value:
                        adjacent_vertices.append(Vertex(v_adj).set_edge(predicate))

        return adjacent_vertices

    def has(self, predicates, vertex):
        """
        Filters all outgoing edges from a vertex that matches a list of predicates.
        :param predicates: List of predicates
        :param vertex: Vertex ID
        :return: self for method chaining.
        """
        if self._cloud:
            p = predicates if isinstance(predicates, list) else ([predicates] if predicates else None)
            return self._cloud_append("has", predicates=p, vertex=vertex)

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
        :param predicates: List of predicates
        :param vertex: Vertex ID
        :return: self for method chaining.
        """
        if self._cloud:
            p = predicates if isinstance(predicates, list) else ([predicates] if predicates else None)
            return self._cloud_append("hasr", predicates=p, vertex=vertex)

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
        """
        Scan vertices or edges in the current graph namespace and return their IDs.
        :param limit: Maximum number of items to return from the scan
        :param scan_type: use 'v' to scan the vertex set or 'e' to scan the edge set
        :return: A dictionary containing a list of scanned item(vertex) IDs, e.g., `{'result': [{'id': '...'}]}`.
        """
        if self._cloud:
            result = self._cloud_client.query_scan(limit, scan_type)
            result.pop("ok", None)
            return result
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
                        parent_path = v._path or [{'vertex': v.id}]
                        v_adjacent_obj._path = parent_path + [
                            {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                            {'vertex': v_adjacent}
                        ]
                        traverse_vertex.append(v_adjacent_obj)
                    elif record.value_type == "l":
                        for v_adjacent in record.value:
                            self.logger.debug("record v: " + str(record.value) + " type: " + str(record.value_type))
                            if func is not None and not func(v_adjacent):
                                continue
                            v_adjacent_obj = Vertex(v_adjacent).set_edge(predicate)
                            v_adjacent_obj.tags.update(v.tags)
                            parent_path = v._path or [{'vertex': v.id}]
                            v_adjacent_obj._path = parent_path + [
                                {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                                {'vertex': v_adjacent}
                            ]
                            traverse_vertex.append(v_adjacent_obj)
        self.last_visited_vertices = traverse_vertex

    def filter(self, func):
        """
            Applies a filter function to the vertices and removes any vertices that do not pass the filter.
        """
        if self._cloud:
            raise RuntimeError(
                "filter() with a Python lambda is not supported in cloud mode. "
                "Use has()/hasr()/is_() for server-side filtering."
            )
        self.last_visited_vertices = [v for v in self.last_visited_vertices if func(v.id)]
        return self

    def both(self, predicates=None):
        """
        Traverse edges in both directions (out + in).
        :param predicates: A string or list of predicate strings to follow.
        :return: self for method chaining.
        """
        if self._cloud:
            p = predicates if isinstance(predicates, list) else ([predicates] if predicates else None)
            return self._cloud_append("both", predicates=p)

        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        self.cog.use_namespace(self.graph_name)
        traverse_vertex = []

        for predicate in predicates:
            for v in self.last_visited_vertices:
                # Outgoing edges
                out_record = self.cog.use_table(predicate).get(out_nodes(v.id))
                if out_record is not None:
                    if out_record.value_type == "s":
                        v_adjacent = str(out_record.value)
                        v_adj = Vertex(v_adjacent).set_edge(predicate)
                        v_adj.tags.update(v.tags)
                        parent_path = v._path or [{'vertex': v.id}]
                        v_adj._path = parent_path + [
                            {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                            {'vertex': v_adjacent}
                        ]
                        traverse_vertex.append(v_adj)
                    elif out_record.value_type == "l":
                        for v_adjacent in out_record.value:
                            v_adj = Vertex(v_adjacent).set_edge(predicate)
                            v_adj.tags.update(v.tags)
                            parent_path = v._path or [{'vertex': v.id}]
                            v_adj._path = parent_path + [
                                {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                                {'vertex': v_adjacent}
                            ]
                            traverse_vertex.append(v_adj)

                # Incoming edges
                in_record = self.cog.use_table(predicate).get(in_nodes(v.id))
                if in_record is not None:
                    if in_record.value_type == "s":
                        v_adjacent = str(in_record.value)
                        v_adj = Vertex(v_adjacent).set_edge(predicate)
                        v_adj.tags.update(v.tags)
                        parent_path = v._path or [{'vertex': v.id}]
                        v_adj._path = parent_path + [
                            {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                            {'vertex': v_adjacent}
                        ]
                        traverse_vertex.append(v_adj)
                    elif in_record.value_type == "l":
                        for v_adjacent in in_record.value:
                            v_adj = Vertex(v_adjacent).set_edge(predicate)
                            v_adj.tags.update(v.tags)
                            parent_path = v._path or [{'vertex': v.id}]
                            v_adj._path = parent_path + [
                                {'edge': self._predicate_reverse_lookup_cache.get(predicate, predicate)},
                                {'vertex': v_adjacent}
                            ]
                            traverse_vertex.append(v_adj)

        self.last_visited_vertices = traverse_vertex
        return self

    def is_(self, *nodes):
        """
        Filter paths to only those currently at the specified node(s).
        :param nodes: One or more node IDs to filter to.
        :return: self for method chaining.
        """
        if self._cloud:
            node_list = list(nodes[0]) if (len(nodes) == 1 and isinstance(nodes[0], list)) else list(nodes)
            return self._cloud_append("is_", nodes=node_list)
        if len(nodes) == 1 and isinstance(nodes[0], list):
            node_set = set(nodes[0])
        else:
            node_set = set(nodes)
        self.last_visited_vertices = [v for v in self.last_visited_vertices if v.id in node_set]
        return self

    def unique(self):
        """
        Remove duplicate vertices from the result set.
        :return: self for method chaining.
        """
        if self._cloud:
            return self._cloud_append("unique")
        seen = set()
        unique_vertices = []
        for v in self.last_visited_vertices:
            if v.id not in seen:
                seen.add(v.id)
                unique_vertices.append(v)
        self.last_visited_vertices = unique_vertices
        return self

    def limit(self, n):
        """
        Limit results to the first N vertices.
        :param n: Maximum number of vertices to return.
        :return: self for method chaining.
        """
        if self._cloud:
            return self._cloud_append("limit", n=n)
        self.last_visited_vertices = self.last_visited_vertices[:n]
        return self

    def skip(self, n):
        """
        Skip the first N vertices in the result set.
        :param n: Number of vertices to skip.
        :return: self for method chaining.
        """
        if self._cloud:
            return self._cloud_append("skip", n=n)
        self.last_visited_vertices = self.last_visited_vertices[n:]
        return self

    def order(self, direction="asc"):
        '''
        Sort vertices by their id.
        :param direction: Sort direction - "asc" (default) or "desc".
        :return: self for method chaining.

        Example:
            g.v("Person").out("created_at").order().all()        # ascending (default)
            g.v("Person").out("created_at").order(desc).all()    # descending
        '''
        if self._cloud:
            return self._cloud_append("order", direction=direction)
        reverse = (direction == "desc")
        self.last_visited_vertices = sorted(self.last_visited_vertices, key=lambda v: v.id, reverse=reverse)
        return self

    def back(self, tag):
        """
        Return to vertices saved at the given tag, preserving all constraints.
        :param tag: A previous tag in the query to jump back to.
        :return: self for method chaining.
        """
        if self._cloud:
            return self._cloud_append("back", tag=tag)
        vertices = []
        for v in self.last_visited_vertices:
            if tag in v.tags:
                tagged_vertex = Vertex(v.tags[tag])
                tagged_vertex.tags = v.tags.copy()
                tagged_vertex.edges = v.edges.copy()
                tagged_vertex._path = v._path
                vertices.append(tagged_vertex)
        self.last_visited_vertices = vertices
        return self

    def tag(self, tag_names):
        """
        Saves vertices with tag name(s). Used to capture vertices while traversing a graph.
        
        Tag names are validated to prevent XSS when used in graph views.
        
        :param tag_names: A string or list of strings.
        :return: self for method chaining.
        """
        if self._cloud:
            names = tag_names if isinstance(tag_names, list) else [tag_names]
            return self._cloud_append("tag", tag_names=names)
        if not isinstance(tag_names, list):
            tag_names = [tag_names]
        for v in self.last_visited_vertices:
            for tag_name in tag_names:
                # Validate tag name to prevent XSS in view output
                if not isinstance(tag_name, str):
                    raise TypeError("Tag names must be strings")
                v.tags[tag_name] = v.id
        return self

    def count(self):
        if self._cloud:
            result = self._cloud_execute_chain("count")
            return result.get("result", result.get("count", 0))
        return len(self.last_visited_vertices)

    def all(self, options=None):
        """
        Returns all the vertices that are resultant of the graph query. Options 'e' would include the edges that were traversed.
        https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md
        :return:
        """
        if self._cloud:
            return self._cloud_execute_chain("all", options=options)
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

    def graph(self):
        """
        Returns graph structure ready for D3.js / vis.js visualization.
        Deduplicates nodes and edges from all traversal paths.

        Returns:
            dict with 'nodes' (list of {id}) and 'links' (list of {source, target, label})

        Example:
            g.v("bob").out("follows").tag("from").out("works_at").tag("to").graph()
            # {'nodes': [{'id': 'bob'}, ...], 'links': [{'source': 'bob', 'target': 'fred', 'label': 'follows'}, ...]}
        """
        if self._cloud:
            return self._cloud_execute_chain("graph")
        nodes = {}
        links = {}

        for v in self.last_visited_vertices:
            path = v._path or []
            for i, step in enumerate(path):
                if 'vertex' in step:
                    nodes[step['vertex']] = {'id': step['vertex']}
                if 'edge' in step and i > 0 and i < len(path) - 1:
                    src = path[i - 1]['vertex']
                    tgt = path[i + 1]['vertex']
                    key = (src, step['edge'], tgt)
                    links[key] = {
                        'source': src,
                        'target': tgt,
                        'label': step['edge']
                    }

        return {
            'nodes': list(nodes.values()),
            'links': list(links.values())
        }

    def triples(self):
        """
        Returns all triples in the graph as a list of (subject, predicate, object) tuples.

        Iterates over every vertex and every predicate to reconstruct
        the complete set of triples stored in the graph.

        :return: List of (subject, predicate, object) tuples.

        Example:
            g.put("alice", "follows", "bob")
            g.put("bob", "follows", "charlie")
            g.triples()
            # [("alice", "follows", "bob"), ("bob", "follows", "charlie")]
        """
        if self._cloud:
            result = self._cloud_client.query_triples()
            result.pop("ok", None)
            # Support both {"triples": [...]} and standard {"result": [...]} envelopes
            raw = result.get("triples", result.get("result", []))
            triples_list = []
            for item in raw:
                if isinstance(item, (list, tuple)) and len(item) >= 3:
                    triples_list.append(tuple(item[:3]))
                elif isinstance(item, dict) and "s" in item:
                    triples_list.append((item["s"], item["p"], item["o"]))
            return triples_list
        from cog.export import get_triples
        return list(get_triples(self))

    def export(self, filepath, fmt="nt", strict=False):
        """
        Export all triples in the graph to a file.

        Writes one triple per line in the specified format.

        :param filepath: Path to the output file.
        :param fmt: Format string — "nt" (N-Triples, default), "csv", or "tsv".
        :param strict: If True and fmt is "nt", output W3C-compliant N-Triples.
                       IRIs are wrapped in <>, blank nodes use _: prefix,
                       and plain literals are quoted with "".
                       See https://www.w3.org/TR/n-triples/
        :return: Number of triples written.

        Example:
            g.export("graph.nt")                        # N-Triples (default)
            g.export("graph.nt", strict=True)            # W3C strict N-Triples
            g.export("graph.csv", fmt="csv")             # CSV with header
            g.export("graph.tsv", fmt="tsv")             # TSV with header
        """
        if self._cloud:
            cloud_triples = self.triples()
            from cog.export import export_triples
            return export_triples(self, filepath, fmt=fmt, strict=strict, triples_iter=cloud_triples)
        from cog.export import export_triples
        return export_triples(self, filepath, fmt=fmt, strict=strict)

    def view(self, view_name, persist=True):
        """
        Returns an interactive D3.js graph view of the query result.

        :param view_name: Name for the view (used as the filename when persisted).
        :param persist: If True (default), save the view as an HTML file.
        :return: A :class:`~cog.view.View` object. Call ``.render()`` to
            display in a Jupyter/Colab notebook.
        """
        assert view_name is not None, "a view name is required to create a view, it can be any string."
        result = self.graph()
        view_html = build_graph_html(result)
        safe_name = os.path.basename(view_name)
        if self.views_dir:
            view_path = os.path.join(self.views_dir, safe_name + ".html")
        else:
            view_path = None
        view = View(view_path, view_html, graph_data=result)
        if persist and view_path:
            view.persist()
        return view

    def show(self, height=500, width=700, dark=False):
        """
        Render the current traversal result as an interactive graph
        directly inside a Jupyter or Google Colab notebook.

        This is a convenience method equivalent to::

            g.v().out().view("tmp", persist=False).render()

        It does not persist an HTML file to disk.

        :param height: Height of the rendered view in pixels.
        :param width: Width of the rendered view in pixels.
        :param dark: If True, use dark background theme.
        :return: A :class:`~cog.view.View` object (after rendering).
        """

        view = self.view("_show_tmp", persist=False)
        view.render(height=height, width=width, dark=dark)
        return view

    def getv(self, view_name):
        if self._cloud:
            raise RuntimeError("getv() is not supported in cloud mode")
        safe_name = os.path.basename(view_name)
        view_path = os.path.join(self.views_dir, safe_name + ".html")
        assert os.path.isfile(view_path), "view not found, create a view by calling .view()"
        with open(view_path, 'r') as f:
            view_html = f.read()
        graph_data = View.extract_graph_data(view_html)
        view = View(view_path, view_html, graph_data=graph_data)
        return view

    def lsv(self):
        if self._cloud:
            raise RuntimeError("lsv() is not supported in cloud mode")
        return [f.split(".")[0] for f in listdir(self.views_dir)]


    def get_new_graph_instance(self):
        return Graph(self.graph_name, self.config.COG_HOME, self.config.COG_PATH_PREFIX)
