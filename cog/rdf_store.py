"""
rdflib Store adapter for CogDB.

CogStore exposes a cog.torque.Graph as an rdflib-compatible triple store,
which gives CogDB full SPARQL 1.1 (SELECT, ASK, CONSTRUCT, DESCRIBE, property
paths, aggregates, OPTIONAL/UNION/FILTER, ...) through rdflib's query engine,
plus rdflib's parsers and serializers (Turtle, N-Triples, JSON-LD, RDF/XML).

The adapter maps rdflib's pattern-at-a-time triples() calls onto CogDB's
per-predicate adjacency tables, so bound lookups hit the same O(1) paths
Torque's out()/inc() use:

    (s, p, o)   membership test in s's out-set for p
    (s, p, ?)   out-neighbors of s in p's table
    (?, p, o)   in-neighbors of o in p's table
    (?, p, ?)   scan of p's table
    (.., ?, ..) the above, looped over every predicate table

Terms are converted at this boundary — and only here — using cog.rdf_terms.

Usage:
    from cog.torque import Graph
    g = Graph("my_graph")
    g.sparql("SELECT ?x WHERE { <alice> <follows> ?x }")   # uses CogStore

    # or explicitly, as a plugin:
    import rdflib
    rg = rdflib.Graph(store="cogdb")
    rg.open("my_graph")
"""
import logging

from rdflib.store import Store, VALID_STORE

from cog.database import hash_predicate
from cog.rdf_terms import decode_term, encode_term

logger = logging.getLogger(__name__)


class CogStore(Store):
    """An rdflib Store backed by a cog.torque.Graph."""

    context_aware = False
    formula_aware = False
    transaction_aware = False
    graph_aware = False

    def __init__(self, configuration=None, identifier=None):
        """
        :param configuration: a cog.torque.Graph instance, or a graph name
            string (the plugin path: rdflib.Graph(store="cogdb").open("name")),
            or None to open() later.
        :param identifier: rdflib graph identifier (unused, kept for the
            plugin protocol).
        """
        self.__namespace = {}
        self.__prefix = {}
        self.cog_graph = None
        graph = None
        if configuration is not None and not isinstance(configuration, str):
            graph, configuration = configuration, None
        super().__init__(configuration=configuration, identifier=identifier)
        if graph is not None:
            self._attach(graph)

    # === Store lifecycle ===

    def _attach(self, graph):
        if getattr(graph, "_cloud", False):
            raise NotImplementedError(
                "SPARQL over cloud-mode graphs is not supported yet; "
                "open the graph locally.")
        self.cog_graph = graph

    def open(self, configuration, create=False):
        if isinstance(configuration, str) and configuration:
            from cog.torque import Graph as CogGraph
            self._attach(CogGraph(graph_name=configuration))
        return VALID_STORE

    def close(self, commit_pending_transaction=False):
        pass  # the cog Graph owns its lifecycle

    def _graph(self):
        if self.cog_graph is None:
            raise RuntimeError(
                "CogStore is not attached to a graph; pass a cog.torque.Graph "
                "to CogStore(...) or call open(graph_name).")
        return self.cog_graph

    # === CogDB access helpers ===

    def _predicate_entries(self, pred=None):
        """Yield (pred_hash, predicate_name) pairs.

        With pred (an encoded predicate name) bound, yields at most one entry
        and only if that predicate exists — a lookup on a missing predicate
        must not create its table files. Unbound, yields every real predicate,
        skipping CogDB-internal tables.
        """
        g = self._graph()
        if pred is not None:
            pred_hash = hash_predicate(pred)
            if pred_hash in g.all_predicates:
                yield pred_hash, pred
            return
        internal = {g.config.GRAPH_NODE_SET_TABLE_NAME,
                    g.config.GRAPH_EDGE_SET_TABLE_NAME,
                    g.config.EMBEDDING_SET_TABLE_NAME}
        for pred_hash in g.all_predicates:
            if pred_hash in internal:
                continue
            name = g._predicate_reverse_lookup_cache.get(pred_hash)
            if name is None:
                record = g.cog.use_namespace(g.graph_name).use_table(
                    g.config.GRAPH_EDGE_SET_TABLE_NAME).get(pred_hash)
                if record is None:
                    continue
                name = record.value
                g._predicate_reverse_lookup_cache[pred_hash] = name
            yield pred_hash, name

    def _neighbors(self, pred_hash, node, direction):
        """Adjacent vertex ids of node for one predicate, or None."""
        g = self._graph()
        mg = g._get_mg(pred_hash)
        if mg is not None:
            nbrs = mg.get_out(node) if direction == 'out' else mg.get_in(node)
        else:
            nbrs = g._disk_get_neighbors(pred_hash, node, direction)
        if isinstance(nbrs, str):
            # Single-value record ('s' type, created via put_new_edge):
            # iterating it directly would yield characters.
            return (nbrs,)
        return nbrs

    def _iter_edges(self, pred_hash):
        """Yield (subject, object) stored-string pairs for one predicate."""
        g = self._graph()
        table = g.cog.get_table(pred_hash, g.graph_name)
        for record in table.indexer.scanner(table.store):
            key = record.key
            if not isinstance(key, (bytes, bytearray)) or key[:1] != b'\x00':
                continue  # only out-direction keys; \x01 mirrors them
            subject = key[1:].decode('utf-8')
            value = record.value
            # scanner() re-wraps records with a default value_type, so
            # branch on the runtime type: str is a single object, list/set
            # is a materialized value chain.
            if isinstance(value, str):
                yield subject, value
            else:
                for obj in value:
                    yield subject, obj

    @staticmethod
    def _contexts():
        return iter(())

    # === Store read interface ===

    def triples(self, triple_pattern, context=None):
        s, p, o = triple_pattern
        g = self._graph()
        g.cog.use_namespace(g.graph_name)

        enc_s = encode_term(s) if s is not None else None
        enc_p = encode_term(p) if p is not None else None
        enc_o = encode_term(o) if o is not None else None

        for pred_hash, pred_name in self._predicate_entries(enc_p):
            p_term = p if p is not None else decode_term(pred_name)
            if enc_s is not None and enc_o is not None:
                nbrs = self._neighbors(pred_hash, enc_s, 'out')
                if nbrs and enc_o in nbrs:
                    yield (s, p_term, o), self._contexts()
            elif enc_s is not None:
                nbrs = self._neighbors(pred_hash, enc_s, 'out')
                if nbrs:
                    for obj in nbrs:
                        yield (s, p_term, decode_term(obj)), self._contexts()
            elif enc_o is not None:
                nbrs = self._neighbors(pred_hash, enc_o, 'in')
                if nbrs:
                    for subj in nbrs:
                        yield (decode_term(subj), p_term, o), self._contexts()
            else:
                for subj, obj in self._iter_edges(pred_hash):
                    yield (decode_term(subj), p_term, decode_term(obj)), \
                        self._contexts()

    def __len__(self, context=None):
        return sum(1 for _ in self.triples((None, None, None)))

    # === Store write interface ===

    def add(self, triple, context, quoted=False):
        s, p, o = triple
        self._graph().put(encode_term(s), encode_term(p), encode_term(o))

    def addN(self, quads):
        batch = [(encode_term(s), encode_term(p), encode_term(o))
                 for s, p, o, _ in quads]
        if batch:
            self._graph().put_batch(batch)

    def remove(self, triple_pattern, context=None):
        # Materialize matches first: don't mutate tables mid-scan.
        matched = [t for t, _ in self.triples(triple_pattern, context)]
        g = self._graph()
        g.cog.use_namespace(g.graph_name)
        for s, p, o in matched:
            g.delete(encode_term(s), encode_term(p), encode_term(o))

    # === Namespace persistence (in-memory, per session) ===

    def bind(self, prefix, namespace, override=True):
        bound_namespace = self.__namespace.get(prefix)
        bound_prefix = self.__prefix.get(namespace)
        if override:
            if bound_prefix is not None:
                del self.__namespace[bound_prefix]
            if bound_namespace is not None:
                del self.__prefix[bound_namespace]
            self.__prefix[namespace] = prefix
            self.__namespace[prefix] = namespace
        else:
            self.__prefix[bound_namespace or namespace] = bound_prefix or prefix
            self.__namespace[bound_prefix or prefix] = bound_namespace or namespace

    def namespace(self, prefix):
        return self.__namespace.get(prefix)

    def prefix(self, namespace):
        return self.__prefix.get(namespace)

    def namespaces(self):
        for prefix, namespace in self.__namespace.items():
            yield prefix, namespace
