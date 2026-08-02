import os
import shutil
import unittest

import pytest

rdflib = pytest.importorskip("rdflib", reason="SPARQL tests require rdflib")

from rdflib import BNode, Literal, URIRef
from rdflib.namespace import XSD

from cog.rdf_terms import decode_term, encode_term
from cog.torque import Graph

DIR_NAME = "SparqlTest"


def graph_home(name):
    path = "/tmp/" + name
    if os.path.exists(path):
        shutil.rmtree(path)
    return name


class TermCodecTest(unittest.TestCase):
    """encode_term/decode_term must be bijective across all term kinds."""

    def test_round_trip_all_kinds(self):
        terms = [
            URIRef("alice"),
            URIRef("http://example.org/alice"),
            Literal("chat"),
            Literal("chat", lang="fr"),
            Literal("42", datatype=XSD.integer),
            Literal(29),
            Literal(4.5),
            Literal(True),
            Literal('he said "hi"\nbye'),
            BNode("b0"),
            BNode("1654006783197959000lIxa"),  # put_json style label
        ]
        for term in terms:
            stored = encode_term(term)
            self.assertIsInstance(stored, str)
            self.assertEqual(decode_term(stored), term,
                             "decode(encode(t)) != t for {!r} (stored as {!r})".format(term, stored))

    def test_distinct_terms_distinct_strings(self):
        terms = [
            Literal("chat"),
            Literal("chat", lang="fr"),
            Literal("chat", lang="en"),
            URIRef("chat"),
            Literal("chat", datatype=XSD.string),
            BNode("chat"),
        ]
        encoded = [encode_term(t) for t in terms]
        self.assertEqual(len(set(encoded)), len(terms), "term encodings collided: " + str(encoded))

    def test_bare_torque_strings_decode_as_iris(self):
        self.assertEqual(decode_term("alice"), URIRef("alice"))
        self.assertEqual(decode_term("http://example.org/x"), URIRef("http://example.org/x"))

    def test_putj_blank_node_ids_decode_as_bnodes(self):
        self.assertEqual(decode_term("_:id_user1"), BNode("id_user1"))

    def test_legacy_junk_never_raises(self):
        # A vertex id that merely starts with a quote but is not valid N3.
        self.assertEqual(decode_term('"unclosed'), URIRef('"unclosed'))

    def test_encode_rejects_non_terms(self):
        with self.assertRaises(TypeError):
            encode_term("plain string")
        with self.assertRaises(TypeError):
            encode_term(29)


class CogStoreTest(unittest.TestCase):
    """Pattern dispatch and write-through at the rdflib Graph level."""

    @classmethod
    def setUpClass(cls):
        cls.g = Graph(graph_name="store_test", cog_home=graph_home("SparqlStoreTest"))
        cls.rg = cls.g.rdf()
        cls.alice = URIRef("http://ex/alice")
        cls.bob = URIRef("http://ex/bob")
        cls.fred = URIRef("http://ex/fred")
        cls.knows = URIRef("http://ex/knows")
        cls.name = URIRef("http://ex/name")
        cls.rg.add((cls.alice, cls.knows, cls.bob))
        cls.rg.add((cls.bob, cls.knows, cls.fred))
        cls.rg.add((cls.alice, cls.name, Literal("Alice")))

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/SparqlStoreTest", ignore_errors=True)

    def _match(self, pattern):
        return set(self.rg.triples(pattern))

    def test_all_eight_pattern_shapes(self):
        a, k, b = self.alice, self.knows, self.bob
        # (s, p, o)
        self.assertEqual(self._match((a, k, b)), {(a, k, b)})
        # (s, p, ?)
        self.assertEqual(self._match((a, k, None)), {(a, k, b)})
        # (?, p, o)
        self.assertEqual(self._match((None, k, b)), {(a, k, b)})
        # (?, p, ?)
        self.assertEqual(self._match((None, k, None)),
                         {(a, k, b), (b, k, self.fred)})
        # (s, ?, ?)
        self.assertEqual(self._match((a, None, None)),
                         {(a, k, b), (a, self.name, Literal("Alice"))})
        # (s, ?, o)
        self.assertEqual(self._match((a, None, b)), {(a, k, b)})
        # (?, ?, o)
        self.assertEqual(self._match((None, None, b)), {(a, k, b)})
        # (?, ?, ?)
        self.assertEqual(len(self._match((None, None, None))), 3)

    def test_len(self):
        self.assertEqual(len(self.rg), 3)

    def test_no_match_returns_empty(self):
        self.assertEqual(self._match((self.fred, self.knows, None)), set())
        self.assertEqual(self._match((None, URIRef("http://ex/none"), None)), set())

    def test_missing_predicate_lookup_creates_no_files(self):
        graph_dir = self.g.config.cog_data_dir(self.g.graph_name)
        before = set(os.listdir(graph_dir))
        list(self.rg.triples((None, URIRef("http://ex/ghost_predicate"), None)))
        self.g.sparql("SELECT ?x WHERE { <nope> <http://ex/ghost_predicate> ?x }")
        after = set(os.listdir(graph_dir))
        self.assertEqual(before, after, "querying a missing predicate created table files")

    def test_write_via_rdflib_visible_to_torque(self):
        result = self.g.v("http://ex/alice").out("http://ex/knows").all()
        self.assertEqual([v["id"] for v in result["result"]], ["http://ex/bob"])


class SparqlQueryTest(unittest.TestCase):
    """SPARQL over Torque-written data — the interop path."""

    @classmethod
    def setUpClass(cls):
        cls.g = Graph(graph_name="people", cog_home=graph_home("SparqlQueryTest"))
        cls.g.put("alice", "follows", "bob")
        cls.g.put("bob", "follows", "fred")
        cls.g.put("dani", "follows", "bob")
        cls.g.put("alice", "status", "cool_person")
        cls.g.put("fred", "status", "cool_person")
        # typed and tagged literals via the RDF surface
        rg = cls.g.rdf()
        rg.add((URIRef("alice"), URIRef("age"), Literal(29)))
        rg.add((URIRef("bob"), URIRef("age"), Literal(35)))
        rg.add((URIRef("alice"), URIRef("greeting"), Literal("chat", lang="fr")))
        rg.add((URIRef("bob"), URIRef("greeting"), Literal("chat")))

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/SparqlQueryTest", ignore_errors=True)

    def bindings(self, result):
        return result["results"]["bindings"]

    def values(self, result, var):
        return sorted(b[var]["value"] for b in self.bindings(result))

    def test_select_over_torque_data(self):
        res = self.g.sparql("SELECT ?x WHERE { <alice> <follows> ?x }")
        self.assertEqual(res["head"]["vars"], ["x"])
        self.assertEqual(self.bindings(res), [{"x": {"type": "uri", "value": "bob"}}])

    def test_join_two_hops(self):
        res = self.g.sparql("SELECT ?x WHERE { <alice> <follows> ?y . ?y <follows> ?x }")
        self.assertEqual(self.values(res, "x"), ["fred"])

    def test_reverse_lookup(self):
        res = self.g.sparql("SELECT ?who WHERE { ?who <follows> <bob> }")
        self.assertEqual(self.values(res, "who"), ["alice", "dani"])

    def test_variable_predicate(self):
        res = self.g.sparql("SELECT ?p WHERE { <alice> ?p <bob> }")
        self.assertEqual(self.values(res, "p"), ["follows"])

    def test_filter_on_typed_literal(self):
        res = self.g.sparql(
            "SELECT ?who WHERE { ?who <age> ?age FILTER(?age > 30) }")
        self.assertEqual(self.values(res, "who"), ["bob"])

    def test_language_tags_do_not_merge(self):
        res = self.g.sparql('SELECT ?who WHERE { ?who <greeting> "chat" }')
        self.assertEqual(self.values(res, "who"), ["bob"])
        res = self.g.sparql('SELECT ?who WHERE { ?who <greeting> "chat"@fr }')
        self.assertEqual(self.values(res, "who"), ["alice"])

    def test_optional(self):
        res = self.g.sparql(
            "SELECT ?x ?age WHERE { <alice> <follows> ?x OPTIONAL { ?x <age> ?age } }")
        rows = self.bindings(res)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["x"]["value"], "bob")
        self.assertEqual(rows[0]["age"]["value"], "35")

    def test_union(self):
        res = self.g.sparql(
            "SELECT ?x WHERE { { <alice> <follows> ?x } UNION { <bob> <follows> ?x } }")
        self.assertEqual(self.values(res, "x"), ["bob", "fred"])

    def test_order_limit(self):
        res = self.g.sparql(
            "SELECT ?s WHERE { ?s <follows> ?o } ORDER BY ?s LIMIT 2")
        self.assertEqual([b["s"]["value"] for b in self.bindings(res)], ["alice", "bob"])

    def test_count_aggregate(self):
        res = self.g.sparql(
            "SELECT (COUNT(?x) AS ?n) WHERE { ?x <status> <cool_person> }")
        self.assertEqual(self.values(res, "n"), ["2"])

    def test_property_path(self):
        res = self.g.sparql("SELECT ?x WHERE { <alice> <follows>+ ?x }")
        self.assertEqual(self.values(res, "x"), ["bob", "fred"])

    def test_ask(self):
        self.assertTrue(self.g.sparql("ASK { <alice> <follows> <bob> }")["boolean"])
        self.assertFalse(self.g.sparql("ASK { <bob> <follows> <alice> }")["boolean"])

    def test_construct_returns_n3_triples(self):
        triples = self.g.sparql(
            "CONSTRUCT { ?x <knows> ?y } WHERE { ?x <follows> ?y }")
        self.assertIn(("<alice>", "<knows>", "<bob>"), triples)
        self.assertEqual(len(triples), 3)

    def test_init_ns_and_bindings(self):
        res = self.g.sparql(
            "SELECT ?x WHERE { ?who ex:follows ?x }",
            init_ns={"ex": ""},
            init_bindings={"who": URIRef("alice")})
        self.assertEqual(self.values(res, "x"), ["bob"])

    def test_differential_against_rdflib_memory_store(self):
        """Same triples, same queries: CogStore must agree with rdflib's
        own in-memory store."""
        mem = rdflib.Graph()
        for t in self.g.rdf():
            mem.add(t)
        queries = [
            "SELECT ?s ?o WHERE { ?s <follows> ?o }",
            "SELECT ?s WHERE { ?s <follows> ?y . ?y <follows> ?o }",
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
            "SELECT ?s WHERE { ?s <age> ?a FILTER(?a >= 29) }",
            "SELECT ?x WHERE { <alice> <follows>* ?x }",
        ]
        for q in queries:
            cog_rows = sorted(map(str, self.g.rdf().query(q)))
            mem_rows = sorted(map(str, mem.query(q)))
            self.assertEqual(cog_rows, mem_rows, "engines disagree on: " + q)


class SparqlMutationTest(unittest.TestCase):
    """remove()/delete paths get a fresh graph per test."""

    def setUp(self):
        self.g = Graph(graph_name="mut", cog_home=graph_home("SparqlMutationTest"))
        self.rg = self.g.rdf()
        self.rg.add((URIRef("a"), URIRef("p"), URIRef("b")))
        self.rg.add((URIRef("a"), URIRef("p"), URIRef("c")))
        self.rg.add((URIRef("a"), URIRef("q"), Literal("x")))

    def tearDown(self):
        self.g.close()
        shutil.rmtree("/tmp/SparqlMutationTest", ignore_errors=True)

    def test_remove_specific_triple(self):
        self.rg.remove((URIRef("a"), URIRef("p"), URIRef("b")))
        self.assertEqual(len(self.rg), 2)
        self.assertFalse(self.g.sparql("ASK { <a> <p> <b> }")["boolean"])
        self.assertTrue(self.g.sparql("ASK { <a> <p> <c> }")["boolean"])

    def test_remove_with_wildcards(self):
        self.rg.remove((URIRef("a"), URIRef("p"), None))
        self.assertEqual(len(self.rg), 1)
        self.assertTrue(self.g.sparql("ASK { <a> <q> ?x }")["boolean"])

    def test_torque_delete_visible_to_sparql(self):
        self.g.delete("a", "p", "b")
        self.assertEqual(self.values(), ["c"])

    def values(self):
        res = self.g.sparql("SELECT ?x WHERE { <a> <p> ?x }")
        return sorted(b["x"]["value"] for b in res["results"]["bindings"])


class LoadRdfTest(unittest.TestCase):

    TTL = """
    @prefix ex: <http://example.org/> .
    ex:alice ex:knows ex:bob ;
             ex:name "Alice" ;
             ex:age 29 .
    ex:bob ex:knows ex:fred .
    """

    def setUp(self):
        self.g = Graph(graph_name="ttl", cog_home=graph_home("SparqlLoadTest"))
        self.ttl_path = "/tmp/SparqlLoadTest_data.ttl"
        with open(self.ttl_path, "w") as f:
            f.write(self.TTL)

    def tearDown(self):
        self.g.close()
        shutil.rmtree("/tmp/SparqlLoadTest", ignore_errors=True)
        os.remove(self.ttl_path)

    def test_load_turtle_and_query_both_surfaces(self):
        self.g.load_rdf(self.ttl_path)
        # SPARQL surface
        res = self.g.sparql(
            "SELECT ?x WHERE { <http://example.org/alice> <http://example.org/knows>+ ?x }")
        got = sorted(b["x"]["value"] for b in res["results"]["bindings"])
        self.assertEqual(got, ["http://example.org/bob", "http://example.org/fred"])
        # Torque surface sees the same vertices
        torque = self.g.v("http://example.org/alice").out("http://example.org/knows").all()
        self.assertEqual([v["id"] for v in torque["result"]], ["http://example.org/bob"])
        # Typed literal survived the load
        res = self.g.sparql(
            "SELECT ?a WHERE { <http://example.org/alice> <http://example.org/age> ?a "
            "FILTER(?a > 25) }")
        self.assertEqual(len(res["results"]["bindings"]), 1)


class SparqlDiskModeTest(unittest.TestCase):
    """Same dispatch with the memory view disabled (pure disk reads)."""

    def setUp(self):
        self.g = Graph(graph_name="disk", cog_home=graph_home("SparqlDiskTest"),
                       use_memory_view=False)
        self.g.put("alice", "follows", "bob")
        self.g.put("bob", "follows", "fred")

    def tearDown(self):
        self.g.close()
        shutil.rmtree("/tmp/SparqlDiskTest", ignore_errors=True)

    def test_select_join_on_disk(self):
        res = self.g.sparql("SELECT ?x WHERE { <alice> <follows> ?y . ?y <follows> ?x }")
        self.assertEqual([b["x"]["value"] for b in res["results"]["bindings"]], ["fred"])


class BlankNodeInteropTest(unittest.TestCase):

    def setUp(self):
        self.g = Graph(graph_name="bn", cog_home=graph_home("SparqlBnodeTest"))

    def tearDown(self):
        self.g.close()
        shutil.rmtree("/tmp/SparqlBnodeTest", ignore_errors=True)

    def test_putj_objects_queryable_as_bnodes(self):
        self.g.putj({"_id": "user1", "name": "bob", "city": "toronto"})
        res = self.g.sparql("SELECT ?s WHERE { ?s <name> <bob> }")
        rows = res["results"]["bindings"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["s"]["type"], "bnode")
        res = self.g.sparql("SELECT ?city WHERE { ?s <name> <bob> . ?s <city> ?city }")
        self.assertEqual(rows and res["results"]["bindings"][0]["city"]["value"], "toronto")


class PluginRegistrationTest(unittest.TestCase):
    """rdflib.Graph(store="cogdb") via the setuptools entry point."""

    def test_open_by_name_through_plugin(self):
        try:
            rdflib.plugin.get("cogdb", rdflib.store.Store)
        except rdflib.plugin.PluginException:
            self.skipTest("cogdb store entry point not registered (package not pip-installed)")
        shutil.rmtree("/tmp/cog_home/plugin_test", ignore_errors=True)
        rg = rdflib.Graph(store="cogdb")
        rg.open("plugin_test")
        try:
            rg.add((URIRef("a"), URIRef("p"), URIRef("b")))
            rows = list(rg.query("SELECT ?o WHERE { <a> <p> ?o }"))
            self.assertEqual(rows, [(URIRef("b"),)])
        finally:
            rg.store.cog_graph.close()
            shutil.rmtree("/tmp/cog_home/plugin_test", ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
