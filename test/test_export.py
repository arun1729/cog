"""Tests for Graph.triples(), Graph.export(), and cog.export module."""
from cog.torque import Graph
from cog.export import get_triples, export_triples
import unittest
import os
import shutil
import csv

DIR_NAME = "ExportTest"


class ExportTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.makedirs("/tmp/" + DIR_NAME, exist_ok=True)

        cls.g = Graph(graph_name="export_test", cog_home=DIR_NAME)
        cls.g.put("alice", "follows", "bob")
        cls.g.put("bob", "follows", "charlie")
        cls.g.put("bob", "status", "cool")
        cls.g.put("charlie", "follows", "alice")

    # --- triples() ---

    def test_triples_returns_list(self):
        t = self.g.triples()
        self.assertIsInstance(t, list)
        self.assertTrue(len(t) > 0)

    def test_triples_content(self):
        t = self.g.triples()
        for triple in t:
            self.assertEqual(len(triple), 3)
        self.assertIn(("alice", "follows", "bob"), t)
        self.assertIn(("bob", "follows", "charlie"), t)
        self.assertIn(("bob", "status", "cool"), t)
        self.assertIn(("charlie", "follows", "alice"), t)
        self.assertEqual(len(t), 4)

    def test_triples_empty_graph(self):
        g_empty = Graph(graph_name="empty_export_test", cog_home=DIR_NAME)
        t = g_empty.triples()
        self.assertEqual(t, [])
        g_empty.close()

    def test_get_triples_standalone(self):
        """Test the standalone get_triples function from cog.export."""
        t = list(get_triples(self.g))
        self.assertEqual(len(t), 4)
        self.assertIn(("alice", "follows", "bob"), t)

    # --- export nt (default) ---

    def test_export_default_is_nt(self):
        filepath = "/tmp/" + DIR_NAME + "/default.nt"
        count = self.g.export(filepath)
        self.assertEqual(count, 4)
        with open(filepath) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 4)
        for line in lines:
            self.assertTrue(line.strip().endswith("."))

    def test_export_nt_roundtrip(self):
        """Export to nt then load into a new graph and verify identical triples."""
        filepath = "/tmp/" + DIR_NAME + "/roundtrip.nt"
        self.g.export(filepath)

        g2 = Graph(graph_name="roundtrip_test", cog_home=DIR_NAME)
        g2.load_triples(filepath)
        t_original = sorted(self.g.triples())
        t_loaded = sorted(g2.triples())
        self.assertEqual(t_original, t_loaded)
        g2.close()

    # --- export nt strict (W3C) ---

    def test_export_nt_strict(self):
        filepath = "/tmp/" + DIR_NAME + "/strict.nt"
        count = self.g.export(filepath, strict=True)
        self.assertEqual(count, 4)
        with open(filepath) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 4)
        for line in lines:
            parts = line.strip().rstrip(" .").split(" ", 2)
            subject, predicate, obj = parts
            self.assertTrue(subject.startswith("<") or subject.startswith("_:"),
                            f"Bad subject: {subject}")
            self.assertTrue(predicate.startswith("<"),
                            f"Bad predicate: {predicate}")
            self.assertTrue(obj.startswith("<") or obj.startswith("_:") or obj.startswith('"'),
                            f"Bad object: {obj}")

    def test_export_nt_strict_with_iris(self):
        """Terms already in <IRI> form are preserved in strict mode."""
        g2 = Graph(graph_name="iri_test", cog_home=DIR_NAME)
        g2.put("<http://example.org/alice>", "<http://example.org/knows>", "<http://example.org/bob>")
        filepath = "/tmp/" + DIR_NAME + "/iri_strict.nt"
        g2.export(filepath, strict=True)
        with open(filepath) as f:
            line = f.readline().strip()
        self.assertEqual(line, "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .")
        g2.close()

    def test_export_nt_strict_blank_nodes(self):
        """Blank nodes (_:label) are preserved in strict mode."""
        g3 = Graph(graph_name="blank_test", cog_home=DIR_NAME)
        g3.put("_:node1", "has", "_:node2")
        filepath = "/tmp/" + DIR_NAME + "/blank_strict.nt"
        g3.export(filepath, strict=True)
        with open(filepath) as f:
            line = f.readline().strip()
        self.assertEqual(line, '_:node1 <has> _:node2 .')
        g3.close()

    def test_export_nt_strict_escapes_quotes(self):
        """Strict mode escapes special characters in literal values."""
        g4 = Graph(graph_name="escape_test", cog_home=DIR_NAME)
        g4.put("alice", "says", 'hello "world"')
        filepath = "/tmp/" + DIR_NAME + "/escape_strict.nt"
        g4.export(filepath, strict=True)
        with open(filepath) as f:
            line = f.readline().strip()
        self.assertIn('\\"', line)
        g4.close()

    # --- export csv ---

    def test_export_csv(self):
        filepath = "/tmp/" + DIR_NAME + "/export.csv"
        count = self.g.export(filepath, fmt="csv")
        self.assertEqual(count, 4)
        with open(filepath) as f:
            reader = csv.reader(f)
            rows = list(reader)
        self.assertEqual(len(rows), 5)  # header + 4 data
        self.assertEqual(rows[0], ["subject", "predicate", "object"])

    # --- export tsv ---

    def test_export_tsv(self):
        filepath = "/tmp/" + DIR_NAME + "/export.tsv"
        count = self.g.export(filepath, fmt="tsv")
        self.assertEqual(count, 4)
        with open(filepath) as f:
            reader = csv.reader(f, delimiter='\t')
            rows = list(reader)
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0], ["subject", "predicate", "object"])

    # --- export_triples standalone ---

    def test_export_triples_standalone(self):
        """Test the standalone export_triples function from cog.export."""
        filepath = "/tmp/" + DIR_NAME + "/standalone.nt"
        count = export_triples(self.g, filepath, fmt="csv")
        self.assertEqual(count, 4)
        self.assertTrue(os.path.exists(filepath))

    # --- error handling ---

    def test_export_bad_format(self):
        with self.assertRaises(ValueError):
            self.g.export("/tmp/bad.xyz", fmt="xml")

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)


if __name__ == "__main__":
    unittest.main()
