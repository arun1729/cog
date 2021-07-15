from cog.database import parse_tripple
import unittest


class TestDB(unittest.TestCase):

    def test_parsing_tripple_1(self):
        subject, predicate, object, context = parse_tripple("""_:100468 </film/performance/character> "Richard Grahame" .""")
        self.assertEqual(subject, "_:100468")
        self.assertEqual(predicate, "</film/performance/character>")
        self.assertEqual(object, "Richard Grahame")
        self.assertEqual(context, ".")

    def test_parsing_tripple_2(self):
        subject, predicate, object, context = parse_tripple("""_:100468 </film/performance/character> "Richard Grahame" """)
        self.assertEqual(subject, "_:100468")
        self.assertEqual(predicate, "</film/performance/character>")
        self.assertEqual(object, "Richard Grahame")
        self.assertEqual(context, None)
