from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueTest2"

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

class TorqueTest2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/"+DIR_NAME):
            shutil.rmtree("/tmp/"+DIR_NAME)

        if not os.path.exists("/tmp/"+DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

    def test_torque_2(self):
        TorqueTest2.g = Graph(graph_name="better_graph", cog_home=DIR_NAME)
        TorqueTest2.g.put("A", "is better than", "B")\
            .put("B", "is better than", "C")\
            .put("A", "is better than", "D")\
            .put("Z", "is better than", "D")\
            .put("D", "is smaller than", "F")
        expected = {'result': [{'id': 'B'}, {'id': 'D'}]}
        actual = TorqueTest2.g.v("A").out(["is better than"]).all()
        self.assertTrue(ordered(expected) == ordered(actual))
        self.assertTrue(TorqueTest2.g.v("A").out(["is better than"]).count() == 2)
        self.assertTrue(TorqueTest2.g.v().count() == 6)
        TorqueTest2.g.close()

    def test_torque_load_csv(self):
        csv_file = "test/test-data/books.csv"
        if os.path.exists("test-data/books.csv"):
            csv_file = "test-data/books.csv"
        g = Graph(graph_name="books5")
        g.load_csv(csv_file, "isbn")
        # print(g.scan())
        actual = g.scan(20,'e')
        expected = {'result': [{'id': 'ratings_4'}, {'id': 'best_book_id'}, {'id': 'work_text_reviews_count'}, {'id': 'original_publication_year'}, {'id': 'average_rating'}, {'id': 'ratings_1'}, {'id': 'language_code'}, {'id': 'image_url'}, {'id': 'books_count'}, {'id': 'work_ratings_count'}, {'id': 'isbn13'}, {'id': 'title'}, {'id': 'ratings_5'}, {'id': 'ratings_3'}, {'id': 'small_image_url'}, {'id': 'ratings_count'}, {'id': 'isbn'}, {'id': 'book_id'}, {'id': 'authors'}, {'id': 'ratings_2'}]}
        self.assertTrue(actual == expected)
        self.assertTrue(ordered(g.v('Kathryn Stockett').inc().out("title").all())==ordered({'result': [{'id': 'The Help'}]}))
        g.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
