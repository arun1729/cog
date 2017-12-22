from cog.QueryEngine import Planner
import unittest


class TestQueryEngine(unittest.TestCase):
    def test_query_planner(self):
        planner = Planner(None, None)
        query_list = planner.get_query_list('select username from test_table;')
        self.assertEqual(query_list[0], 'select username from test_table')

        query_list = planner.get_query_list('select username from test_table; select username, email from test_table')
        self.assertEqual(query_list[0], 'select username from test_table')
        self.assertEqual(query_list[1].strip(), 'select username, email from test_table')

if __name__ == '__main__':
    unittest.main()