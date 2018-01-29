from cog.queryengine import Planner
import unittest


class TestQueryEngine(unittest.TestCase):
    def test_query_planner(self):
        planner = Planner(None, None)
        query_list = planner.get_query_list('select username from test_table;')
        self.assertEqual(query_list[0], 'select username from test_table')

        query_list = planner.get_query_list('select username from test_table; select username, email from test_table')
        self.assertEqual(query_list[0], 'select username from test_table')
        self.assertEqual(query_list[1].strip(), 'select username, email from test_table')

    def test_select_parser(self):
        planner = Planner(None, None)
        select_cmd = planner.get_select_command('select username from test_table where firstname = "Hari" and lastname = "Seldon"')
        print select_cmd
        self.assertEqual(select_cmd[0], ['username'])
        self.assertEqual(select_cmd[1], ['firstname = "Hari"', 'lastname = "Seldon"'])


if __name__ == '__main__':
    unittest.main()