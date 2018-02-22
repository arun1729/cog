from cog.parser import parse
import unittest

'''
    SELECT * FROM TEST;
    SELECT a.* FROM TEST;
    SELECT DISTINCT NAME FROM TEST;
    SELECT ID, COUNT(1) FROM TEST GROUP BY ID;
    SELECT NAME, SUM(VAL) FROM TEST GROUP BY NAME HAVING COUNT(1) > 2;
    SELECT 'ID' COL, MAX(ID) AS MAX FROM TEST;
    SELECT * FROM TEST LIMIT 1000;
'''

class TestQueryEngine(unittest.TestCase):

    # def test_query_planner(self):
    #     parser = Parser(None, None)
    #     query_list = parser.get_query_list('select username from test_table;')
    #     self.assertEqual(query_list[0], 'select username from test_table')
    #
    #     query_list = parser.get_query_list('select username from test_table; select username, email from test_table')
    #     self.assertEqual(query_list[0], 'select username from test_table')
    #     self.assertEqual(query_list[1].strip(), 'select username, email from test_table')
    #
    # def test_select_parser(self):
    #     parser = Parser(None, None)
    #     select_cmd = parser.process_select_statement('select username from test_table where firstname = "Hari" and lastname = "Seldon"')
    #     self.assertEqual(select_cmd[0], ['username'])
    #     self.assertEqual(select_cmd[1], ['firstname = "Hari"', 'lastname = "Seldon"'])
    #
    # def test_select_parser_multiple_columns(self):
    #     parser = Parser(None, None)
    #     columns, where_expression, conditions = parser.process_select_statement('select username, email from user_data where firstname = "Hari" and lastname = "Seldon"')
    #     self.assertEqual(columns, ['username','email'])
    #     self.assertEqual(where_expression, ['firstname = "Hari"', 'lastname = "Seldon"'])
    #     self.assertEqual(conditions,['AND'])
    #
    # def test_select_aggregate(self):
    #     parser = Parser(None, None)
    #     select_expression, where_expressions, conditions = parser.process_select_statement('select max(age) from user_data where firstname = "Hari" and lastname = "Seldon"')
    #     self.assertEqual(select_expression, ['max(age)'])
    #     ops = parser.process_where_expression(where_expressions)
    #     self.assertEqual(ops[0][0],'firstname')
    #     self.assertEqual(ops[0][1], '=')
    #     self.assertEqual(ops[0][2], '"Hari"')
    #     self.assertEqual(ops[1][0], 'lastname')
    #     self.assertEqual(ops[1][1], '=')
    #     self.assertEqual(ops[1][2], '"Seldon"')

    def test_parser(self):
        query_list = parse('select username, email from user_data where firstname = "Hari" and lastname = "Seldon"')
        query_list[0].select.columns, ['username', 'email']
        self.assertEqual(query_list[0].select.table_name, 'user_data')
        i = 0
        for sc in query_list[0].select.conditions:
            if i == 0: self.assertEqual(sc.prefix_op, None)
            if i == 0: self.assertEqual(sc.operation, ['firstname', '=', '"Hari"'])

            if i == 1: self.assertEqual(sc.prefix_op, 'AND')
            if i == 1: self.assertEqual(sc.operation, ['lastname', '=', '"Seldon"'])
            i += 1

if __name__ == '__main__':
    unittest.main()