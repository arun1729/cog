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

class TestParser(unittest.TestCase):

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

    def test_select(self):
        query_list = parse('select username, email from user_data where firstname = "Hari" and lastname = "Seldon" limit 10')
        self.assertEqual(query_list[0].command.columns, ['username', 'email'])
        self.assertEqual(query_list[0].command.table_name, 'user_data')
        i = 0
        for sc in query_list[0].command.conditions:
            if i == 0: self.assertEqual(sc.prefix_op, None)
            if i == 0: self.assertEqual(sc.operation, ['firstname', '=', '"Hari"'])

            if i == 1: self.assertEqual(sc.prefix_op, 'AND')
            if i == 1: self.assertEqual(sc.operation, ['lastname', '=', '"Seldon"'])
            i += 1

            self.assertEqual(query_list[0].command.limit, '10')

    def test_select2(self):
        query_list = parse(
            'select username, email from user_data')
        self.assertEqual(query_list[0].command.columns, ['username', 'email'])
        self.assertEqual(query_list[0].command.table_name, 'user_data')

    def test_create(self):
        commands = parse('create table userbase')
        self.assertEqual(commands[0].tablename, 'userbase')

    def test_addindex(self):
        commands = parse('add index userbase keys username, userid')
        self.assertEqual(commands[0].tablename, 'userbase')
        self.assertEqual(commands[0].keys, ["username", "userid"])


if __name__ == '__main__':
    unittest.main()