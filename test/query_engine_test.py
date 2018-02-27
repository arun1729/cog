from cog.database import Cog
from cog import config
import unittest
from cog.query_engine import execute_query
from cog.parser import parse


class TestQueryEngine(unittest.TestCase):
    def test_db_col_names(self):
        cogdb = Cog(config)
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(('user100','{"firstname":"Hari","lastname":"seldon"}'))
        cogdb.put(('user101', '{"firstname":"Adam","lastname":"Smith"}'))
        cogdb.put(('user102', '{"firstname":"James","lastname":"Bond"}'))

        rows = execute_query(parse("select firstname, lastname from test;")[0], cogdb)

        for row in rows:
            print row

    def test_db_star(self):
        cogdb = Cog(config)
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(('user100','{"firstname":"Hari","lastname":"seldon"}'))
        cogdb.put(('user101', '{"firstname":"Adam","lastname":"Smith"}'))
        cogdb.put(('user102', '{"firstname":"James","lastname":"Bond"}'))

        rows = execute_query(parse("select * from test;")[0], cogdb)



        for row in rows:
            print row


if __name__ == '__main__':
    unittest.main()