from cog.database import Cog
from cog import config
import unittest
from cog.query_engine import execute_query
from cog.parser import parse


class TestQueryEngine(unittest.TestCase):
    def test_db(self):
        data = ('user100','{"firstname":"Hari","lastname":"seldon"}')
        cogdb = Cog(config)
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)

        print execute_query(parse("select fristname from test;")[0], cogdb)


if __name__ == '__main__':
    unittest.main()