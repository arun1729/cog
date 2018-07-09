from cog.database import Cog
import unittest


def qfilter(jsn):
    d = json.loads(jsn[1])
    return d["firstname"]

class TestLib(unittest.TestCase):

    def test_db(self):
        data = ('testKey','testVal')
        cogdb = Cog("~/temp/test")
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        self.assertEqual(cogdb.get("testKey"), ('0', ('testKey', 'testVal')))


if __name__ == '__main__':
    unittest.main()