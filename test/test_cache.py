from cog.core import Record
from cog.cache import Cache
import unittest

DIR_NAME = "TestCore"


class TestCache(unittest.TestCase):

    def test_record(self):
        cache = Cache("test_cache")
        record = Record("rocket", "saturn-v", format_version='1', store_position=25, key_link=5, value_type='l', value_link=54378)
        cache.put(0, record)
        cached = cache.get(0)
        self.assertIs(cached, record)

        cached.key_link = 10075

        retrieved = cache.get(0)
        self.assertEqual(record.key, retrieved.key)
        self.assertEqual(record.value, retrieved.value)
        self.assertEqual(record.format_version, retrieved.format_version)
        self.assertEqual(10075, retrieved.key_link)
        self.assertEqual(record.value_type, retrieved.value_type)
        self.assertEqual(record.value_link, retrieved.value_link)
