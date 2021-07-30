from cog.core import Record
from cog.cache import Cache
import unittest

DIR_NAME = "TestCore"


class TestCache(unittest.TestCase):

    def test_record(self):
        cache = Cache("test_cache")
        record = Record("rocket", "saturn-v", tombstone='0', store_position=25,  key_link=5, value_type='l', value_link=54378)
        marshalled_record = record.marshal()
        cache.put(0, marshalled_record)
        print(cache.get(0))
        byte_partial_value = str(10075).encode().rjust(Record.RECORD_LINK_LEN)
        cache.partial_update_from_zero_index(0, byte_partial_value)
        print(cache.get(0))

        unmarshalled_record = Record.unmarshal(cache.get(0))
        print(unmarshalled_record)
        self.assertTrue(record.is_equal_val(unmarshalled_record))
        self.assertEqual(record.key, unmarshalled_record.key)
        self.assertEqual(record.value, unmarshalled_record.value)
        self.assertEqual(record.tombstone, unmarshalled_record.tombstone)
        self.assertEqual(10075, unmarshalled_record.key_link)
        self.assertEqual(record.value_type, unmarshalled_record.value_type)
        self.assertEqual(record.value_link, unmarshalled_record.value_link)