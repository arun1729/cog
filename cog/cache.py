import logging
from collections import OrderedDict


class Cache:

    def __init__(self, cache_id, shared_cache=None, max_size=50000):
        self.logger = logging.getLogger('table')
        self.cache_id = cache_id
        self.max_size = max_size

        if shared_cache is not None:
            if cache_id not in shared_cache:
                shared_cache[cache_id] = OrderedDict()
            self.cache = shared_cache[cache_id]
        else:
            self.cache = OrderedDict()
        self.logger.info("cache init {}, size: {}, max_size: {}".format(
            self.cache_id, str(len(self.cache)), self.max_size))

    def put(self, key, value):
        key = int(key)

        # Evict LRU (oldest) item if at capacity
        if len(self.cache) >= self.max_size and key not in self.cache:
            # Remove oldest (first) item
            evicted_key, _ = self.cache.popitem(last=False)
            self.logger.debug("Cache evicted key {} for {}".format(
                evicted_key, self.cache_id))

        self.cache[key] = value
        # Move to end (most recently used)
        self.cache.move_to_end(key)

    def get(self, key):
        key = int(key)
        if key in self.cache:
            # Move to end (mark as recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        else:
            return None

    def partial_update_from_zero_index(self, key, partial_value):
        end_pos = len(partial_value)
        if key not in self.cache:
            return

        value_byte_array = bytearray(self.cache[key])
        value_byte_array[0: end_pos] = partial_value
        self.cache[key] = bytes(value_byte_array)

    def size_list(self):
        return [len(self.cache[key]) for key in self.cache]
