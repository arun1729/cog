import logging
from collections import OrderedDict


DEFAULT_MAX_SIZE = 100000


class Cache:

    def __init__(self, cache_id, shared_cache=None, max_size=DEFAULT_MAX_SIZE):
        self.logger = logging.getLogger(__name__)
        self.cache_id = cache_id
        self.max_size = max_size
        if shared_cache is not None:
            if cache_id not in shared_cache:
                shared_cache[cache_id] = OrderedDict()
            self.cache = shared_cache[cache_id]
        else:
            self.cache = OrderedDict()
        self.logger.info("cache init {}, size: {}".format(self.cache_id, str(len(self.cache))))

    def put(self, key, value):
        key = int(key)
        if key in self.cache:
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            self.cache[key] = value
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)

    def get(self, key):
        key = int(key)
        try:
            value = self.cache[key]
            self.cache.move_to_end(key)
            return value
        except KeyError:
            return None

    def peek(self, key):
        """Return cached value without promoting in LRU order."""
        key = int(key)
        return self.cache.get(key)

    def evict(self, key):
        key = int(key)
        try:
            del self.cache[key]
        except KeyError:
            pass

    def size(self):
        return len(self.cache)
