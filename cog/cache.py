import logging


class Cache:

    def __init__(self, cache_id, shared_cache=None):
        self.logger = logging.getLogger(__name__)
        self.cache_id = cache_id
        if shared_cache is not None:
            if cache_id not in shared_cache:
                shared_cache[cache_id]={}
            self.cache = shared_cache[cache_id]
        else:
            self.cache = {}
        self.logger.info("cache init {}, size: {}".format(self.cache_id, str(len(self.cache))))

    def put(self, key, value):
        key = int(key)
        self.cache[key] = value

    def get(self, key):
        key = int(key)
        if key in self.cache:
            return self.cache[key]
        else:
            return None

    def size(self):
        return len(self.cache)
