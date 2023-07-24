import marshal
import mmap
import os
import os.path
import sys
import logging
# from profilehooks import profile
from cog.cache import Cache
import xxhash

RECORD_SEP = b'\xFD'
UNIT_SEP = b'\xAC'


class TableMeta:

    def __init__(self, name, namespace, db_instance_id, column_mode):
        self.name = name
        self.namespace = namespace
        self.db_instance_id = db_instance_id
        self.column_mode = column_mode


class Table:

    def __init__(self, name, namespace, db_instance_id, config, column_mode=False, shared_cache=None):
        self.logger = logging.getLogger('table')
        self.config = config
        self.shared_cache = shared_cache
        self.table_meta = TableMeta(name, namespace, db_instance_id, column_mode)
        self.indexer = self.__create_indexer()
        self.store = self.__create_store(shared_cache)

    def __create_indexer(self):
        return Indexer(self.table_meta, self.config, self.logger)

    def __create_store(self, shared_cache):
        return Store(self.table_meta, self.config, self.logger, shared_cache=shared_cache)

    def close(self):
        self.indexer.close()
        self.store.close()
        self.logger.info("closed table: " + self.table_meta.name)


class Record:
    '''
    Record is the basic unit of storage in cog.
    value_type: s - string, l - list, u - set
    '''
    RECORD_LINK_LEN = 16
    RECORD_LINK_NULL = -1
    VALUE_LINK_NULL = -1

    def __init__(self, key, value, tombstone='0', store_position=None, value_type="s", key_link=-1, value_link=-1):
        self.key = key
        self.value = value
        self.tombstone = tombstone
        self.store_position = store_position
        self.key_link = key_link
        self.value_link = value_link
        self.value_type = value_type

    def set_store_position(self, pos):
        if type(pos) is not int:
            raise ValueError("store position must be int but provided : " + str(pos))
        self.store_position = pos

    def set_key_link(self, pos):
        self.key_link = pos

    def set_value_link(self, pos):
        self.value_link = pos

    def set_value(self, value):
        self.value = value

    def is_equal_val(self, other_record):
        return self.key == other_record.key and self.value == other_record.value

    def get_kv_tuple(self):
        return self.key, self.value

    def serialize(self):
        return marshal.dumps((self.key, self.value))

    def marshal(self):
        key_link_bytes = str(self.key_link).encode().rjust(Record.RECORD_LINK_LEN)
        serialized = self.serialize()
        m_record = key_link_bytes \
                   + self.tombstone.encode() \
                   + self.value_type.encode() \
                   + str(len(serialized)).encode() \
                   + UNIT_SEP \
                   + serialized
        if self.value_type == "l" or self.value_type == "u":
            if self.value_link is not None:
                m_record += str(self.value_link).encode()
        m_record += RECORD_SEP
        return m_record

    def is_empty(self):
        return self.key is None and self.value is None

    def __str__(self):
        return "key: {}, value: {}, tombstone: {}, store_position: {}, key_link: {}, value_link: {}, value_type: {}".format(
            self.key, self.value, self.tombstone, self.store_position, self.key_link, self.value_link, self.value_type)

    @classmethod
    def __read_until(cls, start, sbytes, separtor=UNIT_SEP):
        buff = b''
        i = 0  # default
        for i in range(start, len(sbytes)):
            s_byte = sbytes[i: i + 1]
            if s_byte == separtor:
                break
            buff += s_byte
        return buff, i

    @classmethod
    def unmarshal(cls, store_bytes):
        """reads from bytes and creates object
        """
        base_pos = 0
        key_link = int(store_bytes[base_pos: base_pos + Record.RECORD_LINK_LEN])
        next_base_pos = Record.RECORD_LINK_LEN
        tombstone = store_bytes[next_base_pos:next_base_pos + 1].decode()
        value_type = store_bytes[next_base_pos + 1: next_base_pos + 2].decode()
        value_len, end_pos = cls.__read_until(next_base_pos + 2, store_bytes)
        value_len = int(value_len.decode())
        value = store_bytes[end_pos + 1: end_pos + 1 + value_len]
        record = marshal.loads(value)

        value_link = Record.VALUE_LINK_NULL
        if value_type == 'l' or value_type == 'u':
            value_link, end_pos = cls.__read_until(end_pos + value_len + 1, store_bytes, RECORD_SEP)
            value_link = int(value_link.decode())
        return cls(record[0], record[1], tombstone, store_position=None, value_type=value_type, key_link=key_link,
                   value_link=value_link)

    @classmethod
    def __load_value(cls, store_pointer, val_list, store):
        """loads value from the store"""
        while store_pointer != Record.VALUE_LINK_NULL:
            rec = Record.unmarshal(store.read(store_pointer))
            if rec.value_type == 'l':
                val_list.append(rec.value)
            else:
                val_list.add(rec.value)
            store_pointer = rec.value_link
        return val_list

    @classmethod
    # @profile
    def load_from_store(cls, position: int, store):
        record = cls.unmarshal(store.read(position))
        values = None
        if record.value_type == 'l':
            values = cls.__load_value(record.value_link, [record.value], store)
        elif record.value_type == 'u':
            values = cls.__load_value(record.value_link, {record.value}, store)
        if values is not None:
            record.set_value(values)
        return record


class Index:

    def __init__(self, table_meta, config, logger, index_id=0):
        self.logger = logging.getLogger('index')
        self.table = table_meta
        self.config = config
        self.name = self.config.cog_index(table_meta.namespace, table_meta.name, table_meta.db_instance_id, index_id)
        self.empty_block = '-1'.zfill(self.config.INDEX_BLOCK_LEN).encode()
        if not os.path.exists(self.name):
            self.logger.info("creating index...")
            f = open(self.name, 'wb+')
            i = 0
            e_blocks = []
            while i < config.INDEX_CAPACITY:
                e_blocks.append(self.empty_block)
                i += 1
            f.write(b''.join(e_blocks))
            self.file_limit = f.tell()
            f.close()
            self.logger.info("new index with capacity" + str(config.INDEX_CAPACITY) + "created: " + self.name)
        else:
            self.logger.info("Index: "+self.name+" already exists.")

        self.db = open(self.name, 'r+b')
        self.db_mem = mmap.mmap(self.db.fileno(), 0)

        self.db_mem.seek(0)
        current_block = self.db_mem.read(self.config.INDEX_BLOCK_LEN)

    def close(self):
        self.db.close()

    def get_index_key(self, int_store_position):
        return str(int_store_position).encode().rjust(self.config.INDEX_BLOCK_LEN)

    # @profile
    def put(self, key, store_position, store):
        """
        key chain
        :param key:
        :param store_position:
        :param store:
        :return:
        """

        """
        k5 -> k4 -> k3 -> k2 -> k1
        add: k6
        k6 -> k5 -> k4 -> k3 -> k2 -> k1
        add/update: k4
        1. k4 -> k6 -> k5 -> k4 -> k3 -> k2 -> k1
        2. k4 -> k6 -> k5 -> k3 -> k2 -> k1

        """
        orig_position, orig_hash = self.get_index(key)
        index_value = self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_LEN]
        self.logger.debug('writing : ' + str(key) + ' current data at store position: ' + str(index_value))
        if index_value == self.empty_block:
            # First record in the key bucket, point next link to null
            store.update_record_link_inplace(store_position, Record.RECORD_LINK_NULL)
            key_link = store_position
            # write the store position to the index
            self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_LEN] = self.get_index_key(store_position)
        else:
            # there are records in the bucket
            # read existing record from the store
            existing_record = Record.load_from_store(int(index_value), store)
            existing_record.set_store_position(int(index_value))  # this is a bit confusing, should clean up.

            if existing_record.key == key:
                # the record at the top of the bucket has the same key, update the record in place.
                # a new entry has been made to the store, update pre and next links.
                store.update_record_link_inplace(store_position, int(existing_record.key_link))
                key_link = int(existing_record.key_link)
            else:
                # this is hash collision.
                # the record at the top of the bucket has a different key, add new record to the top of the bucket.
                # set next link to the record at the top of the bucket, prev is null by default, no need to set.
                store.update_record_link_inplace(store_position, existing_record.store_position)
                key_link = existing_record.store_position

                # check if this record exists in the bucket, if yes remove pointer.
                prev_record = None
                while existing_record.key_link != Record.RECORD_LINK_NULL:
                    existing_record = Record.load_from_store(existing_record.key_link, store)
                    existing_record.set_store_position(existing_record.key_link)
                    if existing_record.key == key and prev_record is not None:
                        """
                        if same key found in bucket, update previous record in chain to point to key_link of this record
                        prev_rec -> current rec.key_link
                        curr_rec will not be linked in the bucket anymore.
                        """
                        # update in place the key link pointer of pervios record, ! need to add fixed length padding.
                        store.update_record_link_inplace(prev_record.store_position, existing_record.key_link)
                        key_link = existing_record.key_link
                    prev_record = existing_record

        self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_LEN] = self.get_index_key(store_position)
        return key_link

    def get_index(self, key):
        num = cog_hash(key, self.config.INDEX_CAPACITY) % ((sys.maxsize + 1) * 2)
        self.logger.debug("hash for: " + key + " : " + str(num))
        # there may be diff when using mem slice vs write (+1 needed)
        index = (self.config.INDEX_BLOCK_LEN *
                 (max((num % self.config.INDEX_CAPACITY) - 1, 0)))
        self.logger.debug("offset : " + key + " : " + str(index))
        return index, num

    def cog_hash(self, string):
        return xxhash.xxh32(string, seed=2).intdigest() % self.config.INDEX_CAPACITY

    # @profile
    def get(self, key, store):
        self.logger.debug("GET: Reading index: " + self.name)
        index_position, raw_hash = self.get_index(key)
        data_at_index_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
        if data_at_index_position == self.empty_block:
            return None
        data_at_index_position = int(data_at_index_position)
        record = Record.load_from_store(data_at_index_position, store)
        record.set_store_position(data_at_index_position)
        self.logger.debug("read record " + str(record))

        if record.key == key:
            return record
        else:
            while record.key_link != Record.RECORD_LINK_NULL:
                self.logger.debug("record.key_link: " + str(record.key_link))
                record = Record.load_from_store(record.key_link, store)
                record.set_store_position(record.key_link)
                if record.key == key:
                    return record
        return None

    '''
        Iterates through record in itr_store.
    '''

    def scanner(self, store):
        scan_cursor = 0
        while True:
            data_at_position = self.db_mem[scan_cursor:scan_cursor + self.config.INDEX_BLOCK_LEN]
            if len(data_at_position) == 0:  # EOF index
                self.logger.info("Index EOF reached! Scan terminated.")
                return
            if data_at_position == self.empty_block:
                scan_cursor += self.config.INDEX_BLOCK_LEN
                self.logger.debug("GET: skipping empty block during iteration.")
                continue
            record = Record.load_from_store(int(data_at_position), store)
            if record is None:  # EOF store
                self.logger.error("Store EOF reached! Iteration terminated.")
                return
            yield Record(record.key, record.value, record.tombstone)
            scan_cursor += self.config.INDEX_BLOCK_LEN

    def delete(self, key, store):
        """
               k5 -> k4 -> k3 -> k2 -> k1
               del: k3
               k6 -> k5 -> k4 -> k2 -> k1

        """
        self.logger.debug("GET: Reading index: " + self.name)
        index_position, raw_hash = self.get_index(key)

        data_at_index_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
        if data_at_index_position == self.empty_block:
            return False

        data_at_index_position = int(data_at_index_position)
        record = Record.load_from_store(data_at_index_position, store)
        record.set_store_position(data_at_index_position)
        self.logger.debug("read record " + str(record))
        if record.key == key:
            """delete bucket => map hash table to empty block"""
            self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN] = self.empty_block
        else:
            """search bucket"""
            prev_record = None
            while record.key_link != Record.RECORD_LINK_NULL:
                record = Record.load_from_store(record.key_link, store)
                record.set_store_position(record.key_link)
                if record.key == key:
                    """
                    if same key found in bucket, update previous record in chain to point to key_link of this record
                    prev_rec -> current rec.key_link
                    curr_rec will not be linked in the bucket anymore.
                    """
                    # update in place the key link pointer of pervios record, ! need to add fixed length padding.
                    store.update_record_link_inplace(prev_record.store_position, record.key_link)
                prev_record = record
        return True

    def flush(self):
        self.db_mem.flush()


class Store:

    def __init__(self, tablemeta, config, logger, caching_enabled=True, shared_cache=None):
        self.caching_enabled = caching_enabled
        self.logger = logging.getLogger('store')
        self.tablemeta = tablemeta
        self.config = config
        self.empty_block = '-1'.zfill(self.config.INDEX_BLOCK_LEN).encode()
        self.store = self.config.cog_store(
            tablemeta.namespace, tablemeta.name, tablemeta.db_instance_id)
        self.store_cache = Cache(self.store, shared_cache)
        temp = open(self.store, 'a')  # create if not exist
        temp.close()
        self.store_file = open(self.store, 'rb+')
        logger.info("Store for file init: " + self.store)

    def close(self):
        self.store_file.close()

    def save(self, record):
        """
        Store data
        """
        self.store_file.seek(0, 2)
        store_position = self.store_file.tell()
        record.set_store_position(store_position)
        marshalled_record = record.marshal()
        self.store_file.write(marshalled_record)
        self.store_file.flush()
        if self.caching_enabled:
            self.store_cache.put(store_position, marshalled_record)
        return store_position

    def update_record_link_inplace(self, start_pos, int_value):
        """updates record link in store file in place"""
        if type(int_value) is not int:
            raise ValueError("store position must be int but provided : " + str(start_pos))

        byte_value = str(int_value).encode().rjust(Record.RECORD_LINK_LEN)
        self.logger.debug('update_record_link_inplace: ' + str(byte_value))
        self.store_file.seek(start_pos)
        self.store_file.write(byte_value)

        if self.caching_enabled:
            self.store_cache.partial_update_from_zero_index(start_pos, byte_value)
        self.store_file.flush()

    # @profile
    def read(self, position):
        self.logger.debug("store read request at position: " + str(position))
        if self.caching_enabled:
            cached_record = self.store_cache.get(position)
            if cached_record is not None:
                return cached_record

        self.store_file.seek(position)
        record = self.__read_until()

        if self.caching_enabled:
            self.store_cache.put(position, record)

        return record

    # @profile
    def __read_until(self):
        data = None
        while True:
            chunk = self.store_file.read(self.config.STORE_READ_BUFFER_SIZE)

            if len(chunk) == 0:
                return data
                # raise Exception("EOF store file! Data read error.")
            i = chunk.find(RECORD_SEP)

            if i > 0:
                chunk = chunk[:i + 1]
                if data is None:
                    data = chunk
                else:
                    data += chunk
                break

            if data is None:
                data = chunk
            else:
                data += chunk
        self.logger.debug("store __read_until: " + str(data))
        return data


class Indexer:
    '''
    Manages indexes. Creates new index when an index is full.
    Searches all indexes for get requests.
    Provides same get/put/del method as single index but over multuple files.
    '''

    def __init__(self, tablemeta, config, logger):
        self.tablemeta = tablemeta
        self.config = config
        self.logger = logging.getLogger('indexer')
        self.index_list = []  # future range index.
        self.index_id = 0
        self.load_indexes()
        # if no index currenlty exist, create new live index.
        if len(self.index_list) == 0:
            self.index_list.append(Index(tablemeta, config, logger, self.index_id))
            self.live_index = self.index_list[self.index_id]

    def close(self):
        for idx in self.index_list:
            idx.close()

    def load_indexes(self):
        for f in os.listdir(self.config.cog_data_dir(self.tablemeta.namespace)):
            if self.config.INDEX in f:
                if self.tablemeta.name == self.config.get_table_name(f):
                    self.logger.info("loading index file: " + f)
                    id = self.config.index_id(f)
                    index = Index(self.tablemeta, self.config, self.logger, id)
                    self.index_list.append(index)
                    # make the latest index the live index.
                    if id >= self.index_id:
                        self.index_id = id
                        self.live_index = index

    def put(self, key, store_position, store):
        resp = self.live_index.put(key, store_position, store)
        self.logger.debug("Key: " + key + " indexed in: " + self.live_index.name)
        return resp

    # @profile
    def get(self, key, store):
        idx = self.index_list[0]  # only one index file.
        return idx.get(key, store)

    def scanner(self, store):
        for idx in self.index_list:
            self.logger.debug("SCAN: index: " + idx.name)
            for r in idx.scanner(store):
                yield r

    def delete(self, key, store):
        for idx in self.index_list:
            if idx.delete(key, store):
                return True
            else:
                return False


def cog_hash(string, index_capacity):
    return xxhash.xxh32(string, seed=2).intdigest() % index_capacity
