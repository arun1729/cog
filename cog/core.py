import mmap
import struct
import os
import os.path
import sys
import time
import logging
import threading
import queue
# from profilehooks import profile
from cog.cache import Cache
from cog.codec import (
    LegacyCodec,
    SpindleCodec,
    detect_codec,
    LEGACY_V1_FLAG,
)
from cog.config import INDEX_BLOCK_LEN as _DEFAULT_INDEX_BLOCK_LEN
import xxhash

# Zero-byte sentinel for new indexes: ftruncate provides these for free.
# Identical to struct.pack('<q', 0) — 8 bytes of 0x00.
_ZERO_BLOCK = b'\x00' * _DEFAULT_INDEX_BLOCK_LEN
# Legacy sentinel for backward compat with pre-v4 index files.
_LEGACY_EMPTY_BLOCK = struct.pack('<q', -1)


class TableMeta:
    __slots__ = ('name', 'namespace', 'db_instance_id', 'column_mode')

    def __init__(self, name, namespace, db_instance_id, column_mode):
        self.name = name
        self.namespace = namespace
        self.db_instance_id = db_instance_id
        self.column_mode = column_mode


class Table:

    def __init__(self, name, namespace, db_instance_id, config, column_mode=False, shared_cache=None,
                 flush_interval=1):
        self.logger = logging.getLogger('cog.table')
        self.config = config
        self.shared_cache = shared_cache
        self.flush_interval = flush_interval
        self.table_meta = TableMeta(name, namespace, db_instance_id, column_mode)
        self.indexer = self.__create_indexer()
        self.store = self.__create_store(shared_cache)

    def __create_indexer(self):
        return Indexer(self.table_meta, self.config, self.logger)

    def __create_store(self, shared_cache):
        return Store(self.table_meta, self.config, self.logger, shared_cache=shared_cache,
                     flush_interval=self.flush_interval)

    def sync(self):
        """Force flush pending writes to disk."""
        self.store.sync()

    def close(self):
        self.indexer.close()
        self.store.close()
        self.logger.info("closed table: " + self.table_meta.name)


class Record:
    '''
    Record is the basic unit of storage in cog.
    value_type: s - string, l - list, u - set
    format_version: in-memory tag of which on-disk codec produced or will produce
        this record. '0'/'1' = legacy marshal, '2' = Spindle binary+msgpack. Not
        serialized for Spindle records (format is inferred from the file header).
    timestamp: int64 nanoseconds since epoch. Stamped in Store.save at write
        time for Spindle records; None for legacy records.
    '''
    __slots__ = ('key', 'value', 'format_version', 'timestamp',
                 'store_position', 'key_link', 'value_link', 'value_type')

    RECORD_LINK_LEN = 16
    RECORD_LINK_NULL = -1
    VALUE_LINK_NULL = -1
    CURRENT_FORMAT_VERSION = '1'

    def __init__(self, key, value, format_version=None, store_position=None,
                 value_type="s", key_link=-1, value_link=-1, timestamp=None):
        self.key = key
        self.value = value
        self.format_version = format_version if format_version is not None else Record.CURRENT_FORMAT_VERSION
        self.store_position = store_position
        self.key_link = key_link
        self.value_link = value_link
        self.value_type = value_type
        self.timestamp = timestamp

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

    def marshal(self, codec=None):
        """Serialize to on-disk bytes. Defaults to LegacyCodec v1 for
        backward-compatible in-memory round-trips (tests); Store.save passes
        the owning store's codec."""
        if codec is None:
            codec = LegacyCodec(version_flag=LEGACY_V1_FLAG)
        return codec.encode_record(self)

    def is_empty(self):
        return self.key is None and self.value is None

    def __str__(self):
        return ("key: {}, value: {}, format_version: {}, timestamp: {}, "
                "store_position: {}, key_link: {}, value_link: {}, value_type: {}").format(
            self.key, self.value, self.format_version, self.timestamp,
            self.store_position, self.key_link, self.value_link, self.value_type)

    @classmethod
    def unmarshal(cls, store_bytes, codec=None):
        """Deserialize from on-disk bytes. Defaults to LegacyCodec for
        backward-compatible in-memory round-trips; Store.read paths pass the
        owning store's codec."""
        if codec is None:
            codec = LegacyCodec(version_flag=LEGACY_V1_FLAG)
        return codec.decode_record(store_bytes)

    @classmethod
    def __load_value(cls, store_pointer, val_list, store):
        """loads value from the store"""
        while store_pointer != Record.VALUE_LINK_NULL:
            rec = store.codec.decode_record(store.read(store_pointer))
            if rec.value_type == 'l':
                val_list.append(rec.value)
            else:
                val_list.add(rec.value)
            store_pointer = rec.value_link
        return val_list

    @classmethod
    def materialize_values(cls, record, store):
        """Populate record.value with the full value chain in place for
        list/set records. No-op for scalars. Call this when you already
        decoded the head and need to pay the value-chain cost."""
        if record.value_type == 'l':
            record.set_value(cls.__load_value(record.value_link, [record.value], store))
        elif record.value_type == 'u':
            record.set_value(cls.__load_value(record.value_link, {record.value}, store))
        return record

    @classmethod
    # @profile
    def load_from_store(cls, position: int, store):
        raw = store.read(position)
        if raw is None:
            return None
        record = store.codec.decode_record(raw)
        cls.materialize_values(record, store)
        return record


class Index:

    def __init__(self, table_meta, config, logger, index_id=0):
        self.logger = logging.getLogger('cog.index')
        self.table = table_meta
        self.config = config
        self.name = self.config.cog_index(table_meta.namespace, table_meta.name, table_meta.db_instance_id, index_id)
        block_len = self.config.INDEX_BLOCK_LEN
        capacity = self.config.INDEX_CAPACITY

        if not os.path.exists(self.name):
            self.logger.info("creating index...")
            # ftruncate: O(1) — OS provides zero-filled pages lazily, no disk I/O.
            total_size = block_len * capacity
            fd = os.open(self.name, os.O_RDWR | os.O_CREAT, 0o644)
            os.ftruncate(fd, total_size)
            os.close(fd)
            self.empty_block = _ZERO_BLOCK if block_len == _DEFAULT_INDEX_BLOCK_LEN else b'\x00' * block_len
            self.logger.info("new index with capacity" + str(capacity) + "created: " + self.name)
        else:
            self.logger.info("Index: "+self.name+" already exists.")
            # Detect legacy vs zero-sentinel format.
            self.empty_block = self._detect_sentinel(block_len)

        self.db = open(self.name, 'r+b')
        self.db_mem = mmap.mmap(self.db.fileno(), 0)
        self._closed = False

    def _detect_sentinel(self, block_len):
        """Detect whether an existing index uses legacy ASCII sentinel or zero bytes."""
        legacy = _LEGACY_EMPTY_BLOCK if block_len == _DEFAULT_INDEX_BLOCK_LEN else struct.pack('<q', -1)
        zero = _ZERO_BLOCK if block_len == _DEFAULT_INDEX_BLOCK_LEN else b'\x00' * block_len
        with open(self.name, 'rb') as f:
            # Sample first block — if it matches legacy sentinel, use legacy.
            first = f.read(block_len)
            if first == legacy:
                return legacy
            if first == zero:
                return zero
            # First slot is occupied; scan a few more to determine format.
            for _ in range(min(63, self.config.INDEX_CAPACITY - 1)):
                block = f.read(block_len)
                if block == legacy:
                    return legacy
                if block == zero:
                    return zero
        # All sampled slots occupied — default to zero (new format).
        return zero

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.db_mem.flush()
        self.db_mem.close()
        self.db.close()

    def get_index_key(self, int_store_position):
        return struct.pack('<q', int_store_position)

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
        else:
            # there are records in the bucket
            # read existing record from the store - use unmarshal, not load_from_store (O(1) vs O(n))
            head_pos = struct.unpack_from('<q', index_value)[0]
            existing_record = store.codec.decode_record(store.read(head_pos))
            existing_record.set_store_position(head_pos)

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
                prev_record = existing_record  # start with the head
                while existing_record.key_link != Record.RECORD_LINK_NULL:
                    next_pos = existing_record.key_link
                    # Use unmarshal (O(1)) instead of load_from_store (O(n))
                    existing_record = store.codec.decode_record(store.read(next_pos))
                    existing_record.set_store_position(next_pos)
                    if existing_record.key == key:
                        """
                        if same key found in bucket, update previous record in chain to point to key_link of this record
                        prev_rec -> current rec.key_link
                        curr_rec will not be linked in the bucket anymore.
                        """
                        store.update_record_link_inplace(prev_record.store_position, existing_record.key_link)
                        key_link = existing_record.key_link
                        # unlinked — don't advance prev_record
                    else:
                        prev_record = existing_record

        self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_LEN] = self.get_index_key(store_position)
        return key_link

    def get_index(self, key):
        num = cog_hash(key, self.config.INDEX_CAPACITY) % ((sys.maxsize + 1) * 2)
        self.logger.debug("hash for: " + key + " : " + str(num))
        # NOTE: the max(...-1, 0) causes hash 0 and 1 to collide at slot 0 and
        # leaves slot INDEX_CAPACITY-1 unused.  The impact is negligible
        # (~1 extra collision out of 100k slots) and changing it would break
        # every existing index file on disk, so we keep it as is for now.
        index = (self.config.INDEX_BLOCK_LEN *
                 (max((num % self.config.INDEX_CAPACITY) - 1, 0)))
        self.logger.debug("offset : " + key + " : " + str(index))
        return index, num

    # @profile
    def get(self, key, store):
        self.logger.debug("GET: Reading index: " + self.name)
        index_position, raw_hash = self.get_index(key)
        data_at_index_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
        if data_at_index_position == self.empty_block:
            return None
        # Walk the collision chain with unmarshal (O(1)); only load_from_store
        # (which materializes the value chain) for the matched record.
        store_pos = struct.unpack_from('<q', self.db_mem, index_position)[0]
        record = store.codec.decode_record(store.read(store_pos))
        record.set_store_position(store_pos)
        self.logger.debug("read record " + str(record))

        if record.key == key:
            return Record.materialize_values(record, store)
        while record.key_link != Record.RECORD_LINK_NULL:
            self.logger.debug("record.key_link: " + str(record.key_link))
            store_pos = record.key_link
            record = store.codec.decode_record(store.read(store_pos))
            record.set_store_position(store_pos)
            if record.key == key:
                return Record.materialize_values(record, store)
        return None

    def get_head_only(self, key, store):
        """
        Get only the head record without traversing the value chain.
        This is O(1) compared to get() which is O(n) for multi-value keys.
        
        Returns: (record, store_position) or (None, None)
        """
        index_position, raw_hash = self.get_index(key)
        data_at_index_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
        if data_at_index_position == self.empty_block:
            return None, None
        store_position = struct.unpack_from('<q', self.db_mem, index_position)[0]
        # Only unmarshal, don't load value chain
        record = store.codec.decode_record(store.read(store_position))
        record.set_store_position(store_position)

        if record.key == key:
            return record, store_position
        else:
            # Hash collision - follow key_link chain
            while record.key_link != Record.RECORD_LINK_NULL:
                store_position = record.key_link
                record = store.codec.decode_record(store.read(store_position))
                record.set_store_position(store_position)
                if record.key == key:
                    return record, store_position
        return None, None

    '''
        Iterates through all records in the index, following key_link chains for hash collisions.
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
            
            # Load head record and follow key_link chain to get all records in this bucket
            store_position = struct.unpack_from('<q', self.db_mem, scan_cursor)[0]
            while store_position != Record.RECORD_LINK_NULL:
                record = Record.load_from_store(store_position, store)
                if record is None:  # EOF store
                    self.logger.error("Store EOF reached! Iteration terminated.")
                    return
                yield Record(record.key, record.value, record.format_version)
                store_position = record.key_link
            
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

        data_at_index_position = struct.unpack_from('<q', self.db_mem, index_position)[0]
        # delete only needs key/key_link/store_position — use unmarshal (O(1))
        # instead of load_from_store, which would materialize the value chain.
        record = store.codec.decode_record(store.read(data_at_index_position))
        record.set_store_position(data_at_index_position)
        self.logger.debug("read record " + str(record))
        if record.key == key:
            """delete bucket => map hash table to empty block, or point to next in chain"""
            if record.key_link != Record.RECORD_LINK_NULL:
                # Point index to next record in chain
                self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN] = self.get_index_key(record.key_link)
            else:
                # No more records in chain, clear the bucket
                self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN] = self.empty_block
            return True
        else:
            """search bucket"""
            prev_record = record  # Initialize to the head record
            while record.key_link != Record.RECORD_LINK_NULL:
                next_pos = record.key_link
                next_record = store.codec.decode_record(store.read(next_pos))
                next_record.set_store_position(next_pos)
                if next_record.key == key:
                    """
                    if same key found in bucket, update previous record in chain to point to key_link of this record
                    prev_rec -> current rec.key_link
                    curr_rec will not be linked in the bucket anymore.
                    """
                    # update in place the key link pointer of previous record
                    store.update_record_link_inplace(prev_record.store_position, next_record.key_link)
                    return True
                prev_record = next_record
                record = next_record
        return False

    def flush(self):
        self.db_mem.flush()


class Store:
    """
    Store manages persistence of records to disk with configurable flush behavior.
    
    Args:
        flush_interval: Number of writes before auto-flush. 
                       1 = flush every write (safest, default)
                       0 = manual flush only (fastest, use sync())
                       N>1 = flush every N writes with async background thread
    """

    def __init__(self, tablemeta, config, logger, caching_enabled=True, shared_cache=None,
                 flush_interval=1):
        self.caching_enabled = caching_enabled
        self.batch_mode = False  # When True, defers flush() until end_batch()
        self.logger = logging.getLogger('cog.store')
        self.tablemeta = tablemeta
        self.config = config
        self.flush_interval = flush_interval
        self.write_count = 0
        self._closed = False

        self.store = self.config.cog_store(
            tablemeta.namespace, tablemeta.name, tablemeta.db_instance_id)
        self.store_cache = Cache(self.store, shared_cache)
        fd = os.open(self.store, os.O_RDWR | os.O_CREAT, 0o644)
        self.store_file = os.fdopen(fd, 'rb+')

        # Format detection: pick codec based on file contents. Brand-new files
        # get Spindle; existing files keep their original format indefinitely.
        file_size = os.fstat(self.store_file.fileno()).st_size
        self.codec = detect_codec(self.store_file, file_size)
        self._is_spindle = isinstance(self.codec, SpindleCodec)
        if file_size == 0 and self._is_spindle:
            self.codec.write_header(self.store_file)
            self.store_file.flush()
        self.created_at = getattr(self.codec, 'created_at', None)
        self.data_start = self.codec.HEADER_SIZE

        # Thread safety
        self._lock = threading.Lock()

        # Auto-enable async flush when interval > 1
        self._use_async = flush_interval > 1
        if self._use_async:
            self._flush_queue = queue.Queue()
            self._flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
            self._flush_thread.start()
            self._shutdown = False

        logger.info(f"Store init: {self.store} (flush_interval={flush_interval}, codec=v{self.codec.VERSION})")

    def _flush_worker(self):
        """Background thread that processes flush requests."""
        while True:
            try:
                # Wait for flush signal (blocks until item available)
                item = self._flush_queue.get(timeout=1.0)
                if item == "SHUTDOWN":
                    # Drain remaining items and shutdown
                    while not self._flush_queue.empty():
                        try:
                            self._flush_queue.get_nowait()
                            self._flush_queue.task_done()
                        except queue.Empty:
                            break
                    self._flush_queue.task_done()
                    break
                # Perform actual flush (check if not closed)
                if not self._closed:
                    with self._lock:
                        if not self._closed:
                            self.store_file.flush()
                self._flush_queue.task_done()
            except queue.Empty:
                # Timeout - check if we should continue
                if getattr(self, '_shutdown', False):
                    break
                continue

    def _request_flush(self):
        """Request a flush - async if interval > 1, sync otherwise."""
        if self._closed:
            return
        if self._use_async:
            self._flush_queue.put("FLUSH")
        else:
            self.store_file.flush()

    def _handle_write_flush(self):
        """Increment write count and trigger flush if threshold reached."""
        if not self.batch_mode:
            self.write_count += 1
            if self.flush_interval > 0 and self.write_count >= self.flush_interval:
                self._request_flush()
                self.write_count = 0

    def sync(self):
        """
        Force flush all pending writes to disk.
        Blocks until flush is complete.
        """
        if self._closed:
            return
        with self._lock:
            if not self._closed:
                self.store_file.flush()
        if self._use_async:
            # Wait for async queue to drain
            self._flush_queue.join()

    def close(self):
        """Close the store, ensuring all data is flushed."""
        if self._closed:
            return
            
        # Mark as closed first
        self._closed = True
        
        if self._use_async:
            self._shutdown = True
            self._flush_queue.put("SHUTDOWN")
            self._flush_thread.join(timeout=5.0)
        
        with self._lock:
            try:
                self.store_file.flush()
                self.store_file.close()
            except ValueError:
                pass  # File already closed

    def begin_batch(self):
        """
        Enable batch mode - defers flush() until end_batch() is called.
        Use this when inserting many records for significantly better performance.
        """
        self.batch_mode = True

    def end_batch(self):
        """
        End batch mode and flush all pending writes to disk.
        """
        with self._lock:
            self.store_file.flush()
        self.batch_mode = False

    def save(self, record):
        """
        Store data with configurable flush behavior.
        """
        # Spindle stamps a fresh write timestamp inside the lock so positions and
        # timestamps are consistent under concurrent writers.
        with self._lock:
            if self._is_spindle:
                record.timestamp = time.time_ns()
                record.format_version = '2'
            self.store_file.seek(0, 2)
            store_position = self.store_file.tell()
            record.set_store_position(store_position)
            marshalled_record = self.codec.encode_record(record)
            self.store_file.write(marshalled_record)

            if self.caching_enabled:
                self.store_cache.put(store_position, marshalled_record)

            # Handle flush based on interval
            self._handle_write_flush()

        return store_position

    def update_record_link_inplace(self, start_pos, int_value):
        """updates record link in store file in place"""
        if type(int_value) is not int:
            raise ValueError("store position must be int but provided : " + str(start_pos))

        byte_value = self.codec.key_link_bytes(int_value)
        self.logger.debug('update_record_link_inplace: ' + str(byte_value))

        with self._lock:
            self.store_file.seek(start_pos)
            self.store_file.write(byte_value)

            if self.caching_enabled:
                self.store_cache.partial_update_from_zero_index(start_pos, byte_value)

            self._handle_write_flush()

    # @profile
    def read(self, position):
        self.logger.debug("store read request at position: " + str(position))
        if self.caching_enabled:
            cached_record = self.store_cache.get(position)
            if cached_record is not None:
                return cached_record

        self.store_file.seek(position)
        record = self.codec.read_record(self.store_file)

        if self.caching_enabled:
            self.store_cache.put(position, record)

        return record


class Indexer:
    '''
    Manages indexes. Creates new index when an index is full.
    Searches all indexes for get requests.
    Provides same get/put/del method as single index but over multuple files.
    '''

    def __init__(self, tablemeta, config, logger):
        self.tablemeta = tablemeta
        self.config = config
        self.logger = logging.getLogger('cog.indexer')
        self.index_list = []  # future range index.
        self.index_id = 0
        self.load_indexes()
        # if no index currenlty exist, create new live index.
        if len(self.index_list) == 0:
            self.index_list.append(Index(tablemeta, self.config, logger, self.index_id))
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
        for idx in self.index_list:
            result = idx.get(key, store)
            if result is not None:
                return result
        return None

    def get_head_only(self, key, store):
        """Get head record only, O(1) - doesn't traverse value chain."""
        for idx in self.index_list:
            record, pos = idx.get_head_only(key, store)
            if record is not None:
                return record, pos
        return None, None

    def scanner(self, store):
        for idx in self.index_list:
            self.logger.debug("SCAN: index: " + idx.name)
            for r in idx.scanner(store):
                yield r

    def delete(self, key, store):
        for idx in self.index_list:
            if idx.delete(key, store):
                return True
        return False

def cog_hash(string, index_capacity):
    return xxhash.xxh32(string, seed=2).intdigest() % index_capacity
