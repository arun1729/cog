import marshal
import mmap
import os
import os.path
import sys
import logging
# from profilehooks import profile
import xxhash

class TableMeta:

    def __init__(self, name, namespace, db_instance_id, column_mode):
        self.name = name
        self.namespace = namespace
        self.db_instance_id = db_instance_id
        self.column_mode = column_mode


class Table:

    def __init__(self, name, namespace, db_instance_id, config, logger=None, column_mode=False):
        self.logger = logging.getLogger('table')
        self.config = config
        self.table_meta = TableMeta(name, namespace, db_instance_id, column_mode)
        self.indexer = self.__create_indexer()
        self.store = self.__create_store()

    def __create_indexer(self):
        return Indexer(self.table_meta, self.config, self.logger)

    def __create_store(self):
        return Store(self.table_meta, self.config, self.logger)

    def close(self):
        self.indexer.close()
        self.store.close()
        self.logger.info("closed table: "+self.table_meta.name)


class Record:

    def __init__(self, key, value, tombstone=None, store_position=None, rtype=None,  key_link=None, value_link=None):
        self.key = key
        self.value = value
        self.tombstone = tombstone
        self.store_position = store_position
        self.key_link = key_link
        self.value_link = value_link
        self.value_type = rtype

    def is_equal_val(self, other_record):
        return self.key == other_record.key and self.value == other_record.value

    def get_kv_tuple(self):
        return self.key, self.value

    def serialize(self):
        return marshal.dumps((self.key, self.value))

    def marshal(self):
        key_link_bytes = str(self.key_link).encode()
        serialized = self.serialize()

        m_record = str(len(key_link_bytes)).encode() \
                + b'\x1F' + key_link_bytes \
                + self.tombstone.encode() \
                + self.value_type.encode() \
                + str(len(serialized)).encode() \
                + b'\x1F' \
                + serialized
        if self.value_type == "l":
            m_record += str(self.value_link).encode()
        m_record += b'\x1E'
        return m_record

    @classmethod
    def __read_until(cls, start, sbytes, separtor=b'\x1F'):
        buff = b''
        for i in range(start, len(sbytes)):
            s_byte = sbytes[i: i + 1].tobytes()
            if s_byte == separtor:
                break
            buff += s_byte
        print(buff, i)
        return buff, i

    @classmethod
    def unmarshal(cls, store_bytes):
        """reads from bytes and creates object
        return cls(1,2,3,4..)

        """
        store_bytes = memoryview(store_bytes)

        key_link_len, end_pos = cls.__read_until(0, store_bytes)
        key_link_len = int(key_link_len.decode())
        key_link = int(store_bytes[0: key_link_len].tobytes().decode())
        tombstone = store_bytes[key_link_len + 1:1].tobytes().decode()
        value_type = store_bytes[key_link_len + 2:1].tobytes().decode()
        value_len, end_pos = int(cls.__read_until(key_link_len + 3, store_bytes))
        value_len = int(value_len.decode())
        value = store_bytes[end_pos: value_len].tobytes().decode()
        record = marshal.loads(value)
        value_link = None
        if value_type == 'l':
            value_link, end_pos = cls.__read_until(end_pos + value_len + 1,  store_bytes, b'\x1E')
            value_link = int(value_link.decode())
        return cls(record[0], record[1], tombstone, store_position=None, rtype=value_type,  key_link=key_link, value_link=value_link)


    def is_empty(self):
        return self.key is None and self.value is None

    def __str__(self):
        return "key: {}, value: {}, tombstone: {}, store_position: {}".format(self.key, self.value, self.tombstone, self.store_position)



        # self.store_file.seek(0, 2)
        # store_position = self.store_file.tell()
        # kcp = marshal.dumps(kc_pointer)
        # kcp_len = str(len(kcp))
        # record = record_obj.serialize()
        # rec_len = str(len(record))
        # self.logger.debug("STORE SAVE: length: "+rec_len + " save record: "+ str(record) + " type: "+c_type + " prev pointer: "+str(prev_pointer))
        # self.store_file.seek(0, 2)
        # self.store_file.write(b'0')  # TS
        # self.store_file.write(kcp_len.encode()) #KC len
        # self.store_file.write(b'\x1F')
        # self.store_file.write(kcp) #key chain

        # self.store_file.write(c_type.encode())  # type bit
        # self.store_file.write(rec_len.encode()) # rec len
        # self.store_file.write(b'\x1F') #content length end - unit separator
        # self.store_file.write(record)
        # if c_type == 'l' and prev_pointer is not None:
        #     prevp = str(prev_pointer).encode()
        #     self.logger.debug("STORE SAVE: writing previous pointer: "+str(prevp))
        #     self.store_file.write(prevp)
        # self.store_file.write(b'\x1E') # record separator
        # self.store_file.flush()
        # return store_position
#
# class Block:
#     """
#
#     """
#     #[[TS][key chain pointer][record]]
#     def __init__(self, tombstone, keychain_pointer, record):
#         self.tombstone = tombstone
#         self.keychain_pointer = keychain_pointer
#         self.record = record
#
#     def get_bytes(self):
#         """
#         Returns byte representation of the a block
#         :return:
#         """
#         kcp = self.kc_pointer.decode()
#         kcp_len = str(len(kcp))
#         self.tombstone.encode() + kcp_len.encode() + b'\x1F' +  kcp
#
#
#
#     @classmethod
#     def from_bytes(cls, bytes):
#         """
#         parses bytes into a block object
#         :return:
#         """




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

        self.load = 0
        self.db_mem.seek(0)
        current_block = self.db_mem.read(self.config.INDEX_BLOCK_LEN)
        #computes current load on index file.
        while len(current_block) != 0:
            if current_block != self.empty_block:
                self.load += 1
            current_block = self.db_mem.read(self.config.INDEX_BLOCK_LEN)

        # self.db_mem.seek(0)

    def close(self):
        self.db.close()

    def get_load(self):
        return self.load

    def get_key_bit(self, block_data):
        return int(block_data[self.config.INDEX_BLOCK_BASE_LEN: self.config.INDEX_BLOCK_LEN])

    def get_store_bit(self, block_data):
        return int(block_data[:self.config.INDEX_BLOCK_BASE_LEN])

    def put(self, key, store_position, store):
        """
        key chain
        :param key:
        :param store_position:
        :param store:
        :return:
        """
        orig_position, orig_hash = self.get_index(key)
        data_at_prob_position = self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_LEN]
        self.logger.debug("PUT: probe position: " + str(orig_position) + " value = " + str(data_at_prob_position))
        if data_at_prob_position != self.empty_block:
            """
            Read keys in bucket:
                Read key at position in store
                add that as the prev-key to the key-chain bit
            write the new k:v to store
            put the store position in the index position.
            """
        else:
            store_position = str(store_position).encode().rjust(self.config.INDEX_BLOCK_BASE_LEN)
            self.db_mem[orig_position: orig_position + self.config.INDEX_BLOCK_BASE_LEN]

        looped_back=False

        while data_at_prob_position != self.empty_block:
            if looped_back:# Terminating condition
                if probe_position >= orig_position or len(data_at_prob_position) == 0:
                    self.logger.info("Unable to index data. Index capacity reached!: "+self.name)
                    return None
            if len(data_at_prob_position) == 0:#check if EOF reached.
                    probe_position=0
                    data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                    looped_back=True
                    self.logger.debug("PUT: LOOP BACK to position: "+str(probe_position)+" value = "+str(data_at_prob_position))
                    continue
            key_bit = self.get_key_bit(data_at_prob_position)
            orig_bit = orig_hash % pow(10, self.config.INDEX_BLOCK_KEYBIT_LEN)
            if orig_bit == key_bit:
                self.logger.debug("PUT: key_bit match! for: "+str(orig_bit))
                record = store.read(self.get_store_bit(data_at_prob_position))
                if record[1][0] == key:
                    self.logger.debug("PUT: Updating index: " + self.name)
                    break
                else:
                    #key bit collision
                    probe_position += self.config.INDEX_BLOCK_LEN
                    data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                    self.logger.debug("PUT: key bit collision, probing next position: " + str(probe_position) + " value = " + str(data_at_prob_position))
            else:
                probe_position += self.config.INDEX_BLOCK_LEN
                data_at_prob_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                self.logger.debug("PUT: probing next position: "+str(probe_position)+" value = "+str(data_at_prob_position))

        # if a free index block is found, then write key to that position.
        store_position_bit = str(store_position).encode().rjust(self.config.INDEX_BLOCK_BASE_LEN)
        if len(store_position_bit) > self.config.INDEX_BLOCK_BASE_LEN:
            raise Exception('Store address '+str(len(store_position_bit))+' exceeds index block size '+str(self.config.INDEX_BLOCK_BASE_LEN)+'. Database is full. Please reconfigure database and reload data.')

        key_bit = str(orig_hash % pow(10, self.config.INDEX_BLOCK_KEYBIT_LEN)).encode().rjust(self.config.INDEX_BLOCK_KEYBIT_LEN)
        self.logger.debug("store_position_bit: "+str(store_position_bit)+" key_bit: " + str(key_bit))
        #if store position is greater that index block length, thrwo execption: maxium address length reachde, and link to github error notes for help.
        self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN] = store_position_bit + key_bit
        self.logger.debug("indexed " + key + " @: " + str(probe_position) + " : store position: " + str(store_position) + " : key bit :" + str(key_bit))
        self.load += 1
        return probe_position

    def get_index(self, key):
        num = self.cog_hash(key) % ((sys.maxsize + 1) * 2)
        self.logger.debug("hash for: " + key + " : " + str(num))
        # there may be diff when using mem slice vs write (+1 needed)
        index = (self.config.INDEX_BLOCK_LEN *
                 (max((num % self.config.INDEX_CAPACITY) - 1, 0)))
        self.logger.debug("offset : " + key + " : " + str(index))
        return index, num

    def cog_hash(self, string):
        return xxhash.xxh32(string, seed=2).intdigest() % self.config.INDEX_CAPACITY

    #@profile
    def get(self, key, store):
        self.logger.debug("GET: Reading index: " + self.name)
        orig_position, orig_hash = self.get_index(key)
        probe_position = orig_position
        record = None
        looped_back=False

        while True:
            data_at_probe_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
            self.logger.debug("GET: probe position @1: "+str(probe_position)+" value = "+str(data_at_probe_position))

            if len(data_at_probe_position) == 0:#EOF index
                if not looped_back:
                    probe_position = 0
                    data_at_probe_position = self.db_mem[probe_position:probe_position + self.config.INDEX_BLOCK_LEN]
                    self.logger.debug("GET: LOOP BACK: "+str(probe_position)+" value = "+str(data_at_probe_position))
                    looped_back = True
                else:
                    self.logger.info("Index EOF reached! Key not found.")
                    return Record(None, None, None, None)

            if data_at_probe_position == self.empty_block:
                probe_position += self.config.INDEX_BLOCK_LEN
                self.logger.debug("GET: found empty block, terminating get.")
                return Record(None, None, None, None)

            key_bit = self.get_key_bit(data_at_probe_position)
            orig_bit = orig_hash % pow(10, self.config.INDEX_BLOCK_KEYBIT_LEN)
            #record = None
            if(orig_bit == key_bit):
                record = store.read(self.get_store_bit(data_at_probe_position))
                self.logger.debug("GET: READ BACK RECORD: "+str(record))
                if record is None or len(record) == 0:#EOF store
                    self.logger.error("Store EOF reached! Indexed record not found.")
                    Record(None, None, None, None)

                if record is not None and key == record[1][0]:# found record!
                    self.logger.info("found key in index."+self.name)
                    return Record(record[1][0], record[1][1], record[0], self.get_store_bit(data_at_probe_position))

            self.logger.info("found key "+ key +" but `collision` in index."+self.name + " orig_bit: "+str(orig_bit) + " key_bit: "+str(key_bit) + " record: " + str(record))

            probe_position += self.config.INDEX_BLOCK_LEN

    '''
        Iterates through record in itr_store.
    '''
    def scanner(self,store):
        scan_cursor = 0
        while True:
            data_at_position = self.db_mem[scan_cursor:scan_cursor + self.config.INDEX_BLOCK_LEN]
            if len(data_at_position) == 0:#EOF index
                self.logger.info("Index EOF reached! Scan terminated.")
                return
            if data_at_position == self.empty_block:
                scan_cursor += self.config.INDEX_BLOCK_LEN
                self.logger.debug("GET: skipping empty block during iteration.")
                continue
            tombstone, data = store.read(self.get_store_bit(data_at_position))
            if len(data) == 0:#EOF store
                self.logger.error("Store EOF reached! Iteration terminated.")
                return
            yield Record(data[0], data[1], tombstone)
            scan_cursor += self.config.INDEX_BLOCK_LEN

    def delete(self, key, store):
        index_position, orig_hash = self.get_index(key)
        data_at_probe_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]

        if data_at_probe_position == self.empty_block:
            return False

        key_bit = self.get_key_bit(data_at_probe_position)
        orig_bit = orig_hash % pow(10, self.config.INDEX_BLOCK_KEYBIT_LEN)

        record = None
        if (orig_bit == key_bit):
            record = store.read(self.get_store_bit(data_at_probe_position))

            if record is None:
                self.logger.info("Store EOF reached! Record not found.")
                return False

        while key != record[1][0]:
            index_position += self.config.INDEX_BLOCK_LEN
            current_store_position = self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN]
            if len(current_store_position) == 0:
                self.logger.info("Index EOF reached! Key not found.")
                return False

            key_bit = self.get_key_bit(current_store_position)
            orig_bit = orig_hash % pow(10, self.config.INDEX_BLOCK_KEYBIT_LEN)
            if (orig_bit == key_bit):
                record = store.read(self.get_store_bit(current_store_position))
                if len(record) == 0:
                    self.logger.info("Store EOF reached! Record not found.")
                    return False

        self.db_mem[index_position:index_position + self.config.INDEX_BLOCK_LEN] = self.empty_block
        self.logger.debug("deleted :"+str(data_at_probe_position))
        self.load -= 1
        return True

    def flush(self):
        self.db_mem.flush()


class Store:

    def __init__(self, tablemeta, config, logger):
        self.logger = logging.getLogger('store')
        self.tablemeta = tablemeta
        self.config = config
        self.empty_block = '-1'.zfill(self.config.INDEX_BLOCK_LEN).encode()
        self.store = self.config.cog_store(
            tablemeta.namespace, tablemeta.name, tablemeta.db_instance_id)
        temp = open(self.store, 'a')  # create if not exist
        temp.close()
        self.store_file = open(self.store, 'rb+')
        logger.info("Store for file init: " + self.store)

    def close(self):
        self.store_file.close()

    def save(self, record_obj, kc_pointer, prev_pointer=None, c_type='s'):
        """
        [[TS][key chain pointer][type][record]]
        Store data
        """
        self.store_file.seek(0, 2)
        store_position = self.store_file.tell()
        kcp = marshal.dumps(kc_pointer)
        kcp_len = str(len(kcp))
        record = record_obj.serialize()
        rec_len = str(len(record))
        self.logger.debug("STORE SAVE: length: "+rec_len + " save record: "+ str(record) + " type: "+c_type + " prev pointer: "+str(prev_pointer))
        self.store_file.seek(0, 2)
        self.store_file.write(b'0')  # TS
        self.store_file.write(kcp_len.encode()) #KC len
        self.store_file.write(b'\x1F')
        self.store_file.write(kcp) #key chain
        self.store_file.write(c_type.encode())  # type bit
        self.store_file.write(rec_len.encode()) # rec len
        self.store_file.write(b'\x1F') #content length end - unit separator
        self.store_file.write(record)
        if c_type == 'l' and prev_pointer is not None:
            prevp = str(prev_pointer).encode()
            self.logger.debug("STORE SAVE: writing previous pointer: "+str(prevp))
            self.store_file.write(prevp)
        self.store_file.write(b'\x1E') # record separator
        self.store_file.flush()
        return store_position

    def read_keychain(self, position):
        """
        Reads all the keys in a bucket.
        [[TS][key chain pointer][type][record]]
        :param position:
        :return:
        """
        tombstone, kc_pointer, type_bit, record = self.__read_block(position)

        if type_bit == 'l':
            prev_pointer = self.__read_until(b'\x1E')
            prev_pointer = int(prev_pointer) if prev_pointer != '' else -1
            c_list = [record[1]]
            key = record[0]

            while prev_pointer > -1:
                self.logger.debug("STORE READ: look for prev pointer: "+str(prev_pointer))
                tombstone, type_bit, record = self.__read_block(prev_pointer)
                c_list.append(record[1])
                prev_pointer = self.__read_until(b'\x1E')
                prev_pointer = int(prev_pointer) if prev_pointer != '' else -1

            self.logger.debug("STORE READ: list read terminating, returning: "+str((record[0], c_list)))
            return tombstone, (key, c_list)
        else:
            return tombstone, record

    def read(self, position):
        tombstone, type_bit, record = self.__read_block(position)

        if type_bit == 'l':
            prev_pointer = self.__read_until(b'\x1E')
            prev_pointer = int(prev_pointer) if prev_pointer != '' else -1
            c_list = [record[1]]
            key = record[0]

            while prev_pointer > -1:
                self.logger.debug("STORE READ: look for prev pointer: "+str(prev_pointer))
                tombstone, type_bit, record = self.__read_block(prev_pointer)
                c_list.append(record[1])
                prev_pointer = self.__read_until(b'\x1E')
                prev_pointer = int(prev_pointer) if prev_pointer != '' else -1

            self.logger.debug("STORE READ: list read terminating, returning: "+str((record[0], c_list)))
            return tombstone, (key, c_list)
        else:
            return tombstone, record

    def __read_until(self, separator):
        data = []
        c = self.store_file.read(1)
        while c != separator:
            data.append(c)
            c = self.store_file.read(1)
            if len(c) == 0:
                self.logger.debug("EOF store file! Data read error.")
                return None
        return b''.join(data).decode()

    def __read_block(self, position):
        #  [[TS][key chain pointer][type][record]]
        self.store_file.seek(position)
        tombstone = self.store_file.read(1)
        key_len = int(self.__read_until(b'\x1F'))
        kc_pointer = marshal.loads(self.store_file.read(key_len))
        type_bit = self.store_file.read(1).decode()
        record_len =int(self.__read_until(b'\x1F'))
        record = marshal.loads(self.store_file.read(record_len))
        self.logger.debug("STORE READ: " + str(record) + " type bit: "+type_bit)
        return tombstone, kc_pointer, type_bit, record


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
        self.index_list = []
        self.index_id = 0
        self.load_indexes()
        #if no index currenlty exist, create new live index.
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
                    self.logger.info("loading index file: "+f)
                    id = self.config.index_id(f)
                    index = Index(self.tablemeta, self.config, self.logger, id)
                    self.index_list.append(index)
                    #make the latest index the live index.
                    if id >= self.index_id:
                        self.index_id = id
                        self.live_index = index

    def put(self, key, store_position, store):

        while True:
            if self.live_index.get_load() * 100.0 / self.config.INDEX_CAPACITY > self.config.INDEX_LOAD_FACTOR:
                self.live_index.flush()
                self.index_id += 1
                self.logger.info("Index load reached, creating new index file: "+str(self.index_id))
                self.index_list.append(Index(self.tablemeta, self.config, self.logger, self.index_id))
                self.live_index = self.index_list[self.index_id]
                self.live_index_usage = self.live_index.get_load()

            resp = self.live_index.put(key, store_position, store)

            if resp is not None:
                self.logger.debug("Key: "+key+" indexed in: "+self.live_index.name)
                break

    #@profile
    def get(self, key, store):
        if len(self.index_list) > 1:
            self.logger.info("multiple index: " + str(len(self.index_list)))
        for idx in self.index_list:
            self.logger.info("GET: looking in index: "+idx.name + " for key: "+key)
            record=idx.get(key, store)
            if record:
                return record

        self.logger.info("Key: "+key+ " not found in table: "+self.tablemeta.name)
        return None

    def scanner(self, store):
        for idx in self.index_list:
            self.logger.debug("SCAN: index: "+idx.name)
            for r in idx.scanner(store):
                yield r


    def delete(self, key, store):
        for idx in self.index_list:
            if idx.delete(key, store):
                return True
            else:
                return False
