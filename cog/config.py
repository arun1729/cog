class CogConfig:
    """
    Encapsulates all Cog configuration.
    When passed to Graph(config=...), this prevents mutation of global config state.
    This will be the way to configure cog in the future.

    Example:
        cfg = CogConfig(COG_HOME="my_home", COG_PATH_PREFIX="/data")
        g = Graph("my_graph", config=cfg)
    """

    def __init__(self, **overrides):
        # Copy all module-level defaults
        self.COG_PATH_PREFIX = COG_PATH_PREFIX
        self.COG_ROOT = COG_ROOT
        self.COG_HOME = COG_HOME
        self.COG_SYS_DIR = COG_SYS_DIR
        self.COG_SYS_FILE = COG_SYS_FILE
        self.COG_DEFAULT_NAMESPACE = COG_DEFAULT_NAMESPACE
        self.VIEWS = VIEWS
        self.STORE = STORE
        self.INDEX = INDEX
        self.INDEX_BLOCK_LEN = INDEX_BLOCK_LEN
        self.INDEX_CAPACITY = INDEX_CAPACITY
        self.STORE_READ_BUFFER_SIZE = STORE_READ_BUFFER_SIZE
        self.LEVEL_2_CACHE_SIZE = LEVEL_2_CACHE_SIZE
        self.GRAPH_NODE_SET_TABLE_NAME = GRAPH_NODE_SET_TABLE_NAME
        self.GRAPH_EDGE_SET_TABLE_NAME = GRAPH_EDGE_SET_TABLE_NAME
        self.EMBEDDING_SET_TABLE_NAME = EMBEDDING_SET_TABLE_NAME
        self.CUSTOM_COG_DB_PATH = CUSTOM_COG_DB_PATH
        self.RELAY_URL = RELAY_URL
        self.COGDB_EMBED_URL = COGDB_EMBED_URL
        self.D3_CDN = D3_CDN

        # Apply any overrides
        for key, value in overrides.items():
            if key not in self.__dict__:
                raise ValueError(f"Unknown config option: {key}")
            setattr(self, key, value)

    def cog_db_path(self):
        if self.CUSTOM_COG_DB_PATH:
            return self.CUSTOM_COG_DB_PATH
        else:
            return "/".join([self.COG_PATH_PREFIX, self.COG_HOME])

    def cog_context(self):
        return [self.cog_db_path(), self.COG_SYS_DIR, self.COG_SYS_FILE]

    def cog_instance_sys_file(self):
        return "/".join(self.cog_context())

    def cog_instance_sys_dir(self):
        return "/".join(self.cog_context()[0:-1])

    def cog_views_dir(self):
        return "/".join([self.cog_db_path(), self.VIEWS])

    def cog_data_dir(self, db_name):
        return "/".join([self.cog_db_path(), db_name])

    def cog_index(self, db_name, table_name, instance_id, index_id):
        return "/".join([self.cog_db_path(), db_name, f"{table_name}{self.INDEX}{instance_id}-{index_id}"])

    def get_table_name(self, index_file_name):
        return index_file_name.split(self.INDEX)[0]

    def index_id(self, index_name):
        return int(index_name.split("-")[-1])

    def cog_store(self, db_name, table_name, instance_id):
        return "/".join([self.cog_db_path(), db_name, f"{table_name}{self.STORE}{instance_id}"])

# =============================================================================================
#  --- DEPRECATED ---
# Module level config will be deprecated in the future, leaving it for backward compatibility
# =============================================================================================
COG_PATH_PREFIX = "/tmp"
COG_ROOT = "cog_root"
COG_HOME = "cog-test"
COG_SYS_DIR = "sys"
COG_SYS_FILE = "cogsys.c"
COG_DEFAULT_NAMESPACE = "default"
VIEWS = 'views'
STORE="-store-"
INDEX="-index-"
INDEX_BLOCK_LEN = 32
INDEX_CAPACITY = 100003 # must be a prime number
STORE_READ_BUFFER_SIZE = 512
LEVEL_2_CACHE_SIZE = 100000

''' TORQUE '''
GRAPH_NODE_SET_TABLE_NAME = 'TOR_NODE_SET'
GRAPH_EDGE_SET_TABLE_NAME = 'TOR_EDGE_SET'
EMBEDDING_SET_TABLE_NAME = 'EMBEDDING_SET'

''' CUSTOM COG DB PATH '''
CUSTOM_COG_DB_PATH = None

''' SHARE/TUNNEL RELAY '''
# Set to None to disable the share feature entirely
# When None, calling serve(share=True) will raise an error
RELAY_URL = "wss://s.cogdb.io/register"

''' VECTORIZE '''
COGDB_EMBED_URL = "https://vectors.cogdb.io/embed"

''' GRAPH VISUALIZATION '''
D3_CDN = "https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"

''' CLOUD '''
CLOUD_URL = "https://api.cogdb.io"
CLOUD_API_PREFIX = "/api/v1"


def cog_db_path():
    if CUSTOM_COG_DB_PATH:
        return CUSTOM_COG_DB_PATH
    else:
        return "/".join([COG_PATH_PREFIX, COG_HOME])


def cog_context():
    return [cog_db_path(), COG_SYS_DIR, COG_SYS_FILE]


def cog_instance_sys_file():
    return "/".join(cog_context())


def cog_instance_sys_dir():
    return "/".join(cog_context()[0:-1])


def cog_views_dir():
    return "/".join([cog_db_path(), VIEWS])


def cog_data_dir(db_name):
    return "/".join(cog_context()[0:-2]+[db_name])


def cog_index(db_name, table_name, instance_id, index_id):
    return "/".join(cog_context()[0:-2]+[db_name,table_name+INDEX+instance_id+"-"+str(index_id)])


def get_table_name(index_file_name):
    return index_file_name.split(INDEX)[0]


def index_id(index_name):
    return int(index_name.split("-")[-1])


def cog_store(db_name, table_name, instance_id):
    return "/".join(cog_context()[0:-2]+[db_name, table_name+STORE+instance_id])

