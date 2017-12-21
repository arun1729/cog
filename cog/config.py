COG_PATH_PREFIX = "/tmp"
COG_HOME = "cog-test"
COG_SYS_DIR = "sys"
COG_SYS_FILE = "cogsys.c"
STORE="-store-"
INDEX="-index-"
INDEX_BLOCK_LEN=10
INDEX_CAPACITY = 2000
INDEX_LOAD_FACTOR = 80

cog_context = [COG_PATH_PREFIX,COG_HOME,COG_SYS_DIR,COG_SYS_FILE]

def cog_instance_sys_file():
    return "/".join(cog_context)

def cog_instance_sys_dir():
    return "/".join(cog_context[0:-1])

def cog_data_dir(db_name):
    return "/".join(cog_context[0:-2]+[db_name])

def cog_index(db_name, table_name, instance_id, index_id):
    return "/".join(cog_context[0:-2]+[db_name,table_name+INDEX+instance_id+"-"+str(index_id)])

def index_id(index_name):
    return int(index_name.split("-")[-1])

def cog_store(db_name, table_name, instance_id):
    return "/".join(cog_context[0:-2]+[db_name,table_name+STORE+instance_id])

import logging

logging_config = dict(
    version = 1,
    formatters = {
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
    handlers = {
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.DEBUG}
        },
    root = {
        'handlers': ['h'],
        'level': logging.INFO,
        },
)
