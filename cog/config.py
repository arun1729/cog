DEBUG=False

# DB_ROOT="../mdb/"
# SYS=DB_ROOT+"sys/"
# M=SYS+'m'

COG_PATH_PREFIX = "."
COG_HOME = "cog-db"
COG_SYS_DIR = "sys"
COG_SYS_FILE = "cogsys.c"

cog_context = [COG_PATH_PREFIX,COG_HOME,COG_SYS_DIR,COG_SYS_FILE]

STORE="-store-"
INDEX="-index-"

STORE_BLOCK_HEADER_LEN=4

def cog_instance_sys_file():
    return "/".join(cog_context)

def cog_instance_sys_dir():
    return "/".join(cog_context[0:-1])

def cog_data_dir(db_name):
    return "/".join(cog_context[0:-2]+[db_name])

def cog_index(db_name, table_name, instance_id):
    return "/".join(cog_context[0:-2]+[db_name,table_name+INDEX,instance_id])

def cog_store(db_name, table_name, instance_id):
    return "/".join(cog_context[0:-2]+[db_name,table_name+STORE,instance_id])
