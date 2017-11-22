import marshal
import mmap
import sys,traceback
import os.path
import os
from os import mkdir
import uuid
import socket
import pickle
from operator import pos
import struct
from block import Block
from block import BLOCK_LEN
import logging

class Database:

    def __init__(self,config,clean=True,capacity=1000,db_name="DEFAULT", table_name="DEFAULT"):
        self.config=config
        logging.info("Cog init.")
        self.capacity=capacity
        self.collision_cnt=0
        # checks if a Cog instance has been initiated.
        if os.path.exists(config.cog_instance_sys_file()):
            f=open(config.cog_instance_sys_file(),"rb")
            self.m_info=pickle.load(f)
            self.instance_id=self.m_info["m_instance_id"]
            f.close()
        else:
            self.init_cog_instance(db_name)
        #loads default table
        self.use(db_name,table_name)

    def init_cog_instance(self, db_name):
        """ initiates cog instance - called the 'c instance' for the first time"""
        if not os.path.exists(config.cog_instance_sys_file()):
            self.instance_id=str(uuid.uuid4())
            if not os.path.exists(config.cog_instance_sys_dir()): os.makedirs(config.cog_instance_sys_dir())

            m_file=dict()
            m_file["m_instance_id"]=self.instance_id
            m_file["host_name"]=socket.gethostname()
            m_file["host_ip"]=socket.gethostname()

            f=open(config.cog_instance_sys_file(),'wb')
            pickle.dump(m_file,f)
            f.close()
            logging.info("Cog sys file created.")
            os.mkdir(config.cog_data_dir(db_name))
            logging.info("Database created: "+db_name)
            logging.info("done.")

    def use_db(self,db_name):
        if not os.path.exists(self.config.cog_data_dir(db_name)):
            os.mkdir(self.config.cog_data_dir(db_name))

    def use(self, db_name="DEFAULT", table_name="DEFAULT", capacity=1000):
        self.capacity=capacity
        self.use_db(db_name)
        self.index=self.config.cog_index(db_name,table_name,self.instance_id)
        self.store=self.config.cog_store(db_name,table_name,self.instance_id)
        logging.info("new index creared: "+self.index)
        logging.info("new store created: "+self.store)
        if not os.path.exists(self.index):
            print "creating table..."
            f=open(self.index,'wb+')
            i=0
            e_blocks=[]
            while(i<capacity):
                e_blocks.append(Block().new())
                i+=1
            f.write(b''.join(e_blocks))

            self.file_limit=f.tell()
            f.close()
            print "Done."
        else:
            self.file_limit=self.getLen(self.index)

        #memory map table
        if DEBUG: print "file len ->"+str(self.file_limit)

        self.db=open(self.index,'r+b')
        self.db_mem=mmap.mmap(self.db.fileno(), 0)
        self.store_file=open(self.store,'ab+')


    def info(self):
        """ prints db sys info """
        print "collision count: "+str(self.collision_cnt)

    def drop_db(self):
        """ """

    def set(self,key,value,pos=None,start_pos=None):

        assert type(key) is str, "Only string type is supported is currenlty supported."
        assert type(value) is str, "Only string type is supported is currenlty supported."

        if not pos:
            pos=self.get_offset(key)
            start_pos=pos

        if DEBUG: print "setting pos ->"+str(pos) + " for:"+key

        block=self.read_block(pos)

        if block.isFull() or block.isDeleted():
            self.collision_cnt+=1
            next_block=pos+BLOCK_LEN

            if DEBUG: print "trying next block: "+str(next_block)+" file limit : "+str(self.file_limit) + " start_pos: "+str(start_pos)

            if next_block%self.file_limit != start_pos:
                self.set(key, value,next_block,start_pos)
                return
            else:
                raise ValueError("Database capacity reached!")

        self.store_file.seek(0, 2)
        content=self.store_file.tell()

        if DEBUG: print "[*] index pos set: "+str(pos) + " Store pos " + str(content)

        self.db_mem[pos:pos+BLOCK_LEN]=Block().fill(content)
        rec=marshal.dumps((key,value))
        l=len(rec)
        headerf=str(l).zfill(STORE_BLOCK_HEADER_LEN)
#         if DEBUG: print "[*] new store header ->"+headerf + " @"+str(pos)
        self.store_file.write(headerf+rec)

    def get(self,key,pos=None,start_pos=None):

        if not pos:
            pos=self.get_offset(key)
            start_pos=pos

        block=self.read_block(pos)

        if block.isEmpty():
            return None

        store_pos=int(block.getValue())

        if DEBUG: print "getting store pos: "+str(store_pos)

        line=self.get_record(store_pos)

        res=marshal.loads(line)

        if DEBUG: print "looking for: "+key+" read key: "+res[0]

        if key != res[0]:
            next_block=pos+BLOCK_LEN
            if next_block%self.file_limit != start_pos:
                res=self.get(key,next_block,start_pos)
            else:
                print "Could not find key"
                return None

        if DEBUG: print "[*] found key @ "+str(pos)
        return res

    def get_record(self,pos):
        self.store_file.seek(pos)
        content_len=int(self.store_file.read(STORE_BLOCK_HEADER_LEN))
        if DEBUG: print "content_len: "+str(content_len)
        self.store_file.seek(pos+STORE_BLOCK_HEADER_LEN)
        return self.store_file.read(content_len)

    def read_block(self,pos):
            return Block(self.db_mem[pos:pos+BLOCK_LEN])

    def read(self,pos,num_bytes):
            return self.db_mem[pos:pos+num_bytes]

    def get_offset(self,text):
        num=hash(text) % ((sys.maxsize + 1) * 2)
        if DEBUG: print "num hash : "+str(num)
        offset=(BLOCK_LEN*(max( (num%self.capacity)-1,0) )) #there may be diff when using mem slice vs write (+1 needed)
        if DEBUG: print "offset :"+str(offset)
        return offset

    #get length of table
    def getLen(self,path):
        f = open(path, 'r')
        f.seek(0, 2)
        lenf = f.tell()
        f.close()
        return lenf

    def extend_file(self):
        #!!! -> this operation needs rehashing.
        self.close_table()
        f=open(self.index,'ab')
        i=0

        while(i<self.capacity):
            f.write("256E")
            for k in range(255):
                f.write(" ")
            f.write("\n")
            i+=1
        self.file_limit=f.tell()
        f.close()

        if DEBUG: print "file len ->"+str(self.file_limit)

        self.db=open(self.index,'r+b')
        self.db_mem=mmap.mmap(self.db.fileno(), 0)


    def close_table(self):
        self.db_mem.flush()
        self.db_mem.close()
        self.db.close()
