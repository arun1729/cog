import struct

EMPTY_BLOCK='1'
FULL_BLOCK='2'
DELETED_BLOCK='3'
BLOCK_LEN=4

class Block:

    def __init__(self,block=None):
        if block:
            self.content=struct.unpack('I',block)[0]
        else:
            self.content=self.new()

    def new(self,content=1000000000):
        self.content=content
        return struct.pack("I",self.content)

    def fill(self,content):
        self.content=2000000000+content
        return struct.pack("I",self.content)

    def getContent(self):
        return self.content

    def getValue(self):
        return str(self.content)[1:]

    def isEmpty(self):
        if(str(self.content)[0]==EMPTY_BLOCK):
            return True
        return False

    def isFull(self):
        if(str(self.content)[0]==FULL_BLOCK):
            return True
        return False

    def isDeleted(self):
        if(str(self.content)[0]==DELETED_BLOCK):
            return True
        return False
