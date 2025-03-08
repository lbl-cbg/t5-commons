import binascii
import struct
import time


class TaskObjectId():
    '''
    A TaskObjectId is in the form of:
    4 bytes - queue identifier( used by the Queue manager )
    4 bytes - timestamp of file put in
    29 bits location in file
    3 bits file offset. used only when a task is created in the same second
       but forced to go to a new file

    '''
    def __init__(self, toid=None, c_time=None):
        if toid is None:
            if c_time is None:
                self.bytestring = struct.pack('>3I', 0, int(time.time()), 0)
            else:
                self.bytestring = struct.pack('>3I', 0, c_time, 0)
            return

        if len(toid) == 12:
            'make sure this is binary string'
        elif len(toid) == 24:
            'make sure this is a valid hexstring'
            toid = toid.decode('hex')
        else:
            raise Exception('invalid string length')
        self.bytestring = toid

    def set_task_info(self, queue_id, file_loc, file_offset):
        if file_offset > 7:
            raise Exception('file offset can not be more than 7')
        self.bytestring = struct.pack('>I', queue_id) + self.bytestring[4:8] + struct.pack('>I', file_loc << 3 | file_offset)

    def get_file_loc(self):
        i = struct.unpack_from('>I', self.bytestring, 8)[0]
        return i >> 3

    def get_file_offset(self):
        i = struct.unpack_from('>I', self.bytestring, 8)[0]
        return (i & 7)

    def get_time(self):
        t = struct.unpack('>I', self.bytestring[4:8])[0]
        return t

    def get_queue_ident(self):
        return struct.unpack('>i', self.bytestring[0:4])[0]

    def __str__(self):
        return binascii.hexlify(self.bytestring)

    def __repr__(self):
        return '''TaskObjectId('%s')''' % str(self)

    def __hash__(self):
        return hash(self.bytestring)
