### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import os
import struct
import time
from bson import decode_all
from threading import Lock
from .tid import TaskObjectId

QUEUED = 1
IN_PROGRESS = 2
INVALID = 3

MAX_FILE_SIZE = 2**29


class QueueFile():

    '''
        File starts with:
        4 bytes - record count
        4 bytes - queued count
        4 bytes - invalid count
        4 bytes - loc of first queued record
        DATA:
            4 bytes - 3 bits status 29 bits location
            4 bytes - last status change time
            4 bytes - length
            BSON data
    '''

    def __init__(self, file, queue_id=0, offset=0, creation_time=0):
        self.file_path = file
        self.queue_id = queue_id
        self.offset = offset
        self.creation_time = creation_time
        if not os.path.exists(file):
            with open(file, 'wb') as f:
                f.write(struct.pack('>4I', 0, 0, 0, 16))
        with open(file, 'rb') as f:
            self.num_records, self.queued, self.invalid, self.first_queued_record = struct.unpack('>4I', f.read(4 * 4))
        if self.first_queued_record == 0:
            self.first_queued_record = 16
        self.file_size = os.path.getsize(file)
        self._file_h = None
        self.lock = Lock()

    def __get_file_handle(self):
        if self._file_h is None:
            self._file_h = open(self.file_path, 'rb+')
        return self._file_h

    def can_add(self, data):
        return self.file_size + len(data) + 8 < MAX_FILE_SIZE

    '''
        data should be in bson format already

        return a tid
    '''
    def add(self, data):
        if self.file_size + len(data) + 8 > MAX_FILE_SIZE:
            raise Exception('File is too large to contain new record')
        tid = TaskObjectId(c_time=self.creation_time)
        with self.lock:
            self.num_records += 1
            self.queued += 1
            f = self.__get_file_handle()
            f.seek(0)
            f.write(struct.pack('>2I', self.num_records, self.queued))
            f.seek(self.file_size)
            start_data = struct.pack('>3I', ((self.file_size << 3) | QUEUED), int(time.time()), len(data))
            f.write(start_data)
            tid.set_task_info(self.queue_id, self.file_size, self.offset)
            f.seek(self.file_size + 12)
            f.write(data)
            self.file_size += (len(data) + 12)
        return tid

    '''
        should take the tid and get the location of the task
        seek to the location and return the data
    '''
    def get(self, tid, to_status=None):
        location = tid.get_file_loc()

        with self.lock:
            f = self.__get_file_handle()
            f.seek(location)
            loc_data, change, length = struct.unpack('>3I', f.read(12))
            rec_loc = loc_data >> 3
            c_status = loc_data & 7
            if rec_loc != location:
                raise Exception('Passed an invalid TaskObjectId object')
            data = f.read(length)
            if to_status is not None and to_status != c_status:
                f.seek(location)
                f.write(struct.pack('>2I', (rec_loc << 3) | to_status, int(time.time())))
                if to_status == IN_PROGRESS:
                    self.queued -= 1
                    f.seek(4)
                    f.write(struct.pack('>I', self.queued))
                    if location == self.first_queued_record:
                        self.first_queued_record = location + 12 + len(data)
                        f.seek(12)
                        f.write(struct.pack('>I', self.first_queued_record))
                elif to_status == INVALID:
                    self.invalid += 1
                    f.seek(8)
                    f.write(struct.pack('>I', self.invalid))
        return decode_all(data)[0]

    def get_range(self, num_records, start=0):
        if start == 0:
            start = self.first_queued_record
        ret = []
        with self.lock:
            f = self.__get_file_handle()
            f.seek(start)
            while len(ret) < num_records and start < self.file_size:
                loc_data, change, length = struct.unpack('>3I', f.read(12))
                c_status = loc_data & 7
                start += length + 12
                if c_status != INVALID:
                    tid = TaskObjectId(c_time=self.creation_time)
                    tid.set_task_info(self.queue_id, start - (length + 12), self.offset)
                    data = f.read(length)
                    ret.append({'tid': tid, 'data': decode_all(data)[0]})
                else:
                    f.seek(start)
        return ret

    def __next__(self):
        start = self.first_queued_record
        ret = None
        with self.lock:
            f = self.__get_file_handle()
            f.seek(start)
            while ret is None and start < self.file_size:
                loc_data, change, length = struct.unpack('>3I', f.read(12))
                c_status = loc_data & 7
                if c_status == QUEUED:
                    tid = TaskObjectId(c_time=self.creation_time)
                    tid.set_task_info(self.queue_id, start, self.offset)
                    data = f.read(length)
                    ret = {'tid': tid, 'data': decode_all(data)[0]}
                    f.seek(start)
                    f.write(struct.pack('>2I', (start << 3) | IN_PROGRESS, int(time.time())))
                    self.queued -= 1
                    f.seek(4)
                    f.write(struct.pack('>I', self.queued))
                    if start == self.first_queued_record:
                        self.first_queued_record = start + 12 + length
                        f.seek(12)
                        f.write(struct.pack('>I', self.first_queued_record))
                start += length + 12
        return ret

    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    def next(self):
        return self.__next__()
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

    def delete(self):
        if self._file_h is not None:
            self._file_h.close()
        os.unlink(self.file_path)

    def __eq__(self, other):
        return self.creation_time == other.creation_time and self.offset == other.offset

    def __ne__(self, other):
        return self.creation_time != other.creation_time or self.offset != other.offset

    def __lt__(self, other):
        if self.creation_time == other.creation_time:
            return self.offset < other.offset
        return self.creation_time < other.creation_time

    def __le__(self, other):
        if self.creation_time == other.creation_time:
            return self.offset <= other.offset
        return self.creation_time <= other.creation_time

    def __gt__(self, other):
        if self.creation_time == other.creation_time:
            return self.offset > other.offset
        return self.creation_time > other.creation_time

    def __ge__(self, other):
        if self.creation_time == other.creation_time:
            return self.offset >= other.offset
        return self.creation_time >= other.creation_time

    def __hash__(self):
        return hash('%d_%d' % (self.creation_time, self.offset))
