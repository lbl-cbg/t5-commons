### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from builtins import map
from builtins import object
import os
import time
import yaml
from bson import BSON
from .queuefile import QueueFile, INVALID

states = {
    'Normal': 0,
    'Paused': 1,
    'Errored': 2,
}


class Queue(object):

    def __init__(self, name, basedir='.', queue_id=0):
        self.name = name
        root = os.path.join(basedir, name)
        self.root = root
        self.settings_file = os.path.join(root, 'settings')
        self.settings = {}
        self.file_map = {}
        self._status = states['Normal']
        self.state_map = [None] * len(states)
        for name, state in list(states.items()):
            self.state_map[state] = name

        if not os.path.exists(root):
            os.makedirs(root)
        if os.path.exists(self.settings_file):
            with open(self.settings_file) as f:
                self.settings = yaml.full_load(f.read())
        if self.settings is None:
            self.settings = {}
        self.queue_id = self.settings['queue_id'] if 'queue_id' in self.settings else queue_id
        if 'status' in self.settings:
            self._status = self.settings['status']
        working_files = []
        error_files = []
        for file in os.listdir(root):
            if not (file.endswith('w') or file.endswith('e')):
                continue
            full_file = os.path.join(root, file)
            c_time, offset = None, 0
            if file.count('_') > 0:
                c_time, offset = list(map(int, file.split('.')[0].split('_')))
            else:
                c_time = int(file.split('.')[0])
            q_file = QueueFile(full_file, queue_id=self.queue_id, creation_time=c_time, offset=offset)
            if file.endswith('.w'):
                working_files.append(q_file)
            elif file.endswith('.e'):
                error_files.append(q_file)
            self.file_map['%d_%d.%s' % (c_time, offset, file[-1])] = q_file
        if 'status' in self.settings:
            self._status = self.settings['status']
        working_files.sort()
        error_files.sort()
        self.working_files = working_files
        self.error_files = error_files

    def get_failed(self):
        ret = 0
        for file in self.error_files:
            ret += file.num_records - file.invalid
        return ret

    def get_queued(self):
        ret = 0
        for file in self.working_files:
            ret += file.queued
        return ret

    def get_total(self):
        ret = 0
        for file in self.working_files:
            ret += file.num_records
        return ret

    def get_invalid(self):
        ret = 0
        for file in self.working_files:
            ret += file.invalid
        return ret

    def get_working(self):
        return self.get_total() - self.get_invalid() - self.get_queued()

    def write_setting(self, name, value):
        self.settings[name] = value
        with open(self.settings_file, 'w') as f:
            f.write(yaml.safe_dump(self.settings, default_flow_style=False))

    def new_file(self, extension):
        seconds = int(time.time())
        path = os.path.join(self.root, '%d.%s' % (seconds, extension))
        offset = 0
        while os.path.exists(path):
            offset += 1
            path = os.path.join(self.root, '%d_%d.%s' % (seconds, offset, extension))
        q_file = QueueFile(path, self.queue_id, offset, creation_time=seconds)
        self.file_map['%d_%d.%s' % (seconds, offset, extension)] = q_file
        return q_file

    def add(self, record):
        data = BSON.encode(record)
        if len(self.working_files) == 0 or not self.working_files[-1].can_add(data):
            self.working_files.append(self.new_file('w'))
        return {'tid': self.working_files[-1].add(data)}

    def add_error(self, record):
        data = BSON.encode(record)
        if len(self.error_files) == 0 or not self.error_files[-1].can_add(data):
            self.error_files.append(self.new_file('e'))
        return self.error_files[-1].add(data)

    def get(self, tid):
        t = tid.get_time()
        offset = tid.get_file_offset()
        return self.file_map['%d_%d.w' % (t, offset)].get(tid)

    def get_error_range(self, num_records, last_tid=0):
        ret = []
        for file in self.error_files:
            if file.queued == 0:
                continue
            ret.extend(file.get_range(num_records - len(ret)))
            if len(ret) >= num_records:
                return ret
        return ret

    def get_range(self, num_records, last_tid=0):
        ret = []
        for file in self.working_files:
            if file.queued == 0:
                continue
            ret.extend(file.get_range(num_records - len(ret)))
            if len(ret) >= num_records:
                return ret
        return ret

    def get_file(self, tid, extension='w'):
        t = tid.get_time()
        offset = tid.get_file_offset()
        return self.file_map['%d_%d.%s' % (t, offset, extension)]

    def invalidate(self, tid, extension='w'):
        file = self.get_file(tid, extension)
        ret = file.get(tid, INVALID)
        if file.invalid == file.num_records:
            if extension == 'w':
                self.working_files.remove(file)
            else:
                self.error_files.remove(file)
            file.delete()
            t = tid.get_time()
            offset = tid.get_file_offset()
            del self.file_map['%d_%d.%s' % (t, offset, extension)]
        return ret

    def requeue(self, tid):
        record = self.invalidate(tid, 'e')
        return self.add(record['record'])

    def fail(self, tid, reason):
        record = self.invalidate(tid)
        return self.add_error({'record': record, 'message': reason})

    def __next__(self):
        for file in self.working_files:
            if file.queued > 0:
                return next(file)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value not in states:
            raise Exception('invalid state passed')
        self.write_setting('status', states[value])
        self._status = states[value]

    def can_work(self, resources):
        if 'resources' not in self.settings:
            return False
        for resource in self.settings['resources']:
            if resource not in resources:
                return False
        return True
