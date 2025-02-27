### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
from future.utils import iteritems
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import os
from .queue import Queue


class QueueList():
    def __init__(self, resources):
        self.resources = resources
        self.on = 0
        self.li = []

    def add(self, queue):
        self.li.append(queue)

    def remove(self, queue):
        self.li.remove(queue)
        if len(self.li) < self.on:
            self.on = 0

    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    def next(self):
        return self.__next__()
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

    def __next__(self):
        length = len(self.li)
        for i in range(length):
            queue = self.li[(i + self.on) % length]
            if queue.get_queued() > 0 and queue.status == 0:
                self.on = (i + self.on + 1) % length
                ret = next(queue)
                ret['queue'] = queue.name
                return ret


class QueueManager():

    def __init__(self, name, base='.', max_resources=None):
        root = os.path.join(base, name)
        if not os.path.exists(root):
            os.makedirs(root)
        self.root = root
        self.max_resources = max_resources
        self.r_map = {}
        self.k_r_map = {}
        self.failure_count = {}
        self.name = name
        queue_names = []
        for rooty, dirs, files in os.walk(root):
            if 'settings' in files:
                queue_names.append(rooty.replace(root + '/', '', 1))
        self.queues = {}
        for queue in queue_names:
            queue_o = Queue(queue, root)
            queue_id = queue_o.settings['queue_id'] if 'queue_id' in queue_o.settings else self.__generate_id(queue)
            self.failure_count[queue_id] = 0
            if 'queue_id' not in queue_o.settings:
                queue_o.write_setting('queue_id', queue_id)
                queue_o.queue_id = queue_id
            self.queues[str(queue_id)] = queue_o

    def get_queue(self, name, create_new=True, call_back=None, **kwargs):
        queue_id = abs(hash(self.name + '.' + name)) >> 32
        ret = None
        while True:
            s_q = str(queue_id)
            if s_q not in self.queues:
                if not create_new:
                    raise Exception('''There is no queue by the name of '%s'.''' % name)
                ret = Queue(name, self.root, queue_id)
                self.queues[s_q] = ret
                self.failure_count[queue_id] = 0
                ret.write_setting('queue_id', queue_id)
                ret.call_back = call_back
                if 'resources' in kwargs:
                    ret.settings['resources'] = kwargs['resources']
                    self.add_to_resource_list(ret)
                break
            if self.queues[s_q].name == name:
                ret = self.queues[s_q]
                ret.call_back = call_back
                break
            queue_id += 1
        for arg in kwargs:
            ret.write_setting(arg, kwargs[arg])
        return ret

    def __generate_id(self, name):
        queue_id = abs(hash(self.name + '.' + name)) >> 32
        while (str(queue_id) in self.queues):
            queue_id += 1
        return queue_id

    def get_queue_from_tid(self, tid):
        queue_id = str(tid.get_queue_ident())
        return self.queues[queue_id]

    def add_to_resource_list(self, queue):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, a_resources in iteritems(self.k_r_map):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, a_resources in self.k_r_map.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if queue.can_work(a_resources):
                self.r_map[key].add(queue)

    '''

    '''
    def next(self, available_resources):
        key = self.__hash_list(available_resources)

        if key not in self.r_map:
            queues = self.__create_queue_list(available_resources)
            self.r_map[key] = queues
            self.k_r_map[key] = available_resources
        return next(self.r_map[key])

    def __hash_list(self, li):
        ret = 0
        for el in li:
            ret ^= hash(el)
        return ret

    def __create_queue_list(self, resources):
        ret = QueueList(resources)
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for _id, queue in iteritems(self.queues):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for _id, queue in self.queues.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if queue.can_work(resources):
                ret.add(queue)
        return ret

    def fail(self, tid, message):
        queue = self.get_queue_from_tid(tid)
        self.failure_count[tid.get_queue_ident()] += 1
        if self.failure_count[tid.get_queue_ident()] > 3:
            queue.set_status('Errored')
        return queue.fail(tid, message)

    def finished(self, tid):
        self.failure_count[tid.get_queue_ident()] = 0
        return self.get_queue_from_tid(tid).invalidate(tid)

    def requeue(self, tid):
        return self.get_queue_from_tid(tid).requeue(tid)

    def status(self):
        ret = []
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for queue_id, queue in iteritems(self.queues):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for queue_id, queue in self.queues.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ret.append({'queue': queue.name, 'queued': queue.get_queued(), 'failed': queue.get_failed(), 'in_progress': queue.get_working(), 'status': queue.getstatus()})
        return ret
