import datetime
import random
import string
import threading
from lapinpy import sdmlogger
from collections import deque

# Number of priority queues (+1, there is a zero priority, which is meant for manual intervention to move something to the top)
QUEUES = 8

#  Queues
#  0 - top priority, run before everything
#  1 - internal short, no delay
#  2 - external short, runs with short queue
#  3 - internal medium, runs with short queue
#  4 - external medium, runs with long queue
#  5 - internal huge, runs with long queue
#  6 - external huge, runs with long queue
#  7 - manual low priority, runs with long queue
#  8 - manual low priority, not picked up (useful for putting work that will be picked up if a volume is selected, but otherwise is held)


def contains_list(list1, list2):
    for item in list1:
        if item not in list2:
            # print 'item %s is not in list %s'%(item, str(list2))
            return False
    return True


class Queue:
    # Constructor should set the size, priority
    def __init__(self, task_name, priority, queue=None, default_features=[], task_selected=None, get_task_features=None,
                 on_lost=None, on_fail=None, on_finish=None):
        self.logger = sdmlogger.getLogger('task')
        # feature_queues is a list of the distinct set of features that a queue has (e.g., 'hsi_1,dna_w', 'hsi_2,dna_w')
        self.feature_queues = {}
        self.default_features = default_features
        self.priority = priority
        self.name = task_name
        self.on_task_selected = task_selected
        self.get_task_feature = get_task_features
        # round_robin_queues is a list of the distinct set of features that a queue has (e.g., 'hsi_1,dna_w', 'hsi_2,dna_w')
        # and a deque of all the tasks that belong to that distinct set of features
        self.round_robin_queues = []
        self.round_robin_on = 0
        self.record_count = 0
        self.file_size = 0
        self.on_lost = on_lost
        self.on_fail = on_fail
        self.on_finish = on_finish
        self.currently_running = 0
        self._lock = threading.Lock()
        feature_set = default_features
        if queue is not None:
            for record in queue:
                if get_task_features is not None:
                    feature_set = get_task_features(record)
                self.add(record, feature_set)

    def add_all(self, records, featurez=[], add_default_features=True):
        """Adds record tasks to the queue.

        :param list[dict] records: Records to add to queue
        :param list[str] featurez: Features to be associated with the tasks added
        :param bool add_default_features: Whether to associate default features (as defined by the queue) to the added tasks
        """
        for record in records:
            self.add(record, featurez, add_default_features)

    def add(self, record, featurez=[], add_default_features=True):
        """Add a record task to the queue.

        :param dict record: Dictionary for the record and should contain values for the following keys:
            ['file_size']: (int) Size of the file (optional)
        :param list[str] featurez: Features to be associated with the tasks added
        :param bool add_default_features: Whether to associate default features (as defined by the queue) to the added tasks
        """
        features = featurez[:]
        if add_default_features:
            for feature in self.default_features:
                if feature not in features:
                    features.append(feature)
        feature_str = ','.join(features)
        if feature_str in self.feature_queues:
            self.feature_queues.get(feature_str).append(record)
        else:
            self.feature_queues[feature_str] = deque([record])
            self.round_robin_queues.append(feature_str)
        with self._lock:
            self.record_count += 1
            if 'file_size' in record:
                self.file_size += record['file_size']

    def next(self, available_features):
        for i in range(len(self.round_robin_queues)):
            getting = (self.round_robin_on + i) % len(self.round_robin_queues)
            feature_str = self.round_robin_queues[getting]
            features = feature_str.split(',')
            if len(self.feature_queues.get(feature_str)) > 0 and contains_list(features, available_features):
                self.round_robin_on = getting + 1
                data = self.feature_queues.get(feature_str).popleft()
                with self._lock:
                    self.record_count -= 1
                    if 'file_size' in data:
                        self.file_size -= data['file_size']
                if self.on_task_selected is not None:
                    ret = self.on_task_selected(data)
                    if ret is not None:
                        if len(ret) > 0:
                            data = ret
                    else:
                        return self.next(available_features)
                with self._lock:
                    self.currently_running += 1
                return {'uses_resources': features, 'data': data}

    def lost(self, data):
        with self._lock:
            self.currently_running -= 1
        if self.on_lost is not None:
            self.on_lost(data)

    def failed(self, data):
        with self._lock:
            self.currently_running -= 1
        if self.on_fail is not None:
            self.on_fail(data)

    def finished(self, data):
        with self._lock:
            self.currently_running -= 1
        if self.on_finish is not None:
            self.on_finish(data)

    def reset(self):
        with self._lock:
            self.record_count = 0
            self.file_size = 0
            for feature_str in self.feature_queues:
                self.record_count += len(self.feature_queues.get(feature_str))
                self.file_size += sum([x['file_size'] for x in self.feature_queues.get(feature_str) if 'file_size' in x])
            if self.record_count == 0:
                self.currently_running = 0

    def get_current_count(self):
        return self.currently_running

    def get_size(self):
        return self.record_count

    def get_file_size(self):
        return self.file_size

    def get_status(self):
        return {'record_count': self.get_size(), 'file_size': self.get_file_size(), 'currently_running': self.get_current_count()}


class TaskManager:
    def __init__(self, division_name: str, max_resources: dict[str, int] = {}):
        self.queues = []
        self.task_cache = {}
        self.division_name = division_name
        self.max_resources = max_resources
        self.robin_tasks = {}
        self.current_tasks = {}
        self.current_resource_counts = {}
        self.services = {}
        self.task_prefix = ''.join(random.sample(string.ascii_uppercase + string.ascii_lowercase + string.digits, 8))
        self.on_task = 0
        self.task_name_to_queue = {}
        self._lock = threading.Lock()

    def set_queues(self, *queues):
        self.queues = []
        # sort the queues by provided priority
        q = {}
        for queue in queues:
            key = f'{queue.priority:3}{queue.name}'
            q[key] = queue
        for key, value in sorted(q.items()):
            self.queues.append(value)
        for queue in queues:
            self.task_name_to_queue[queue.name] = queue

    def reset(self):
        for queue in self.queues:
            queue.reset()

    def get_status(self):
        ret = {}
        for task in self.queues:
            ret[task.name] = task.get_status()
        return {'tasks': ret, 'current_used_resources': self.current_resource_counts,
                'current_tasks': self.current_tasks, 'services': self.services}

    def get_short_status(self):
        ret = {}
        for task in self.queues:
            ret[task.name] = {'record_count': task.get_size(), 'file_size': task.get_file_size(),
                              'currently_running': task.get_current_count()}
        return ret

    def add_service(self, service_id, threads, host):
        self.services[str(service_id)] = {'started': datetime.datetime.now(), 'threads': threads, 'host': host}

    def heartbeat(self, service):
        service = str(service)
        now = datetime.datetime.now()
        if service not in self.services:
            self.services[service] = {'started': now}
        self.services[service]['heartbeat'] = now
        return self.get_short_status()

    def monitor_lost_tasks(self):
        # Lost dt services
        now = datetime.datetime.now()
        lost_services = []
        for service_id in self.services:
            service = self.services.get(service_id)
            if 'heartbeat' in service and (now - service['heartbeat']).total_seconds() >= 600:
                lost_services.append(service_id)
        for task_id, task in self.current_tasks.items():
            if str(task['service']) in lost_services:
                self.task_name_to_queue[str(task['task'])].lost(task['data'])

        for service_id in lost_services:
            del self.services[service_id]

    def set_task_complete(self, task_id, ret):
        if task_id in self.current_tasks:
            task = self.current_tasks.get(task_id)
            del self.current_tasks[task_id]
            if not ret:
                self.task_name_to_queue[task['task']].failed(task['data'])
            else:
                self.task_name_to_queue[task['task']].finished(task['data'])

            for feature in task['features']:
                if feature in self.current_resource_counts:
                    with self._lock:
                        self.current_resource_counts[feature] -= 1

    def get_task(self, has_resources, has_tasks, previous_task, service_id, ret):
        if previous_task is not None:
            self.set_task_complete(previous_task, ret)
        useable_resources = []
        for resource in has_resources:
            if resource not in self.max_resources or resource not in self.current_resource_counts or self.current_resource_counts.get(resource) < self.max_resources.get(resource):
                useable_resources.append(resource)
        feature_string = ','.join(has_tasks)
        # create a cache of callable tasks for this task configuration
        if feature_string not in self.task_cache:
            subset = []
            levelset = []
            cur_priority = None
            # create an array of an array of round robin tasks.
            # position 0 will be a round robin of all the top level tasks,
            # position 1 will be a round robin of the next level tasks, etc
            for queue in self.queues:
                if queue.name not in has_tasks:
                    continue
                if cur_priority != queue.priority:
                    cur_priority = queue.priority
                    if levelset:
                        subset.append(levelset)
                        levelset = []
                levelset.append(queue)
            if levelset:
                subset.append(levelset)
            on = [0] * len(subset)
            self.task_cache[feature_string] = subset
            self.robin_tasks[feature_string] = on
        else:
            subset = self.task_cache.get(feature_string)
            on = self.robin_tasks.get(feature_string)

        # Step through the hierarchy of priorities
        for priority in range(len(subset)):
            # then round robin the list of tasks at this priority level
            for i in range(len(subset[priority])):
                getting = (on[priority] + i) % len(subset[priority])
                queue = subset[priority][getting]
                if queue.get_size() > 0:
                    self.robin_tasks[feature_string][priority] = getting + 1
                    task = queue.next(useable_resources)
                    if task is not None:
                        self.on_task += 1
                        task_id = self.task_prefix + str(self.on_task)
                        task_data = {'task': queue.name, 'data': task['data'], 'task_id': task_id,
                                     'features': task['uses_resources'], 'service': service_id,
                                     'created': datetime.datetime.now(), 'division': self.division_name}
                        if queue.name == 'pull':
                            task_data['records'] = len(task['data'])
                        for resource in task['uses_resources']:
                            if resource in self.current_resource_counts:
                                with self._lock:
                                    self.current_resource_counts[resource] += 1
                            else:
                                with self._lock:
                                    self.current_resource_counts[resource] = 1
                        self.current_tasks[task_id] = task_data
                        return task_data
