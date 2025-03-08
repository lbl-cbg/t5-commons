import unittest
import task
from task import Queue, TaskManager
from parameterized import parameterized
from datetime import datetime
from unittest.mock import patch


class TestTask(unittest.TestCase):

    @parameterized.expand([
        ('contains_list', ['a', 'b'], ['b', 'c', 'a'], True),
        ('does_not_contain_list', ['a', 'b'], ['b', 'c', 'd'], False),
    ])
    def test_contains_list(self, _description, sublist, full_list, expected):
        self.assertEqual(task.contains_list(sublist, full_list), expected)

    @parameterized.expand([
        ('add_default_features', True, 'foo,bar,foobar'),
        ('no_add_default_features', False, 'foo,bar'),
    ])
    def test_Queue_add(self, _description, add_default_features, features):
        queue = Queue('foobar', 0, default_features=['foobar'])

        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'], add_default_features=add_default_features)

        self.assertIn({'name': 'foobar', 'file_size': 100}, queue.feature_queues.get(features))
        self.assertEqual(len(queue.round_robin_queues), 1)
        self.assertEqual(queue.record_count, 1)
        self.assertEqual(queue.file_size, 100)

    @parameterized.expand([
        ('add_default_features', True, 'foo,bar,foobar'),
        ('no_add_default_features', False, 'foo,bar'),
    ])
    def test_Queue_add_all(self, _description, add_default_features, features):
        queue = Queue('foobar', 0, default_features=['foobar'])

        queue.add_all([{'name': 'foo', 'file_size': 100}, {'name': 'bar', 'file_size': 200}], ['foo', 'bar'],
                      add_default_features=add_default_features)

        self.assertIn({'name': 'foo', 'file_size': 100}, queue.feature_queues.get(features))
        self.assertIn({'name': 'bar', 'file_size': 200}, queue.feature_queues.get(features))
        self.assertEqual(len(queue.round_robin_queues), 1)
        self.assertEqual(queue.record_count, 2)
        self.assertEqual(queue.file_size, 300)

    def test_Queue_next(self):
        queue = Queue('foobar', 0)
        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'])

        self.assertEqual(queue.next(['foo', 'bar', 'baz']),
                         {'uses_resources': ['foo', 'bar'], 'data': {'name': 'foobar', 'file_size': 100}})
        self.assertEqual(queue.record_count, 0)
        self.assertEqual(queue.currently_running, 1)

    def test_Queue_next_with_onTaskSelected(self):
        queue = Queue('foobar', 0, task_selected=lambda x: {'name': x.get('name')})
        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'])

        self.assertEqual(queue.next(['foo', 'bar', 'baz']),
                         {'uses_resources': ['foo', 'bar'], 'data': {'name': 'foobar'}})
        self.assertEqual(queue.record_count, 0)
        self.assertEqual(queue.currently_running, 1)

    def test_Queue_lost(self):
        def on_lost(_data):
            count[0] -= 1
        count = [1]
        queue = Queue('foobar', 0, on_lost=on_lost)
        queue.currently_running = 1

        queue.lost({'name': 'foobar'})

        self.assertEqual(count[0], 0)
        self.assertEqual(queue.currently_running, 0)

    def test_Queue_failed(self):
        def on_fail(_data):
            count[0] -= 1
        count = [1]
        queue = Queue('foobar', 0, on_fail=on_fail)
        queue.currently_running = 1

        queue.failed({'name': 'foobar'})

        self.assertEqual(count[0], 0)
        self.assertEqual(queue.currently_running, 0)

    def test_Queue_finished(self):
        def on_finish(_data):
            count[0] -= 1
        count = [1]
        queue = Queue('foobar', 0, on_finish=on_finish)
        queue.currently_running = 1

        queue.finished({'name': 'foobar'})

        self.assertEqual(count[0], 0)
        self.assertEqual(queue.currently_running, 0)

    def test_Queue_reset(self):
        queue = Queue('foobar', 0)
        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'])
        queue.record_count = 0
        queue.file_size = 0

        queue.reset()

        self.assertEqual(queue.record_count, 1)
        self.assertEqual(queue.file_size, 100)

    def test_Queue_get_current_count(self):
        queue = Queue('foobar', 0)
        queue.currently_running = 1

        self.assertEqual(queue.get_current_count(), 1)

    def test_Queue_get_size(self):
        queue = Queue('foobar', 0)
        queue.record_count = 1

        self.assertEqual(queue.get_size(), 1)

    def test_Queue_get_file_size(self):
        queue = Queue('foobar', 0)
        queue.file_size = 100

        self.assertEqual(queue.get_file_size(), 100)

    def test_Queue_get_status(self):
        queue = Queue('foobar', 0)
        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'])

        self.assertEqual(queue.get_status(), {'currently_running': 0, 'record_count': 1, 'file_size': 100})

    def test_TaskManager_set_queues(self):
        task_manager = TaskManager('jgi')
        queue_1 = Queue('foo', 1)
        queue_2 = Queue('bar', 0)
        queue_3 = Queue('baz', 3)

        task_manager.set_queues(queue_1, queue_2, queue_3)

        self.assertListEqual(task_manager.queues, [queue_2, queue_1, queue_3])
        for queue in queue_1, queue_2, queue_3:
            self.assertEqual(task_manager.task_name_to_queue.get(queue.name), queue)

    def test_TaskManager_reset(self):
        task_manager = TaskManager('jgi')
        queue = Queue('foobar', 0)
        queue.add({'name': 'foobar', 'file_size': 100}, ['foo', 'bar'])
        queue.record_count = 0
        queue.file_size = 0
        task_manager.set_queues(queue)

        task_manager.reset()

        self.assertEqual(queue.record_count, 1)
        self.assertEqual(queue.file_size, 100)

    @patch('task.datetime')
    def test_TaskManager_get_status(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        queue = Queue('foobar', 0)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.add(
            {'file_id': 11479549, 'transaction_id': 1, 'file_name': 'bar.txt', 'file_path': '/path/to',
             'origin_file_name': 'bar.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132795,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'bar', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.next(['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)
        expected = {'current_tasks': {},
                    'current_used_resources': {},
                    'services': {},
                    'tasks': {'foobar': {'currently_running': 1,
                                         'file_size': 1132795,
                                         'record_count': 1}}}

        self.assertEqual(task_manager.get_status(), expected)

    def test_TaskManager_get_short_status(self):
        queue = Queue('foobar', 0)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.add(
            {'file_id': 11479549, 'transaction_id': 1, 'file_name': 'bar.txt', 'file_path': '/path/to',
             'origin_file_name': 'bar.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132795,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'bar', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.next(['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)

        self.assertEqual(task_manager.get_short_status(),
                         {'foobar': {'currently_running': 1, 'file_size': 1132795, 'record_count': 1}})

    @patch('task.datetime')
    def test_TaskManager_add_service(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        task_manager = TaskManager('jgi')

        task_manager.add_service('service_id', 5, 'service_host')

        self.assertEqual(task_manager.services.get('service_id'), {'host': 'service_host',
                                                                   'started': datetime(2000, 1, 2, 3, 4, 5),
                                                                   'threads': 5})

    @patch('task.datetime')
    def test_TaskManager_heartbeat(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        queue = Queue('foobar', 0)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.add(
            {'file_id': 11479549, 'transaction_id': 1, 'file_name': 'bar.txt', 'file_path': '/path/to',
             'origin_file_name': 'bar.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132795,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'bar', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
            ['foo', 'bar'])
        queue.next(['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)

        self.assertEqual(task_manager.heartbeat('service_id'),
                         {'foobar': {'currently_running': 1, 'file_size': 1132795, 'record_count': 1}})
        self.assertEqual(task_manager.services.get('service_id'), {'started': datetime(2000, 1, 2, 3, 4, 5),
                                                                   'heartbeat': datetime(2000, 1, 2, 3, 4, 5)})

    @patch('task.datetime')
    def test_TaskManager_monitor_lost_tasks(self, datetime_mock):
        def on_lost(data):
            on_lost_result.append(data)

        on_lost_result = []
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        queue = Queue('foobar', 0, on_lost=on_lost)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0, 'service': 'service_id'},
            ['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)
        task_manager.add_service('service_id', 5, 'service_host')
        task_manager.services.get('service_id')['heartbeat'] = datetime(2000, 1, 2, 1, 4, 5)
        task_manager.current_tasks = {'task_id': {'service': 'service_id', 'task': 'foobar', 'data': {'name': 'foobar'}}}

        task_manager.monitor_lost_tasks()

        self.assertNotIn('service_id', task_manager.services)
        self.assertIn({'name': 'foobar'}, on_lost_result)

    @parameterized.expand([
        ('failed', False),
        ('finished', True),
    ])
    def test_TaskManager_set_task_complete(self, _description, finished):
        def on_fail(data):
            fail.append(data)

        def on_finish(data):
            finish.append(data)

        fail = []
        finish = []
        queue = Queue('foobar', 0, on_fail=on_fail, on_finish=on_finish)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0, 'service': 'service_id'},
            ['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)
        task_manager.add_service('service_id', 5, 'service_host')
        task_manager.current_tasks = {'task_id': {'service': 'service_id', 'task': 'foobar', 'data': {'name': 'foobar'},
                                                  'features': ['foo']}}
        task_manager.current_resource_counts = {'foo': 1}

        task_manager.set_task_complete('task_id', finished)

        self.assertEqual(task_manager.current_resource_counts.get('foo'), 0)
        self.assertNotIn('task_id', task_manager.current_tasks)
        if finished:
            self.assertIn({'name': 'foobar'}, finish)
        else:
            self.assertIn({'name': 'foobar'}, fail)

    @patch('task.datetime')
    def test_TaskManager_get_task(self, datetime_mock):
        def on_finish(data):
            finish.append(data)
        finish = []
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        queue = Queue('copy', 0, on_finish=on_finish)
        queue.add(
            {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
             'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
             'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
             'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
             'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
             'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
             'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
             'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0, 'service': 'service_id'},
            ['foo', 'bar'])
        task_manager = TaskManager('jgi')
        task_manager.set_queues(queue)
        task_manager.add_service('service_id', 5, 'service_host')
        task_manager.current_tasks = {'task_id': {'service': 'service_id', 'task': 'copy', 'data': {'name': 'foobar'},
                                                  'features': ['foo']}}
        task_manager.current_resource_counts = {'foo': 1}
        task_manager.task_prefix = 'AAA'
        expected = {'created': datetime(2000, 1, 2, 3, 4, 5),
                    'data': {'auto_uncompress': 0,
                             'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                             'file_date': datetime(2020, 12, 1, 12, 27, 27),
                             'file_group': 'foobar',
                             'file_id': 11479548,
                             'file_name': 'foo.txt',
                             'file_owner': 'foo',
                             'file_path': '/path/to',
                             'file_permissions': '0100644',
                             'file_size': 1132794,
                             'file_status_id': 1,
                             'local_purge_days': 2,
                             'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                             'metadata_id': '5eecdfb86263bf2148833b40',
                             'modified_dt': datetime(2021, 6, 7, 6, 11, 46),
                             'origin_file_name': 'foo.txt',
                             'origin_file_path': '/path/to/origin',
                             'remote_purge_days': None,
                             'service': 'service_id',
                             'transaction_id': 1,
                             'transfer_mode': 0,
                             'user_save_till': datetime(2021, 6, 21, 6, 11, 46),
                             'validate_mode': 0},
                    'features': ['foo', 'bar'],
                    'service': 'service_id',
                    'division': 'jgi',
                    'task': 'copy',
                    'task_id': 'AAA1'}

        self.assertEqual(task_manager.get_task(['foo', 'bar'], ['copy'], 'task_id', 'service_id', True),
                         expected)
        self.assertIn({'name': 'foobar'}, finish)
        self.assertEqual(task_manager.current_resource_counts.get('foo'), 1)
        self.assertEqual(task_manager.current_resource_counts.get('bar'), 1)


if __name__ == '__main__':
    unittest.main()
