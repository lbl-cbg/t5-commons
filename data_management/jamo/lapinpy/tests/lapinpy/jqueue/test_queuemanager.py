import unittest
import os
import struct
from lapinpy.jqueue.queuemanager import QueueList, QueueManager
from lapinpy.jqueue.queue import Queue
from lapinpy.jqueue.tid import TaskObjectId
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestQueueManager(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        os.mkdir('{}/foo'.format(self.temp_dir.name))
        with open('{}/foo/20220712.w'.format(self.temp_dir.name), 'wb') as f:
            f.write(struct.pack('>4I', 6, 2, 3, 16))
        with open('{}/foo/settings'.format(self.temp_dir.name), 'w') as f:
            f.write('resources:\n  - foo\n')
            # f.write('queue_id: 1730737365\n')
        with open('{}/foo/20220712.e'.format(self.temp_dir.name), 'wb') as f:
            f.write(struct.pack('>4I', 2, 1, 1, 16))
        self.queue = Queue(name='foo', basedir=self.temp_dir.name)
        self.queue_list = QueueList(resources=[])
        self.queue_manager = QueueManager(name='foo', base=self.temp_dir.name)

    def tearDown(self):
        def delete_queue(queue):
            for f in queue.working_files + queue.error_files:
                try:
                    f.delete()
                except OSError:
                    pass

        delete_queue(self.queue)
        for qm in self.queue_manager.r_map.values():
            for queue in qm.li:
                delete_queue(queue)
        for q in self.queue_manager.queues.values():
            delete_queue(q)
        self.temp_dir.cleanup()

    def test_QueueManager_generate_id_no_collision(self):
        self.assertEqual(self.queue_manager._QueueManager__generate_id('bar'), abs(hash('foo.bar')) >> 32)

    def test_QueueManager_generate_id_collision(self):
        queue_id = abs(hash('foo.bar')) >> 32
        self.queue_manager.queues[str(queue_id)] = self.queue

        self.assertEqual(self.queue_manager._QueueManager__generate_id('bar'), queue_id + 1)

    def test_QueueList_add(self):
        self.queue_list.add(self.queue)

        self.assertIn(self.queue, self.queue_list.li)

    def test_QueueList_remove(self):
        self.queue_list.add(self.queue)

        self.queue_list.remove(self.queue)

        self.assertNotIn(self.queue, self.queue_list.li)

    def test_QueueList_next(self):
        self.queue.add({'foo': 'bar'})

        self.queue_list.add(self.queue)

        tid = next(self.queue_list)

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_QueueManager_get_queue_existing(self):
        queue = self.queue_manager.get_queue('{}/foo'.format(self.temp_dir.name))

        self.assertEqual(queue.working_files, self.queue.working_files)

    def test_QueueManager_get_queue_new(self):
        queue = self.queue_manager.get_queue('{}/bar'.format(self.temp_dir.name))

        self.assertNotEqual(queue.working_files, self.queue.working_files)

    def test_QueueManager_get_queue_create_new_disabled(self):
        self.assertRaises(Exception, self.queue_manager.get_queue, 'bar', create_new=False)

    def test_QueueManager_get_queue_from_tid(self):
        tid = TaskObjectId(c_time=20220712)
        queue_id, queue = list(self.queue_manager.queues.items())[0]
        working_file = queue.working_files[0]
        tid.set_task_info(int(queue_id), working_file.first_queued_record, working_file.offset)

        queue = self.queue_manager.get_queue_from_tid(tid)

        self.assertEqual(queue, list(self.queue_manager.queues.values())[0])

    def test_QueueManager_add_to_resource_list(self):
        queue_list = QueueList(['foo'])
        self.queue_manager.r_map = {'foo': queue_list}
        self.queue_manager.k_r_map = {'foo': ['foo']}

        self.queue_manager.add_to_resource_list(self.queue)

        self.assertIn(self.queue, queue_list.li)

    def test_QueueManager_next(self):
        queue = self.queue_manager.get_queue('{}/foo'.format(self.temp_dir.name))

        queue.add({'foo': 'bar'})

        self.assertEqual(self.queue_manager.next(['foo']).get('data'), {'foo': 'bar'})

    def test_QueueManager_fail(self):
        tid = TaskObjectId(c_time=20220712)
        queue_id, queue = list(self.queue_manager.queues.items())[0]
        working_file = queue.working_files[0]
        tid.set_task_info(int(queue_id), working_file.first_queued_record, working_file.offset)
        queue.add({'foo': 'bar'})

        self.queue_manager.fail(tid, 'Error')

        self.assertEqual(self.queue_manager.failure_count.get(tid.get_queue_ident()), 1)
        self.assertEqual(len(queue.error_files), 1)

    def test_QueueManager_finished(self):
        tid = TaskObjectId(c_time=20220712)
        queue_id, queue = list(self.queue_manager.queues.items())[0]
        working_file = queue.working_files[0]
        tid.set_task_info(int(queue_id), working_file.first_queued_record, working_file.offset)
        queue.add({'foo': 'bar'})

        record = self.queue_manager.finished(tid)

        self.assertEqual(record, {'foo': 'bar'})

    def test_QueueManager_requeue(self):
        tid = TaskObjectId(c_time=20220712)
        queue_id, queue = list(self.queue_manager.queues.items())[0]
        working_file = queue.working_files[0]
        tid.set_task_info(int(queue_id), working_file.first_queued_record, working_file.offset)
        queue.add_error({'record': {'foo': 'bar'}})

        tid = self.queue_manager.requeue(tid).get('tid')

        self.assertEqual(queue.get(tid), {'foo': 'bar'})

    def test_QueueManager_status(self):
        status = self.queue_manager.status()

        self.assertEqual(status, [{'queue': '{}/foo'.format(self.temp_dir.name), 'failed': 1, 'in_progress': 1, 'status': 'Normal', 'queued': 2}])


if __name__ == '__main__':
    unittest.main()
