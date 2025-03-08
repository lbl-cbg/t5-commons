import unittest
from lapinpy.jqueue.queue import Queue, states
import os
import struct
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestQueue(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        os.mkdir('{}/foo'.format(self.temp_dir.name))
        with open('{}/foo/20220712.w'.format(self.temp_dir.name), 'wb') as f:
            f.write(struct.pack('>4I', 6, 2, 3, 16))
        with open('{}/foo/20220712.e'.format(self.temp_dir.name), 'wb') as f:
            f.write(struct.pack('>4I', 2, 1, 1, 16))
        self.queue = Queue(name='foo', basedir=self.temp_dir.name)

    def tearDown(self):
        for f in self.queue.working_files + self.queue.error_files:
            f.delete()
        self.temp_dir.cleanup()

    def test_Queue_get_failed(self):
        self.assertEqual(self.queue.get_failed(), 1)

    def test_Queue_get_queued(self):
        self.assertEqual(self.queue.get_queued(), 2)

    def test_Queue_get_total(self):
        self.assertEqual(self.queue.get_total(), 6)

    def test_Queue_get_invalid(self):
        self.assertEqual(self.queue.get_invalid(), 3)

    def test_Queue_get_working(self):
        self.assertEqual(self.queue.get_working(), 1)

    def test_Queue_write_setting(self):
        self.queue.write_setting('foo', 'bar')
        with open(self.queue.settings_file) as f:
            self.assertEqual(f.readline().strip(), 'foo: bar')

    def test_Queue_new_file(self):
        queue_file = self.queue.new_file('txt')

        self.assertTrue(os.path.exists(queue_file.file_path))

    def test_Queue_add(self):
        # Remove any existing working files
        self.queue.working_files = []

        tid = self.queue.add({'foo': 'bar'}).get('tid')

        self.assertEqual(tid.get_file_loc(), 16)
        self.assertEqual(len(self.queue.working_files), 1)

    def test_Queue_add_error(self):
        # Remove any existing error files
        self.queue.error_files = []

        tid = self.queue.add_error({'foo': 'bar'})

        self.assertEqual(tid.get_file_loc(), 16)
        self.assertEqual(len(self.queue.error_files), 1)

    def test_Queue_get(self):
        tid = self.queue.add({'foo': 'bar'}).get('tid')

        entry = self.queue.get(tid)

        self.assertEqual(entry, {'foo': 'bar'})

    def test_Queue_get_error_range(self):
        self.queue.add_error({'foo': 'bar'})

        tid = self.queue.get_error_range(1)[0]

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_Queue_get_range(self):
        self.queue.add({'foo': 'bar'}).get('tid')

        tid = self.queue.get_range(1)[0]

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_Queue_get_file(self):
        tid = self.queue.add({'foo': 'bar'}).get('tid')

        queue_file = self.queue.get_file(tid)

        self.assertEqual(queue_file.file_path, '{}/foo/20220712.w'.format(self.temp_dir.name))

    def test_Queue_invalidate(self):
        # Remove any existing working files
        self.queue.working_files = []
        tid = self.queue.add({'foo': 'bar'}).get('tid')
        queue_file = self.queue.working_files[0]

        record = self.queue.invalidate(tid)

        self.assertEqual(record, {'foo': 'bar'})
        # Queue file has been deleted
        self.assertFalse(os.path.exists(queue_file.file_path))

    def test_Queue_requeue(self):
        tid = self.queue.add_error({'foo': 'bar', 'record': {'bar': 'foo'}})

        requeued_tid = self.queue.requeue(tid).get('tid')

        self.assertEqual(self.queue.get(requeued_tid), {u'bar': u'foo'})

    def test_Queue_fail(self):
        tid = self.queue.add({'foo': 'bar'}).get('tid')

        self.queue.fail(tid, 'Failed')

        # Originally only 1 failured
        self.assertEqual(self.queue.get_failed(), 2)

    def test_Queue_next(self):
        self.queue.add({'foo': 'bar'})

        tid = next(self.queue)

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_Queue_status(self):
        self.assertEqual(states.get('Normal'), self.queue.status)

    def test_Queue_set_status(self):
        self.queue.set_status('Paused')

        self.assertEqual(states.get('Paused'), self.queue.status)

    def test_Queue_set_status_invalid_state(self):
        self.assertRaises(Exception, self.queue.status, 'foo')

    @parameterized.expand([
        (True, {'resources': ['foo']}, 'foo'),
        (False, {'resources': ['foo']}, 'bar'),
    ])
    def test_Queue_can_work(self, expected, resources, resource):
        self.queue.settings = resources

        self.assertEqual(self.queue.can_work(resource), expected)


if __name__ == '__main__':
    unittest.main()
