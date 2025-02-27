import os.path
import unittest
from .queuefile import QueueFile, MAX_FILE_SIZE, QUEUED, IN_PROGRESS, INVALID
from bson import BSON
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestQueueFile(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        self.queue_file = QueueFile('{}/20200713.w'.format(self.temp_dir.name))

    def tearDown(self):
        try:
            self.queue_file.delete()
        except OSError:
            pass
        self.temp_dir.cleanup()

    def test_QueueFile_can_add_true(self):
        self.assertTrue(self.queue_file.can_add('foobar'))

    def test_QueueFile_can_add_false(self):
        self.queue_file.file_size = MAX_FILE_SIZE

        self.assertFalse(self.queue_file.can_add('foobar'))

    def test_QueueFile_add(self):
        tid = self.queue_file.add(BSON.encode({'foo': 'bar'}))

        self.assertEqual(self.queue_file.queued, 1)
        self.assertEqual(self.queue_file.get(tid), {'foo': 'bar'})

    @parameterized.expand([
        ('queued', QUEUED),
        ('in_progress', IN_PROGRESS),
        ('invalid', INVALID),
    ])
    def test_QueueFile_get(self, _description, status):
        tid = self.queue_file.add(BSON.encode({'foo': 'bar'}))

        self.assertEqual(self.queue_file.get(tid, status), {'foo': 'bar'})

    def test_QueueFile_get_range(self):
        self.queue_file.add(BSON.encode({'foo': 'bar'}))

        tid = self.queue_file.get_range(1)[0]

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_QueueFile_next(self):
        self.queue_file.add(BSON.encode({'foo': 'bar'}))

        tid = next(self.queue_file)

        self.assertEqual(tid.get('data'), {'foo': 'bar'})

    def test_QueueFile_delete(self):
        self.queue_file.delete()

        # Verify file deleted
        self.assertFalse(os.path.exists(self.queue_file.file_path))

    def test_QueueFile_eq(self):
        self.assertTrue(self.queue_file == QueueFile('{}/20200713.w'.format(self.temp_dir.name)))

    def test_QueueFile_neq(self):
        self.assertTrue(self.queue_file != QueueFile('{}/20200713.w'.format(self.temp_dir.name), creation_time=1))

    def test_QueueFile_lt(self):
        self.assertTrue(self.queue_file < QueueFile('{}/20200713.w'.format(self.temp_dir.name), creation_time=1))

    def test_QueueFile_le(self):
        self.assertTrue(self.queue_file <= QueueFile('{}/20200713.w'.format(self.temp_dir.name), creation_time=1))

    def test_QueueFile_gt(self):
        self.assertTrue(QueueFile('{}/20200713.w'.format(self.temp_dir.name), creation_time=1) > self.queue_file)

    def test_QueueFile_ge(self):
        self.assertTrue(QueueFile('{}/20200713.w'.format(self.temp_dir.name), creation_time=1) >= self.queue_file)

    def test_QueueFile_hash(self):
        self.assertEqual(hash(self.queue_file),
                         hash('{}_{}'.format(self.queue_file.creation_time, self.queue_file.offset)))


if __name__ == '__main__':
    unittest.main()
