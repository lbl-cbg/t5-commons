import unittest
import struct
from .tid import TaskObjectId
from parameterized import parameterized


class TestTid(unittest.TestCase):

    def setUp(self):
        queue_id = 1234
        time = 20220715
        file_loc = 10
        file_offset = 1
        self.toid = struct.pack('>I', queue_id) + struct.pack('>I', time) + struct.pack('>I', file_loc << 3 | file_offset)
        self.tid = TaskObjectId(toid=self.toid)

    def test_TaskObjectId_set_task_info(self):
        tid = TaskObjectId()

        tid.set_task_info(1234, 10, 1)

        self.assertEqual(tid.get_file_loc(), 10)
        self.assertEqual(tid.get_queue_ident(), 1234)
        self.assertEqual(tid.get_file_offset(), 1)

    @parameterized.expand([
        ('get_file_loc', 10),
        ('get_file_offset', 1),
        ('get_time', 20220715),
        ('get_queue_ident', 1234),
    ])
    def test_TaskObjectId_get_methods(self, method, expected):
        self.assertEqual(getattr(self.tid, method)(), expected)


if __name__ == '__main__':
    unittest.main()
