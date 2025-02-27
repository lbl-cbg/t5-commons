import unittest
from lapinpy.jqueue.queueui import QueueUI
import struct
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import Mock, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import Mock, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestQueueUI(unittest.TestCase):

    def setUp(self):
        self.queue_manager = Mock()
        self.queueui = QueueUI(self.queue_manager)

    def test_QueueUI_post_clearqueue(self):
        self.queueui.post_clearqueue(None, {'queue': 'foo'})

        self.assertIn(call.get_queue('foo', False), self.queue_manager.mock_calls)
        self.assertIn(call.get_queue().set_status('Normal'), self.queue_manager.mock_calls)

    def test_QueueUI_post_pausequeue(self):
        self.queueui.post_pausequeue(None, {'queue': 'foo'})

        self.assertIn(call.get_queue('foo', False), self.queue_manager.mock_calls)
        self.assertIn(call.get_queue().set_status('Paused'), self.queue_manager.mock_calls)

    def test_QueueUI_get_queuestatus(self):
        self.queue_manager.status.return_value = {'status': 'ok'}

        self.assertEqual(self.queueui.get_queuestatus(None, None), {'status': 'ok'})

        self.assertIn(call.status(), self.queue_manager.mock_calls)

    @unittest.skip("Method doesn't work")
    def test_QueueUI_put_queuestatus(self):
        queue = Mock()
        self.queue_manager.get_queue.return_value = queue

        self.queueui.put_queuestatus(['foo'], {'status': 'Paused'})

        self.assertEqual(queue.status, 'Paused')

    def test_QueueUI_post_requeue(self):
        queue_id = 1234
        time = 20220715
        file_loc = 10
        file_offset = 1
        tid = struct.pack('>I', queue_id) + struct.pack('>I', time) + struct.pack('>I', file_loc << 3 | file_offset)
        self.queueui.post_requeue(None, {'tid': tid})

        self.queue_manager.requeue.assert_called()

    def test_QueueUI_get_queueerrors(self):
        queue = Mock()
        queue.get_error_range.return_value = {'error': 'Failed'}
        self.queue_manager.get_queue.return_value = queue

        self.assertEqual(self.queueui.get_queueerrors(['foo', 'bar'], None), {'error': 'Failed'})

        self.assertIn(call.get_queue('foo/bar', False), self.queue_manager.mock_calls)
        self.assertIn(call.get_error_range(50), queue.mock_calls)

    def test_QueueUI_get_get_queuetasks(self):
        queue = Mock()
        queue.get_range.return_value = {'tasks': ['foo', 'bar']}
        self.queue_manager.get_queue.return_value = queue

        self.assertEqual(self.queueui.get_queuetasks(['foo', 'bar'], None), {'tasks': ['foo', 'bar']})

        self.assertIn(call.get_queue('foo/bar', False), self.queue_manager.mock_calls)
        self.assertIn(call.get_range(50), queue.mock_calls)


if __name__ == '__main__':
    unittest.main()
