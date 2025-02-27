import unittest
from lapinpy.apps import queue
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestQueue(unittest.TestCase):

    @patch.object(queue, 'restful')
    def setUp(self, restserver):
        self.config = Mock()
        self.queue_manager = Mock()
        self.server = Mock()
        self.server.queueManager = self.queue_manager
        restserver.RestServer.Instance.return_value = self.server
        self.queue = queue.QueueApp(self.config)

    @patch.object(queue, 'restful')
    def test_Queue_finishLoading(self, restserver):
        restserver.RestServer.Instance.return_value = self.server

        self.queue.finishLoading()

        self.assertEqual(self.queue.queueManager, self.server.queueManager)

    def test_Queue_post_task(self):
        self.queue.queueManager = self.server.queue_manager

        self.queue.post_task(['foo'], {'bar': 'bar1', 'baz': 'baz1'})

        self.assertIn(call('foo'), self.server.queue_manager.get_queue.mock_calls)
        self.assertIn(call().add({'baz': 'baz1', 'bar': 'bar1'}), self.server.queue_manager.get_queue.mock_calls)


if __name__ == '__main__':
    unittest.main()
