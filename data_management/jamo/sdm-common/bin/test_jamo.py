import unittest
from jamo import JAMO
from unittest.mock import patch, call
from parameterized import parameterized


class TestJamo(unittest.TestCase):

    def setUp(self):
        self.__init()

    @patch('jamo.sdm_curl')
    def __init(self, sdm_curl_mock):
        sdm_curl_mock.Curl.return_value = sdm_curl_mock
        self.curl = sdm_curl_mock
        self.jamo = JAMO()

    @parameterized.expand([
        ('source', ['-s', 'my_source', 'id', 'my_id'],
         call.post('api/tape/grouprestore', files=['my_id'], days=90, requestor='foobar', source='my_source')),
        ('no_source', ['id', 'my_id'],
         call.post('api/tape/grouprestore', files=['my_id'], days=90, requestor='foobar', source=None)),
    ])
    @patch('jamo.getpass.getuser')
    def test_JAMO_fetch(self, _description, args, expected_curl_call, get_user_mock):
        self.curl.post.side_effect = [
            {'records': [{'file_path': '/path/to', 'file_name': 'my_file.txt', '_id': 'my_id',
                          'file_status': 'BACKUP_COMPLETE'}],
             'cursor_id': 'cursor_id', 'record_count': 1}, {}]
        get_user_mock.return_value = 'foobar'

        self.jamo.fetch(args)

        self.assertIn(expected_curl_call, self.curl.mock_calls)

    @parameterized.expand([
        ('dm_archive_root_replace_current_location_in_file', 'my_source',
         {'_id': 'my_id', 'current_location': '/path/to/nersc/dm_archive/my_file.txt'},
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source='my_source'),
         call('/path/to/my_source/dm_archive/my_file.txt', '/path/to/destination/my_file.txt')),
        ('dm_archive_root_not_found_current_location_in_file', 'other_source',
         {'_id': 'my_id', 'current_location': '/path/to/nersc/dm_archive/my_file.txt'},
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source='other_source'),
         call('/path/to/nersc/dm_archive/my_file.txt', '/path/to/destination/my_file.txt')),
        ('dm_archive_root_replace_current_location_not_in_file', 'my_source',
         {'_id': 'my_id', 'file_path': '/path/to/nersc/dm_archive', 'file_name': 'my_file.txt'},
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source='my_source'),
         call('/path/to/my_source/dm_archive/my_file.txt', '/path/to/destination/my_file.txt')),
        ('dm_archive_root_not_found_current_location_not_in_file', 'other_source',
         {'_id': 'my_id', 'file_path': '/path/to/nersc/dm_archive', 'file_name': 'my_file.txt'},
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source='other_source'),
         call('/path/to/nersc/dm_archive/my_file.txt', '/path/to/destination/my_file.txt')),
    ])
    @patch('jamo.os.symlink')
    @patch('jamo.getpass.getuser')
    def test_JAMO_link_single(self, _description, source, file, get_dm_archive_roots_return_value,
                              expected_curl_call, expected_symlink_call, get_user_mock, symlink_mock):
        self.curl.get.side_effect = [get_dm_archive_roots_return_value]
        self.curl.post.side_effect = [{}]
        get_user_mock.return_value = 'foobar'

        self.jamo.link_single(file, '/path/to/destination/my_file.txt', source=source)

        self.assertIn(expected_curl_call, self.curl.mock_calls)
        self.assertIn(expected_symlink_call, symlink_mock.mock_calls)

    @parameterized.expand([
        ('source', ['-s', 'my_source', 'id', 'my_id'],
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source='my_source'),
         call('/path/to/my_source/dm_archive/my_file.txt', 'my_id.my_file.txt')),
        ('no_source', ['id', 'my_id'],
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         call.post('api/tape/grouprestore', files=['my_id'], requestor='foobar', source=None),
         call('/path/to/nersc/dm_archive/my_file.txt', 'my_id.my_file.txt')),
    ])
    @patch('jamo.os.symlink')
    @patch('jamo.getpass.getuser')
    def test_JAMO_link(self, _description, args, get_dm_archive_roots_return_value, expected_curl_call,
                       expected_symlink_call, get_user_mock, symlink_mock):
        self.curl.post.side_effect = [
            {'records': [{'file_path': '/path/to/nersc/dm_archive', 'file_name': 'my_file.txt', '_id': 'my_id',
                          'file_status': 'BACKUP_COMPLETE'}],
             'cursor_id': 'cursor_id', 'record_count': 1}, {}, {}]
        self.curl.get.side_effect = [get_dm_archive_roots_return_value]
        get_user_mock.return_value = 'foobar'

        self.jamo.link(args)

        self.assertIn(expected_curl_call, self.curl.mock_calls)
        self.assertIn(expected_symlink_call, symlink_mock.mock_calls)

    @parameterized.expand([
        ('source_found', 'my_source',
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         '/path/to/nersc/dm_archive/my_file.txt', '/path/to/my_source/dm_archive/my_file.txt'),
        ('source_not_found', 'my_source',
         {'nersc': '/path/to/nersc/dm_archive', 'other_source': '/path/to/other_source/dm_archive'},
         '/path/to/nersc/dm_archive/my_file.txt', '/path/to/nersc/dm_archive/my_file.txt'),
        ('no_source', None,
         {'nersc': '/path/to/nersc/dm_archive', 'my_source': '/path/to/my_source/dm_archive'},
         '/path/to/nersc/dm_archive/my_file.txt', '/path/to/nersc/dm_archive/my_file.txt'),
    ])
    def test_JAMO_replace_nersc_dm_archive_root(self, _description, source, curl_get_value, nersc_path, expected_path):
        self.curl.get.return_value = curl_get_value

        self.assertEqual(self.jamo._replace_nersc_dm_archive_root(source, nersc_path), expected_path)


if __name__ == '__main__':
    unittest.main()
