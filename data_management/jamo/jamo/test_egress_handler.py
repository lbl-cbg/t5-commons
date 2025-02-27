import unittest
import egress_handler
import sdm_curl
from unittest.mock import patch, Mock, call, ANY, MagicMock
from parameterized import parameterized


class TestEgressHandler(unittest.TestCase):

    @parameterized.expand([
        ('file_exists', True, None, [],
         [call('api/tape/egress_request/1', egress_status_id=3, bytes_transferred=0)]),
        ('file_does_not_exist_success', False, 2000, [], [call('api/tape/egress_request/1', egress_status_id=2),
                                                          call('api/tape/egress_request/1',
                                                               egress_status_id=3, bytes_transferred=2000)]),
        ('file_does_not_exist_failure', False, 0, [], [call('api/tape/egress_request/1', egress_status_id=2),
                                                       call('api/tape/egress_request/1', egress_status_id=4)]),
        ('file_does_not_exist_existing_in_progress_request', False, 2000, [{'egress_status_id': 2}], []),
    ])
    @patch('egress_handler.sdm_logger')
    @patch('egress_handler.subprocess')
    @patch('egress_handler.os.makedirs')
    @patch('egress_handler.os.path.exists')
    def test_EgressHandler_run(self, _description, path_exists, rsync_file_size, egress_requests_response,
                               expected_curl_put_calls,
                               os_path_exists_mock, os_makedirs_mock, subprocess_mock, sdm_logger_mock):
        os_path_exists_mock.return_value = path_exists
        curl = sdm_curl.Curl('http://host/path')
        curl_get = MagicMock(name='get')
        curl.get = curl_get
        curl_put = MagicMock(name='put')
        curl.put = curl_put
        curl_get.side_effect = [
            {'dm_archive_root': '/path/to/remote/dm_archive',
             'remote_sources': {'my_source': {'dm_archive_root_source': '/path/to/local/dm_archive',
                                              'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                                              'rsync_password': 'rsync'}}},
            {"queue_status": {
                "0": "HOLD",
                "1": "REGISTERED",
                "2": "IN_PROGRESS",
                "3": "COMPLETE",
                "4": "FAILED",
                "5": "CALLBACK_FAILED",
                "6": "PREP_FAILED",
                "HOLD": 0,
                "REGISTERED": 1,
                "IN_PROGRESS": 2,
                "COMPLETE": 3,
                "FAILED": 4,
                "CALLBACK_FAILED": 5,
                "PREP_FAILED": 6
            }},
            [{'file_path': '/path/to/remote/dm_archive/data', 'file_name': 'my_file.txt', 'egress_id': 1}],
            egress_requests_response,
        ]
        rsync_results = Mock()
        rsync_results.stdout.decode.return_value = f'total_transferred_file_size: {rsync_file_size}\n'
        subprocess_mock.run.side_effect = [rsync_results]
        handler = egress_handler.EgressHandler(curl, 'my_source')

        handler.run()

        for c in expected_curl_put_calls:
            self.assertIn(c, curl_put.mock_calls)

    def test_EgressHandler_get_config(self):
        curl_mock = Mock()
        curl_mock.get.return_value = {
            'dm_archive_root': '/path/to/local/dm_archive',
            'remote_sources': {'my_source': {'dm_archive_root_source': '/path/to/local/dm_archive',
                                             'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                                             'rsync_password': 'rsync'}}}

        self.assertEqual(('/path/to/local/dm_archive', {'dm_archive_root_source': '/path/to/local/dm_archive',
                                                        'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                                                        'rsync_password': 'rsync'}),
                         egress_handler.EgressHandler._get_config('my_source', curl_mock))

    @parameterized.expand([
        ('missing_remote_sources', {}),
        ('missing_remote_config', {'remote_sources': {}}),
        ('missing_remote_config_rsync_uri',
         {'remote_sources': {'my_source': {'dm_archive_root_source': '/path/to/dm_archive', 'rsync_password': 'rsync'}}}),
        ('missing_remote_config_rsync_pass',
         {'remote_sources': {
             'my_source': {'dm_archive_root_source': '/path/to/dm_archive',
                           'rsync_uri': 'rsync://user@rsync_uri/dm_archive'}}}),
        ('missing_remote_config_dm_archive_root_source',
         {'remote_sources': {
             'my_source': {'rsync_uri': 'rsync://user@rsync_uri/dm_archive', 'rsync_password': 'rsync'}}}),
    ])
    def test_EgressHandler_get_config_missing_configuration_raises_RemoteSourceConfigurationException(self,
                                                                                                      _description,
                                                                                                      config):
        curl_mock = Mock()
        curl_mock.get.return_value = config

        self.assertRaises(egress_handler.RemoteSourceConfigurationException,
                          egress_handler.EgressHandler._get_config, 'my_source', curl_mock)

    @patch('egress_handler.subprocess')
    def test_EgressHandler_rsync(self, subprocess_mock):
        rsync_results = Mock()
        rsync_results.stdout.decode.return_value = 'total_transferred_file_size: 2000\n'
        subprocess_mock.run.side_effect = [rsync_results]

        self.assertEqual({'total_transferred_file_size': 2000},
                         egress_handler.EgressHandler._rsync('path/to/remote/my_file.txt', '/path/to/local/my_file.txt',
                                                             'rsync://user@rsync_uri/dm_archive', 'rsync_password'))
        self.assertIn(call.run(
            ['rsync', '-rtL', '--chmod=ugo=rwX', '--no-h', '--stats',
             'rsync://user@rsync_uri/dm_archive/path/to/remote/my_file.txt', '/path/to/local/my_file.txt'],
            env={'RSYNC_PASSWORD': 'rsync_password'}, stdout=ANY, check=True),
            subprocess_mock.mock_calls)


if __name__ == '__main__':
    unittest.main()
