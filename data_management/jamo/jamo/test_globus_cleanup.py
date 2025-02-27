import unittest
import globus_cleanup
import subprocess
from unittest.mock import patch, Mock, call, mock_open
from parameterized import parameterized


class TestGlobusCleanup(unittest.TestCase):

    def setUp(self):
        self.__initialize()

    @patch('globus_cleanup.sdm_logger')
    def __initialize(self, sdm_logger_mock):
        self.smd_logger_mock = sdm_logger_mock
        self.curl_mock = Mock()
        self.curl_mock.get.return_value = {
            'backup_services': {'my_backup_service': {'source_path': '/path/to/source',
                                                      'destination_endpoint': 'globus-endpoint'}}}
        self.globus_cleanup = globus_cleanup.GlobusCleanup(self.curl_mock, 'my_backup_service')

    @patch('globus_cleanup.subprocess.run')
    @patch('globus_cleanup.os.walk')
    @patch('globus_cleanup.os.remove')
    def test_GlobusCleanup_run(self, os_remove_mock, os_walk_mock, subprocess_run_mock):
        os_walk_mock.return_value = [('/path/to/source', ['dir1', 'dir2'], ['root_file_1', 'root_file_2']),
                                     ('/path/to/source/dir1', [], ['dir1_file_1', 'dir1_file_2']),
                                     ('/path/to/source/dir2', [], [])]
        jgi_globus_timer_results = Mock()
        jgi_globus_timer_results.stdout.decode.side_effect = [
            'root_file_1\n',
            'dir1/dir1_file_1\n',
        ]
        subprocess_run_mock.return_value = jgi_globus_timer_results

        self.globus_cleanup.run()

        # `root_file_2` and `dir1/dir1_file_2` are not in the Globus endpoint, so they should not be deleted
        self.assertEqual([call('/path/to/source/root_file_1'), call('/path/to/source/dir1/dir1_file_1')],
                         os_remove_mock.mock_calls)

    @patch('globus_cleanup.subprocess.run')
    @patch('globus_cleanup.os.walk')
    def test_GlobusCleanup_run_globus_error_not_found_ignores_error(self, os_walk_mock, subprocess_run_mock):
        os_walk_mock.return_value = [('/path/to/source', ['dir1', 'dir2'], ['root_file_1', 'root_file_2'])]
        subprocess_run_mock.side_effect = [subprocess.CalledProcessError(1, 'my_cmd', stderr=b'ClientError.NotFound')]

        self.globus_cleanup.run()

        self.assertIn(call(['jgi_globus_timer.sh', 'ls', '--level', '0', 'globus-endpoint', ''], stdout=-1, stderr=-1,
                           check=True), subprocess_run_mock.mock_calls)

    @patch('globus_cleanup.subprocess.run')
    @patch('globus_cleanup.os.walk')
    def test_GlobusCleanup_run_globus_error_propagates_error(self, os_walk_mock, subprocess_run_mock):
        os_walk_mock.return_value = [('/path/to/source', ['dir1', 'dir2'], ['root_file_1', 'root_file_2'])]
        subprocess_run_mock.side_effect = [subprocess.CalledProcessError(1, 'my_cmd', stderr=b'Error')]

        self.assertRaises(subprocess.CalledProcessError, self.globus_cleanup.run)

        self.assertIn(call(['jgi_globus_timer.sh', 'ls', '--level', '0', 'globus-endpoint', ''], stdout=-1, stderr=-1,
                           check=True), subprocess_run_mock.mock_calls)

    def test_GlobusCleanup_get_config(self):
        curl_mock = Mock()
        curl_mock.get.return_value = {
            'backup_services': {'my_backup_service': {'source_path': '/path/to/source',
                                                      'destination_endpoint': 'globus-endpoint'}}}

        self.assertEqual({'source_path': '/path/to/source',
                                         'destination_endpoint': 'globus-endpoint'},
                         globus_cleanup.GlobusCleanup._get_config('my_backup_service', curl_mock))

    @parameterized.expand([
        ('missing_backup_services', {}),
        ('missing_backup_service_config', {'backup_services': {}}),
        ('missing_backup_service_config_source_path',
         {'backup_services': {'my_backup_service': {'destination_endpoint': 'globus-endpoint'}}}),
        ('missing_backup_service_config_destination_endpoint',
         {'backup_services': {'my_backup_service': {'source_path': '/path/to/source'}}}),
    ])
    def test_GlobusCleanup_get_config_missing_configuration_raises_BackupServiceConfigurationException(self,
                                                                                                       _description,
                                                                                                       config):
        curl_mock = Mock()
        curl_mock.get.return_value = config

        self.assertRaises(globus_cleanup.BackupServiceConfigurationException,
                          globus_cleanup.GlobusCleanup._get_config, 'my_backup_service', curl_mock)

    @patch('globus_cleanup.Curl')
    @patch('globus_cleanup.GlobusCleanup')
    @patch('builtins.open',
           new_callable=mock_open, read_data='backup_service_name: my_service\njamo_token: my_token\nhost: some_host')
    def test_main(self, open_mock, globus_cleanup_mock, curl_mock):
        args = ['', '-c', 'my_config.config']
        curl_mock.return_value = curl_mock

        with patch('sys.argv', args):
            globus_cleanup.main()

        self.assertIn(call(curl_mock, 'my_service'), globus_cleanup_mock.mock_calls)
        self.assertIn(call('some_host', appToken='my_token', errorsToRetry=[524]), curl_mock.mock_calls)


if __name__ == '__main__':
    unittest.main()
