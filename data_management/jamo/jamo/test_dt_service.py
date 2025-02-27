import datetime
import os
import subprocess
import sys
import unittest
import dt_service
import sdm_curl
from parameterized import parameterized
from types import SimpleNamespace
from unittest.mock import patch, Mock, call, ANY, MagicMock, mock_open


class TestDTService(unittest.TestCase):

    def setUp(self):
        self.__init()

    @patch('dt_service.multiprocessing.Process')
    @patch('dt_service.sdm_logger')
    @patch('dt_service.hsi.HSI_status')
    def __init(self, hsi_status_mock, logger_mock, process_mock):
        self.logger = logger_mock
        self.curl = sdm_curl.Curl('http://host/path')
        self.curl_get = MagicMock(name='get')
        self.curl.get = self.curl_get
        self.curl_post = MagicMock(name='post')
        self.curl.post = self.curl_post
        self.curl_put = MagicMock(name='put')
        self.curl.put = self.curl_put
        self.hsi_status = Mock()
        hsi_status_mock.return_value = self.hsi_status
        self.curl_get.side_effect = [
            {'division': {
                'jgi': {'default_backup_service': 1,
                        'tape_temp_dir': '/path/to/temp'}}},
            {
                "file_status": {
                    "1": "REGISTERED",
                    "2": "COPY_READY",
                    "3": "COPY_IN_PROGRESS",
                    "4": "COPY_COMPLETE",
                    "5": "COPY_FAILED",
                    "6": "BACKUP_READY",
                    "7": "BACKUP_IN_PROGRESS",
                    "8": "BACKUP_COMPLETE",
                    "9": "BACKUP_FAILED",
                    "10": "PURGED",
                    "11": "DELETE",
                    "12": "RESTORE_IN_PROGRESS",
                    "13": "RESTORED",
                    "14": "TAR_READY",
                    "15": "TAR_IN_PROGRESS",
                    "16": "TAR_COMPLETE",
                    "17": "TAR_FAILED",
                    "18": "RECORDS_TO_FIX",
                    "19": "INGEST_STATS_COMPLETE",
                    "20": "INGEST_STATS_FAILED",
                    "21": "INGEST_FILE_MISSING",
                    "22": "INGEST_COMPLETE",
                    "23": "INGEST_FAILED",
                    "REGISTERED": 1,
                    "COPY_READY": 2,
                    "COPY_IN_PROGRESS": 3,
                    "COPY_COMPLETE": 4,
                    "COPY_FAILED": 5,
                    "BACKUP_READY": 6,
                    "BACKUP_IN_PROGRESS": 7,
                    "BACKUP_COMPLETE": 8,
                    "BACKUP_FAILED": 9,
                    "PURGED": 10,
                    "DELETE": 11,
                    "RESTORE_IN_PROGRESS": 12,
                    "RESTORED": 13,
                    "TAR_READY": 14,
                    "TAR_IN_PROGRESS": 15,
                    "TAR_COMPLETE": 16,
                    "TAR_FAILED": 17,
                    "RECORDS_TO_FIX": 18,
                    "INGEST_STATS_COMPLETE": 19,
                    "INGEST_STATS_FAILED": 20,
                    "INGEST_FILE_MISSING": 21,
                    "INGEST_COMPLETE": 22,
                    "INGEST_FAILED": 23,
                },
                "backup_record_status": {
                    "1": "REGISTERED",
                    "2": "TRANSFER_READY",
                    "3": "TRANSFER_IN_PROGRESS",
                    "4": "TRANSFER_COMPLETE",
                    "5": "TRANSFER_FAILED",
                    "6": "WAIT_FOR_TAPE",
                    "7": "ON_TAPE",
                    "8": "MD5_PREP",
                    "9": "MD5_IN_PROGRESS",
                    "10": "MD5_COMPLETE",
                    "11": "MD5_FAILED",
                    "12": "VALIDATION_COMPLETE",
                    "13": "VALIDATION_FAILED",
                    "14": "VALIDATION_READY",
                    "15": "VALIDATION_IN_PROGRESS",
                    "16": "HOLD",
                    "REGISTERED": 1,
                    "TRANSFER_READY": 2,
                    "TRANSFER_IN_PROGRESS": 3,
                    "TRANSFER_COMPLETE": 4,
                    "TRANSFER_FAILED": 5,
                    "WAIT_FOR_TAPE": 6,
                    "ON_TAPE": 7,
                    "MD5_PREP": 8,
                    "MD5_IN_PROGRESS": 9,
                    "MD5_COMPLETE": 10,
                    "MD5_FAILED": 11,
                    "VALIDATION_COMPLETE": 12,
                    "VALIDATION_FAILED": 13,
                    "VALIDATION_READY": 14,
                    "VALIDATION_IN_PROGRESS": 15,
                    "HOLD": 16
                },
                "queue_status": {
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
                }
            }
        ]
        dt_service.sdmCurl = self.curl
        self.dt_service = dt_service.DTService(self.curl, ['dna_w'], ['copy'], 1, False, 'my_service', 'jgi')
        self.curl_get.side_effect = None

    # Helper function for checking the presence of multiple mock calls
    def _assertAllIn(self, expected_calls, mock_object):
        for c in expected_calls:
            self.assertIn(c, mock_object.mock_calls)

    def test_ResourceLostException(self):
        exception = dt_service.ResourceLostException('my_resource', 'my_service', True)

        self.assertEqual(exception.resource, 'my_resource')
        self.assertEqual(exception.service, 'my_service')
        self.assertEqual(exception.globally, True)

    @patch('dt_service.HSI')
    @patch('dt_service.multiprocessing.Process.start')
    def test_HSIQueue_queue(self, hsi, process_start):
        logger = Mock()
        hsi_queue = dt_service.HSIQueue('hsi_server', logger)

        hsi_queue.queue('ls -al', {'foo': 'bar'})

        self.assertIn({'command': 'ls -al', 'info': {'foo': 'bar'}}, hsi_queue.commands)

    @patch('dt_service.HSI')
    def test_HSIQueue_run(self, hsi):
        logger = Mock()
        hsi_queue = dt_service.HSIQueue('hsi_server', logger)
        hsi_queue.commands.append({'command': 'ls -al', 'info': {'foo': 'bar'}})

        hsi_queue.run()

        self.assertIn({'foo': 'bar'}, hsi_queue.finished)

    def test_DTService_check_services(self):
        self.dt_service.hsi_list = ['foo', 'bar_123']
        self.dt_service.hsi_gone = {'foo': 'resource_id_foo'}
        self.dt_service.remote_services = {'resource_id_foo': {'server': 'some_server_foo'},
                                           123: {'server': 'some_server_bar'}}
        self.dt_service.features.append('bar_123')
        self.hsi_status.isup.side_effect = [True, False]

        self.dt_service.check_services()

        self.assertEqual(self.dt_service.hsi_gone, {'bar_123': 123})
        self.assertEqual(self.dt_service.features, ['dna_w', 'foo'])
        self._assertAllIn([call('api/tape/resourceonline', resource='foo', service_id='my_service'),
                           call('api/tape/resourceoffline', resource='bar_123', globally=True,
                                service_id='my_service')], self.curl_post)

    def test_DTService_to_folder_str(self):
        self.assertEqual(self.dt_service.to_folder_str(1234678987, 10, 2), '12/34/67/89/87')

    @patch('dt_service.multiprocessing.Process')
    def test_DTService_set_threads(self, process_mock):
        stop = Mock()
        stop.value = 1
        self.dt_service.stop = stop
        current_thread_count = Mock()
        current_thread_count.value = 1
        self.dt_service.current_thread_count = current_thread_count

        self.dt_service.set_threads(3)

        self.assertIn(call(target=ANY, args=(stop, current_thread_count)), process_mock.mock_calls)

    def test_DTService_runner(self):
        def func(*args, **kwargs):
            stop.value = 1
            return 'prev_task'

        stop = Mock()
        stop.value = 0
        thread_count = MagicMock()
        thread_count.value = 1
        self.curl_post.return_value = {'task_id': 'my_task_id', 'task': 'my_task', 'data': {'foo', 'bar'}}
        self.dt_service.task_runners['my_task'] = func

        self.dt_service.runner(stop, thread_count)

        self.assertIn(call('api/tape/taskcomplete', task_id='my_task_id', returned='prev_task', division='jgi'),
                      self.curl_put.mock_calls)
        self.assertEqual(thread_count.value, 1)

    def test_DTService_stop_threads(self):
        self.dt_service.stop_threads()

        self.assertEqual(self.dt_service.stop.value, 1)

    def test_DTService_get_service(self):
        self.curl_get.return_value = {2: 'my_service'}

        self.dt_service.get_service(2)

        self.assertEqual(self.dt_service.remote_services.get(2), {2: 'my_service'})

    @parameterized.expand([
        ('hpss_path_does_not_exist',
         [],
         [FileNotFoundError()],
         [{2: 'my_service', 'server': 'my_server', 'type': 'HPSS'}],
         [],
         [{}],
         {'service': 2, 'records': [{'file_path': '/path/to/', 'file_name': 'my_file.txt',
                                     'backup_record_id': 123, }]},
         False,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5})],
         [],
         [],
         [],
         ),
        ('hpss_path_exists',
         [],
         [SimpleNamespace(**{'st_size': 999})],
         [{2: 'my_service', 'server': 'my_server', 'default_path': '/path/to/service',
           'type': 'HPSS'}, {'file_id': 456, 'file_status_id': 8}],
         [],
         [{}, subprocess.CompletedProcess('cmd', 0, stdout=b'999 /path/to/temp/my_file.txt')],
         {'service': 2, 'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt',
                                     'backup_record_id': 123, 'file_id': 456}]},
         True,
         [call('api/tape/backuprecord/123',
               data={'backup_record_status_id': 4, 'remote_file_name': 'my_file.txt.123',
                     'remote_file_path': '/path/to/service_2022/path/to/temp',
                     'tar_record_id': None}),
          call('api/tape/savefile', file='/path/to/temp/my_file.txt', days=1)],
         [call.run(['hsi', '-h', 'my_server',
                    'put -p -P /path/to/temp/my_file.txt : /path/to/service_2022/path/to/temp/my_file.txt.123'], check=True)],
         [],
         [],
         ),
        ('hpss_failed_hsi',
         [True],
         [{}],
         [{2: 'my_service', 'server': 'my_server', 'default_path': '/path/to/service',
           'type': 'HPSS'},
          {'file_id': 456, 'file_status_id': 8}],
         [],
         [subprocess.CalledProcessError(1, 'cmd')],
         {'service': 2, 'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt',
                                     'backup_record_id': 123, 'file_id': 456}]},
         False,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5})],
         [call.run(['hsi', '-h', 'my_server',
                    'put -p -P /path/to/temp/my_file.txt : /path/to/service_2022/path/to/temp/my_file.txt.123'], check=True)],
         [],
         [],
         ),
        ('hpss_failed_size_validation',
         [True],
         [SimpleNamespace(**{'st_size': 999})],
         [{2: 'my_service', 'server': 'my_server', 'default_path': '/path/to/service',
           'type': 'HPSS'},
          {'file_id': 456, 'file_status_id': 8}],
         [],
         [{}, subprocess.CompletedProcess('cmd', 0, stdout=b'1000 /path/to/temp/my_file.txt')],
         {'service': 2, 'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt',
                                     'backup_record_id': 123, 'file_id': 456}]},
         False,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5})],
         [call.run(['hsi', '-h', 'my_server',
                    'put -p -P /path/to/temp/my_file.txt : /path/to/service_2022/path/to/temp/my_file.txt.123'], check=True)],
         [],
         [],
         ),
        ('hpss_multiple_file_records',
         [False, True],
         [FileNotFoundError(), SimpleNamespace(**{'st_size': 999})],
         [{2: 'my_service', 'server': 'my_server', 'type': 'HPSS', 'default_path': '/path/to/default'}],
         [{'tar_record_id': 111}],
         [{}, subprocess.CompletedProcess('cmd', 0, stdout=b'999 /path/to/temp/my_file_2.txt')],
         {'service': 2, 'root_dir': '/path/to/root',
          'records': [{'file_path': '/path/to/', 'file_name': 'my_file.txt',
                       'backup_record_id': 123, 'file_id': 456},
                      {'file_path': '/path/to/', 'file_name': 'my_file_2.txt' + ('A' * 90),
                       'backup_record_id': 321, 'file_id': 654}
                      ]},
         True,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5}),
          call('api/tape/tar/111', remote_path='/path/to/default_2022/000/000/111.tar'),
          call('api/tape/backuprecords', records=[
              {'backup_record_status_id': 4, 'remote_file_path': '.', 'tar_record_id': 111,
               'backup_record_id': 321,
               'remote_file_name': 'my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321'}])],
         [call.run(
             'cd /path/to/temp/111_tar; htar -P -h -H server=my_server -cf /path/to/default_2022/000/000/111.tar -T 10 .',
             shell=True, check=True)],
         [call.rmtree('/path/to/temp/111_tar')],
         [call.makedirs('/path/to/temp/111_tar'), call.unlink('/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321'),
          call.symlink('/path/to/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321')],
         ),
        ('hpss_multiple_file_records_failed_htar',
         [False, True, True],
         [FileNotFoundError(), SimpleNamespace(**{'st_size': 999}), SimpleNamespace(**{'st_size': 888})],
         [{2: 'my_service', 'server': 'my_server', 'type': 'HPSS', 'default_path': '/path/to/default'}],
         [{'tar_record_id': 111}],
         [subprocess.CalledProcessError(1, 'cmd')],
         {'service': 2, 'root_dir': '/path/to/root',
          'records': [{'file_path': '/path/to/', 'file_name': 'my_file.txt',
                       'backup_record_id': 123, 'file_id': 456},
                      {'file_path': '/path/to/', 'file_name': 'my_file_2.txt' + ('A' * 90),
                       'backup_record_id': 321, 'file_id': 654},
                      {'file_path': '/path/to/', 'file_name': 'my_file_3.txt' + ('A' * 90),
                       'backup_record_id': 987, 'file_id': 789}
                      ]},
         False,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5}),
          call('api/tape/tar/111', remote_path='/path/to/default_2022/000/000/111.tar'),
          call('api/tape/backuprecords', records=[{'backup_record_id': 321, 'backup_record_status_id': 5},
                                                  {'backup_record_id': 987, 'backup_record_status_id': 5}])],
         [call.run(
             'cd /path/to/temp/111_tar; htar -P -h -H server=my_server -cf /path/to/default_2022/000/000/111.tar -T 10 .',
             shell=True, check=True)],
         [],
         [call.makedirs('/path/to/temp/111_tar'), call.unlink(
             '/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321'),
          call.symlink(
              '/path/to/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
              '/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321')],
         ),
        ('hpss_multiple_file_records_failed_size_validation',
         [False, True, True],
         [FileNotFoundError(), SimpleNamespace(**{'st_size': 999}), SimpleNamespace(**{'st_size': 888})],
         [{2: 'my_service', 'server': 'my_server', 'type': 'HPSS', 'default_path': '/path/to/default'}],
         [{'tar_record_id': 111}],
         [{}, subprocess.CompletedProcess('cmd', 0, stdout=b'1000 /path/to/default_2022/000/000/111.tar')],
         {'service': 2, 'root_dir': '/path/to/root',
          'records': [{'file_path': '/path/to/', 'file_name': 'my_file.txt',
                       'backup_record_id': 123, 'file_id': 456},
                      {'file_path': '/path/to/', 'file_name': 'my_file_2.txt' + ('A' * 90),
                       'backup_record_id': 321, 'file_id': 654},
                      {'file_path': '/path/to/', 'file_name': 'my_file_3.txt' + ('A' * 90),
                       'backup_record_id': 987, 'file_id': 789}
                      ]},
         False,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5}),
          call('api/tape/tar/111', remote_path='/path/to/default_2022/000/000/111.tar'),
          call('api/tape/backuprecords', records=[{'backup_record_id': 321, 'backup_record_status_id': 5},
                                                  {'backup_record_id': 987, 'backup_record_status_id': 5}])],
         [call.run(
             'cd /path/to/temp/111_tar; htar -P -h -H server=my_server -cf /path/to/default_2022/000/000/111.tar -T 10 .',
             shell=True, check=True)],
         [],
         [call.makedirs('/path/to/temp/111_tar'), call.unlink(
             '/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321'),
          call.symlink(
              '/path/to/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
              '/path/to/temp/111_tar/my_file_2.txtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.321')],
         ),
        ('not_hpss_or_globus_not_supported',
         [],
         [],
         [{2: 'my_service', 'server': 'my_server', 'type': 'NOT_SUPPORTED',
           'default_path': '/path/to/default'}],
         [{'tar_record_id': 111}],
         [{}],
         {'service': 2, 'backup_record_id': 123},
         True,
         [call('api/tape/backuprecord/123', data={'backup_record_status_id': 5})],
         [],
         [],
         [],
         ),
    ])
    @patch('dt_service.datetime')
    @patch('dt_service.os')
    @patch('dt_service.subprocess')
    @patch('dt_service.shutil')
    def test_DTService_run_put(self, _description, path_exists_responses, os_stat_responses, curl_get_responses, curl_post_responses,
                               subprocess_run_responses, in_file, expected, expected_curl_put_calls,
                               expected_subprocess_calls, expected_shutil_calls, expected_os_calls,
                               shutil_mock, subprocess_mock, os_mock, datetime_mock):
        self.curl_get.side_effect = curl_get_responses
        self.curl_post.side_effect = curl_post_responses
        self.hsi_status.isup.return_value = True
        os_mock.path.exists.side_effect = path_exists_responses
        os_mock.stat.side_effect = os_stat_responses
        os_mock.path.join = os.path.join
        os_mock.path.split = os.path.split
        subprocess_mock.run.side_effect = subprocess_run_responses
        subprocess_mock.CalledProcessError = subprocess.CalledProcessError
        datetime_mock.datetime.today.return_value = datetime.datetime(2022, 2, 2)

        self.assertEqual(self.dt_service.run_put(in_file), expected)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)
        if expected_subprocess_calls:
            self._assertAllIn(expected_subprocess_calls, subprocess_mock)
        else:
            self.assertEqual(len(subprocess_mock.mock_calls) == 0, True)
        if expected_shutil_calls:
            self._assertAllIn(expected_shutil_calls, shutil_mock)
        else:
            self.assertEqual(len(shutil_mock.mock_calls) == 0, True)
        self._assertAllIn(expected_os_calls, os_mock)

    @parameterized.expand([
        ('file_does_not_exist',
         {'_file': '/path/to/file/my_file.txt', '_callback': 'file_ingest', 'file_ingest_id': 123},
         False,
         True,
         True,
         [{}],
         False,
         call('api/tape/file_ingest/123', data={'file_ingest_status_id': 21}),
         ),
        ('file_exists_file_owner_and_group_call_success',
         {'_file': '/path/to/file/my_file.txt', '_callback': 'file_ingest', 'file_ingest_id': 123},
         True,
         True,
         True,
         [{}],
         True,
         call('api/tape/file_ingest/123',
              data={'file_path': '/real/path/to/file', 'file_name': 'my_file.txt', 'file_owner': 'foobar',
                    'file_group': 'my_group', 'file_size': 1000, 'file_permissions': '0o600',
                    'file_date': datetime.datetime(2022, 11, 22, 14, 12, 34), '_is_folder': False, '_is_file': True,
                    'file_ingest_status_id': 19}),
         ),
        ('folder_exists_folder_owner_and_group_call_success',
         {'_file': '/path/to/folder', '_callback': 'file_ingest', 'file_ingest_id': 123},
         True,
         True,
         True,
         [{}],
         True,
         call('api/tape/file_ingest/123',
              data={'file_path': '/real/path/to', 'file_name': 'folder', 'file_owner': 'foobar',
                    'file_group': 'my_group', 'file_size': 1000, 'file_permissions': '0o600',
                    'file_date': datetime.datetime(2022, 11, 22, 14, 12, 34), '_is_folder': False, '_is_file': True,
                    'file_ingest_status_id': 19}),
         ),
        ('file_exists_file_owner_and_group_call_failure',
         {'_file': '/path/to/file/my_file.txt', '_callback': 'file_ingest', 'file_ingest_id': 123},
         True,
         True,
         False,
         [{}],
         True,
         call('api/tape/file_ingest/123',
              data={'file_path': '/real/path/to/file', 'file_name': 'my_file.txt', 'file_owner': 111, 'file_group': 222,
                    'file_size': 1000, 'file_permissions': '0o600',
                    'file_date': datetime.datetime(2022, 11, 22, 14, 12, 34), '_is_folder': False, '_is_file': True,
                    'file_ingest_status_id': 19}),
         ),
        ('file_exists_curl_post_failure',
         {'_file': '/path/to/file/my_file.txt', '_callback': 'file_ingest', 'file_ingest_id': 123},
         True,
         True,
         False,
         [Exception('Error'), {}],
         False,
         call('api/tape/file_ingest/123', data={'file_ingest_status_id': 20}),
         )
    ])
    @patch('dt_service.getgrgid')
    @patch('dt_service.getpwuid')
    @patch('dt_service.os')
    def test_DTService_run_ingest_info(self, _description, in_file, path_exists, is_file, getpwuid_getgrgid_success,
                                       curl_post_responses, expected, expected_curl_post_call,
                                       os_mock, getpwuid_mock, getgrgid_mock):
        self.curl_post.side_effect = curl_post_responses
        os_mock.path.realpath.return_value = f'/real{in_file.get("_file")}'
        os_mock.path.exists.return_value = path_exists
        os_mock.path.isdir.return_value = not is_file
        os_mock.path.isfile.return_value = is_file
        os_stat_value = Mock()
        os_stat_value.st_uid = 111
        os_stat_value.st_gid = 222
        os_stat_value.st_size = 1000
        os_stat_value.st_mode = 0o600
        os_stat_value.st_mtime = 1669155154
        os_mock.stat.return_value = os_stat_value
        os_mock.path.split = os.path.split
        if getpwuid_getgrgid_success:
            getpwuid_value = Mock()
            getpwuid_value.pw_name = 'foobar'
            getpwuid_mock.return_value = getpwuid_value
            getgrgid_mock.return_value = ['my_group']
        else:
            getpwuid_mock.side_effect = [Exception('getpwuid error')]
            getgrgid_mock.side_effect = [Exception('getgrgid error')]

        self.assertEqual(self.dt_service.run_ingest_info(in_file), expected)
        self.assertIn(expected_curl_post_call, self.curl_post.mock_calls)

    @parameterized.expand([
        ('success_no_cleanup', True, '/path/to/local', []),
        ('success_cleanup', True, '/path/to/temp', [call.remove('/path/to/temp/my_file.txt')]),
        ('failure', False, '/path/to/local', []),
    ])
    @patch('dt_service.os')
    @patch('dt_service.shutil')
    def test_DTService_run_copy_local(self, _description, success, origin_file_path, expected_os_calls, shutil_mock,
                                      os_mock):
        os_mock.path.join = os.path.join
        os_mock.path.exists.return_value = False
        os_mock.path.getsize.return_value = 2000
        shutil_mock.copyfile.side_effect = [''] if success else [Exception('Error')]
        self.dt_service.remote_sources = {
            'foo': {
                'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                'rsync_password': 'rsync',
                'path_prefix_source': '/path/to/remote',
                'path_prefix_destination': '/path/to/local',
            }
        }
        in_file = {'origin_file_path': origin_file_path, 'origin_file_name': 'my_file.txt',
                   'file_path': '/path/to/destination', 'file_name': 'my_file_copy.txt',
                   'file_size': 1000, 'file_id': 123,
                   'auto_uncompress': True, 'local_purge_days': 10}

        self.assertEqual(self.dt_service.run_copy(in_file), success)
        if success:
            os_mock_calls = [call.makedirs('/path/to/destination', 0o751),
                             call.rename('/path/to/destination/.my_file_copy.txt', '/path/to/destination/my_file_copy.txt'),
                             call.chmod('/path/to/destination/my_file_copy.txt', 0o640)]
            curl_put_mock_calls = [call('api/tape/file/123', data={'file_size': 2000}),
                                   call('api/tape/file/123', data={'file_status_id': 4})]
            shutil_mock_calls = [
                call.copyfile(f'{origin_file_path}/my_file.txt', '/path/to/destination/.my_file_copy.txt'),
                call.copystat(f'{origin_file_path}/my_file.txt', '/path/to/destination/my_file_copy.txt')]
        else:
            os_mock_calls = [call.path.exists('/path/to/destination'),
                             call.makedirs('/path/to/destination', 0o751)]
            curl_put_mock_calls = [call('api/tape/file/123', data={'file_status_id': 5})]
            shutil_mock_calls = [call.copyfile('/path/to/local/my_file.txt', '/path/to/destination/.my_file_copy.txt')]
        self._assertAllIn(os_mock_calls, os_mock)
        self._assertAllIn(curl_put_mock_calls, self.curl_put)
        self._assertAllIn(shutil_mock_calls, shutil_mock)
        self._assertAllIn(expected_os_calls, os_mock)

    @parameterized.expand([
        ('success_no_cleanup', True, '/path/to/remote', []),
        ('success_cleanup', True, '/path/to/remote/temp', [call.remove('/path/to/remote/temp/my_file.txt')]),
        ('failure', False, '/path/to/remote', []),
    ])
    @patch('dt_service.os')
    @patch('dt_service.subprocess')
    @patch('dt_service.TemporaryDirectory')
    def test_DTService_run_copy_remote(self, _description, success, origin_file_path, expected_os_calls, temp_dir_mock,
                                       subprocess_mock, os_mock):
        os_mock.path.join = os.path.join
        os_mock.path.dirname = os.path.dirname
        os_mock.path.basename.return_value = '/path/to/local/my_file_copy.txt'
        temp_dir_mock.return_value.__enter__.return_value = '/path/to/temp'
        rsync_results = Mock()
        rsync_results.stdout.decode.return_value = 'total_transferred_file_size: 2000\n'
        subprocess_mock.run.side_effect = [rsync_results] if success else [Exception('Error')]
        self.dt_service.remote_sources = {
            'foo': {
                'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                'rsync_password': 'rsync',
                'path_prefix_source': '/path/to/remote',
                'path_prefix_destination': '/path/to/local',
                'path_temp': '/path/to/remote/temp',
            }
        }

        in_file = {'origin_file_path': origin_file_path, 'origin_file_name': 'my_file.txt',
                   'file_path': '/path/to/local', 'file_name': 'my_file_copy.txt',
                   'file_size': 1000, 'file_id': 123,
                   'auto_uncompress': True, 'local_purge_days': 10}

        self.assertEqual(self.dt_service.run_copy(in_file), success)
        curl_put_calls = [call('api/tape/file/123', data={'file_size': 2000}),
                          call('api/tape/file/123', data={'file_status_id': 4})] if success else [
            call('api/tape/file/123', data={'file_status_id': 5})]
        self._assertAllIn(curl_put_calls, self.curl_put)
        self._assertAllIn(expected_os_calls, os_mock)

    @parameterized.expand([
        ('success',
         {'file_path': '/path/to/my_file.txt', 'md5_queue_id': 123},
         [{}],
         True,
         call('api/tape/md5/123', {'md5sum': 'MD5SUM', 'queue_status_id': 3}),
         ),
        ('failure',
         {'file_path': '/path/to/my_file.txt', 'md5_queue_id': 123},
         [subprocess.CalledProcessError(2, 'cmd'), {}],
         False,
         call('api/tape/md5/123', data={'queue_status_id': 4})
         ),
    ])
    @patch('dt_service.subprocess')
    def test_DTService_run_md5(self, _description, in_file, curl_put_responses, expected, expected_curl_put_call,
                               subprocess_mock):
        subprocess_mock.CalledProcessError = subprocess.CalledProcessError
        subprocess_run_return_value = Mock()
        subprocess_run_return_value.stdout = b'MD5SUM'
        subprocess_mock.run.return_value = subprocess_run_return_value
        self.curl_put.side_effect = curl_put_responses

        self.assertEqual(self.dt_service.run_md5(in_file), expected)
        self.assertIn(expected_curl_put_call, self.curl_put.mock_calls)

    @parameterized.expand([
        ('success',
         [{'pull_queue_id': 1,
           'remote_file_name': 'my_file_1.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000},
          {'pull_queue_id': 2,
           'remote_file_name': 'my_file_2.gz',
           'remote_file_path': '/path/to/remote',
           'remote_path': None,
           'service': 1,
           'tar_record_id': None},
          {'pull_queue_id': 3,
           'remote_file_name': 'my_file_3.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000}],
         True,
         b'FILE\t/path/to/remote/1000.tar\t51320295424\t51320295424\t427+1351567309546\tAU297200,AU297300\t12\t0\t1\t04/27/2013\t15:19:08\t09/12/2013\t10:04:51\nA:/home/f/foobar-> FILE\t/path/to/remote/my_file_2.gz\t41454833152\t41454833152\t5711+0\tAG457000\t5\t0\t1\t04/27/2013\t15:03:04\t09/12/2013\t10:04:3',
         True,
         [call('api/tape/pull/1', volume='AU2972', position_a='427', position_b='1351567309546',
               queue_status_id=1),
          call('api/tape/pull/2', volume='AG4570', position_a='5711', position_b='0',
               queue_status_id=1),
          call('api/tape/pull/3', volume='AU2972', position_a='427', position_b='1351567309546',
               queue_status_id=1)]),
        ('hsi_down',
         [{'pull_queue_id': 1,
           'remote_file_name': 'my_file_1.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000},
          {'pull_queue_id': 2,
           'remote_file_name': 'my_file_2.gz',
           'remote_file_path': '/path/to/remote',
           'remote_path': None,
           'service': 1,
           'tar_record_id': None},
          {'pull_queue_id': 3,
           'remote_file_name': 'my_file_3.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000}],
         False,
         None,
         False,
         [call('api/tape/pull/1', queue_status_id=1), call('api/tape/pull/2', queue_status_id=1),
          call('api/tape/pull/3', queue_status_id=1)]),
        ('some_files_not_found_on_tape',
         [{'pull_queue_id': 1,
           'remote_file_name': 'my_file_1.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000},
          {'pull_queue_id': 2,
           'remote_file_name': 'my_file_2.gz',
           'remote_file_path': '/path/to/remote',
           'remote_path': None,
           'service': 1,
           'tar_record_id': None}],
         True,
         b'FILE\t/path/to/remote/1000.tar\t51320295424\t51320295424\t427+1351567309546\tAU297200,AU297300\t12\t0\t1\t04/27/2013\t15:19:08\t09/12/2013\t10:04:51A:/home/e/edlee-> *** ls: No such file or directory [-2: HPSS_ENOENT]\n    /path/to/remote/my_file_2.gz',
         False,
         [call('api/tape/pull/1', volume='AU2972', position_a='427', position_b='1351567309546', queue_status_id=1),
          call('api/tape/pull/2', queue_status_id=6)]),
        ('subprocess_call_failed',
         [{'pull_queue_id': 1,
           'remote_file_name': 'my_file_1.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000},
          {'pull_queue_id': 2,
           'remote_file_name': 'my_file_2.gz',
           'remote_file_path': '/path/to/remote',
           'remote_path': None,
           'service': 1,
           'tar_record_id': None},
          {'pull_queue_id': 3,
           'remote_file_name': 'my_file_3.tar',
           'remote_file_path': '.',
           'remote_path': '/path/to/remote/1000.tar',
           'service': 1,
           'tar_record_id': 1000}],
         True,
         b'',
         False,
         [call('api/tape/pull/1', queue_status_id=6), call('api/tape/pull/2', queue_status_id=6),
          call('api/tape/pull/3', queue_status_id=6)]),
    ])
    @patch('dt_service.subprocess')
    @patch('dt_service.time')
    def test_DTService_run_prep_batch(self, _description, records, hsi_is_up, subprocess_return_value, expected,
                                      expected_curl_put_calls, time_mock, subprocess_mock):
        self.hsi_status.isup.return_value = hsi_is_up
        subprocess_mock.run.return_value = SimpleNamespace(stdout=subprocess_return_value)

        self.assertEqual(self.dt_service.run_prep_batch(records), expected)

        for c in expected_curl_put_calls:
            self.assertIn(c, self.curl_put.mock_calls)

    @parameterized.expand([
        ('pull_cmd_success',
         [{'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file.txt',
           'tar_record_id': 123, 'remote_path': '/path/to/remote',
           'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file.txt',
           'position_a': 100, 'position_b': 200, 'pull_queue_id': 111},
          {'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file_2.txt',
           'tar_record_id': 456, 'remote_path': '/path/to/remote',
           'remote_file_path': '/path/to/remote/my_file_2.txt', 'remote_file_name': 'my_file_2.txt',
           'position_a': 300, 'position_b': 400, 'pull_queue_id': 2},
          {'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file_3.txt',
           'tar_record_id': None, 'remote_path': '/path/to/remote',
           'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file_3.txt',
           'position_a': 500, 'position_b': 600, 'pull_queue_id': 333},
          ],
         [{2: 'my_service', 'server': 'some_server'}],
         [{}],
         False,
         1,
         [call('api/tape/pull/111', queue_status_id=3),
          call('api/tape/pull/2', queue_status_id=4),
          call('api/tape/pull/333', queue_status_id=3),
          call('api/tape/releaselockedvolume/jgi/my_volume')],
         [call.makedirs('/path/to/temp/tape_my_volume_20220202_000000'),
          call.makedirs('/path/to', 489),
          call.rename('/path/to/.my_file.txt', '/path/to/my_file.txt'),
          call.rename('/path/to/.my_file_3.txt', '/path/to/my_file_3.txt')],
         [call.copy2('/path/to/temp/tape_my_volume_20220202_000000/path/to/remote/my_file.txt',
                     '/path/to/.my_file.txt'),
          call.copy2('/path/to/temp/tape_my_volume_20220202_000000/path/to/remote/my_file_2.txt',
                     '/path/to/.my_file_2.txt'),
          call.rmtree('/path/to/temp/tape_my_volume_20220202_000000')],
         [call().write('get /path/to/temp/tape_my_volume_20220202_000000/remote : /path/to/remote\n'),
          call().write('get /path/to/.my_file_3.txt : /path/to/remote/my_file_3.txt\n')],
         ),
        ('pull_cmd_failure',
         [{'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file.txt',
           'tar_record_id': 123, 'remote_path': '/path/to/remote',
           'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file.txt',
           'position_a': 100, 'position_b': 200, 'pull_queue_id': 111}],
         [{2: 'my_service', 'server': 'some_server'}],
         [Exception('Error')],
         False,
         0,
         [call('api/tape/pull/111', queue_status_id=4), call('api/tape/releaselockedvolume/jgi/my_volume')],
         [call.makedirs('/path/to/temp/tape_my_volume_20220202_000000'), call.makedirs('/path/to', 489)],
         [call.rmtree('/path/to/temp/tape_my_volume_20220202_000000')],
         [call().write('get /path/to/temp/tape_my_volume_20220202_000000/remote : /path/to/remote\n')],
         ),
    ])
    @patch('dt_service.shutil')
    @patch('dt_service.subprocess')
    @patch('builtins.open' if sys.version_info[0] >= 3 else '__builtin__.open', new_callable=mock_open)
    @patch('dt_service.datetime')
    @patch('dt_service.os')
    def test_DTService_run_pull(self, _description, files, curl_get_responses, subprocess_responses, path_exists,
                                expected, expected_curl_put_calls, expected_os_calls, expected_shutil_calls,
                                expected_file_write_calls, os_mock, datetime_mock, open_mock, subprocess_mock,
                                shutil_mock):
        self.curl_get.side_effect = curl_get_responses
        self.hsi_status.isup.return_value = True
        os_mock.path.join = os.path.join
        os_mock.getcwd.return_value = '/path/to/current/dir'
        os_mock.path.exists.return_value = path_exists
        os_mock.path.basename = os.path.basename
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 2, 2)
        shutil_mock.copy2.side_effect = [{}, Exception('Error'), {}]
        subprocess_mock.run.side_effect = subprocess_responses

        self.assertEqual(self.dt_service.run_pull(files), expected)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)
        self._assertAllIn(expected_os_calls, os_mock)
        self._assertAllIn(expected_shutil_calls, shutil_mock)
        self.assertIn(expected_file_write_calls, open_mock.mock_calls)

    def test_DTService_run_pull_hsi_down_reset_records(self):
        files = [{'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file_1.txt',
                  'tar_record_id': 123, 'remote_path': '/path/to/remote',
                  'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file_1.txt',
                  'position_a': 100, 'position_b': 200, 'pull_queue_id': 111},
                 {'service': 2, 'volume': 'my_volume', 'file_path': '/path/to', 'file_name': 'my_file_2.txt',
                  'tar_record_id': 123, 'remote_path': '/path/to/remote',
                  'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file_2.txt',
                  'position_a': 100, 'position_b': 200, 'pull_queue_id': 222}
                 ]
        expected_curl_put_calls = [call('api/tape/releaselockedvolume/jgi/my_volume'),
                                   call('api/tape/pull/111', queue_status_id=1),
                                   call('api/tape/pull/222', queue_status_id=1)]

        self.curl_get.side_effect = [{2: 'my_service', 'server': 'some_server'}]
        self.hsi_status.isup.return_value = False

        self.dt_service.run_pull(files)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)

    def test_DTService_add_extracted_file(self):
        in_file = {'service': 2, 'volume': 'my_volume'}
        info = {'file_type': 'txt', 'metadata': {'bar': 'bar1'}, 'path': 'my_file_copy.txt'}
        metadata_record = {'validate_mode': 0, 'local_purge_days': 1, 'metadata': {'foo': 'foo1'}, 'user': 'foobar',
                           'origin_file_name': 'my_file.txt', 'file_path': '/path/to', 'file_name': 'my_file.txt'}
        self.curl_post.return_value = {'metadata_id': 123}

        self.assertEqual(self.dt_service.add_extracted_file(in_file, info, metadata_record), 123)
        self.assertIn(call('api/metadata/file', data={'file': {'service': 2, 'volume': 'my_volume'}, 'file_type': 'txt', 'validate_mode': 0, 'local_purge_days': 1, 'metadata': {'foo': 'foo1', 'bar': 'bar1'}, 'user': 'foobar', 'destination': '/path/to/my_file.txt/my_file_copy.txt'}),
                      self.curl_post.mock_calls)

    def test_DTService_get_relative_link(self):
        link_loc = '/path/to/links/link'
        file_loc = '/path/to/files/file'
        expected = '../files/file'

        self.assertEqual(self.dt_service.get_relative_link(link_loc, file_loc), expected)

    @parameterized.expand([
        ('dir', [True], call('api/tape/task/123', task_status_is=3), [], [call.rmtree({'service': 2, 'volume': 'my_volume'})]),
        ('file', [False], call('api/tape/task/123', task_status_is=3), [call.unlink({'service': 2, 'volume': 'my_volume'})], []),
        ('error', [Exception('Error')], call('api/tape/task/123', task_status_is=4), [], [])
    ])
    @patch('dt_service.shutil')
    @patch('dt_service.os')
    def test_DTService_remove_file(self, _description, path_is_dir_response, expected_curl_put_call, expected_os_calls,
                                   expected_shutil_calls, os_mock, shutil_mock):
        record = {'file': {'service': 2, 'volume': 'my_volume'}, 'task_id': 123}
        os_mock.path.isdir.side_effect = path_is_dir_response

        self.dt_service.remove_file(record)

        self.assertIn(expected_curl_put_call, self.curl_put.mock_calls)
        self._assertAllIn(expected_os_calls, os_mock)
        self._assertAllIn(expected_shutil_calls, shutil_mock)

    @parameterized.expand([
        ('copy_tar_local',
         {'file_name': 'my_file', 'file_id': 123,
          'origin_file_name': 'my_folder.tar', 'origin_file_path': '/path/to/origin',
          'local_purge_days': 1, 'extract': [{'path': 'my_folder_2/my_file_3.txt',
                                              'file_type': 'txt',
                                              'metadata': {'foo': 'foo1'}}],
          'index': 1, 'ignore': ['bar'],
          'validate_mode': 0, 'metadata': {}, 'user': 'foobar',
          'file_path': '/path/to/destination', 'path': '/path/to/file',
          '_id': '5fab1aca47675a20c853bc11'},
         ['/real/path/to/origin/my_folder/my_folder_1/my_file.txt', '/path/to/origin/my_folder/my_folder_link/my_file_2.txt', '/path/to/origin/my_folder/my_folder_3/my_file_3.txt', '/real/path/to/origin/my_folder/my_folder_4/my_file_4.txt'],
         [call('api/metadata/file', data={'id': '5fab1aca47675a20c853bc11', 'data': {'folder_index': [{'file_name': 'my_file.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc10'}, {'file_name': 'my_file_3.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc12'}], 'current_location': '/path/to/temp/my_file.123.tar', 'file_size': 1000}}),
          call('api/tape/file/123', file_status_id=16, origin_file_path='/path/to/temp', origin_file_name='my_file.123.tar', file_size=1000, next_status=2)]
         ),
        ('backup_tar_local',
         {'file_name': 'my_folder.tar', 'file_id': 123,
          'origin_file_name': 'my_folder.tar', 'origin_file_path': '/path/to/origin',
          'local_purge_days': 0, 'extract': [{'path': 'my_folder_2/my_file_3.txt',
                                              'file_type': 'txt',
                                              'metadata': {'foo': 'foo1'}}],
          'index': 1, 'ignore': ['bar'],
          'validate_mode': 0, 'metadata': {}, 'user': 'foobar',
          'file_path': '/path/to/origin', 'path': '/path/to/file',
          '_id': '5fab1aca47675a20c853bc11'},
         ['/real/path/to/origin/my_folder/my_folder_1/my_file.txt', '/path/to/origin/my_folder/my_folder_link/my_file_2.txt', '/path/to/origin/my_folder/my_folder_3/my_file_3.txt', '/real/path/to/origin/my_folder/my_folder_4/my_file_4.txt'],
         [call('api/metadata/file', data={'id': '5fab1aca47675a20c853bc11', 'data': {'folder_index': [{'file_name': 'my_file.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc10'}, {'file_name': 'my_file_3.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc12'}], 'current_location': '/path/to/temp/my_folder.tar.123.tar', 'file_size': 1000}}),
          call('api/tape/file/123', file_status_id=16, file_path='/path/to/temp', file_name='my_folder.tar.123.tar', origin_file_path='/path/to/origin', origin_file_name='my_folder.tar', file_size=1000, next_status=6)],
         ),
        ('copy_tar_remote',
         {'file_name': 'my_file', 'file_id': 123,
          'origin_file_name': 'my_folder.tar', 'origin_file_path': '/path/to/remote',
          'local_purge_days': 1, 'extract': [{'path': 'my_folder_2/my_file_3.txt',
                                              'file_type': 'txt',
                                              'metadata': {'foo': 'foo1'}}],
          'index': 1, 'ignore': ['bar'],
          'validate_mode': 0, 'metadata': {}, 'user': 'foobar',
          'file_path': '/path/to/destination', 'path': '/path/to/file',
          '_id': '5fab1aca47675a20c853bc11'},
         ['/real/path/to/remote/my_folder/my_folder_1/my_file.txt', '/path/to/remote/my_folder/my_folder_link/my_file_2.txt', '/path/to/remote/my_folder/my_folder_3/my_file_3.txt', '/real/path/to/remote/my_folder/my_folder_4/my_file_4.txt'],
         [call('api/metadata/file', data={'id': '5fab1aca47675a20c853bc11', 'data': {'folder_index': [{'file_name': 'my_file.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc10'}, {'file_name': 'my_file_3.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc12'}], 'current_location': '/path/to/temp_remote/my_file.123.tar', 'file_size': 1000}}),
          call('api/tape/file/123', file_status_id=16, origin_file_path='/path/to/temp_remote', origin_file_name='my_file.123.tar', file_size=1000, next_status=2)]
         ),
        ('backup_tar_remote',
         {'file_name': 'my_folder.tar', 'file_id': 123,
          'origin_file_name': 'my_folder.tar', 'origin_file_path': '/path/to/remote',
          'local_purge_days': 0, 'extract': [{'path': 'my_folder_2/my_file_3.txt',
                                              'file_type': 'txt',
                                              'metadata': {'foo': 'foo1'}}],
          'index': 1, 'ignore': ['bar'],
          'validate_mode': 0, 'metadata': {}, 'user': 'foobar',
          'file_path': '/path/to/remote', 'path': '/path/to/file',
          '_id': '5fab1aca47675a20c853bc11'},
         ['/real/path/to/remote/my_folder/my_folder_1/my_file.txt', '/path/to/remote/my_folder/my_folder_link/my_file_2.txt', '/path/to/remote/my_folder/my_folder_3/my_file_3.txt', '/real/path/to/remote/my_folder/my_folder_4/my_file_4.txt'],
         [call('api/metadata/file', data={'id': '5fab1aca47675a20c853bc11', 'data': {'folder_index': [{'file_name': 'my_file.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc10'}, {'file_name': 'my_file_3.txt', 'file_path': 'my_folder_2', 'id': '5fab1aca47675a20c853bc12'}], 'current_location': '/path/to/temp_remote/my_folder.tar.123.tar', 'file_size': 1000}}),
          call('api/tape/file/123', file_status_id=16, file_path='/path/to/temp_remote', file_name='my_folder.tar.123.tar', origin_file_path='/path/to/remote', origin_file_name='my_folder.tar', file_size=1000, next_status=6)],
         ),
    ])
    @patch('dt_service.copy')
    @patch('dt_service.tarfile')
    @patch('dt_service.os')
    def test_DTService_run_tar(self, _description, metadata_record, realpath_responses, expected_curl_put_calls, os_mock, tarfile_mock, copy_mock):
        curl_get_responses = [{'metadata_id': '5fab1aca47675a20c853bc10'}, None]
        os_mock.path.join = os.path.join
        os_mock.path.split = os.path.split
        os_mock.path.realpath.side_effect = realpath_responses
        os_mock.walk.return_value = [('/path/to/origin/my_folder/my_folder_2', ['foo', 'bar'], ['my_file.txt', 'file_2_link.txt', 'my_file_3.txt', 'my_file_4.txt'])]
        os_mock.path.islink.side_effect = [True, True, False, True]
        os_mock.path.getsize.return_value = 1000
        copy_mock.copy.return_value = ['foo', 'bar']
        self.curl_get.side_effect = curl_get_responses
        self.curl_post.return_value = {'metadata_id': '5fab1aca47675a20c853bc12'}
        tar_info = Mock()
        tarfile_mock.TarInfo.return_value = tar_info
        self.dt_service.remote_sources = {
            'foo': {
                'path_prefix_source': '/path/to/remote',
                'path_prefix_destination': '/path/to/local',
                'path_temp': '/path/to/temp_remote',
            }
        }

        self.assertEqual(self.dt_service.run_tar(metadata_record), True)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)
        self.assertIn(call(['foo', 'bar']), copy_mock.copy.mock_calls)

    @parameterized.expand([
        ('already_purged',
         {'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_id': 123,
          'modified_dt': '2022-01-31T00:00:00'},
         [False],
         True,
         [call('api/tape/file/123', file_status_id=10)],
         [],
         ),
        ('extended',
         {'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_id': 123,
          'modified_dt': '2022-01-31T00:00:00'},
         [True],
         True,
         [call('api/tape/savefile', file='/path/to/my_file.txt', days=10)],
         [],
         ),
        ('purge_success',
         {'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_id': 123,
          'modified_dt': '2022-02-01T00:00:00'},
         [True, False],
         True,
         [call('api/tape/file/123', file_status_id=10)],
         [call.remove('/path/to/my_file.txt')]
         ),
        ('purge_failed',
         {'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_id': 123,
          'modified_dt': '2022-02-01T00:00:00'},
         [True, True],
         False,
         [],
         [call.remove('/path/to/my_file.txt')],
         ),
    ])
    @patch('dt_service.time')
    @patch('dt_service.datetime')
    @patch('dt_service.os')
    def test_DTService_run_purge(self, _description, in_file, path_exists_responses, expected, expected_curl_put_calls,
                                 expected_os_calls, os_mock, datetime_mock, time_mock):
        os_mock.path.join = os.path.join
        os_mock.path.exists.side_effect = path_exists_responses
        os_stat_return_value = Mock()
        os_stat_return_value.st_atime = 500
        os_mock.stat.return_value = os_stat_return_value
        datetime_mock.datetime.strptime = datetime.datetime.strptime
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 2, 2)
        time_mock.time.return_value = 1000

        self.assertEqual(self.dt_service.run_purge(in_file), expected)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)
        self._assertAllIn(expected_os_calls, os_mock)

    @patch('dt_service.os')
    def test_DTService_run_delete(self, os_mock):
        in_file = {'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_size': 1000}
        os_mock.path.join = os.path.join
        os_mock.path.exists.return_value = True
        os_mock.path.getsize.return_value = 1000
        os_mock.listdir.return_value = ['foo']

        self.assertEqual(self.dt_service.run_delete(in_file), True)
        self.assertIn(call.remove('/path/to/my_file.txt'), os_mock.mock_calls)

    @patch('dt_service.os')
    def test_DTService_remove_dir(self, os_mock):
        os_mock.listdir.side_effect = [[], ['foo']]

        self.assertEqual(self.dt_service.remove_dir('/path/to'), 1)
        self.assertIn(call.rmdir('/path/to'), os_mock.mock_calls)

    @patch('dt_service.os')
    @patch('dt_service.subprocess')
    @patch('dt_service.TemporaryDirectory')
    def test_DTService_rsync(self, temp_dir_mock, subprocess_mock, os_mock):
        temp_dir_mock.return_value = temp_dir_mock
        temp_dir_mock.__enter__.return_value = '/path/to/tmp'
        os_mock.path.join = os.path.join
        os_mock.path.dirname = os.path.dirname
        os_mock.path.basename = os.path.basename
        rsync_results = Mock()
        rsync_results.stdout.decode.return_value = 'total_transferred_file_size: 2000\n'
        subprocess_mock.run.side_effect = [rsync_results]

        self.assertEqual({'total_transferred_file_size': 2000},
                         self.dt_service.rsync('/path/to/remote/my_file.txt', '/path/to/local/data/my_file_copy.txt',
                                               'rsync://user@rsync_uri/dm_archive', 'rsync_password',
                                               '/path/to/local'))
        self._assertAllIn([call.makedirs('/path/to/tmp/data', exist_ok=True),
                           call.symlink('/path/to/remote/my_file.txt', '/path/to/tmp/data/my_file_copy.txt')], os_mock)
        self.assertIn(call.run(['rsync', '-aL', '--no-h', '--stats', '/path/to/tmp/', 'rsync://user@rsync_uri/dm_archive/'],
                               env={'RSYNC_PASSWORD': 'rsync_password'}, stdout=ANY, check=True),
                      subprocess_mock.mock_calls)

    @parameterized.expand([
        ('path_does_not_end_with_slash', '/path/to/temp', '/path/to/temp_2022'),
        ('path_ends_with_slash', '/path/to/temp/', '/path/to/temp_2022'),
    ])
    @patch('dt_service.datetime')
    def test_DTService_get_sharded_path(self, _description, path, expected, datetime_mock):
        datetime_mock.datetime.today.return_value = datetime.datetime(2022, 2, 2)

        self.assertEqual(self.dt_service._get_sharded_path(path), expected)

    @parameterized.expand([
        ('by_source', '/path/to/bar', {'source': 'bar'}, {'rsync_uri': 'rsync://user@rsync_uri/dm_archive', 'rsync_password': 'rsync', 'path_prefix_destination': '/path/to/local/bar'}),
        ('by_path', '/path/to/foo', {}, {'rsync_uri': 'rsync://user@rsync_uri/dm_archive', 'rsync_password': 'rsync', 'path_prefix_source': '/path/to/foo', 'path_prefix_destination': '/path/to/local/foo'}),
        ('no_match', '/path/to/non_valid', {}, None),
    ])
    def test_DTService_get_remote_config(self, _description, file_path, record, expected):
        self.dt_service.remote_sources = {
            'foo': {
                'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                'rsync_password': 'rsync',
                'path_prefix_source': '/path/to/foo',
                'path_prefix_destination': '/path/to/local/foo',
            },
            'bar': {
                'rsync_uri': 'rsync://user@rsync_uri/dm_archive',
                'rsync_password': 'rsync',
                'path_prefix_destination': '/path/to/local/bar',
            }
        }

        self.assertEqual(self.dt_service._get_remote_config(file_path, record), expected)

    @parameterized.expand([
        ('exact_size', 999, True),
        ('minimum_size', 800, False),
    ])
    @patch('dt_service.subprocess')
    def test_DTService_verify_path_in_hsi(self, _description, expected_size, exact_size, subprocess_mock):
        subprocess_mock.run.side_effect = [subprocess.CompletedProcess('cmd', 0, stdout=b'999 /path/to/remote')]
        subprocess_mock.PIPE = subprocess.PIPE
        self.dt_service._verify_path_in_hsi('server', '/path/to/remote', expected_size, exact_size)

        self.assertIn(
            call.run(['hsi', '-P', '-q', '-h', 'server', 'ls -1s /path/to/remote'], check=True, stdout=subprocess.PIPE),
            subprocess_mock.mock_calls)

    @parameterized.expand([
        ('hsi_error', 999, True, [subprocess.CalledProcessError(1, 'cmd')], subprocess.CalledProcessError),
        ('not_exact_size', 800, True, [subprocess.CompletedProcess('cmd', 0, stdout=b'999 /path/to/remote')],
         dt_service.HSIVerificationFailedException),
        ('not_minimum_size', 1000, False, [subprocess.CompletedProcess('cmd', 0, stdout=b'999 /path/to/remote')],
         dt_service.HSIVerificationFailedException),
    ])
    @patch('dt_service.subprocess')
    def test_DTService_verify_path_in_hsi_call_fails_raises_exception(self, _description, expected_size, exact_size,
                                                                      subprocess_responses, expected_exception,
                                                                      subprocess_mock):
        subprocess_mock.run.side_effect = subprocess_responses
        subprocess_mock.CalledProcessError = subprocess.CalledProcessError
        subprocess_mock.PIPE = subprocess.PIPE

        self.assertRaises(expected_exception,
                          self.dt_service._verify_path_in_hsi, 'server', '/path/to/remote', expected_size, exact_size)

        self.assertIn(
            call.run(['hsi', '-P', '-q', '-h', 'server', 'ls -1s /path/to/remote'], check=True, stdout=subprocess.PIPE),
            subprocess_mock.mock_calls)

    @parameterized.expand([
        ('single_file_success',
         {'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt', 'backup_record_id': 456,
                       'file_id': 123}]},
         [{}],
         [{}],
         True,
         [call('api/tape/backuprecord/456',
               data={'backup_record_status_id': 4, 'remote_file_name': 'my_file.txt.456',
                     'remote_file_path': '/path/to/my_service/2022/path/to/temp', 'tar_record_id': None}),
          call('api/tape/savefile', file='/path/to/temp/my_file.txt', days=1)],
         [call.makedirs('/path_to_globus_timer/2022/path/to/temp', exist_ok=True),
          call.rename('/path_to_globus_temp/my_file.txt.456',
                      '/path_to_globus_timer/2022/path/to/temp/my_file.txt.456')],
         [],
         ),
        ('single_file_does_not_exist',
         {'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt', 'backup_record_id': 456,
                       'file_id': 123}]},
         [FileNotFoundError('Error')],
         [{}],
         False,
         [call('api/tape/backuprecord/456', data={'backup_record_status_id': 5})],
         [],
         [],
         ),
        ('single_file_copy_error',
         {'records': [{'file_path': '/path/to/temp', 'file_name': 'my_file.txt', 'backup_record_id': 456,
                       'file_id': 123}]},
         [{}],
         [Exception('Error')],
         False,
         [call('api/tape/backuprecord/456', data={'backup_record_status_id': 5})],
         [call.makedirs('/path_to_globus_timer/2022/path/to/temp', exist_ok=True),
          call.rename('/path_to_globus_temp/my_file.txt.456',
                      '/path_to_globus_timer/2022/path/to/temp/my_file.txt.456'),
          call.remove('/path_to_globus_temp/my_file.txt.456')],
         [],
         ),
        ('multiple_files_success',
         {'records': [{'file_path': '/path/to/root', 'file_name': 'my_file_1.txt', 'backup_record_id': 1111,
                       'file_id': 111},
                      {'file_path': '/path/to/root', 'file_name': f'my_file_2_{"#" * 90}.txt',
                       'backup_record_id': 2222,
                       'file_id': 222}
                      ],
          'root_dir': '/path/to/root'},
         [{}, {}],
         [{}],
         True,
         [call('api/tape/backuprecords', records=[
             {'backup_record_id': 1111, 'backup_record_status_id': 4, 'tar_record_id': 999,
              'remote_file_name': 'my_file_1.txt.1111', 'remote_file_path': '.'},
             {'backup_record_id': 2222, 'backup_record_status_id': 4, 'tar_record_id': 999,
              'remote_file_name': 'my_file_2_###########################################################################.2222',
              'remote_file_path': '.'}]),
          call('api/tape/tar/999', remote_path='/path/to/my_service/2022/000/000/999.tar')],
         [call.makedirs('/path_to_globus_timer/2022/000/000', exist_ok=True),
          call.rename('/path_to_globus_temp/999.tar',
                      '/path_to_globus_timer/2022/000/000/999.tar')],
         [call.add('/path/to/root/my_file_1.txt', arcname='my_file_1.txt.1111'),
          call.add(
              '/path/to/root/my_file_2_##########################################################################################.txt',
              arcname='my_file_2_###########################################################################.2222')],
         ),
        ('multiple_files_some_files_missing',
         {'records': [{'file_path': '/path/to/root', 'file_name': 'my_file_1.txt', 'backup_record_id': 1111,
                       'file_id': 111},
                      {'file_path': '/path/to/root', 'file_name': f'my_file_2_{"#" * 90}.txt',
                       'backup_record_id': 2222,
                       'file_id': 222}
                      ],
          'root_dir': '/path/to/root'},
         [{}, FileNotFoundError('Error')],
         [{}],
         True,
         [call('api/tape/backuprecords', records=[
             {'backup_record_id': 1111, 'backup_record_status_id': 4, 'tar_record_id': 999,
              'remote_file_name': 'my_file_1.txt.1111', 'remote_file_path': '.'}]),
          call('api/tape/backuprecord/2222', data={'backup_record_status_id': 5})],
         [call.makedirs('/path_to_globus_timer/2022/000/000', exist_ok=True),
          call.rename('/path_to_globus_temp/999.tar',
                      '/path_to_globus_timer/2022/000/000/999.tar')],
         [call.add('/path/to/root/my_file_1.txt', arcname='my_file_1.txt.1111')],
         ),
        ('multiple_files_copy_error',
         {'records': [{'file_path': '/path/to/root', 'file_name': 'my_file_1.txt', 'backup_record_id': 1111,
                       'file_id': 111},
                      {'file_path': '/path/to/root', 'file_name': f'my_file_2_{"#" * 90}.txt',
                       'backup_record_id': 2222,
                       'file_id': 222}
                      ],
          'root_dir': '/path/to/root'},
         [{}, {}],
         [Exception('Error')],
         False,
         [call('api/tape/backuprecords',
               records=[{'backup_record_id': 1111, 'backup_record_status_id': 5},
                        {'backup_record_id': 2222, 'backup_record_status_id': 5}])],
         [call.makedirs('/path_to_globus_timer/2022/000/000', exist_ok=True),
          call.rename('/path_to_globus_temp/999.tar',
                      '/path_to_globus_timer/2022/000/000/999.tar')],
         [call.add('/path/to/root/my_file_1.txt', arcname='my_file_1.txt.1111')],
         ),
    ])
    @patch('dt_service.tarfile')
    @patch('dt_service.shutil')
    @patch('dt_service.datetime')
    @patch('dt_service.os')
    def test_DTService_put_globus(self, _description, in_file, os_stat_responses, os_rename_responses,
                                  expected, expected_curl_put_calls, expected_os_calls, expected_tar_calls,
                                  os_mock, datetime_mock, shutil_mock, tarfile_mock):
        service = {'name': 'my_service', 'default_path': '/path/to/my_service'}
        os_mock.stat.side_effect = os_stat_responses
        os_mock.path.join = os.path.join
        os_mock.path.split = os.path.split
        os_mock.rename.side_effect = os_rename_responses
        os_mock.path.exists.return_value = True
        datetime_mock.datetime.today.return_value = datetime.datetime(2022, 2, 2)
        self.dt_service.backup_services = {'my_service': {'source_path': '/path_to_globus_timer',
                                                          'temp_path': '/path_to_globus_temp'}}
        self.curl_get.return_value = {'file_status_id': self.dt_service.cv.file_status.BACKUP_COMPLETE}
        self.curl_post.return_value = {'tar_record_id': 999}
        tar = MagicMock()
        tar.__enter__.return_value = tar
        tarfile_mock.open.return_value = tar

        self.assertEqual(self.dt_service._put_globus(service, in_file), expected)
        self._assertAllIn(expected_curl_put_calls, self.curl_put)
        self._assertAllIn(expected_os_calls, os_mock)
        self._assertAllIn(expected_tar_calls, tar)

    def test_DTService_get_backup_service_source_path(self):
        self.dt_service.backup_services = {'my_service': {'source_path': '/path_to_globus_timer',
                                                          'temp_path': '/path_to_globus_temp'}}

        self.assertEqual(self.dt_service._get_backup_service_paths('my_service'), ('/path_to_globus_timer',
                                                                                   '/path_to_globus_temp'))

    @parameterized.expand([
        ('missing_backup_service', 'not_found', {'my_service': {'source_path': '/path_to_globus_timer'}}),
        ('missing_source_path', 'my_service', {'my_service': {'temp_path': '/path_to_globus_temp'}}),
        ('missing_temp_path', 'my_service', {'my_service': {'source_path': 'path_to_globus_timer'}}),
    ])
    def test_DTService_get_backup_service_source_path_config_error(self, _description, name, config):
        self.dt_service.backup_services = config

        self.assertRaises(dt_service.BackupServiceConfigurationException,
                          self.dt_service._get_backup_service_paths, name)

    @patch('dt_service.platform')
    @patch('dt_service.time')
    @patch('dt_service.datetime')
    @patch('dt_service.DTService')
    @patch('dt_service.Curl')
    @patch.dict(os.environ, {"JAMO_TOKEN": "123"})
    def test_main(self, curl_mock, dtservice_mock, datetime_mock, time_mock, platform_mock):
        args = ['', 'http://127.0.0.1', '-f', 'feature1,feature2', '-k', 'task1,task2', '-r', '1', '-D', 'jgi']
        curl_mock.return_value = self.curl
        datetime_mock.datetime.now.side_effect = [datetime.datetime(2022, 2, 2), datetime.datetime(2022, 2, 2),
                                                  datetime.datetime(2022, 2, 2), datetime.datetime(2022, 2, 2),
                                                  datetime.datetime(2022, 2, 3)]
        self.curl_get.side_effect = [{'task1': {'record_count': 5}}, {}]
        self.curl_post.return_value = {'service_id': 123}
        dtservice_mock.return_value = dtservice_mock
        dtservice_mock.current_thread_count.value = 0
        platform_mock.node.return_value = 'localhost'

        with patch('sys.argv', args):
            dt_service.main()

        self.assertIn(call('api/tape/service', data={'tasks': 'task1,task2', 'started_dt': 'now()', 'hostname': 'localhost', 'available_threads': 1, 'seconds_to_run': 1, 'division': 'jgi'}), self.curl_post.mock_calls)
        self.assertIn(call('api/tape/service/123', data={'ended_dt': 'now()'}), self.curl_put.mock_calls)


if __name__ == '__main__':
    unittest.main()
