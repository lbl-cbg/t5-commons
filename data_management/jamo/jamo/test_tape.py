import unittest
import tape
import pymysql
from datetime import datetime, timedelta
from parameterized import parameterized
from bson.objectid import ObjectId
from lapinpy import common
from tape import Tape
from unittest.mock import patch, Mock, call, ANY
from types import SimpleNamespace


class TestTape(unittest.TestCase):

    def setUp(self):
        self.__initialize()

    @patch('pymysql.connect')
    @patch('task.random.sample')
    def __initialize(self, sample, connect):
        config = SimpleNamespace(**{
            'mysql_host': 'mysql_host',
            'mysql_user': 'mysql_user',
            'mysql_pass': 'mysql_pass',
            'tape_db': 'tape_db',
            'dm_archive_root': '/path/to/archive',
            'instance_type': 'dev',
            'disk_reserve': 10,
            'disk_size': 100000000,
            'queue_1_limit': [100, 1000],
            'queue_2_limit': [25, 1000],
            'queue_2_match': 'foobar',
            'remote_sources': {'foo': {'rsync_uri': 'rsync://foobar@127.0.0.1:8888/dm_archive',
                                       'rsync_password': 'rsync',
                                       'path_prefix_source': '/path/to/remote',
                                       'path_prefix_destination': '/path/to/local'},
                               'bar': {'rsync_uri': 'rsync://foobar@127.0.0.1:8888/dm_archive',
                                       'rsync_password': 'rsync',
                                       'path_prefix_destination': '/path/to/local',
                                       'dm_archive_root_source': '/path/to/bar/dm_archive'},
                               },
            'backup_services_to_feature_name': {'HPSS': 'hsi'},
            'division': {
                'jgi': {'default_backup_service': 1,
                        'tape_temp_dir': '/path/to/tape_temp_dir',
                        'default_queue_features': {'ingest': ['nersc'], 'prep': ['hsi_1'], 'pull': ['hsi_1', 'dna_w'],
                                                   'copy': ['dna_w'], 'tar': ['compute'], 'purge': ['dna_w'],
                                                   'delete': ['dna_w'], 'put': [], 'md5': ['compute']},
                        'max_resources': {'hsi_1': 18, 'hsi_2': 18}}
            },
            'dm_archive_root_by_division': {'jgi': '/path/to/archive'},
        })

        sample.return_value = 'AA'
        self.connection = Mock()
        connect.return_value = self.connection
        self.connect = Mock()
        self.connection.return_value = self.connect
        self.cursor = Mock()
        self.connection.cursor.return_value = self.cursor
        self.cursor.fetchall.side_effect = [
            [{'backup_service_id': 2, 'name': 'hpss', 'server': 'hpss.nersc.gov',
              'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS', 'division': 'jgi'}],
            [{'file_status_id': 1, 'status': 'REGISTERED'}, {'file_status_id': 2, 'status': 'COPY_READY'},
             {'file_status_id': 14, 'status': 'TAR_READY'}, {'file_status_id': 6, 'status': 'BACKUP_READY'},
             {'file_status_id': 7, 'status': 'BACKUP_IN_PROGRESS'},
             {'file_status_id': 20, 'status': 'INGEST_STATS_FAILED'},
             {'file_status_id': 21, 'status': 'INGEST_FILE_MISSING'}, {'file_status_id': 5, 'status': 'COPY_FAILED'},
             {'file_status_id': 17, 'status': 'TAR_FAILED'}, {'file_status_id': 11, 'status': 'DELETE'},
             {'file_status_id': 8, 'status': 'BACKUP_COMPLETE'}, {'file_status_id': 9, 'status': 'BACKUP_FAILED'},
             {'file_status_id': 10, 'status': 'PURGED'},
             {'file_status_id': 12, 'status': 'RESTORE_IN_PROGRESS'}, {'file_status_id': 4, 'status': 'COPY_COMPLETE'},
             {'file_status_id': 16, 'status': 'TAR_COMPLETE'}, {'file_status_id': 13, 'status': 'RESTORED'},
             {'file_status_id': 22, 'status': 'INGEST_COMPLETE'},
             {'file_status_id': 19, 'status': 'INGEST_STATS_COMPLETE'},
             {'file_status_id': 15, 'status': 'TAR_IN_PROGRESS'}, {'file_status_id': 3, 'status': 'COPY_IN_PROGRESS'},
             {'file_status_id': 28, 'status': 'RESTORE_REGISTERED'},
             ],
            [{'backup_record_status_id': 1, 'status': 'REGISTERED'},
             {'backup_record_status_id': 2, 'status': 'TRANSFER_READY'},
             {'backup_record_status_id': 16, 'status': 'HOLD'},
             {'backup_record_status_id': 4, 'status': 'TRANSFER_COMPLETE'},
             {'backup_record_status_id': 3, 'status': 'TRANSFER_IN_PROGRESS'},
             {'backup_record_status_id': 6, 'status': 'WAIT_FOR_TAPE'},
             {'backup_record_status_id': 12, 'status': 'VALIDATION_COMPLETE'},
             {'backup_record_status_id': 5, 'status': 'TRANSFER_FAILED'}],
            [{'queue_status_id': 1, 'status': 'REGISTERED'},
             {'queue_status_id': 2, 'status': 'IN_PROGRESS'}, {'queue_status_id': 4, 'status': 'FAILED'},
             {'queue_status_id': 7, 'status': 'PREP_IN_PROGRESS'}, {'queue_status_id': 6, 'status': 'PREP_FAILED'},
             {'queue_status_id': 3, 'status': 'COMPLETE'}],
            [{'quota': 100000000, 'used': 50000000}],
            [{'files': 10, 'disk_usage': 50}],
            [{'files': 2, 'disk_usage': 10}],
            [{'ymdh': '22-10-15 12', 'vol': 5, 'N': 10, 'gb': 50}],
            [{'tar_record_id': 224967, 'volume': 'AG1583', 'position_a': 1375, 'position_b': 14800029}],
            [{'requestor': 'foobar', 'n': 5}],
            [{'tar_record_id': 224967}],
            [{'file_ingest_id': 1001, 'file_ingest_status_id': 22, 'file_id': 14452074, 'file_size': 3753,
              'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90, 'auto_uncompress': 0, '_put_mode': 0,
              '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
              '_file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              '_services': '[1]',
              '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
              'metadata_id': '626b286a682a7f997d28e4a5', '_metadata_ingest_id': '626b286a682a7f997d28e4a4',
              'file_date': datetime(2022, 4, 28, 15, 34, 51), 'file_owner': 'qc_user', 'file_group': 'qc_user',
              'file_permissions': '0100755', 'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'},
             {'file_ingest_id': 1002, 'file_ingest_status_id': 22, 'file_id': 14452075, 'file_size': 3753,
              'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90, 'auto_uncompress': 0,
              '_put_mode': 0,
              '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
              '_file': '/path/to/remote/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
              '_services': '[1]',
              '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
              '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
              'metadata_id': '626b286a682a7f997d28e4a5', '_metadata_ingest_id': '626b286a682a7f997d28e4a4',
              'file_date': datetime(2022, 4, 28, 15, 34, 51), 'file_owner': 'qc_user', 'file_group': 'qc_user',
              'file_permissions': '0100755',
              'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
              'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'}],
            [{'file_id': 11479542, 'transaction_id': 1, 'file_name': '3300038674_26.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/223350',
              'origin_file_name': '3300038674_26.tar.gz',
              'origin_file_path': '/global/cfs/cdirs/m342/img/mbin_jamo_src_tarball_files', 'file_size': 1132794,
              'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'img', 'file_group': 'genome',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
              'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
              'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
              'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
              'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
             {'file_id': 11479543, 'transaction_id': 1, 'file_name': '3300038674_26-2.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/223350',
              'origin_file_name': '3300038674_26-2.tar.gz',
              'origin_file_path': '/path/to/remote/img/mbin_jamo_src_tarball_files', 'file_size': 1132794,
              'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'img', 'file_group': 'genome',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
              'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
              'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
              'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
              'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0}],
            [{'file_id': 1660, 'transaction_id': 108, 'file_name': 'pbio-89.1031.fastq',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/89',
              'origin_file_name': 'filtered_subreads.fastq',
              'origin_file_path': '/house/pacbio/jobs/019/019025/data',
              'file_size': 301326599,
              'file_date': datetime(2012, 5, 18, 16, 50, 12), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100664', 'local_purge_days': 90, 'md5sum': 'c27d89f18849636b3d4437060bb4c91c',
              'file_status_id': 14, 'created_dt': datetime(2013, 4, 26, 17, 7, 29),
              'modified_dt': datetime(2022, 4, 13, 21, 4, 10), 'validate_mode': 0,
              'user_save_till': datetime(2022, 5, 13, 0, 0, 0), 'metadata_id': '51d48dff067c014cd6e9e44f',
              'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
             {'file_id': 1662, 'transaction_id': 108, 'file_name': 'pbio-89.1031-3.fastq',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/89',
              'origin_file_name': 'filtered_subreads-3.fastq',
              'origin_file_path': '/path/to/remote/jobs/019/019025/data',
              'file_size': 301326599,
              'file_date': datetime(2012, 5, 18, 16, 50, 12), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100664', 'local_purge_days': 90, 'md5sum': 'c27d89f18849636b3d4437060bb4c91c',
              'file_status_id': 14, 'created_dt': datetime(2013, 4, 26, 17, 7, 29),
              'modified_dt': datetime(2022, 4, 13, 21, 4, 10), 'validate_mode': 0,
              'user_save_till': datetime(2022, 5, 13, 0, 0, 0), 'metadata_id': '51d48dff067c014cd6e9e44f',
              'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0}],
            [{'md5_queue_id': 431849, 'file_path': '/projectb/scratch/jgi_dna/15.3371978.tar',
              'queue_status_id': 1, 'file_size': 202827386880, 'md5sum': None,
              'dt_modified': datetime(2014, 3, 28, 22, 59, 34), 'callback': 'local://put_file/3371978'}],
            [{'local_purge_days': 60,
              'origin_file_path': '/global/cfs/cdirs/seqfs/prod/illumina/staging/novaseq01/220427_A00178_0328_AHNWMCDSX3',
              'origin_file_name': '52689.3.420190.AGCTCCTA-AGCTCCTA.fastq.gz', 'file_id': 14462356,
              'file_name': '52689.3.420190.AGCTCCTA-AGCTCCTA.fastq.gz',
              'file_path': '/global/dna/dm_archive/sdm/illumina/05/26/89', 'service': 2,
              'backup_record_id': 18774872, 'file_status_id': 6, 'backup_record_status_id': 1,
              'file_size': 959524387, 'division': 'jgi'}],
            [{'volume': 'AG2910'}],
        ]
        self.hsi = Mock()
        self.hsi_state = Mock()
        self.tape = Tape(config)
        self.tape.connections = [self.connection]
        self.tape.hsi = self.hsi
        self.tape.hsi_state = self.hsi_state
        self.cursor.fetchall.side_effect = None

    def test_Tape_get_hpss_file_info(self):
        self.hsi.getAllFileInfo.return_value = ('600', 'user', 'group', 1000, datetime(2000, 1, 1, 1, 2, 3))

        self.assertEqual(self.tape.getHpssFileInfo('foo'),
                         ('600', 'user', 'group', 1000, datetime(2000, 1, 1, 1, 2, 3)))

    def test_Tape_shutdown(self):
        self.tape.shutdown()

        self.hsi.exit.assert_called_with()

    @parameterized.expand([
        ('local_purge_days_0', 0,
         {'file_name': 'origin_file_name.gz', 'file_id': 1, 'backup_record_id': 3, 'file_path': '/origin/file/path'}),
        ('local_purge_days_greater_than_0', 5,
         {'file_name': 'file_name.gz', 'file_id': 1, 'backup_record_id': 3, 'file_path': '/file/path'})
    ])
    def test_Tape_add_files(self, _description, local_purge_days, expected):
        self.tape.add_files([{'local_purge_days': local_purge_days,
                              'origin_file_path': '/origin/file/path',
                              'origin_file_name': 'origin_file_name.gz', 'file_id': 1,
                              'file_name': 'file_name.gz',
                              'file_path': '/file/path', 'service': 2,
                              'backup_record_id': 3, 'file_status_id': 6, 'backup_record_status_id': 1,
                              'division': 'jgi', 'file_size': 959524387}],
                            True)

        self.assertIn(expected, self.tape.divisions.get('jgi').put_queue.feature_queues.get('hsi_2')[0].get('records'))
        self.cursor.execute.assert_called_with(
            'update backup_record set backup_record_status_id=%s,dt_to_release=null where backup_record_id=%s and backup_record_status_id=%s',
            (2, 3, 16))

    def test_Tape_init_put_queue(self):
        self.cursor.fetchall.side_effect = [[{'local_purge_days': 3,
                                              'origin_file_path': '/origin/file/path',
                                              'origin_file_name': 'origin_file_name.gz', 'file_id': 1,
                                              'file_name': 'file_name.gz',
                                              'file_path': '/file/path', 'service': 2,
                                              'backup_record_id': 3, 'file_status_id': 6, 'backup_record_status_id': 1,
                                              'division': 'jgi',
                                              'file_size': 959524387}]]

        self.tape.init_put_queue('jgi')

        self.assertIn({'file_name': 'file_name.gz', 'file_id': 1, 'backup_record_id': 3, 'file_path': '/file/path'},
                      self.tape.divisions.get('jgi').put_queue.feature_queues.get('hsi_2')[0].get('records'))

    def test_Tape_put_taskcomplete(self):
        self.tape.divisions.get('jgi').task_manager.current_tasks = {
            'copy': {'service': 'service_id', 'task': 'copy', 'data': {'name': 'foobar'},
                     'features': ['foo']}}
        self.tape.divisions.get('jgi').task_manager.queues[1].currently_running = 1

        self.tape.put_taskcomplete(None, {'task_id': 'copy', 'returned': True, 'division': 'jgi'})
        self.assertEqual(self.tape.divisions.get('jgi').task_manager.queues[1].currently_running, 0)

    @parameterized.expand([
        ('default_feature',
         {
             'features': ['dna_w'],
             'tasks': ['copy'],
             'service': 'copy',
             'returned': True,
             'division': 'jgi'
         },
         {'created': datetime(2000, 1, 2, 3, 4, 5),
          'division': 'jgi',
          'data': {'auto_uncompress': 0,
                   'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                   'file_date': datetime(2020, 12, 1, 12, 27, 27),
                   'file_group': 'genome',
                   'file_id': 11479542,
                   'file_name': '3300038674_26.tar.gz',
                   'file_owner': 'img',
                   'file_path': '/global/dna/dm_archive/img/submissions/223350',
                   'file_permissions': '0100644',
                   'file_size': 1132794,
                   'file_status_id': 1,
                   'local_purge_days': 2,
                   'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                   'metadata_id': '5eecdfb86263bf2148833b40',
                   'modified_dt': datetime(2021, 6, 7, 6, 11, 46),
                   'origin_file_name': '3300038674_26.tar.gz',
                   'origin_file_path': '/global/cfs/cdirs/m342/img/mbin_jamo_src_tarball_files',
                   'remote_purge_days': None,
                   'transaction_id': 1,
                   'transfer_mode': 0,
                   'user_save_till': datetime(2021, 6, 21, 6, 11, 46),
                   'validate_mode': 0},
          'features': ['dna_w'],
          'service': 'copy',
          'task': 'copy',
          'task_id': 'AA1'}
         ),
        ('remote_feature',
         {
             'features': ['foo'],
             'tasks': ['copy'],
             'service': 'copy',
             'returned': True,
             'division': 'jgi'},
         {'created': datetime(2000, 1, 2, 3, 4, 5),
          'division': 'jgi',
          'data': {'auto_uncompress': 0,
                   'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                   'file_date': datetime(2020, 12, 1, 12, 27, 27),
                   'file_group': 'genome',
                   'file_id': 11479543,
                   'file_name': '3300038674_26-2.tar.gz',
                   'file_owner': 'img',
                   'file_path': '/global/dna/dm_archive/img/submissions/223350',
                   'file_permissions': '0100644',
                   'file_size': 1132794,
                   'file_status_id': 1,
                   'local_purge_days': 2,
                   'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                   'metadata_id': '5eecdfb86263bf2148833b40',
                   'modified_dt': datetime(2021, 6, 7, 6, 11, 46),
                   'origin_file_name': '3300038674_26-2.tar.gz',
                   'origin_file_path': '/path/to/remote/img/mbin_jamo_src_tarball_files',
                   'remote_purge_days': None,
                   'transaction_id': 1,
                   'transfer_mode': 0,
                   'user_save_till': datetime(2021, 6, 21, 6, 11, 46),
                   'validate_mode': 0},
          'features': ['foo'],
          'service': 'copy',
          'task': 'copy',
          'task_id': 'AA1'}
         ),
    ])
    @patch('task.datetime')
    def test_Tape_post_nexttask(self, _description, kwargs, expected, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        self.tape.enable_portal_long()

        actual = self.tape.post_nexttask(None, kwargs)

        self.assertEqual(actual, expected)

    @parameterized.expand([
        ('pull_in_tasks_no_previous_task_id',
         {'features': ['hsi_1', 'dna_w'], 'tasks': ['pull'], 'service': 'pull_service', 'returned': True,
          'division': 'jgi'},
         {'data': [{'foo': 'bar'}], 'uses_resources': ['hsi_1', 'dna_w']},
         {'created': datetime(2000, 1, 2, 3, 4, 5), 'data': [{'foo': 'bar'}], 'features': ['hsi_1', 'dna_w'],
          'records': 1, 'service': 'pull_service', 'task': 'pull', 'task_id': None, 'division': 'jgi'},
         [call.next(['hsi_1', 'dna_w'])],
         []),
        ('pull_in_tasks_previous_task_id',
         {'features': ['hsi_1', 'dna_w'], 'tasks': ['pull'], 'service': 'pull_service', 'returned': True,
          'previous_task_id': 'some_task', 'division': 'jgi'},
         {'data': [{'foo': 'bar'}], 'uses_resources': ['hsi_1', 'dna_w']},
         {'created': datetime(2000, 1, 2, 3, 4, 5), 'data': [{'foo': 'bar'}], 'features': ['hsi_1', 'dna_w'],
          'records': 1, 'service': 'pull_service', 'task': 'pull', 'task_id': None, 'division': 'jgi'},
         [call.next(['hsi_1', 'dna_w'])],
         [call.set_task_complete('some_task', True)]),
        ('pull_not_in_tasks',
         {'features': ['hsi_1', 'dna_w'], 'tasks': ['copy'], 'service': 'copy_service', 'returned': True,
          'division': 'jgi'},
         {},
         None,
         [],
         [call.get_task(['hsi_1', 'dna_w'], ['copy'], None, 'copy_service', True)]),
    ])
    @patch('tape.datetime')
    def test_Tape_post_nexttask_pull_queue_db(self, _description, kwargs, pull_queue_next_return_value, expected,
                                              expected_pull_queue_db_calls, expected_task_manager_calls, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        pull_queue_mock = Mock()
        pull_queue_mock.next.return_value = pull_queue_next_return_value
        pull_queue_mock.name = 'pull'
        task_manager_mock = Mock()
        task_manager_mock.getTask.return_value = {'task': 'foo'}
        self.tape.use_db_pull_tasks = True
        self.tape.divisions.get('jgi').pull_queue = pull_queue_mock
        self.tape.divisions.get('jgi').task_manager = task_manager_mock

        task = self.tape.post_nexttask(None, kwargs)

        self.assertEqual(expected_pull_queue_db_calls, pull_queue_mock.mock_calls)
        self.assertEqual(expected_task_manager_calls, task_manager_mock.mock_calls)
        if expected:
            self.assertEqual(expected, task)

    @parameterized.expand([
        ('prep_in_tasks_no_previous_task_id',
         {'features': ['hsi_1'], 'tasks': ['prep'], 'service': 'prep_service', 'returned': True, 'division': 'jgi'},
         {'data': [{'foo': 'bar'}], 'uses_resources': ['hsi_1']},
         {'created': datetime(2000, 1, 2, 3, 4, 5), 'data': [{'foo': 'bar'}], 'features': ['hsi_1'],
          'service': 'prep_service', 'task': 'prep', 'task_id': None, 'division': 'jgi'},
         [call.next(['hsi_1'])],
         []),
        ('prep_in_tasks_previous_task_id',
         {'features': ['hsi_1'], 'tasks': ['prep'], 'service': 'prep_service', 'returned': True,
          'previous_task_id': 'some_task', 'division': 'jgi'},
         {'data': [{'foo': 'bar'}], 'uses_resources': ['hsi_1']},
         {'created': datetime(2000, 1, 2, 3, 4, 5), 'data': [{'foo': 'bar'}], 'features': ['hsi_1'],
          'service': 'prep_service', 'task': 'prep', 'task_id': None, 'division': 'jgi'},
         [call.next(['hsi_1'])],
         [call.set_task_complete('some_task', True)]),
        ('prep_not_in_tasks',
         {'features': ['hsi_1'], 'tasks': ['copy'], 'service': 'copy_service', 'returned': True, 'division': 'jgi'},
         {'data': [{'foo': 'bar'}], 'uses_resources': ['hsi_1']},
         None,
         [],
         [call.get_task(['hsi_1'], ['copy'], None, 'copy_service', True)]),
    ])
    @patch('tape.datetime')
    def test_Tape_post_nexttask_prep_queue_db(self, _description, kwargs, prep_queue_next_return_value, expected,
                                              expected_prep_queue_db_calls, expected_task_manager_calls, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        prep_queue_mock = Mock()
        prep_queue_mock.next.return_value = prep_queue_next_return_value
        prep_queue_mock.name = 'prep'
        task_manager_mock = Mock()
        task_manager_mock.getTask.return_value = {'task': 'foo'}
        self.tape.use_db_prep_tasks = True
        self.tape.divisions.get('jgi').prep_queue = prep_queue_mock
        self.tape.divisions.get('jgi').task_manager = task_manager_mock

        task = self.tape.post_nexttask(None, kwargs)

        self.assertEqual(expected_prep_queue_db_calls, prep_queue_mock.mock_calls)
        self.assertEqual(expected_task_manager_calls, task_manager_mock.mock_calls)
        if expected:
            self.assertEqual(expected, task)

    @parameterized.expand([
        ('dict', {'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
                  'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
                  'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
                  'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                  'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                  'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
                  'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
                  'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0}),
        ('list', [{'file_id': 11479548, 'transaction_id': 1, 'file_name': 'foo.txt', 'file_path': '/path/to',
                   'origin_file_name': 'foo.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132794,
                   'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'foo', 'file_group': 'foobar',
                   'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                   'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                   'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
                   'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
                   'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0},
                  {'file_id': 11479549, 'transaction_id': 1, 'file_name': 'bar.txt', 'file_path': '/path/to',
                   'origin_file_name': 'bar.txt', 'origin_file_path': '/path/to/origin', 'file_size': 1132795,
                   'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'bar', 'file_group': 'foobar',
                   'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                   'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                   'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0,
                   'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40',
                   'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0}])
    ])
    def test_Tape_put_task_not_remote(self, _description, data):
        self.tape.put_task(None, {'task': {
            'task': 'copy',
            'features': ['dna_w'],
            'data': data,
            'division': 'jgi',
        }})

        if isinstance(data, dict):
            self.assertIn(data, self.tape.divisions.get('jgi').copy_queue.feature_queues.get('dna_w'))
        else:
            for d in data:
                self.assertIn(d, self.tape.divisions.get('jgi').copy_queue.feature_queues.get('dna_w'))

    def test_Tape_put_task_remote(self):
        data = {'file_id': 11479544, 'transaction_id': 1, 'file_name': '3300038674_26-3.tar.gz', 'file_path': '/global/dna/dm_archive/img/submissions/223350', 'origin_file_name': '3300038674_26-3.tar.gz', 'origin_file_path': '/path/to/remote/img/mbin_jamo_src_tarball_files', 'file_size': 1132794, 'file_date': datetime(2020, 12, 1, 12, 27, 27), 'file_owner': 'img', 'file_group': 'genome', 'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': 'acdfb3004514e62fa322840cf2c1b119', 'file_status_id': 1, 'created_dt': datetime(2020, 12, 1, 12, 27, 27), 'modified_dt': datetime(2021, 6, 7, 6, 11, 46), 'validate_mode': 0, 'user_save_till': datetime(2021, 6, 21, 6, 11, 46), 'metadata_id': '5eecdfb86263bf2148833b40', 'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0}, {'files': 1, 'volume': 'AG2198', 'pull_queue_id': 11421748, 'position_b': 1073457001052, 'tar_record_id': 224967, 'position_a': 3, 'priority': 4, 'file_id': 7431043, 'file_size': 0, 'active': True}

        self.tape.put_task(None, {'task': {
            'task': 'copy',
            'features': ['foo'],
            'data': data,
            'division': 'jgi',
        }})

        self.assertIn(data, self.tape.divisions.get('jgi').copy_queue.feature_queues.get('foo'))

    def test_Tape_put_releaselockedvolume(self):
        self.assertEqual(len(self.tape.divisions.get('jgi').pull_queue.volume_locks), 1)
        self.tape.put_releaselockedvolume(['jgi', 'AG2910'], None)
        self.assertEqual(len(self.tape.divisions.get('jgi').pull_queue.volume_locks), 0)

    def test_Tape_get_tasklist(self):
        copy_queue = {'dna_w': [{'auto_uncompress': 0,
                                 'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                                 'file_date': datetime(2020, 12, 1, 12, 27, 27),
                                 'file_group': 'genome',
                                 'file_id': 11479542,
                                 'file_name': '3300038674_26.tar.gz',
                                 'file_owner': 'img',
                                 'file_path': '/global/dna/dm_archive/img/submissions/223350',
                                 'file_permissions': '0100644',
                                 'file_size': 1132794,
                                 'file_status_id': 1,
                                 'local_purge_days': 2,
                                 'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                                 'metadata_id': '5eecdfb86263bf2148833b40',
                                 'modified_dt': datetime(2021, 6, 7, 6, 11, 46),
                                 'origin_file_name': '3300038674_26.tar.gz',
                                 'origin_file_path': '/global/cfs/cdirs/m342/img/mbin_jamo_src_tarball_files',
                                 'remote_purge_days': None,
                                 'transaction_id': 1,
                                 'transfer_mode': 0,
                                 'user_save_till': datetime(2021, 6, 21, 6, 11, 46),
                                 'validate_mode': 0}],
                      'foo': [{'auto_uncompress': 0,
                               'created_dt': datetime(2020, 12, 1, 12, 27, 27),
                               'file_date': datetime(2020, 12, 1, 12, 27, 27),
                               'file_group': 'genome',
                               'file_id': 11479543,
                               'file_name': '3300038674_26-2.tar.gz',
                               'file_owner': 'img',
                               'file_path': '/global/dna/dm_archive/img/submissions/223350',
                               'file_permissions': '0100644',
                               'file_size': 1132794,
                               'file_status_id': 1,
                               'local_purge_days': 2,
                               'md5sum': 'acdfb3004514e62fa322840cf2c1b119',
                               'metadata_id': '5eecdfb86263bf2148833b40',
                               'modified_dt': datetime(2021, 6, 7, 6, 11, 46),
                               'origin_file_name': '3300038674_26-2.tar.gz',
                               'origin_file_path': '/path/to/remote/img/mbin_jamo_src_tarball_files',
                               'remote_purge_days': None,
                               'transaction_id': 1,
                               'transfer_mode': 0,
                               'user_save_till': datetime(2021, 6, 21, 6, 11, 46),
                               'validate_mode': 0}]}
        ingest_queue = {'nersc': [{'_call_source': 'file',
                                   '_callback': 'file_ingest',
                                   '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
                                   '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                   '_file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
                                   '_is_file': 1,
                                   '_is_folder': 0,
                                   '_metadata_ingest_id': '626b286a682a7f997d28e4a4',
                                   '_put_mode': 0,
                                   '_services': '[1]',
                                   '_status': 'new',
                                   'auto_uncompress': 0,
                                   'file_date': datetime(2022, 4, 28, 15, 34, 51),
                                   'file_group': 'qc_user',
                                   'file_id': 14452074,
                                   'file_ingest_id': 1001,
                                   'file_ingest_status_id': 22,
                                   'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
                                   'file_owner': 'qc_user',
                                   'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                                   'file_permissions': '0100755',
                                   'file_size': 3753,
                                   'local_purge_days': 90,
                                   'metadata_id': '626b286a682a7f997d28e4a5',
                                   'transfer_mode': 0,
                                   'validate_mode': 0}],
                        'foo': [{'_call_source': 'file',
                                 '_callback': 'file_ingest',
                                 '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
                                 '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                 '_file': '/path/to/remote/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
                                 '_is_file': 1,
                                 '_is_folder': 0,
                                 '_metadata_ingest_id': '626b286a682a7f997d28e4a4',
                                 '_put_mode': 0,
                                 '_services': '[1]',
                                 '_status': 'new',
                                 'auto_uncompress': 0,
                                 'file_date': datetime(2022, 4, 28, 15, 34, 51),
                                 'file_group': 'qc_user',
                                 'file_id': 14452075,
                                 'file_ingest_id': 1002,
                                 'file_ingest_status_id': 22,
                                 'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report-2.txt',
                                 'file_owner': 'qc_user',
                                 'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                                 'file_permissions': '0100755',
                                 'file_size': 3753,
                                 'local_purge_days': 90,
                                 'metadata_id': '626b286a682a7f997d28e4a5',
                                 'transfer_mode': 0,
                                 'validate_mode': 0}]}
        md5_queue = {'compute': [{'callback': 'local://put_file/3371978',
                                  'dt_modified': datetime(2014, 3, 28, 22, 59, 34),
                                  'file_path': '/projectb/scratch/jgi_dna/15.3371978.tar',
                                  'file_size': 202827386880,
                                  'md5_queue_id': 431849,
                                  'md5sum': None,
                                  'queue_status_id': 1}]}
        orphan_files = {'2': {'backup_records': [{'backup_record_id': 18774872,
                                                  'file_id': 14462356,
                                                  'file_name': '52689.3.420190.AGCTCCTA-AGCTCCTA.fastq.gz',
                                                  'file_path': '/global/dna/dm_archive/sdm/illumina/05/26/89'}],
                              'root_dir': '/global/dna/dm_archive/sdm/illumina/05/26/89',
                              'size': 959524387}}
        tar_queue = {'compute': [{'auto_uncompress': 0,
                                  'created_dt': datetime(2013, 4, 26, 17, 7, 29),
                                  'file_date': datetime(2012, 5, 18, 16, 50, 12),
                                  'file_group': 'pacbio',
                                  'file_id': 1660,
                                  'file_name': 'pbio-89.1031.fastq',
                                  'file_owner': 'smrt',
                                  'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/89',
                                  'file_permissions': '0100664',
                                  'file_size': 301326599,
                                  'file_status_id': 14,
                                  'local_purge_days': 90,
                                  'md5sum': 'c27d89f18849636b3d4437060bb4c91c',
                                  'metadata_id': '51d48dff067c014cd6e9e44f',
                                  'modified_dt': datetime(2022, 4, 13, 21, 4, 10),
                                  'origin_file_name': 'filtered_subreads.fastq',
                                  'origin_file_path': '/house/pacbio/jobs/019/019025/data',
                                  'remote_purge_days': None,
                                  'transaction_id': 108,
                                  'transfer_mode': 0,
                                  'user_save_till': datetime(2022, 5, 13, 0, 0),
                                  'validate_mode': 0}],
                     'foo': [{'auto_uncompress': 0,
                              'created_dt': datetime(2013, 4, 26, 17, 7, 29),
                              'file_date': datetime(2012, 5, 18, 16, 50, 12),
                              'file_group': 'pacbio',
                              'file_id': 1662,
                              'file_name': 'pbio-89.1031-3.fastq',
                              'file_owner': 'smrt',
                              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/89',
                              'file_permissions': '0100664',
                              'file_size': 301326599,
                              'file_status_id': 14,
                              'local_purge_days': 90,
                              'md5sum': 'c27d89f18849636b3d4437060bb4c91c',
                              'metadata_id': '51d48dff067c014cd6e9e44f',
                              'modified_dt': datetime(2022, 4, 13, 21, 4, 10),
                              'origin_file_name': 'filtered_subreads-3.fastq',
                              'origin_file_path': '/path/to/remote/jobs/019/019025/data',
                              'remote_purge_days': None,
                              'transaction_id': 108,
                              'transfer_mode': 0,
                              'user_save_till': datetime(2022, 5, 13, 0, 0),
                              'validate_mode': 0}]}

        actual = self.tape.get_tasklist(None, None)

        self.assertEqual(actual.get('jgi').get('copy_queue'), copy_queue)
        self.assertEqual(actual.get('jgi').get('ingest_queue'), ingest_queue)
        self.assertEqual(actual.get('jgi').get('md5_queue'), md5_queue)
        self.assertEqual(actual.get('jgi').get('tar_queue'), tar_queue)
        self.assertEqual(actual.get('jgi').get('orphan_files'), orphan_files)

    def test_Tape_get_tarrecordinfo(self):
        self.assertEqual(self.tape.get_tarrecordinfo([224967], None),
                         {'position_a': 1375, 'position_b': 14800029, 'volume': 'AG1583'})

    def test_Tape_get_taskstatus(self):
        expected = {'jgi': {'current_tasks': {},
                            'current_used_resources': {},
                            'services': {},
                            'tasks': {'copy': {'currently_running': 0,
                                               'file_size': 2265588,
                                               'record_count': 2},
                                      'delete': {'currently_running': 0,
                                                 'file_size': 0,
                                                 'record_count': 0},
                                      'ingest': {'currently_running': 0,
                                                 'file_size': 7506,
                                                 'record_count': 2},
                                      'md5': {'currently_running': 0,
                                              'file_size': 202827386880,
                                              'record_count': 1},
                                      'purge': {'currently_running': 0, 'file_size': 0, 'record_count': 0},
                                      'put': {'currently_running': 0, 'file_size': 0, 'record_count': 0},
                                      'tar': {'currently_running': 0,
                                              'file_size': 602653198,
                                              'record_count': 2}}},
                    'resources_gone': {}}

        self.assertEqual(self.tape.get_taskstatus(['reset'], None), expected)

    @patch('tape.restful.RestServer')
    def test_Tape_get_tape_status(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.hsi_state.isup.return_value = True
        self.cursor.fetchall.side_effect = [
            [{'N': 1}],
            [{'N': 2}],
            [{'N': 3}],
            [{'N': 4}],
            [{'N': 5}],
            [{'N': 6}],
            [{'N': 7}],
            [{'N': 8}],
            [{'N': 9}],
            [{'N': 10}],
            [{'priority': 4, 'min': datetime(2022, 5, 8, 10, 20, 20), 'dt_modified': datetime(2022, 5, 7, 18, 2, 24),
              'n': 4710}],
            [{'label': 'Ingest', 'N': 113, 'id': 1, 'status': 'REGISTERED'}],
            [{'requestor': 'portal/foobar@foo.com', 'N': 22814, 'gb': 85.947542876}],
            [{'tab': 'Pulll_Queue', 'state': 'In Progress', 'group_vol': 'AG8046', 'status_id': 0, 'queue': 6,
              'section': 4, 'N': 1, 'min_date': datetime(2022, 1, 24, 15, 20, 30)}],
            [{'requestor': 'portal/foobar@foo.com', 'N': 71874, 'gb': 16174.277529125}],
            [{'file_owner': 'foobar', 'n': 1514, 'gb': 835.463351332}],
            [{'ymdh': datetime(22, 5, 9, 6), 'vol': 277, 'N': 2331, 'gb': 1331.300473485}],
            [{'ymdh': datetime(22, 5, 9, 6), 'N': 1156, 'gb': 92.501109338}],
        ]
        expected = {'active': [{'N': 22814,
                                'gb': 85.947542876,
                                'requestor': 'portal/foobar@foo.com'}],
                    'active_states': [{'N': 113,
                                       'id': 1,
                                       'label': 'Ingest',
                                       'status': 'REGISTERED'}],
                    'age': [{'N': 1,
                             'group_vol': 'AG8046',
                             'min_date': datetime(2022, 1, 24, 15, 20, 30),
                             'queue': 6,
                             'section': 4,
                             'state': 'In Progress',
                             'status_id': 0,
                             'tab': 'Pulll_Queue'}],
                    'dna_free_tb': 4.999999e-05,
                    'enabled_queues': {'jgi': []},
                    'error': 22,
                    'foot_print': 50,
                    'hpss': 0,
                    'ingest': 1,
                    'other': 8,
                    'prep': 2,
                    'publish': [{'file_owner': 'foobar', 'gb': 835.463351332, 'n': 1514}],
                    'pull': 1,
                    'pull_stats': [{'N': 2331,
                                    'gb': 1331.300473485,
                                    'vol': 277,
                                    'ymdh': datetime(22, 5, 9, 6, 0)}],
                    'put': 3,
                    'put_stats': [{'N': 1156,
                                   'gb': 92.501109338,
                                   'ymdh': datetime(22, 5, 9, 6, 0)}],
                    'queue_status': [{'dt_modified': datetime(2022, 5, 7, 18, 2, 24),
                                      'min': datetime(2022, 5, 8, 10, 20, 20),
                                      'n': 4710,
                                      'priority': 4}],
                    'recent': 10,
                    'request': [{'N': 71874,
                                 'gb': 16174.277529125,
                                 'requestor': 'portal/foobar@foo.com'}],
                    'requested_restores': [{'N': 10, 'gb': 50, 'vol': 5, 'ymdh': '22-10-15 12'}],
                    'vol': 9}

        self.assertEqual(self.tape.get_tape_status(None, None), expected)

    @patch('pymysql.connect')
    def test_Tape_post_reset_failed(self, connect):
        def assert_value_in_queue(value, queue, features):
            q = queue.feature_queues.get(features)
            if isinstance(q, list):
                for q2 in q:
                    if value in q2:
                        return
            else:
                if value in q:
                    return
            self.fail(f'{value} not in queue {queue}')

        connect.return_value = self.connection
        file_ingest_failure_stats_not_remote = {'file_ingest_id': 1002, 'file_ingest_status_id': 20, 'file_id ': 14452073, 'file_size': 1570,
                                                'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90,
                                                'auto_uncompress': 0, '_put_mode': 0,
                                                '_is_folder': 0, '_is_file': 1,
                                                '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                                '_file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods.txt',
                                                '_services': '[1]',
                                                '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods.txt',
                                                '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
                                                'metadata_id': '626b286a682a7f997d28e4a7',
                                                '_metadata_ingest_id': '626b286a682a7f997d28e4a6',
                                                'file_date': datetime(2022, 4, 28, 15, 34, 40), 'file_owner': 'qc_user',
                                                'file_group': 'qc_user',
                                                'file_permissions': '0100644',
                                                'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods.txt',
                                                'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'}
        file_ingest_failure_stats_remote = {'file_ingest_id': 2002, 'file_ingest_status_id': 20, 'file_id ': 24452073, 'file_size': 1570,
                                            'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90,
                                            'auto_uncompress': 0, '_put_mode': 0,
                                            '_is_folder': 0, '_is_file': 1,
                                            '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                            '_file': '/path/to/remote/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods-2.txt',
                                            '_services': '[1]',
                                            '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods-2.txt',
                                            '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
                                            'metadata_id': '626b286a682a7f997d28e4a7',
                                            '_metadata_ingest_id': '626b286a682a7f997d28e4a6',
                                            'file_date': datetime(2022, 4, 28, 15, 34, 40), 'file_owner': 'qc_user',
                                            'file_group': 'qc_user',
                                            'file_permissions': '0100644',
                                            'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-methods-2.txt',
                                            'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'}
        file_ingest_failure_file_missing_not_remote = {'file_ingest_id': 1003, 'file_ingest_status_id': 22, 'file_id': 14452075, 'file_size': 7335,
                                                       'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90,
                                                       'auto_uncompress': 0, '_put_mode': 0,
                                                       '_is_folder': 0, '_is_file': 1,
                                                       '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                                       '_file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA.sh',
                                                       '_services': '[1]',
                                                       '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA.sh',
                                                       '_call_source': 'file', '_status': 'new',
                                                       '_callback': 'file_ingest',
                                                       'metadata_id': '626b286a682a7f997d28e4a9',
                                                       '_metadata_ingest_id': '626b286a682a7f997d28e4a8',
                                                       'file_date': datetime(2022, 4, 28, 15, 20, 17),
                                                       'file_owner': 'qc_user', 'file_group': 'qc_user',
                                                       'file_permissions': '0100644',
                                                       'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA.sh',
                                                       'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'}
        file_ingest_failure_file_missing_remote = {'file_ingest_id': 2003, 'file_ingest_status_id': 22, 'file_id': 24452075, 'file_size': 7335,
                                                   'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90,
                                                   'auto_uncompress': 0, '_put_mode': 0,
                                                   '_is_folder': 0, '_is_file': 1,
                                                   '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                                   '_file': '/path/to/remote/rqc/pipelines/filter/archive/03/14/72/79/52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA-2.sh',
                                                   '_services': '[1]',
                                                   '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621/52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA-2.sh',
                                                   '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
                                                   'metadata_id': '626b286a682a7f997d28e4a9',
                                                   '_metadata_ingest_id': '626b286a682a7f997d28e4a8',
                                                   'file_date': datetime(2022, 4, 28, 15, 20, 17),
                                                   'file_owner': 'qc_user', 'file_group': 'qc_user',
                                                   'file_permissions': '0100644',
                                                   'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filter_cmd-RNA-2.sh',
                                                   'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79'}
        file_ingest_lost_not_remote = {'file_ingest_id': 1004, 'file_ingest_status_id': 1, 'file_id': 14452077, 'file_size': 2194710185,
                                       'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90,
                                       'auto_uncompress': 0, '_put_mode': 0,
                                       '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                       '_file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/80/52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq.gz',
                                       '_services': '[1]',
                                       '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400622/52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq.gz',
                                       '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
                                       'metadata_id': '626b286a682a7f997d28e4ac',
                                       '_metadata_ingest_id': '626b286a682a7f997d28e4ab',
                                       'file_date': datetime(2022, 4, 28, 15, 10, 1), 'file_owner': 'qc_user',
                                       'file_group': 'qc_user',
                                       'file_permissions': '0100644',
                                       'file_name': '52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq.gz',
                                       'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/80'}
        file_ingest_lost_remote = {'file_ingest_id': 2004, 'file_ingest_status_id': 1, 'file_id': 24452077, 'file_size': 2194710185,
                                   'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 90, 'auto_uncompress': 0,
                                   '_put_mode': 0,
                                   '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 5, 13, 54, 19),
                                   '_file': '/path/to/remote/rqc/pipelines/filter/archive/03/14/72/80/52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq-2.gz',
                                   '_services': '[1]',
                                   '_destination': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400622/52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq-2.gz',
                                   '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
                                   'metadata_id': '626b286a682a7f997d28e4ac',
                                   '_metadata_ingest_id': '626b286a682a7f997d28e4ab',
                                   'file_date': datetime(2022, 4, 28, 15, 10, 1), 'file_owner': 'qc_user',
                                   'file_group': 'qc_user',
                                   'file_permissions': '0100644',
                                   'file_name': '52687.1.419438.AGTAGTCC-AGTAGTCC.filter-RNA.fastq.gz',
                                   'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/80'}
        file_copy_failed_not_remote = {'file_id': 14502052, 'transaction_id': 1, 'file_name': 'Ga0535814_imgap.info',
                                       'file_path': '/global/dna/dm_archive/img/submissions/268263',
                                       'origin_file_name': 'Ga0535814_imgap.info',
                                       'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268263',
                                       'file_size': 405,
                                       'file_date': datetime(2022, 5, 6, 22, 22, 13), 'file_owner': 'gbp',
                                       'file_group': 'img',
                                       'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None,
                                       'file_status_id': 5,
                                       'created_dt': datetime(2022, 5, 8, 21, 3, 36),
                                       'modified_dt': datetime(2022, 5, 8, 21, 18, 59), 'validate_mode': 0,
                                       'user_save_till': None,
                                       'metadata_id': '6278922dc2c506c5afdf7a94', 'auto_uncompress': 0,
                                       'remote_purge_days': None,
                                       'transfer_mode': 0}
        file_copy_failed_remote = {'file_id': 24502052, 'transaction_id': 1, 'file_name': 'Ga0535814_imgap-2.info',
                                   'file_path': '/global/dna/dm_archive/img/submissions/268263',
                                   'origin_file_name': 'Ga0535814_imgap-2.info',
                                   'origin_file_path': '/path/to/remote/img/annotated_submissions/268263',
                                   'file_size': 405,
                                   'file_date': datetime(2022, 5, 6, 22, 22, 13), 'file_owner': 'gbp',
                                   'file_group': 'img',
                                   'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None,
                                   'file_status_id': 5,
                                   'created_dt': datetime(2022, 5, 8, 21, 3, 36),
                                   'modified_dt': datetime(2022, 5, 8, 21, 18, 59), 'validate_mode': 0,
                                   'user_save_till': None,
                                   'metadata_id': '6278922dc2c506c5afdf7a94', 'auto_uncompress': 0,
                                   'remote_purge_days': None,
                                   'transfer_mode': 0}
        file_tar_failed_not_remote = {'file_id': 2001, 'transaction_id': 114, 'file_name': 'pbio-99.1183-0.h5',
                                      'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/99',
                                      'origin_file_name': 'm120519_011136_42173_c100323092550000001523016809061240_s2_p0.bas.h5',
                                      'origin_file_path': '/house/pacbio/runs/PB02_Run0312_94/A01_1/Analysis_Results',
                                      'file_size': 505964451,
                                      'file_date': datetime(2012, 5, 18, 20, 20, 58), 'file_owner': 'smrt',
                                      'file_group': 'pacbio',
                                      'file_permissions': '0100644', 'local_purge_days': 90,
                                      'md5sum': '0a4d1f55de2f90e5c46aad7a493738b9',
                                      'file_status_id': 10, 'created_dt': datetime(2013, 4, 26, 17, 8, 17),
                                      'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0,
                                      'user_save_till': None,
                                      'metadata_id': '51d48e26067c014cd6e9e5a4', 'auto_uncompress': 0,
                                      'remote_purge_days': None,
                                      'transfer_mode': 0}
        file_tar_failed_remote = {'file_id': 4001, 'transaction_id': 114, 'file_name': 'pbio-99.1183-0-4.h5',
                                  'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/99',
                                  'origin_file_name': 'm120519_011136_42173_c100323092550000001523016809061240_s2_p0.bas-4.h5',
                                  'origin_file_path': '/path/to/remote/PB02_Run0312_94/A01_1/Analysis_Results',
                                  'file_size': 505964451,
                                  'file_date': datetime(2012, 5, 18, 20, 20, 58), 'file_owner': 'smrt',
                                  'file_group': 'pacbio',
                                  'file_permissions': '0100644', 'local_purge_days': 90,
                                  'md5sum': '0a4d1f55de2f90e5c46aad7a493738b9',
                                  'file_status_id': 10, 'created_dt': datetime(2013, 4, 26, 17, 8, 17),
                                  'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0,
                                  'user_save_till': None,
                                  'metadata_id': '51d48e26067c014cd6e9e5a4', 'auto_uncompress': 0,
                                  'remote_purge_days': None,
                                  'transfer_mode': 0}
        file_tar_deleted_not_remote = {'file_id': 2002, 'transaction_id': 114, 'file_name': 'pbio-99.1183-1.h5',
                                       'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/99',
                                       'origin_file_name': 'm120519_011136_42173_c100323092550000001523016809061240_s1_p0.bas.h5',
                                       'origin_file_path': '/house/pacbio/runs/PB02_Run0312_94/A01_1/Analysis_Results',
                                       'file_size': 583930186,
                                       'file_date': datetime(2012, 5, 18, 19, 27, 12), 'file_owner': 'smrt',
                                       'file_group': 'pacbio',
                                       'file_permissions': '0100644', 'local_purge_days': 90,
                                       'md5sum': '5731670f721d71585dad473f99a74b6d',
                                       'file_status_id': 11, 'created_dt': datetime(2013, 4, 26, 17, 8, 17),
                                       'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0,
                                       'user_save_till': None,
                                       'metadata_id': '51d48e26067c014cd6e9e5a5', 'auto_uncompress': 0,
                                       'remote_purge_days': None,
                                       'transfer_mode': 0}
        file_tar_deleted_remote = {'file_id': 4002, 'transaction_id': 114, 'file_name': 'pbio-99.1183-1-4.h5',
                                   'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/99',
                                   'origin_file_name': 'm120519_011136_42173_c100323092550000001523016809061240_s1_p0.bas-4.h5',
                                   'origin_file_path': '/path/to/remote/PB02_Run0312_94/A01_1/Analysis_Results',
                                   'file_size': 583930186,
                                   'file_date': datetime(2012, 5, 18, 19, 27, 12), 'file_owner': 'smrt',
                                   'file_group': 'pacbio',
                                   'file_permissions': '0100644', 'local_purge_days': 90,
                                   'md5sum': '5731670f721d71585dad473f99a74b6d',
                                   'file_status_id': 11, 'created_dt': datetime(2013, 4, 26, 17, 8, 17),
                                   'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0,
                                   'user_save_till': None,
                                   'metadata_id': '51d48e26067c014cd6e9e5a5', 'auto_uncompress': 0,
                                   'remote_purge_days': None,
                                   'transfer_mode': 0}
        pull_queue_failed = {'pull_queue_id': 11544316, 'file_id': 6016691, 'queue_status_id': 4,
                             'dt_modified': datetime(2022, 5, 8, 10, 20, 19),
                             'requestor': 'data-portal/sbenler@gmail.com',
                             'priority': 6, 'tar_record_id': 173305, 'volume': 'AG2798', 'position_a': 3473,
                             'position_b': 0}
        pull_queue_prep_failed = {'pull_queue_id': 11573194, 'file_id': 7298117, 'queue_status_id': 6,
                                  'dt_modified': datetime(2022, 5, 8, 10, 20, 20),
                                  'requestor': 'data-portal/sbenler@gmail.com',
                                  'priority': 6, 'tar_record_id': 214243, 'volume': 'AG2798', 'position_a': 40,
                                  'position_b': 0}
        pull_queue_prep_lost = {'pull_queue_id': 11657721, 'file_id': 5128869, 'queue_status_id': 1,
                                'dt_modified': datetime(2022, 5, 9, 6, 59, 56),
                                'requestor': 'portal/wuxy2@shanghaitech.edu.cn',
                                'priority': 4, 'tar_record_id': 143723, 'volume': None, 'position_a': None,
                                'position_b': None}
        pull_queue_hanging = {'pull_queue_id': 11445141, 'file_id': 13590906, 'queue_status_id': 1,
                              'dt_modified': datetime(2022, 5, 3, 11, 11, 34), 'requestor': 'portal/larkinsa@uci.edu',
                              'priority': 6, 'tar_record_id': 401092, 'volume': 'AG1524', 'position_a': 160,
                              'position_b': 0}
        self.cursor.fetchall.side_effect = [
            [file_ingest_failure_stats_not_remote, file_ingest_failure_stats_remote],
            [file_ingest_failure_file_missing_not_remote, file_ingest_failure_file_missing_remote],
            [file_ingest_lost_not_remote, file_ingest_lost_remote],
            [file_copy_failed_not_remote, file_copy_failed_remote],
            [file_tar_failed_not_remote, file_tar_failed_remote],
            [file_tar_deleted_not_remote, file_tar_deleted_remote],
            [None],
            [pull_queue_failed],
            [pull_queue_prep_failed],
            [pull_queue_prep_lost],
            [pull_queue_hanging],
        ]

        self.tape.post_reset_failed(None, None)

        assert_value_in_queue(file_ingest_failure_stats_not_remote, self. tape.divisions.get('jgi').ingest_queue, 'nersc')
        assert_value_in_queue(file_ingest_failure_stats_remote, self. tape.divisions.get('jgi').ingest_queue, 'foo')
        assert_value_in_queue(file_ingest_failure_file_missing_not_remote, self. tape.divisions.get('jgi').ingest_queue, 'nersc')
        assert_value_in_queue(file_ingest_failure_file_missing_remote, self. tape.divisions.get('jgi').ingest_queue, 'foo')
        assert_value_in_queue(file_ingest_lost_not_remote, self. tape.divisions.get('jgi').ingest_queue, 'nersc')
        assert_value_in_queue(file_ingest_lost_remote, self. tape.divisions.get('jgi').ingest_queue, 'foo')
        assert_value_in_queue(file_copy_failed_not_remote, self. tape.divisions.get('jgi').copy_queue, 'dna_w')
        assert_value_in_queue(file_copy_failed_remote, self. tape.divisions.get('jgi').copy_queue, 'foo')
        assert_value_in_queue(file_tar_failed_not_remote, self. tape.divisions.get('jgi').tar_queue, 'compute')
        assert_value_in_queue(file_tar_failed_remote, self. tape.divisions.get('jgi').tar_queue, 'foo')
        assert_value_in_queue(file_tar_deleted_not_remote, self. tape.divisions.get('jgi').tar_queue, 'compute')
        assert_value_in_queue(file_tar_deleted_remote, self. tape.divisions.get('jgi').tar_queue, 'foo')
        for c in [
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 1002)),
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 2002)),
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 1003)),
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 2003)),
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 1004)),
            call.execute('update file_ingest set file_ingest_status_id = %s where file_ingest_id = %s', (1, 2004)),
            call.execute('update file set file_status_id = %s where file_id = %s', (2, 14502052)),
            call.execute('update file set file_status_id = %s where file_id = %s', (2, 24502052)),
            call.execute('update file set file_status_id = %s where file_id = %s', (14, 2001)),
            call.execute('update file set file_status_id = %s where file_id = %s', (14, 4001)),
            call.execute('update file set file_status_id = %s where file_id = %s', (14, 2002)),
            call.execute('update file set file_status_id = %s where file_id = %s', (14, 4002)),
            call.execute('update file set file_status_id = %s where file_status_id = %s and division = %s', [6, 9, 'jgi']),
            call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (1, 11544316)),
            call.execute('update file set file_status_id = %s where file_id = %s', (28, 6016691)),
            call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (1, 11573194)),
            call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (1, 11657721)),
            call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (1, 11445141)),
            call.execute('update file set file_status_id = %s where file_id = %s', (28, 13590906)),
            call.execute('select * from file where file_status_id = %s and division = %s', [2, 'jgi']),
            call.execute('select * from file where file_status_id = %s and division = %s', [14, 'jgi']),
            call.execute('select * from md5_queue where queue_status_id = %s and division = %s', [1, 'jgi']),
            call.execute(
                'select f.local_purge_days, f.origin_file_path, f.origin_file_name, f.file_id, file_name, file_path, service, backup_record_id, file_status_id, backup_record_status_id, file_size, division from backup_record b left join file f on f.file_id = b.file_id where f.file_status_id in (%s,%s) and backup_record_status_id in (%s,%s) and dt_to_release is null and division = %s',
                [6, 7, 1, 2, 'jgi']),
            call.execute('select distinct volume from pull_queue join file using(file_id) where queue_status_id = %s and division = %s', [2, 'jgi']),
            call.execute('select * from file_ingest where file_ingest_status_id = %s and division = %s ', [20, 'jgi']),
            call.execute('select * from file_ingest where file_ingest_status_id = %s and division = %s ', [21, 'jgi']),
            call.execute(
                'select * from file_ingest where file_ingest_status_id = %s and division = %s and _dt_modified < now() - interval 30 minute',
                [1, 'jgi']),
            call.execute('select * from file where file_status_id = %s and division = %s ', [5, 'jgi']),
            call.execute('select * from file where file_status_id = %s and division = %s ', [17, 'jgi']),
            call.execute('select * from file where file_status_id = %s and division = %s ', [11, 'jgi']),
            call.execute('select pull_queue_id, file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s ', [4, 'jgi']),
            call.execute('select pull_queue_id, file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s ', [6, 'jgi']),
            call.execute(
                'select pull_queue_id, file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s and dt_modified < now() - interval 10 minute', [7, 'jgi']),
            call.execute(
                'select pull_queue_id, file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s and dt_modified < now() - interval 3 hour',
                [2, 'jgi']),
        ]:
            self.assertIn(c, self.cursor.mock_calls)

    def test_Tape_get_cachestatus(self):
        self.assertEqual(self.tape.get_cachestatus(None, None), {'requestor_counts': {'foobar': 5},
                                                                 'tar_record_cache': {224967: {'position_a': 1375,
                                                                                               'position_b': 14800029,
                                                                                               'volume': 'AG1583'}}})

    def test_Tape_get_pullcompletestats(self):
        self.cursor.fetchall.side_effect = [
            [{'files': 11214291, 'gbytes': 10773873.448}]
        ]

        self.assertEqual(self.tape.get_pullcompletestats([2], None), {'files': 11214291, 'gbytes': 10773873.448})

    @parameterized.expand([
        ('safe', 2, True),
        ('unsafe', 0, False),
    ])
    def test_Tape_get_filesafe(self, _description, count, safe):
        self.cursor.fetchall.side_effect = [
            [{'c': count}]
        ]

        self.assertEqual(self.tape.get_filesafe([14452073], None), safe)

    @parameterized.expand([
        ('missing_file_id', [], None),
        ('missing_file_id_not_found', [14452073], []),
    ])
    def test_Tape_get_filesafe_missing_file_id(self, _description, args, sql_response):
        if sql_response is not None:
            self.cursor.fetchall.side_effect = [
                sql_response
            ]

        self.assertRaises(common.HttpException, self.tape.get_filesafe, args, None)

    @parameterized.expand([
        ('file_large', tape.MIN_SINGLE_SIZE + 1),
        ('file_small', tape.MIN_SINGLE_SIZE - 1)
    ])
    def test_Tape_add_file_add_to_put_queue(self, _description, file_size):
        self.tape.add_file({'local_purge_days': 5,
                            'origin_file_path': '/origin/file/path',
                            'origin_file_name': 'origin_file_name.gz', 'file_id': 1,
                            'file_name': 'file_name.gz',
                            'file_path': '/file/path',
                            'backup_records': [{'backup_record_id': 3, 'service': 2}], 'file_status_id': 6,
                            'backup_record_status_id': 1,
                            'division': 'jgi',
                            'file_size': file_size})

        self.assertIn({'file_name': 'file_name.gz', 'file_id': 1, 'backup_record_id': 3, 'file_path': '/file/path'},
                      self.tape.divisions.get('jgi').put_queue.feature_queues.get('hsi_2')[0].get('records'))

    @parameterized.expand([
        ('existing_orphan_files', '2'),
        ('new_orphan_files', '3'),
    ])
    def test_Tape_add_file_not_large_enough(self, _description, service_id):
        self.tape.add_file({'local_purge_days': 5,
                            'origin_file_path': '/origin/file/path',
                            'origin_file_name': 'origin_file_name.gz', 'file_id': 1,
                            'file_name': 'file_name.gz',
                            'file_path': '/file/path',
                            'backup_records': [{'backup_record_id': 3, 'service': service_id}], 'file_status_id': 6,
                            'backup_record_status_id': 1,
                            'file_size': 1, 'division': 'jgi'})

        self.assertIn({'file_name': 'file_name.gz', 'file_id': 1, 'backup_record_id': 3, 'file_path': '/file/path'},
                      self.tape.divisions.get('jgi').orphan_files.get(service_id).get('backup_records'))
        self.assertEqual(len(self.tape.divisions.get('jgi').put_queue.feature_queues), 0)

    @patch('tape.datetime')
    def test_Tape_repository_footprint(self, datetime_mock):
        self.cursor.fetchall.side_effect = [
            [{'files': 19, 'disk_usage': 79691776}],
            [{'files': 1, 'disk_usage': 1776}],
        ]
        datetime_mock.datetime.today.return_value = datetime(2000, 1, 2, 3, 4, 5)

        self.tape.repository_footprint()

        self.assertEqual(self.tape.disk_usage, {'bytes_free': 49998224,
                                                'bytes_used': 50001776,
                                                'date_updated': datetime(2000, 1, 2, 3, 4, 5),
                                                'disk_reserve': 10,
                                                'disk_usage_files': 79691776,
                                                'disk_usage_files_restoring': 1776,
                                                'disk_usage_other': -29690000,
                                                'dna_free_tb': 4.9998224e-05,
                                                'files': 19,
                                                'files_restoring': 1})

    @patch('task.datetime')
    def test_Tape_monitor(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        task_manager = self.tape.divisions.get('jgi').task_manager
        task_manager.add_service('service_id', 5, 'service_host')
        task_manager.services.get('service_id')['heartbeat'] = datetime(2000, 1, 2, 1, 4, 5)
        task_manager.current_tasks = {'task_id': {'service': 'service_id', 'task': 'copy', 'data': {'name': 'foobar'}}}

        self.tape.monitor()

        self.assertNotIn('service_id', task_manager.services)

    def test_Tape_fix_stalled_files(self):
        self.tape.fix_stalled_files()

        self.assertIn(call.execute(
            'update file f set file_status_id=%s where file_status_id=%s and validate_mode=0 and f.md5sum is not null and not exists (select backup_record_id from backup_record b where b.file_id=f.file_id and backup_record_status_id<>%s)',
            (8, 7, 4)), self.cursor.mock_calls)
        self.assertIn(call.execute('update backup_record set backup_record_status_id=%s where backup_record_status_id=%s and dt_modified<date_sub(curdate(), interval 1 day)', (2, 3)),
                      self.cursor.mock_calls)

    def test_Tape_delete_records(self):
        self.cursor.fetchall.side_effect = [
            [{'tar_record_id': 31, 'file_date': datetime(2012, 3, 27, 19, 25, 26), 'service': 1,
              'backup_record_id': 2001, 'backup_record_status_id': 4, 'file_id': 1001,
              'remote_file_name': 'pbio-66.743-0.h5', 'remote_file_path': 'pbio-66.743-0.h5',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/66', 'file_name': 'pbio-66.743-0.h5'}]
        ]

        self.tape.delete_records()

        self.assertIn(call.execute('delete from backup_record_status_history where backup_record_id in (2001)', ()),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('delete from backup_record where backup_record_id in (2001)', ()),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('delete from file_status_history where file_id in (1001)', ()),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('delete from pull_queue where file_id in (1001)', ()),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('delete from file where file_id in (1001)', ()),
                      self.cursor.mock_calls)

    def test_Tape_release_backup_records(self):
        self.cursor.fetchall.side_effect = [
            [{'local_purge_days': 2, 'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268204',
              'origin_file_name': 'Ga0506519_trna.gff', 'file_id': 14497587, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'service': 1, 'backup_record_id': 18810203,
              'file_status_id': 7, 'backup_record_status_id': 3, 'file_size': 3587990, 'division': 'jgi'}]
        ]

        self.tape.release_backup_records()

        self.assertIn({'file_name': 'Ga0506519_trna.gff', 'file_id': 14497587, 'backup_record_id': 18810203, 'file_path': '/global/dna/dm_archive/img/submissions/268204'},
                      self.tape.divisions.get('jgi').orphan_files.get('1').get('backup_records'))

    def test_Tape_refresh_tar_info(self):
        self.cursor.fetchall.side_effect = [
            [{'tar_record_id': 21956, 'volume': 'EP3107', 'position_a': 1553, 'position_b': 0}]
        ]

        self.tape.refresh_tar_info()

        self.assertEqual(self.tape.tar_record_info, {21956: {'volume': 'EP3107', 'position_b': 0, 'position_a': 1553}})

    def test_Tape_reset_priority_counts(self):
        self.tape.reset_priority_counts()

        self.assertEqual(len(self.tape.requestor_counts), 0)

    def test_Tape_refresh_priority_counts(self):
        self.cursor.fetchall.side_effect = [
            [{'requestor': 'foobar', 'n': 30799}]
        ]

        self.tape.refresh_priority_counts()

        self.assertEqual(self.tape.requestor_counts, {'foobar': 30799})

    @parameterized.expand([
        ('no_source_no_existing_pull_requests', None, [], []),
        ('source_no_pending_egress_record', 'my_source', [{'cnt': 0}],
         [call.execute('select count(*) as cnt from egress where file_id=%s and source=%s and egress_status_id in (%s, %s) limit 500',
                       [14497587, 'my_source', 1, 2]),
          call.execute('insert into egress(file_id, egress_status_id, requestor, source, request_id) values(%s, %s, %s, %s, %s)',
                       (14497587, 1, 'foobar', 'my_source', 1)),
          ]
         ),
        ('source_pending_egress_record', 'my_source', [{'cnt': 1}],
         [call.execute('select count(*) as cnt from egress where file_id=%s and source=%s and egress_status_id in (%s, %s) limit 500',
                       [14497587, 'my_source', 1, 2]),
          ]
         ),
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_add_to_pull_queue(self, _description, source, egress_check_value, expected_egress_calls, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.lastrowid = 1
        egress_check_value_return_value = [egress_check_value] if source else []
        self.cursor.fetchall.side_effect = egress_check_value_return_value + [
            [{'tar_record_id': 224967}],
            [],
            [{'file_id': 14497587, 'transaction_id': 1, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'origin_file_name': 'Ga0506519_trna.gff',
              'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268204', 'file_size': 3587990,
              'file_date': datetime(2022, 5, 6, 20, 43, 52), 'file_owner': 'gbp', 'file_group': 'img',
              'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': 'b059f4bd49e682dfd55ef3e7f3e43ddf',
              'file_status_id': 7, 'created_dt': datetime(2022, 5, 7, 9, 30, 46),
              'modified_dt': datetime(2022, 5, 8, 10, 2, 17), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62769e4d945720fe9292369e', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'file_status': 'BACKUP_IN_PROGRESS', 'dt_to_purge': None}],
            [{'backup_record_id': 18810203, 'file_id': 14497587, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 3, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 8, 10, 2, 17), 'dt_to_release': None}],
            [{'file_status_history_id': 140638665, 'file_id': 14497587, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 7, 9, 30, 46),
              'dt_end': datetime(2022, 5, 7, 9, 30, 46)}],
        ]

        self.assertTrue(
            self.tape.add_to_pull_queue({'file_id': 14497587, 'file_status_id': 10, 'file_size': 100}, 5, 'foobar',
                                        source))
        for c in expected_egress_calls:
            self.assertIn(c, self.cursor.mock_calls)
        self.assertEqual({'foobar': 6}, self.tape.requestor_counts)

    @parameterized.expand([
        ('no_existing_pull_requests', [], {'foobar': 5}, [], {'foobar': 5}),
        ('existing_pull_requests_lower_priority', [{'priority': 1, 'requestor': 'foobar'}], {'foobar': 5},
         [], {'foobar': 5}),
        ('existing_pull_requests_higher_priority_previous_requestor_count_greater_than_1',
         [{'priority': 8, 'requestor': 'foobar', 'pull_queue_id': 123}], {'foobar': 5},
         [call.execute('update pull_queue set priority=%s, requestor=%s where pull_queue_id=%s', (2, 'foo', 123))],
         {'foobar': 4, 'foo': 1}),
        ('existing_pull_requests_higher_priority_previous_requestor_count_equal_1',
         [{'priority': 8, 'requestor': 'foobar', 'pull_queue_id': 123}], {'foobar': 1},
         [call.execute('update pull_queue set priority=%s, requestor=%s where pull_queue_id=%s', (2, 'foo', 123))],
         {'foo': 1}),
        ('existing_pull_requests_higher_priority_previous_requestor_count_not_in_memory',
         [{'priority': 8, 'requestor': 'foobar', 'pull_queue_id': 123}], {},
         [call.execute('update pull_queue set priority=%s, requestor=%s where pull_queue_id=%s', (2, 'foo', 123))],
         {'foo': 1}),
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_addToPullQueue_pull_queue_db(self, _description, existing_pq_records, requestor_counts,
                                               expected_pq_updates, expected_requestor_counts, restserver_mock):
        server_mock = Mock()
        server_mock.run_method.return_value = {'foo': 'bar'}
        restserver_mock.Instance.return_value = server_mock
        self.tape.use_db_pull_tasks = True
        self.cursor.lastrowid = 1
        self.cursor.fetchall.side_effect = [existing_pq_records]
        self.tape.requestor_counts = requestor_counts
        self.tape.config.queue_2_match = 'foo'

        self.assertTrue(
            self.tape.add_to_pull_queue({'file_id': 14497587, 'file_status_id': 28, 'file_size': 100}, 5, 'foo'))
        self.assertEqual(expected_requestor_counts, self.tape.requestor_counts)
        for c in expected_pq_updates:
            self.assertIn(c, self.cursor.mock_calls)

    @patch('tape.restful.RestServer')
    def test_Tape_post_grouprestore(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.lastrowid = 1
        self.cursor.fetchall.side_effect = [
            [{'file_id': 14497587, 'file_status_id': 10, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'file_size': 3587990},
             {'file_id': 14452074, 'file_status_id': 13,
              'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              'file_path': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621', 'file_size': 3753}],
            [{'tar_record_id': 224967}],
            [],
            [{'file_id': 14497587, 'transaction_id': 1, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'origin_file_name': 'Ga0506519_trna.gff',
              'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268204', 'file_size': 3587990,
              'file_date': datetime(2022, 5, 6, 20, 43, 52), 'file_owner': 'gbp', 'file_group': 'img',
              'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': 'b059f4bd49e682dfd55ef3e7f3e43ddf',
              'file_status_id': 7, 'created_dt': datetime(2022, 5, 7, 9, 30, 46),
              'modified_dt': datetime(2022, 5, 8, 10, 2, 17), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62769e4d945720fe9292369e', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'file_status': 'BACKUP_IN_PROGRESS', 'dt_to_purge': None}],
            [{'backup_record_id': 18810203, 'file_id': 14497587, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 3, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 8, 10, 2, 17), 'dt_to_release': None}],
            [{'file_status_history_id': 140638665, 'file_id': 14497587, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 7, 9, 30, 46),
              'dt_end': datetime(2022, 5, 7, 9, 30, 46)}],
            [{'file_id': 14452074, 'transaction_id': 1,
              'file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              'file_path': '/global/dna/dm_archive/rqc/analyses-40/AUTO-400621',
              'origin_file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt',
              'origin_file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79', 'file_size': 3753,
              'file_date': datetime(2022, 4, 28, 15, 34, 51), 'file_owner': 'qc_user', 'file_group': 'qc_user',
              'file_permissions': '0100755', 'local_purge_days': 90, 'md5sum': '7b7bc475335673a767e99963aa1c494e',
              'file_status_id': 13, 'created_dt': datetime(2022, 4, 28, 16, 51, 17),
              'modified_dt': datetime(2022, 5, 8, 0, 5, 45), 'validate_mode': 0,
              'user_save_till': datetime(2022, 5, 21, 0, 0), 'metadata_id': '626b286a682a7f997d28e4a5',
              'auto_uncompress': 0, 'remote_purge_days': None, 'transfer_mode': 0, 'file_status': 'RESTORED',
              'dt_to_purge': None}],
            [{'backup_record_id': 18763802, 'file_id': 14452074, 'service': 1,
              'remote_file_name': '52687.1.419438.TACGCCTT-TACGCCTT.filtered-report.txt.18763802',
              'remote_file_path': '.', 'tar_record_id': 439787, 'backup_record_status_id': 4, 'md5sum': None,
              'dt_modified': datetime(2022, 4, 29, 17, 9, 6), 'dt_to_release': None}],
            [{'file_status_history_id': 139708714, 'file_id': 14452074, 'file_status_id': 1,
              'dt_begin': datetime(2022, 4, 28, 16, 51, 16), 'dt_end': datetime(2022, 4, 28, 16, 51, 16)}],
        ]

        self.assertEqual(self.tape.post_grouprestore(None,
                                                     {'files': ['62769e4d945720fe9292369e', '626b286a682a7f997d28e4a5'],
                                                      'days': 10, 'requestor': 'foo'}),
                         {'restored_count': 1, 'updated_count': 1, 'url': '/tape/pullqueue'})

    @patch('tape.restful.RestServer')
    def test_Tape_post_restore(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.lastrowid = 1
        self.cursor.fetchall.side_effect = [
            [{'file_id': 14497587, 'file_status_id': 10, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'file_size': 3587990}],
            [{'tar_record_id': 224967}],
            [],
            [{'file_id': 14497587, 'transaction_id': 1, 'file_name': 'Ga0506519_trna.gff',
              'file_path': '/global/dna/dm_archive/img/submissions/268204', 'origin_file_name': 'Ga0506519_trna.gff',
              'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268204', 'file_size': 3587990,
              'file_date': datetime(2022, 5, 6, 20, 43, 52), 'file_owner': 'gbp', 'file_group': 'img',
              'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': 'b059f4bd49e682dfd55ef3e7f3e43ddf',
              'file_status_id': 7, 'created_dt': datetime(2022, 5, 7, 9, 30, 46),
              'modified_dt': datetime(2022, 5, 8, 10, 2, 17), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62769e4d945720fe9292369e', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'file_status': 'BACKUP_IN_PROGRESS', 'dt_to_purge': None}],
            [{'backup_record_id': 18810203, 'file_id': 14497587, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 3, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 8, 10, 2, 17), 'dt_to_release': None}],
            [{'file_status_history_id': 140638665, 'file_id': 14497587, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 7, 9, 30, 46),
              'dt_end': datetime(2022, 5, 7, 9, 30, 46)}],
        ]

        self.tape.post_restore(None, {'file': '/global/dna/dm_archive/img/submissions/268204/Ga0506519_trna.gff',
                                      'requestor': 'foobar', 'days': 10})

        self.assertIn(call.execute(
            'insert into pull_queue (file_id, requestor, priority, tar_record_id, volume, position_a, position_b) values (%s, %s, %s, %s, %s, %s, %s)',
            (14497587, 'foobar', 2, 224967, 'AG1583', 1375, 14800029)), self.cursor.mock_calls)

    @parameterized.expand([
        ('no_file_id_or_file', {}),
        ('not_found_on_tape', {'file_id': 14497587}),
    ])
    def test_Tape_post_restore_input_errors(self, _description, kwargs):
        self.cursor.fetchall.side_effect = [[]]
        kwargs.update({'requestor': 'foobar'})

        self.assertRaises(common.HttpException, self.tape.post_restore, None, kwargs)

    def test_Tape_post_pullrequest(self):
        self.cursor.lastrowid = 1

        self.tape.post_pullrequest(None, {'file_id': 14497587, 'queue_status_id': 3,
                                          'dt_modified': datetime(2015, 2, 5, 14, 49, 29),
                                          'requestor': 'foobar', 'priority': None, 'tar_record_id': None,
                                          'volume': None, 'position_a': None, 'position_b': None,
                                          'callback': 'local://put_file/14497587'})

        self.assertIn(
            call.execute(
                'insert into pull_queue ( file_id, queue_status_id, dt_modified, requestor, callback) values (%s,%s,%s,%s,%s)',
                [14497587, 3, datetime(2015, 2, 5, 14, 49, 29), 'foobar', 'local://put_file/14497587']),
            self.cursor.mock_calls)

    def test_Tape_post_tar(self):
        self.cursor.lastrowid = 1

        self.tape.post_tar(None, {'root_path': '/path/to/root', 'remote_path': '/path/to/remote'})

        self.cursor.execute.assert_called_with('insert into tar_record ( root_path, remote_path) values (%s,%s)',
                                               ['/path/to/root', '/path/to/remote'])

    @parameterized.expand([
        ('found', 1, 'jgi', True),
        ('not_found', 3, 'jgi', False),
        ('found_wrong_division', 1, 'foo', False),
    ])
    def test_Tape_validate_backup_service(self, _description, service_id, division, expected):
        self.cursor.fetchall.side_effect = [
            [{'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
              'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS', 'division': 'jgi'}],
        ]

        self.assertEqual(self.tape.validate_backup_service(service_id, division), expected)

    def test_Tape_get_cvs(self):
        expected = {'backup_record_status': {'1': 'REGISTERED',
                                             '16': 'HOLD',
                                             '2': 'TRANSFER_READY',
                                             '3': 'TRANSFER_IN_PROGRESS',
                                             '4': 'TRANSFER_COMPLETE',
                                             '6': 'WAIT_FOR_TAPE',
                                             '12': 'VALIDATION_COMPLETE',
                                             '5': 'TRANSFER_FAILED',
                                             'HOLD': 16,
                                             'REGISTERED': 1,
                                             'TRANSFER_COMPLETE': 4,
                                             'TRANSFER_IN_PROGRESS': 3,
                                             'TRANSFER_READY': 2,
                                             'WAIT_FOR_TAPE': 6,
                                             'VALIDATION_COMPLETE': 12,
                                             'TRANSFER_FAILED': 5},
                    'file_status': {'1': 'REGISTERED',
                                    '10': 'PURGED',
                                    '11': 'DELETE',
                                    '12': 'RESTORE_IN_PROGRESS',
                                    '14': 'TAR_READY',
                                    '16': 'TAR_COMPLETE',
                                    '17': 'TAR_FAILED',
                                    '2': 'COPY_READY',
                                    '20': 'INGEST_STATS_FAILED',
                                    '21': 'INGEST_FILE_MISSING',
                                    '4': 'COPY_COMPLETE',
                                    '5': 'COPY_FAILED',
                                    '6': 'BACKUP_READY',
                                    '7': 'BACKUP_IN_PROGRESS',
                                    '8': 'BACKUP_COMPLETE',
                                    '9': 'BACKUP_FAILED',
                                    '13': 'RESTORED',
                                    '22': 'INGEST_COMPLETE',
                                    '19': 'INGEST_STATS_COMPLETE',
                                    '15': 'TAR_IN_PROGRESS',
                                    '3': 'COPY_IN_PROGRESS',
                                    '28': 'RESTORE_REGISTERED',
                                    'COPY_IN_PROGRESS': 3,
                                    'TAR_IN_PROGRESS': 15,
                                    'INGEST_COMPLETE': 22,
                                    'INGEST_STATS_COMPLETE': 19,
                                    'BACKUP_COMPLETE': 8,
                                    'BACKUP_FAILED': 9,
                                    'BACKUP_IN_PROGRESS': 7,
                                    'BACKUP_READY': 6,
                                    'COPY_COMPLETE': 4,
                                    'COPY_FAILED': 5,
                                    'COPY_READY': 2,
                                    'DELETE': 11,
                                    'INGEST_FILE_MISSING': 21,
                                    'INGEST_STATS_FAILED': 20,
                                    'PURGED': 10,
                                    'REGISTERED': 1,
                                    'RESTORE_IN_PROGRESS': 12,
                                    'TAR_COMPLETE': 16,
                                    'TAR_FAILED': 17,
                                    'TAR_READY': 14,
                                    'RESTORED': 13,
                                    'RESTORE_REGISTERED': 28,
                                    },
                    'queue_status': {'1': 'REGISTERED',
                                     '2': 'IN_PROGRESS',
                                     '3': 'COMPLETE',
                                     '4': 'FAILED',
                                     '6': 'PREP_FAILED',
                                     '7': 'PREP_IN_PROGRESS',
                                     'COMPLETE': 3,
                                     'FAILED': 4,
                                     'IN_PROGRESS': 2,
                                     'PREP_FAILED': 6,
                                     'PREP_IN_PROGRESS': 7,
                                     'REGISTERED': 1}}

        self.assertEqual(self.tape.get_cvs(None, None), expected)

    def test_Tape_post_backupservice(self):
        self.cursor.lastrowid = 1

        self.tape.post_backupservice(None, {'name': 'foobar',
                                            'server': 'foobar.org',
                                            'type': 'HPSS',
                                            'default_path': '/home/projects/dm_archive/root'
                                            })

        self.assertIn(
            call.execute(
                'insert into backup_service ( name, server, type, default_path) values (%s,%s,%s,%s)',
                ['foobar', 'foobar.org', 'HPSS', '/home/projects/dm_archive/root']),
            self.cursor.mock_calls)

    def test_Tape_get_backupservice(self):
        backup_service = {'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
                          'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS'}
        self.cursor.fetchall.side_effect = [[backup_service]]

        self.assertEqual(self.tape.get_backupservice([1], None), backup_service)

    def test_Tape_get_backupservices_no_queryResults(self):
        backup_service = {'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
                          'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS'}
        self.cursor.fetchall.side_effect = [[backup_service]]

        self.assertIn(backup_service, self.tape.get_backupservices(None, {}))

    def test_Tape_get_backupservices_queryResults(self):
        backup_service = {'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
                          'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS'}
        self.cursor.fetchall.side_effect = [
            [{'record_count': 1}],
            [backup_service]
        ]

        self.assertEqual(self.tape.get_backupservices(None,
                                                      {'queryResults': True, 'fields': None, 'query': ''}),
                         {'data': [backup_service], 'record_count': 1, 'return_count': 100})

    def test_Tape_post_backuprecord(self):
        self.cursor.lastrowid = 1

        self.tape.post_backuprecord(None, {'backup_record_id': 3001, 'file_id': 1501, 'service': 1,
                                           'remote_file_name': 'pbio-81.947-0.h5',
                                           'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20,
                                           'backup_record_status_id': 4, 'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                           'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                                           'dt_to_release': None})

        self.assertIn(
            call.execute(
                'insert into backup_record ( backup_record_id, file_id, service, remote_file_name, remote_file_path, tar_record_id, backup_record_status_id, md5sum, dt_modified) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                [3001, 1501, 1, 'pbio-81.947-0.h5', 'pbio-81.947-0.h5', 20, 4, 'c9f3f65b0c05a68e6189d1f77febdc5a',
                 datetime(2015, 3, 10, 17, 39, 45)]),
            self.cursor.mock_calls)

    @patch('tape.restful.RestServer')
    def test_Tape_put_backuprecords(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.fetchall.side_effect = [
            [{'backup_record_id': 3001, 'file_id': 1501, 'service': 1, 'remote_file_name': 'pbio-81.947-0.h5',
              'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
              'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
              'dt_to_release': None, 'transaction_id': 105, 'file_name': 'pbio-81.947-0.h5',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
              'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
              'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 'file_size': 2559491696,
              'file_date': datetime(2012, 4, 19, 15, 59, 40), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100644', 'local_purge_days': 90, 'file_status_id': 10,
              'created_dt': datetime(2013, 4, 26, 17, 7, 7),
              'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '51d48dec067c014cd6e9e3b0', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [],
        ]

        self.tape.put_backuprecords(None, {
            'records': [
                {'backup_record_id': 3001, 'file_id': 1501, 'service': 1,
                 'remote_file_name': 'pbio-81.947-0.h5',
                 'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20,
                 'backup_record_status_id': 4, 'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                 'dt_to_release': None}
            ]
        })

        self.assertIn(
            call.execute(
                'update backup_record set  backup_record_id=%s, file_id=%s, service=%s, remote_file_name=%s, remote_file_path=%s, tar_record_id=%s, backup_record_status_id=%s, md5sum=%s, dt_modified=%s where backup_record_id=3001',
                [3001, 1501, 1, 'pbio-81.947-0.h5', 'pbio-81.947-0.h5', 20, 4, 'c9f3f65b0c05a68e6189d1f77febdc5a',
                 datetime(2015, 3, 10, 17, 39, 45)]),
            self.cursor.mock_calls)
        self.assertIn(call.execute('update file set  file_status_id=%s where file_id=1501', [8]), self.cursor.mock_calls)

    def test_Tape_delete_file(self):
        self.cursor.fetchall.side_effect = [
            [{'file_id': 1501, 'transaction_id': 105, 'file_name': 'pbio-81.947-0.h5',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
              'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
              'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 'file_size': 2559491696,
              'file_date': datetime(2012, 4, 19, 15, 59, 40), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100644', 'local_purge_days': 90, 'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
              'file_status_id': 10, 'created_dt': datetime(2013, 4, 26, 17, 7, 7),
              'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '51d48dec067c014cd6e9e3b0', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'division': 'jgi'}],
            [{'file_status_history_id': 3001, 'file_id': 1501, 'file_status_id': 1,
              'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
              'dt_end': datetime(2013, 4, 26, 17, 7, 7)}],
            [{'pull_queue_id': 11616875, 'file_id': 1501, 'queue_status_id': 3,
              'dt_modified': datetime(2022, 5, 9, 7, 0, 14), 'requestor': 'foobar', 'priority': 4,
              'tar_record_id': None, 'volume': 'AG5997', 'position_a': 210, 'position_b': 0}],
            [{'request_id': 27306583, 'file_id': 13144016, 'dt_modified': datetime(2022, 5, 9, 7, 0, 14),
              'requestor': 'portal/395652685@qq.com'}],
            [{'backup_record_id': 3001, 'file_id': 1501, 'service': 1, 'remote_file_name': 'pbio-81.947-0.h5',
              'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
              'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
              'dt_to_release': None}],
            [{'backup_record_status_history_id': 3001, 'backup_record_id': 3001, 'backup_record_status_id': 1,
              'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
              'dt_end': datetime(2013, 4, 26, 18, 33, 50)}],
            [{'pull_queue_status_history_id': 35873427, 'pull_queue_id': 11616875, 'queue_status_id': 1,
              'dt_begin': datetime(2022, 5, 8, 22, 21, 19),
              'dt_end': datetime(2022, 5, 9, 6, 56, 22)}],
        ]
        self.cursor.execute.return_value = 1
        expected = {'tape_data': {'backup_record': [{'backup_record_id': 3001,
                                                     'backup_record_status_id': 4,
                                                     'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                                                     'dt_to_release': None,
                                                     'file_id': 1501,
                                                     'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                                     'remote_file_name': 'pbio-81.947-0.h5',
                                                     'remote_file_path': 'pbio-81.947-0.h5',
                                                     'service': 1,
                                                     'tar_record_id': 20}],
                                  'backup_record_status_history': [{'backup_record_id': 3001,
                                                                    'backup_record_status_history_id': 3001,
                                                                    'backup_record_status_id': 1,
                                                                    'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
                                                                    'dt_end': datetime(2013, 4, 26, 18, 33, 50)}],
                                  'file': [{'auto_uncompress': 0,
                                            'created_dt': datetime(2013, 4, 26, 17, 7, 7),
                                            'file_date': datetime(2012, 4, 19, 15, 59, 40),
                                            'file_group': 'pacbio',
                                            'file_id': 1501,
                                            'file_name': 'pbio-81.947-0.h5',
                                            'file_owner': 'smrt',
                                            'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
                                            'file_permissions': '0100644',
                                            'file_size': 2559491696,
                                            'file_status_id': 10,
                                            'local_purge_days': 90,
                                            'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                            'metadata_id': '51d48dec067c014cd6e9e3b0',
                                            'modified_dt': datetime(2016, 7, 12, 12, 15, 35),
                                            'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
                                            'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results',
                                            'remote_purge_days': None,
                                            'transaction_id': 105,
                                            'transfer_mode': 0,
                                            'user_save_till': None,
                                            'division': 'jgi',
                                            'validate_mode': 0}],
                                  'file_status_history': [{'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
                                                           'dt_end': datetime(2013, 4, 26, 17, 7, 7),
                                                           'file_id': 1501,
                                                           'file_status_history_id': 3001,
                                                           'file_status_id': 1}],
                                  'pull_queue': [{'dt_modified': datetime(2022, 5, 9, 7, 0, 14),
                                                  'file_id': 1501,
                                                  'position_a': 210,
                                                  'position_b': 0,
                                                  'priority': 4,
                                                  'pull_queue_id': 11616875,
                                                  'queue_status_id': 3,
                                                  'requestor': 'foobar',
                                                  'tar_record_id': None,
                                                  'volume': 'AG5997'}],
                                  'pull_queue_status_history': [{'dt_begin': datetime(2022, 5, 8, 22, 21, 19),
                                                                 'dt_end': datetime(2022, 5, 9, 6, 56, 22),
                                                                 'pull_queue_id': 11616875,
                                                                 'pull_queue_status_history_id': 35873427,
                                                                 'queue_status_id': 1}],
                                  'request': [{'dt_modified': datetime(2022, 5, 9, 7, 0, 14),
                                               'file_id': 13144016,
                                               'request_id': 27306583,
                                               'requestor': 'portal/395652685@qq.com'}]},
                    'tape_records': 6}
        expected_delete_in_queue = {'validate_mode': 0, 'remote_purge_days': None, 'file_name': 'pbio-81.947-0.h5',
                                    'file_size': 2559491696, 'file_owner': 'smrt',
                                    'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'file_status_id': 10,
                                    'transfer_mode': 0, 'created_dt': datetime(2013, 4, 26, 17, 7, 7),
                                    'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results',
                                    'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81', 'transaction_id': 105,
                                    'user_save_till': None, 'file_permissions': '0100644', 'file_id': 1501,
                                    'file_group': 'pacbio', 'metadata_id': '51d48dec067c014cd6e9e3b0',
                                    'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
                                    'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                    'file_date': datetime(2012, 4, 19, 15, 59, 40), 'auto_uncompress': 0,
                                    'local_purge_days': 90, 'division': 'jgi'}

        self.assertEqual(self.tape.delete_file([1501], None), expected)
        self.assertIn(expected_delete_in_queue,
                      list(self.tape.divisions.get('jgi').delete_queue.feature_queues.values())[0])

    def test_Tape_post_undelete_file(self):
        self.cursor.lastrowid = 1
        request = {'backup_record': [{'backup_record_id': 3001,
                                      'backup_record_status_id': 4,
                                      'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                                      'dt_to_release': None,
                                      'file_id': 1501,
                                      'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                      'remote_file_name': 'pbio-81.947-0.h5',
                                      'remote_file_path': 'pbio-81.947-0.h5',
                                      'service': 1,
                                      'tar_record_id': 20}],
                   'backup_record_status_history': [{'backup_record_id': 3001,
                                                     'backup_record_status_history_id': 3001,
                                                     'backup_record_status_id': 1,
                                                     'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
                                                     'dt_end': datetime(2013, 4, 26, 18, 33, 50)}],
                   'file': [{'auto_uncompress': 0,
                             'created_dt': datetime(2013, 4, 26, 17, 7, 7),
                             'file_date': datetime(2012, 4, 19, 15, 59, 40),
                             'file_group': 'pacbio',
                             'file_id': 1501,
                             'file_name': 'pbio-81.947-0.h5',
                             'file_owner': 'smrt',
                             'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
                             'file_permissions': '0100644',
                             'file_size': 2559491696,
                             'file_status_id': 10,
                             'local_purge_days': 90,
                             'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                             'metadata_id': '51d48dec067c014cd6e9e3b0',
                             'modified_dt': datetime(2016, 7, 12, 12, 15, 35),
                             'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
                             'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results',
                             'remote_purge_days': None,
                             'transaction_id': 105,
                             'transfer_mode': 0,
                             'user_save_till': None,
                             'validate_mode': 0}],
                   'file_status_history': [{'dt_begin': datetime(2013, 4, 26, 17, 7, 7),
                                            'dt_end': datetime(2013, 4, 26, 17, 7, 7),
                                            'file_id': 1501,
                                            'file_status_history_id': 3001,
                                            'file_status_id': 1}],
                   'pull_queue': [{'dt_modified': datetime(2022, 5, 9, 7, 0, 14),
                                   'file_id': 1501,
                                   'position_a': 210,
                                   'position_b': 0,
                                   'priority': 4,
                                   'pull_queue_id': 11616875,
                                   'queue_status_id': 3,
                                   'requestor': 'foobar',
                                   'tar_record_id': None,
                                   'volume': 'AG5997'}],
                   'pull_queue_status_history': [{'dt_begin': datetime(2022, 5, 8, 22, 21, 19),
                                                  'dt_end': datetime(2022, 5, 9, 6, 56, 22),
                                                  'pull_queue_id': 11616875,
                                                  'pull_queue_status_history_id': 35873427,
                                                  'queue_status_id': 1}],
                   'request': [{'dt_modified': datetime(2022, 5, 9, 7, 0, 14),
                                'file_id': 13144016,
                                'request_id': 27306583,
                                'requestor': 'portal/395652685@qq.com'}]}

        self.assertEqual(self.tape.post_undelete_file([request], None), {'tape_records': 7})
        self.assertIn(
            call.execute(
                'insert into file ( auto_uncompress, created_dt, file_date, file_group, file_id, file_name, file_owner, file_path, file_permissions, file_size, file_status_id, local_purge_days, md5sum, metadata_id, modified_dt, origin_file_name, origin_file_path, transaction_id, transfer_mode, validate_mode) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                [0, datetime(2013, 4, 26, 17, 7, 7), datetime(2012, 4, 19, 15, 59, 40), 'pacbio',
                 1501, 'pbio-81.947-0.h5', 'smrt', '/global/dna/dm_archive/sdm/pacbio/00/00/81', '0100644', 2559491696,
                 10, 90, 'c9f3f65b0c05a68e6189d1f77febdc5a', '51d48dec067c014cd6e9e3b0',
                 datetime(2016, 7, 12, 12, 15, 35),
                 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
                 '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 105, 0, 0]),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute(
                'insert into file_status_history ( dt_begin, dt_end, file_id, file_status_history_id, file_status_id) values (%s,%s,%s,%s,%s)',
                [datetime(2013, 4, 26, 17, 7, 7), datetime(2013, 4, 26, 17, 7, 7), 1501, 3001, 1]),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute(
                'insert into backup_record ( backup_record_id, backup_record_status_id, dt_modified, file_id, md5sum, remote_file_name, remote_file_path, service, tar_record_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                [3001, 4, datetime(2015, 3, 10, 17, 39, 45), 1501, 'c9f3f65b0c05a68e6189d1f77febdc5a',
                 'pbio-81.947-0.h5', 'pbio-81.947-0.h5', 1, 20]),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute(
                'insert into backup_record_status_history ( backup_record_id, backup_record_status_history_id, backup_record_status_id, dt_begin, dt_end) values (%s,%s,%s,%s,%s)',
                [3001, 3001, 1, datetime(2013, 4, 26, 17, 7, 7), datetime(2013, 4, 26, 18, 33, 50)]),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute(
                'insert into pull_queue ( dt_modified, file_id, position_a, position_b, priority, pull_queue_id, queue_status_id, requestor, volume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                [datetime(2022, 5, 9, 7, 0, 14), 1501, 210, 0, 4, 11616875, 3, 'foobar', 'AG5997']),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute(
                'insert into pull_queue_status_history ( dt_begin, dt_end, pull_queue_id, pull_queue_status_history_id, queue_status_id) values (%s,%s,%s,%s,%s)',
                [datetime(2022, 5, 8, 22, 21, 19), datetime(2022, 5, 9, 6, 56, 22), 11616875,
                 35873427, 1]),
            self.cursor.mock_calls)
        self.assertIn(
            call.execute('insert into request ( dt_modified, file_id, request_id, requestor) values (%s,%s,%s,%s)',
                         [datetime(2022, 5, 9, 7, 0, 14), 13144016, 27306583, 'portal/395652685@qq.com']),
            self.cursor.mock_calls)

    def test_Tape_purgefiles(self):
        self.cursor.fetchall.side_effect = [
            [{'file_id': 13341021, 'file_path': '/global/dna/dm_archive/rqc/analyses/AUTO-349464',
              'file_name': 'pbio-2429.22780.bc1003_IsoSeq--bc1003_IsoSeq.ccs.filtered-report.txt',
              'file_permissions': '0100775', 'modified_dt': datetime(2022, 2, 16, 10, 31, 56)}],
        ]

        self.tape.purgefiles()

        self.assertIn({'file_name': 'pbio-2429.22780.bc1003_IsoSeq--bc1003_IsoSeq.ccs.filtered-report.txt',
                       'modified_dt': datetime(2022, 2, 16, 10, 31, 56), 'file_permissions': '0100775',
                       'file_id': 13341021, 'file_path': '/global/dna/dm_archive/rqc/analyses/AUTO-349464'},
                      list(self.tape.divisions.get('jgi').purge_queue.feature_queues.values())[0])

    def test_Tape_enable_portal_short(self):
        self.tape.enable_portal_short()

        self.assertIn(2, self.tape.divisions.get('jgi').pull_queue.enabled_queues)
        self.assertIn(3, self.tape.divisions.get('jgi').pull_queue.enabled_queues)

    def test_Tape_enable_portal_long(self):
        self.tape.enable_portal_long()

        self.assertIn(4, self.tape.divisions.get('jgi').pull_queue.enabled_queues)
        self.assertIn(5, self.tape.divisions.get('jgi').pull_queue.enabled_queues)
        self.assertIn(6, self.tape.divisions.get('jgi').pull_queue.enabled_queues)
        self.assertIn(7, self.tape.divisions.get('jgi').pull_queue.enabled_queues)

    def test_Tape_get_purgeable(self):
        record = {'file_id': 13341021, 'file_path': '/global/dna/dm_archive/rqc/analyses/AUTO-349464',
                  'file_name': 'pbio-2429.22780.bc1003_IsoSeq--bc1003_IsoSeq.ccs.filtered-report.txt',
                  'file_permissions': '0100775', 'modified_dt': datetime(2022, 2, 16, 10, 31, 56),
                  'division': 'jgi'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertIn(record, self.tape.get_purgeable(['jgi'], {}))
        self.assertIn(call.execute(
            'select file_id, file_path, file_name, file_permissions, modified_dt, division from file where ((file_status_id = %s and local_purge_days is not null) or file_status_id = %s) and GREATEST(ifnull(DATE_ADD(created_dt, INTERVAL local_purge_days DAY),\'0000-00-00 00:00:00\'), ifnull(user_save_till,\'0000-00-00 00:00:00\')) < now() and division = %s',
            [8, 13, 'jgi']), self.cursor.mock_calls)

    @parameterized.expand([
        ('transfer_complete', [
            [{'backup_record_id': 3001, 'file_id': 1501, 'service': 1, 'remote_file_name': 'pbio-81.947-0.h5',
              'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
              'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
              'dt_to_release': None, 'transaction_id': 105, 'file_name': 'pbio-81.947-0.h5',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
              'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
              'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 'file_size': 2559491696,
              'file_date': datetime(2012, 4, 19, 15, 59, 40), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100644', 'local_purge_days': 90, 'file_status_id': 10,
              'created_dt': datetime(2013, 4, 26, 17, 7, 7),
              'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '51d48dec067c014cd6e9e3b0', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [],
        ], 4),
        ('validation_complete', [
            [{'file_status_id': 10, 'file_id': 1501, 'backup_record_id': 3001, 'backup_record_status_id': 12,
              'file_md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'backup_md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a'}],
        ], 12)
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_put_backuprecord(self, _description, sql_responses, backup_record_status_id, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.fetchall.side_effect = sql_responses

        self.tape.put_backuprecord([1501],
                                   {'backup_record_id': 3001, 'file_id': 1501, 'service': 1,
                                    'remote_file_name': 'pbio-81.947-0.h5',
                                    'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20,
                                    'backup_record_status_id': backup_record_status_id,
                                    'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                                    'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                                    'dt_to_release': None})

        self.assertIn(
            call.execute(
                'update backup_record set  backup_record_id=%s, file_id=%s, service=%s, remote_file_name=%s, remote_file_path=%s, tar_record_id=%s, backup_record_status_id=%s, md5sum=%s, dt_modified=%s where backup_record_id=1501',
                [3001, 1501, 1, 'pbio-81.947-0.h5', 'pbio-81.947-0.h5', 20, backup_record_status_id,
                 'c9f3f65b0c05a68e6189d1f77febdc5a',
                 datetime(2015, 3, 10, 17, 39, 45)]),
            self.cursor.mock_calls)

        self.assertIn(call.execute('update file set  file_status_id=%s where file_id=1501', [8]),
                      self.cursor.mock_calls)

    @parameterized.expand([
        ('no_query_results',
         [[{'backup_record_id': 3001, 'file_id': 1501, 'service': 1, 'remote_file_name': 'pbio-81.947-0.h5',
            'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
            'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
            'dt_to_release': None}]], {},
         [{'backup_record_id': 3001, 'file_id': 1501, 'service': 1, 'remote_file_name': 'pbio-81.947-0.h5',
           'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
           'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a', 'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
           'dt_to_release': None}
          ]),
        ('query_results',
         [[{'record_count': 1}],
          [{'backup_record_id': 3001, 'file_id': 1501, 'service': 1,
            'remote_file_name': 'pbio-81.947-0.h5',
            'remote_file_path': 'pbio-81.947-0.h5', 'tar_record_id': 20, 'backup_record_status_id': 4,
            'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
            'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
            'dt_to_release': None}]],
         {'queryResults': True, 'fields': None, 'query': 'where file_id=1501'},
         {'data': [{'backup_record_id': 3001,
                    'backup_record_status_id': 4,
                    'dt_modified': datetime(2015, 3, 10, 17, 39, 45),
                    'dt_to_release': None,
                    'file_id': 1501,
                    'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
                    'remote_file_name': 'pbio-81.947-0.h5',
                    'remote_file_path': 'pbio-81.947-0.h5',
                    'service': 1,
                    'tar_record_id': 20}],
          'record_count': 1,
          'return_count': 100}
         )
    ])
    def test_Tape_get_backuprecords(self, _description, sql_responses, request, expected):
        self.cursor.fetchall.side_effect = sql_responses

        self.assertEqual(self.tape.get_backuprecords([1501], request), expected)

    def test_Tape_get_hpss_state(self):
        self.hsi_state.isup.side_effect = [True, False]

        self.assertEqual(self.tape.get_hpss_state(None, None),
                         [{'status': True, 'id': 1, 'system': 'archive'}, {'status': False, 'id': 2, 'system': 'hpss'}])

    def test_Tape_get_db_states(self):
        record = {'label': 'Ingest', 'N': 64436, 'id': 22, 'status': 'INGEST_COMPLETE'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_db_states(None, None), [record])
        self.cursor.execute.assert_called_with('select * from status limit 500')

    def test_Tape_get_active_db_states(self):
        record = {'label': 'Ingest', 'N': 113, 'id': 1, 'status': 'REGISTERED'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_db_states(None, None), [record])
        self.cursor.execute.assert_called_with('select * from status limit 500')

    def test_Tape_get_pullqueuesummary(self):
        record = {'status_id': 1, 'status': 'REGISTERED', 'requestor': 'foobar', 'N': 8526,
                  'priority_queue': 6, 'gb': 3201.797141566}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_pullqueuesummary(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select a.queue_status_id as status_id, c.status, a.requestor, count(*) as N, a.priority as priority_queue, sum(b.file_size) / 1e9 as gb from pull_queue a join file b on a.file_id = b.file_id join queue_status_cv c on a.queue_status_id = c.queue_status_id where a.queue_status_id not in (3) group by 1, 2, 3, 5 order by 1, 2, 3, 5 limit 500')

    def test_Tape_get_tape_volumes(self):
        record = {'n': 3570}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_tape_volumes(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select count(distinct volume) as n from pull_queue a where a.queue_status_id <> 3 limit 500')

    def test_Tape_get_prep_count(self):
        record = {'n': 3}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_prep_count(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select count(*) as n from pull_queue where queue_status_id = 1 and volume is null limit 500')

    def test_Tape_get_restored_in_last_hour(self):
        record = {'N': 2310, 'priority_queue': 1, 'gb': 6190.747419636}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_restored_in_last_hour(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select count(*) as N, a.priority as priority_queue, sum(b.file_size) / 1e9 as gb from pull_queue a join file b on a.file_id = b.file_id where a.queue_status_id = 3 and a.dt_modified > now() - interval 1 hour group by priority_queue order by priority_queue limit 500')

    def test_Tape_get_ingested_last_five_days_by_hour(self):
        record = {'ymdh': '22-05-09 06', 'N': 1156, 'gb': 92.501109338}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_ingested_last_five_days_by_hour(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select date_format(created_dt, "%y-%m-%d %H") as ymdh, count(*) as N, source, sum(file_size) / 1e9 as gb from file where created_dt > now() - interval 5 day group by ymdh, source order by ymdh desc limit 500')

    def test_Tape_get_ingested_last_five_days_by_user(self):
        record = {'file_owner': 'foobar', 'n': 1181, 'gb': 699.880510533}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_ingested_last_five_days_by_user(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select file_owner, count(*) as N, source, sum(file_size) / 1e9 as gb from file where created_dt > now() - interval 5 day group by file_owner, source order by file_owner')

    def test_Tape_get_ingest_stats(self):
        hour = [{'ymdh': '22-05-09 06', 'N': 2331, 'gb': 1331.300473485}]
        user = [{'file_owner': 'foobar', 'N': 2331, 'gb': 1331.300473485}]
        graph = ''
        self.cursor.fetchall.side_effect = [
            hour,
            user,
            graph
        ]
        expected = {'hour': hour,
                    'user': user,
                    'graph': graph}

        self.assertEqual(self.tape.get_ingest_stats(None, None), expected)

    def test_Tape_get_requested_last_five_days_by_hour(self):
        record = {'ymdh': '22-05-09 06', 'N': 58784, 'gb': 13681.38999538}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_requested_last_five_days_by_hour(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select date_format(dt_modified, "%y-%m-%d %H") as ymdh, count(*) as N, sum(b.file_size) / 1e9 as gb from request a join file b on a.file_id = b.file_id where dt_modified > now() - interval 5 day group by 1 order by ymdh desc limit 500')

    def test_Tape_get_requested_last_five_days_by_user(self):
        record = {'requestor': 'foobar', 'N': 58784, 'gb': 13681.38999538}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_requested_last_five_days_by_user(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select a.requestor, count(*) as N, sum(b.file_size) / 1e9 as gb from request a join file b on a.file_id = b.file_id where dt_modified > now() - interval 5 day group by 1 order by count(*) desc')

    def test_Tape_get_request_stats(self):
        hour = [{'ymdh': '22-05-09 06', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}]
        user = [{'requestor': 'foobar', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}]
        graph = ''
        self.cursor.fetchall.side_effect = [
            hour,
            user,
            graph
        ]
        expected = {'hour': hour,
                    'user': user,
                    'graph': graph}

        self.assertEqual(self.tape.get_request_stats(None, None), expected)

    def test_Tape_get_restored_last_five_days_by_hour(self):
        record = {'ymdh': '22-05-09 06', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_restored_last_five_days_by_hour(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select date_format(dt_modified, "%y-%m-%d %H") as ymdh, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from pull_queue p use index (pull_queue_status_id_dt_modified) join file f on f.file_id = p.file_id where queue_status_id = 3 and dt_modified > now() - interval 5 day group by ymdh order by ymdh desc limit 500')

    def test_Tape_get_restored_last_five_days_by_user(self):
        record = {'requestor': 'foobar', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_restored_last_five_days_by_user(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select requestor, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from pull_queue p use index (pull_queue_status_id_dt_modified) join file f on f.file_id = p.file_id where queue_status_id = 3 and dt_modified > now() - interval 5 day group by 1 order by N desc')

    def test_Tape_get_restore_stats(self):
        hour = [{'ymdh': '22-05-09 06', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}]
        user = [{'requestor': 'foobar', 'vol': 277, 'N': 2331, 'gb': 1331.300473485}]
        graph = ''
        self.cursor.fetchall.side_effect = [
            hour,
            user,
            graph
        ]
        expected = {'hour': hour,
                    'user': user,
                    'graph': graph}

        self.assertEqual(self.tape.get_restore_stats(None, None), expected)

    def test_Tape_plot_data(self):
        data = [{'ymdh': '22-05-09 06', 'N': 10, 'gb': 10.1},
                {'ymdh': '22-05-09 06', 'N': 20, 'gb': 20.2}]
        fmt = '%y-%m-%d %H'
        idx = datetime.now().replace(second=0, microsecond=0, minute=0)
        # set the first element back four hours
        idx -= timedelta(hours=4)
        data[0]['ymdh'] = idx.strftime(fmt)
        # set the next element back two hours
        idx += timedelta(hours=2)
        data[1]['ymdh'] = idx.strftime(fmt)
        expected = ANY

        self.assertEqual(self.tape.plot_data(data, ('N', 'gb'), ('Number of files', 'Footprint'), ('N Files', 'Size in GB'), 'Graph Title'), expected)

    def test_Tape_save_requested_restores(self):
        record = {'Vol': 13, 'Gb': 2.589374624, 'Ymdh': '22-08-05 13', 'N': 42}
        self.cursor.fetchall.side_effect = [[record]]

        self.tape.save_requested_restores()

        self.assertEqual(self.tape.requested_restores, [record])
        self.cursor.execute.assert_called_with(
            'select date_format(dt_begin, "%y-%m-%d %H") as ymdh, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from (select p.pull_queue_id, p.file_id, min(dt_begin) as dt_begin, volume   from pull_queue p   join pull_queue_status_history pqsh on p.pull_queue_id = pqsh.pull_queue_id   join request r on p.file_id = r.file_id     and p.requestor = r.requestor   where p.dt_modified > now() - interval 20 day     and r.dt_modified > now() - interval 5 day     and dt_begin > now() - interval 5 day     and pqsh.queue_status_id = 1   group by 1, 2 ) as x join file f on f.file_id = x.file_id group by ymdh order by ymdh desc limit 500')

    def test_Tape_get_request_age(self):
        record = {'priority_queue': 4, 'status_date': datetime(2022, 5, 8, 10, 20, 20),
                  'oldest_date': datetime(2022, 5, 7, 18, 2, 24), 'n': 4710, 'gb': 3872.839168662}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_request_age(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select a.priority as priority_queue, min(a.dt_modified) as status_date, min(b.dt_begin) as oldest_date, count(distinct a.pull_queue_id) as n, sum(f.file_size) / 1e9 as gb from pull_queue a join file f on a.file_id = f.file_id join pull_queue_status_history b on a.pull_queue_id = b.pull_queue_id where a.queue_status_id not in (0, 3) group by 1 order by 1 limit 500')

    def test_Tape_get_pull_error_list(self):
        record = {'queue_status_id': 4, 'file_id': 5615803, 'requestor': 'foobar',
                  'dt_begin': datetime(2022, 5, 8, 11, 0, 25), 'volume': 'AG6293', 'file_size': 7117888003}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_request_age(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select a.priority as priority_queue, min(a.dt_modified) as status_date, min(b.dt_begin) as oldest_date, count(distinct a.pull_queue_id) as n, sum(f.file_size) / 1e9 as gb from pull_queue a join file f on a.file_id = f.file_id join pull_queue_status_history b on a.pull_queue_id = b.pull_queue_id where a.queue_status_id not in (0, 3) group by 1 order by 1 limit 500')

    def test_Tape_get_footprint(self):
        self.tape.disk_usage['files'] = 10
        self.tape.disk_usage['disk_usage_files'] = 10 * 1e12
        self.tape.disk_usage['files_restoring'] = 2
        self.tape.disk_usage['disk_usage_files_restoring'] = 0.5 * 1e12
        self.tape.disk_usage['bytes_used'] = 4 * 1e12
        self.tape.disk_usage['bytes_free'] = 6 * 1e12
        self.tape.disk_usage['date_updated'] = datetime(2000, 1, 2, 3, 4, 5)
        self.tape.disk_usage['disk_usage_other'] = 1 * 1e12
        self.tape.disk_usage['disk_reserve'] = 5 * 1e12

        self.assertEqual(self.tape.get_footprint(None, None), {'disk_reserve': 5.0,
                                                               'files': 10,
                                                               'disk_usage_files': 10.0,
                                                               'files_restoring': 2,
                                                               'disk_usage_files_restoring': 0.5,
                                                               'dna_free': 6.0,
                                                               'dna_stats_updated': datetime(2000, 1, 2, 3, 4, 5),
                                                               'dna_used': 4.0,
                                                               'disk_usage_other': 1.0})

    def test_Tape_get_pullqueue(self):
        record = {'queue_id': 10252435, 'file_name': 'Ga0495603_proteins.img_nr.last.blasttab',
                  'date_modified': datetime(2022, 1, 24, 15, 20, 30), 'priority_queue': 6, 'status': 'HOLD',
                  'file_size': 60080042896, 'requestor': 'foobar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_pullqueue(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select pull_queue_id as queue_id, file_name, q.dt_modified as date_modified, q.priority as priority_queue, status, file_size, requestor from pull_queue q left join queue_status_cv c on c.queue_status_id=q.queue_status_id left join file f on f.file_id=q.file_id where q.queue_status_id<>3 order by status, priority_queue, date_modified')

    def test_Tape_get_active_restores(self):
        record = {'pull_queue_id': 11527020, 'file_name': 'Ga0466400_contigs.fna', 'time_running': '8:59:59',
                  'priority_queue': 6, 'file_size': 12882655, 'volume': 'AG8142', 'requestor': 'foobar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_active_restores(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select pull_queue_id, file_name, timediff(now(), q.dt_modified) as time_running, q.priority as priority_queue, file_size, q.volume, requestor from pull_queue q left join queue_status_cv c on c.queue_status_id=q.queue_status_id left join file f on f.file_id=q.file_id where q.queue_status_id=2 order by time_running desc')

    def test_Tape_get_status(self):
        active_db_states = [{'label': 'Ingest', 'N': 113, 'id': 1, 'status': 'REGISTERED'}]
        active_restores = [{'file_name': 'Ga0466400_contigs.fna', 'file_size': 12882655, 'priority_queue': 6, 'pull_queue_id': 11527020,
                            'requestor': 'foobar', 'time_running': '8:59:59', 'volume': 'AG8142'}]
        in_queue = [{'N': 8526, 'gb': 3201.797141566, 'priority_queue': 6, 'requestor': 'foobar', 'status': 'REGISTERED', 'status_id': 1}]
        tape_volumes = [{'n': 3570}]
        prep_count = [{'n': 3}]
        restored_in_last_hour = [{'N': 2310, 'priority_queue': 1, 'gb': 6190.747419636}]
        request_age = [{'priority_queue': 4, 'status_date': datetime(2022, 5, 8, 10, 20, 20),
                        'oldest_date': datetime(2022, 5, 7, 18, 2, 24), 'n': 4710, 'gb': 3872.839168662}]
        pull_errors = [{'queue_status_id': 4, 'file_id': 5615803, 'requestor': 'foobar',
                        'dt_begin': datetime(2022, 5, 8, 11, 0, 25), 'volume': 'AG6293', 'file_size': 7117888003}]
        self.cursor.fetchall.side_effect = [
            active_db_states,
            active_restores,
            in_queue,
            prep_count,
            pull_errors,
            request_age,
            restored_in_last_hour,
            tape_volumes,
        ]
        self.tape.disk_usage['files'] = 10
        self.tape.disk_usage['disk_usage_files'] = 1000 * 1e12
        self.tape.disk_usage['disk_usage_files_restoring'] = 0.5 * 1e12
        self.tape.disk_usage['bytes_used'] = 400000000000000
        self.tape.disk_usage['bytes_free'] = 600000000000000
        self.tape.disk_usage['date_updated'] = datetime(2000, 1, 2, 3, 4, 5)
        self.tape.disk_usage['disk_usage_other'] = 18 * 1e12
        self.tape.disk_usage['disk_reserve'] = 5 * 1e12
        expected = {'active_db_states': active_db_states,
                    'active_restores': active_restores,
                    'footprint': {'disk_reserve': 5.0,
                                  'disk_usage_files': 1000.0,
                                  'dna_free': 600.0,
                                  'dna_stats_updated': datetime(2000, 1, 2, 3, 4, 5),
                                  'dna_used': 400.0,
                                  'files': 10,
                                  'files_restoring': 2,
                                  'disk_usage_files_restoring': 0.5,
                                  'disk_usage_other': 18.0},
                    'in_queue': in_queue,
                    'prep_count': prep_count,
                    'pull_errors': pull_errors,
                    'request_age': request_age,
                    'restored_in_last_hour': restored_in_last_hour,
                    'tape_volumes': tape_volumes
                    }

        self.assertEqual(self.tape.get_status(None, None), expected)

    @patch('tape.restful.RestServer')
    def test_Tape_put_md5(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.fetchall.side_effect = [
            [{'md5_queue_id': 431849, 'file_path': '/projectb/scratch/jgi_dna/15.3371978.tar', 'queue_status_id': 2,
              'file_size': 202827386880, 'md5sum': None, 'dt_modified': datetime(2014, 3, 28, 22, 59, 34),
              'callback': 'local://put_file/3371978'}],
        ]

        self.tape.put_md5([431849], {'queue_status_id': 3, 'md5sum': 'fc33fde6efe1047ae6dc6336546ac4a7'})

        self.assertIn(call.execute('update md5_queue set queue_status_id=3 where md5_queue_id=431849', ()), self.cursor.mock_calls)
        self.assertIn(call.execute(
            'update md5_queue set md5sum="fc33fde6efe1047ae6dc6336546ac4a7", queue_status_id=3 where md5_queue_id=431849',
            ()),
            self.cursor.mock_calls)
        server.run_method.assert_called_with('tape', 'put_file', '3371978', md5sum='fc33fde6efe1047ae6dc6336546ac4a7')

    def test_Tape_post_md5(self):
        record = {'md5_queue_id': 431849, 'file_path': '/projectb/scratch/jgi_dna/15.3371978.tar', 'queue_status_id': 2,
                  'file_size': 202827386880, 'md5sum': None, 'dt_modified': datetime(2014, 3, 28, 22, 59, 34),
                  'callback': 'local://put_file/3371978', 'division': 'jgi'}
        self.cursor.lastrowid = 1

        self.tape.post_md5(None, record)

        self.assertIn(
            call.execute(
                'insert into md5_queue ( md5_queue_id, file_path, queue_status_id, file_size, dt_modified, callback, division) values (%s,%s,%s,%s,%s,%s,%s)',
                [431849, '/projectb/scratch/jgi_dna/15.3371978.tar', 2, 202827386880,
                 datetime(2014, 3, 28, 22, 59, 34), 'local://put_file/3371978', 'jgi']),
            self.cursor.mock_calls)
        self.assertIn(record, list(self.tape.divisions.get('jgi').md5_queue.feature_queues.values())[0])

    def test_Tape_post_hpssfile(self):
        self.cursor.lastrowid = 1
        self.hsi.getAllFileInfo.return_value = ('600', 'user', 'group', 1000, datetime(2000, 1, 1, 1, 2, 3))

        self.assertEqual(
            self.tape.post_hpssfile(None, {'file': '/path/to/hpss/file', 'destination': '/path/to/destination'}),
            {'file_id': 1, 'file_status': 'PURGED', 'file_status_id': 10, 'status': 'new'})
        self.assertIn(
            call.execute(
                'insert into backup_record ( service, file_id, remote_file_path, remote_file_name, backup_record_status_id) values (%s,%s,%s,%s,%s)',
                [1, 1, '/path/to/hpss', 'file', 4]),
            self.cursor.mock_calls)

    def test_Tape_post_hpssfile_IntegrityError(self):
        self.hsi.getAllFileInfo.return_value = ('600', 'user', 'group', 1000, datetime(2000, 1, 1, 1, 2, 3))
        self.cursor.execute.side_effect = [
            pymysql.IntegrityError(),
            [{'file_id': 10523953, 'metadata_id': '5d72e42b414254df79d762b0'}],
        ]
        self.cursor.fetchall.side_effect = [
            [{'file_id': 10523953, 'metadata_id': '5d72e42b414254df79d762b0'}],
        ]

        self.assertEqual(
            self.tape.post_hpssfile(None, {'file': '/path/to/hpss/file', 'destination': '/path/to/destination'}),
            {'file_id': 10523953,
             'metadata_id': '5d72e42b414254df79d762b0',
             'status': 'old'})
        self.assertIn(
            call.execute(
                'insert into file ( file_owner, file_group, file_size, file_date, created_dt, file_status_id, file_name, file_permissions, file_path) values (%s,%s,%s,%s,now(),%s,%s,%s,%s)',
                ['user', 'group', 1000, '2000-01-01 01:02:03', 10, 'destination', 33204, '/path/to']
            ),
            self.cursor.mock_calls)

    def test_Tape_post_file_is_file(self):
        kwargs = {
            'file': '/path/to/file',
            'backup_services': [1],
            'destination': '/path/to/archive/destination',
            'put_mode': 0,
            'validate_mode': 0,
            'local_purge_days': 10,
            'transfer_mode': 0,
            'call_source': 'file',
            'auto_uncompress': 0,
            '__auth': {'division': 'jgi'},
        }
        self.cursor.fetchall.side_effect = [
            [{'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
              'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS', 'division': 'jgi'}],
            [{'file_id': 1501, 'transaction_id': 105, 'file_name': 'pbio-81.947-0.h5',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
              'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
              'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 'file_size': 2559491696,
              'file_date': datetime(2012, 4, 19, 15, 59, 40), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100644', 'local_purge_days': 90, 'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
              'file_status_id': 10, 'created_dt': datetime(2013, 4, 26, 17, 7, 7),
              'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '51d48dec067c014cd6e9e3b0', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
              'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
              '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
              '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
              '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
              '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
              'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
              'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
        ]

        self.assertEqual(self.tape.post_file([1501], kwargs), {'file_id': 1501,
                                                               'file_ingest_id': 64391,
                                                               'file_name': 'destination',
                                                               'file_path': '/path/to/archive',
                                                               'metadata_id': '51d48dec067c014cd6e9e3b0',
                                                               'status': 'old'})

    def test_Tape_post_file_is_folder(self):
        kwargs = {
            'file': '/path/to/folder/',
            'backup_services': [1],
            'put_mode': 0,
            'validate_mode': 0,
            'local_purge_days': 10,
            'transfer_mode': 0,
            'call_source': 'folder',
            'auto_uncompress': 0,
            '__auth': {'division': 'jgi'},
        }
        self.cursor.fetchall.side_effect = [
            [{'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
              'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS', 'division': 'jgi'}],
            [{'file_id': 1501, 'transaction_id': 105, 'file_name': 'pbio-81.947-0.h5.tar',
              'file_path': '/global/dna/dm_archive/sdm/pacbio/00/00/81',
              'origin_file_name': 'm120419_202003_42173_c100324982550000001523017909061231_s2_p0.bas.h5',
              'origin_file_path': '/house/pacbio/runs/PB02_Run0293_84/A02_7/Analysis_Results', 'file_size': 2559491696,
              'file_date': datetime(2012, 4, 19, 15, 59, 40), 'file_owner': 'smrt', 'file_group': 'pacbio',
              'file_permissions': '0100644', 'local_purge_days': 90, 'md5sum': 'c9f3f65b0c05a68e6189d1f77febdc5a',
              'file_status_id': 10, 'created_dt': datetime(2013, 4, 26, 17, 7, 7),
              'modified_dt': datetime(2016, 7, 12, 12, 15, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '51d48dec067c014cd6e9e3b0', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [],
            [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
              'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
              '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
              '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
              '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
              '_call_source': 'folder', '_status': 'new', '_callback': 'file_ingest',
              'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
              'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
        ]
        self.cursor.lastrowid = 1

        self.assertEqual(self.tape.post_file([1501], kwargs), {'file_id': 1501,
                                                               'file_ingest_id': 1,
                                                               'file_name': '',
                                                               'file_path': '/path/to/folder',
                                                               'metadata_id': '51d48dec067c014cd6e9e3b0',
                                                               'status': 'old'})

    def test_Tape_put_file_ingest(self):
        record = {'validate_mode': 0,
                  '_callback': 'file_ingest',
                  'file_name': '3300052084.tar.gz',
                  '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
                  '_put_mode': 2,
                  'file_size': 3323992,
                  'file_owner': 'img',
                  '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
                  '_call_source': 'folder',
                  'transfer_mode': 0,
                  '_status': 'new',
                  'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                  '_services': '[1]',
                  'file_permissions': '0100644',
                  '_dt_modified': datetime(2022, 5, 9, 6, 45),
                  'file_id': 14512785,
                  '_is_file': 1,
                  '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
                  'file_group': 'img',
                  'metadata_id': '62791a11c2c506c5afdfce76',
                  'file_date': datetime(2022, 5, 9, 6, 41, 30),
                  'file_ingest_id': 64391,
                  '_is_folder': 0,
                  'auto_uncompress': 0,
                  'file_ingest_status_id': 22,
                  'local_purge_days': 2}

        self.tape.put_file_ingest([64391], record)

        self.assertIn(
            call.execute(
                'update file_ingest set  validate_mode=%s, _callback=%s, file_name=%s, _file=%s, _put_mode=%s, file_size=%s, file_owner=%s, _destination=%s, _call_source=%s, transfer_mode=%s, _status=%s, file_path=%s, _services=%s, file_permissions=%s, _dt_modified=%s, file_id=%s, _is_file=%s, _metadata_ingest_id=%s, file_group=%s, metadata_id=%s, file_date=%s, file_ingest_id=%s, _is_folder=%s, auto_uncompress=%s, file_ingest_status_id=%s, local_purge_days=%s where file_ingest_id=64391',
                [0, 'file_ingest', '3300052084.tar.gz',
                 '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz', 2, 3323992, 'img',
                 '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz', 'folder', 0, 'new',
                 '/global/dna/projectdirs/microbial/img_web_data_ava/download', '[1]', '0100644',
                 datetime(2022, 5, 9, 6, 45), 14512785, 1, '62791a11c2c506c5afdfce75', 'img',
                 '62791a11c2c506c5afdfce76', datetime(2022, 5, 9, 6, 41, 30), 64391, 0, 0, 22, 2]
            ),
            self.cursor.mock_calls)

    def test_Tape_post_file_ingest_replace_if_newer_in_request_existing_file_is_newer(self):
        record = {'file_ingest_id': 64391, 'file_ingest_status_id': 19, 'file_id': 14512785, 'file_size': 3323992,
                  'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
                  '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
                  '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
                  '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
                  '_call_source': 'folder', '_status': 'new', '_callback': 'file_ingest',
                  'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
                  'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
                  'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
                  'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}
        self.cursor.fetchall.side_effect = [
            [record],
            [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
              'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
              'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
              'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45),
              'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
        ]

        self.assertEqual(self.tape.post_file_ingest([64391], record), {'file_id': 14512785,
                                                                       'file_ingest_id': 64391,
                                                                       'metadata_id': '62791a11c2c506c5afdfce76',
                                                                       'status': 'old'})

    @patch('tape.restful.RestServer')
    def test_Tape_post_file_ingest_replace_if_newer_in_request_existing_file_is_older(self, restserver_mock):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver_mock.Instance.return_value = server
        self.cursor.lastrowid = 1
        record = {'file_ingest_id': 64391, 'file_ingest_status_id': 19, 'file_id': 14512785, 'file_size': 3323992,
                  'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
                  '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
                  '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
                  '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
                  '_call_source': 'folder', '_status': 'new', '_callback': 'file_ingest',
                  'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
                  'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
                  'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
                  'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}
        self.cursor.fetchall.side_effect = [
            [record],
            [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
              'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
              'file_date': datetime(2021, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
              'file_status_id': 6, 'created_dt': datetime(2021, 5, 9, 6, 45), 'division': 'jgi',
              'modified_dt': datetime(2021, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
              'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
              'file_date': datetime(2021, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
              'file_status_id': 6, 'created_dt': datetime(2021, 5, 9, 6, 45), 'division': 'jgi',
              'modified_dt': datetime(2021, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [{'backup_record_id': 18825401, 'file_id': 14512785, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 16, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 9, 6, 45, 51),
              'dt_to_release': datetime(2022, 5, 10, 6, 45, 51)}],
            [{'file_status_history_id': 140881594, 'file_id': 14512785, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 9, 6, 45), 'dt_end': datetime(2022, 5, 9, 6, 45)}],
        ]

        self.assertEqual(self.tape.post_file_ingest([64391], record), {'file_id': 14512785,
                                                                       'file_ingest_id': 64391,
                                                                       'metadata_id': '62791a11c2c506c5afdfce76',
                                                                       'status': 'replaced'})
        self.assertIn(call.execute('delete from pull_queue where file_id = %s and queue_status_id = %s', (14512785, 1)),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('update file set source = NULL where file_id = %s', (14512785,)),
                      self.cursor.mock_calls)

    @patch('task.datetime')
    @patch('tape.restful.RestServer')
    def test_Tape_post_file_ingest_replace_if_newer_in_request_no_existing_file(self, restserver, datetime_mock):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        self.cursor.lastrowid = 1
        record = {'file_ingest_id': 64391, 'file_ingest_status_id': 19, 'file_id': None, 'file_size': None,
                  'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
                  '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
                  '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
                  '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
                  '_call_source': 'folder', '_status': 'new', '_callback': 'file_ingest',
                  'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
                  'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
                  'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
                  'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}
        self.cursor.fetchall.side_effect = [
            [record],
            [],
            [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
              'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
              'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
              'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45), 'division': 'jgi',
              'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [{'backup_record_id': 18825401, 'file_id': 14512785, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 16, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 9, 6, 45, 51),
              'dt_to_release': datetime(2022, 5, 10, 6, 45, 51)}],
            [{'file_status_history_id': 140881594, 'file_id': 14512785, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 9, 6, 45), 'dt_end': datetime(2022, 5, 9, 6, 45)}],
        ]

        self.assertEqual(self.tape.post_file_ingest([64391], record), {'file_id': 1,
                                                                       'file_ingest_id': 64391,
                                                                       'metadata_id': '62791a11c2c506c5afdfce76',
                                                                       'status': 'new'})

    @parameterized.expand([
        ('success',
         [
             [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
               'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
               '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
               '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
               '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
               '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
               'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
             [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
               'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
               'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45),
               'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
               'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
               'transfer_mode': 0}],
         ],
         {'metadata_id': '62791a11c2c506c5afdfce76'},
         {'metadata_id': '62791a11c2c506c5afdfce76'}
         ),
        ['no_record', [[]], None, {'error': 'No record found'}],
        ('invalid_file_ingest_status_id',
         [
             [{'file_ingest_id': 64391, 'file_ingest_status_id': 20, 'file_id': 14512785, 'file_size': 3323992,
               'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
               '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
               '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
               '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
               '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
               'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
         ],
         None,
         {'error': 'file_ingest_status_id not in a complete state'},
         ),
        ('record_missing_fields',
         [
             [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
               'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
               '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
               '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
               '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
               '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
               'metadata_id': None, '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
         ],
         None,
         {'error': 'Record 64391 is missing metadata_id, _metadata_ingest_id, file_id or has an incorrect _callback'},
         ),
        ('file_record_not_found',
         [
             [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
               'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
               '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
               '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
               '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
               '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
               'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
             [],
         ],
         None,
         {'error': 'file record not found for file_id 14512785'},
         ),
        ('metadata_failed',
         [
             [{'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
               'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
               '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
               '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
               '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
               '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
               'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download'}],
             [{'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
               'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
               'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'file_size': 3323992,
               'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
               'file_permissions': '0100644', 'local_purge_days': 2, 'md5sum': '588d01941eafad0769608c3ca873f3f2',
               'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45),
               'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
               'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
               'transfer_mode': 0}],
         ],
         ValueError('Wrong values'),
         {'error': f'Update failed with exception {ValueError("Wrong values")}'},
         )
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_put_file_ingest_retry(self, _description, sql_responses, metadata_response, expected, restserver):
        server = Mock()
        server.run_method.side_effect = [metadata_response]
        restserver.Instance.return_value = server
        self.cursor.fetchall.side_effect = sql_responses

        self.assertEqual(self.tape.put_file_ingest_retry([64391], None), expected)

    def test_Tape_post_replacefile(self):
        self.cursor.lastrowid = 1
        file_ingest_record = {
            'file_ingest_id': 64391, 'file_ingest_status_id': 22, 'file_id': 14512785, 'file_size': 3323992,
            'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
            '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
            '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
            '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
            '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
            'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
            'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
            'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
            'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
            'source': 'baz', 'division': 'jgi',
        }
        self.cursor.fetchall.side_effect = [
            [{'file_id': 14512785, 'file_path': '/global/dna/dm_archive/img/submissions/268439',
              'file_name': '3300052084.tar.gz', 'division': 'jgi'}],
            [file_ingest_record],
        ]

        self.tape.post_replacefile(None, {'dest': ObjectId('62791a11c2c506c5afdfce76'),
                                          'src': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
                                          'source': 'baz'})

        self.assertIn(file_ingest_record, self.tape.divisions.get('jgi').ingest_queue.feature_queues.get('baz'))
        self.assertIn(call.execute(
            'insert into file_ingest ( file_id, metadata_id, division, _file, _destination, _put_mode, _callback, _status, source) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            [14512785, '62791a11c2c506c5afdfce76', 'jgi',
             '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
             '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz', 3, 'replacefile2', 'new', 'baz']),
            self.cursor.mock_calls)

    @patch('tape.restful.RestServer')
    def test_Tape_post_replacefile2(self, restserver):
        self.cursor.lastrowid = 1
        server = Mock()
        server.run_method.side_effect = {'foo': 'bar'}
        restserver.Instance.return_value = server
        file_record = {'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                       'file_path': '/global/dna/dm_archive/img/submissions/268439',
                       'origin_file_name': '3300052084.tar.gz',
                       'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                       'file_size': 3323992,
                       'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
                       'file_permissions': '0100644', 'local_purge_days': 2,
                       'md5sum': '588d01941eafad0769608c3ca873f3f2',
                       'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45), 'division': 'jgi',
                       'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                       'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                       'transfer_mode': 0, 'file_status': 'BACKUP_READY', 'dt_to_purge': None}
        self.cursor.fetchall.side_effect = [
            [{'file_ingest_id': 64391, 'file_ingest_status_id': 19, 'file_id': 14512785, 'file_size': 3323992,
              'validate_mode': 0, 'transfer_mode': 0, 'local_purge_days': 2, 'auto_uncompress': 0, '_put_mode': 2,
              '_is_folder': 0, '_is_file': 1, '_dt_modified': datetime(2022, 5, 9, 6, 45),
              '_file': '/global/dna/projectdirs/microbial/img_web_data_ava/download/3300052084.tar.gz',
              '_services': '[1]', '_destination': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
              '_call_source': 'file', '_status': 'new', '_callback': 'file_ingest',
              'metadata_id': '62791a11c2c506c5afdfce76', '_metadata_ingest_id': '62791a11c2c506c5afdfce75',
              'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
              'file_permissions': '0100644', 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download', 'division': 'jgi'}],
            [file_record],
            [{'backup_record_id': 18825401, 'file_id': 14512785, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 16, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 9, 6, 45, 51),
              'dt_to_release': datetime(2022, 5, 10, 6, 45, 51)}],
            [{'file_status_history_id': 140881594, 'file_id': 14512785, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 9, 6, 45), 'dt_end': datetime(2022, 5, 9, 6, 45)}],
        ]

        self.tape.post_replacefile2([64391], {'file_ingest_status_id': 22})

        copy_queue_data = []
        for q1 in self.tape.divisions.get('jgi').copy_queue.feature_queues.values():
            for q2 in q1:
                copy_queue_data.append(q2)
        self.assertIn(file_record, copy_queue_data)
        self.assertIn({'md5_queue_id': 1, 'callback': 'local://put_file/14512785',
                       'file_path': '/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz',
                       'file_size': 3323992, 'division': 'jgi'},
                      list(self.tape.divisions.get('jgi').md5_queue.feature_queues.values())[0])
        self.assertIn(call.execute('delete from pull_queue where file_id = %s and queue_status_id = %s', (14512785, 1)),
                      self.cursor.mock_calls)
        self.assertIn(call.execute('update file set source = NULL where file_id = %s', (14512785,)),
                      self.cursor.mock_calls)

    def test_Tape_post_generatemd5(self):
        file_record = {'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                       'file_path': '/global/dna/dm_archive/img/submissions/268439',
                       'origin_file_name': '3300052084.tar.gz',
                       'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                       'file_size': 3323992,
                       'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img', 'file_group': 'img',
                       'file_permissions': '0100644', 'local_purge_days': 2,
                       'md5sum': '588d01941eafad0769608c3ca873f3f2',
                       'file_status_id': 6, 'created_dt': datetime(2022, 5, 9, 6, 45),
                       'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                       'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                       'transfer_mode': 0, 'file_status': 'BACKUP_READY', 'dt_to_purge': None, 'division': 'jgi'}
        self.cursor.fetchall.side_effect = [[file_record]]
        self.cursor.lastrowid = 1

        self.tape.post_generatemd5([14512785], None)

        self.assertIn(
            call.execute('insert into md5_queue ( file_path, file_size, callback, division) values (%s,%s,%s,%s)',
                         ['/global/dna/dm_archive/img/submissions/268439/3300052084.tar.gz', 3323992,
                          'local://put_file/14512785', 'jgi']),
            self.cursor.mock_calls)

    def test_Tape_put_file_status(self):
        self.tape.put_file_status([8, 14512785], None)

        self.assertIn(call.execute('update file set file_status_id=%s where file_id=%s', (8, 14512785)),
                      self.cursor.mock_calls)
        self.assertIn(
            call.execute('update backup_record set backup_record_status_id=%s where file_id=%s', (1, 14512785)),
            self.cursor.mock_calls)

    def test_Tape_get_filehistory(self):
        record = {'file_status_history_id': 140881594, 'file_id': 14512785, 'file_status_id': 1,
                  'dt_begin': datetime(2022, 5, 9, 6, 45), 'dt_end': datetime(2022, 5, 9, 6, 45)}
        self.cursor.fetchall.side_effect = [
            [record],
        ]

        self.assertEqual(self.tape.get_filehistory([14512785], None), [record])
        self.cursor.execute.assert_called_with('select * from file_status_history where file_id=%s limit 500',
                                               [14512785])

    @parameterized.expand([
        ('no_args_kwargs',
         [
             [
                 {
                     'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                     'file_path': '/global/dna/dm_archive/img/submissions/268439',
                     'origin_file_name': '3300052084.tar.gz',
                     'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                     'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
                     'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
                     'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
                     'created_dt': datetime(2022, 5, 9, 6, 45),
                     'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                     'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                     'transfer_mode': 0
                 },
             ],
         ],
         [],
         {},
         [call.execute('select * from file limit 500')],
         [{
             'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
             'file_path': '/global/dna/dm_archive/img/submissions/268439',
             'origin_file_name': '3300052084.tar.gz',
             'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
             'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
             'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
             'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
             'created_dt': datetime(2022, 5, 9, 6, 45),
             'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
             'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
             'transfer_mode': 0
         }],
         ),
        ('query_results',
         [
             [{'record_count': 1}],
             [
                 {
                     'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                     'file_path': '/global/dna/dm_archive/img/submissions/268439',
                     'origin_file_name': '3300052084.tar.gz',
                     'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                     'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
                     'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
                     'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
                     'created_dt': datetime(2022, 5, 9, 6, 45),
                     'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                     'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                     'transfer_mode': 0
                 },
             ]
         ],
         [],
         {'queryResults': True, 'fields': None, 'query': ''},
         [call.execute('select count(*) as record_count from file '),
          call.execute('select * from file  limit 0,100'),
          ],
         {'data': [{'auto_uncompress': 0,
                    'created_dt': datetime(2022, 5, 9, 6, 45),
                    'file_date': datetime(2022, 5, 9, 6, 41, 30),
                    'file_group': 'img',
                    'file_id': 14512785,
                    'file_name': '3300052084.tar.gz',
                    'file_owner': 'img',
                    'file_path': '/global/dna/dm_archive/img/submissions/268439',
                    'file_permissions': '0100644',
                    'file_size': 3323992,
                    'file_status_id': 6,
                    'local_purge_days': 2,
                    'md5sum': '588d01941eafad0769608c3ca873f3f2',
                    'metadata_id': '62791a11c2c506c5afdfce76',
                    'modified_dt': datetime(2022, 5, 9, 6, 47, 25),
                    'origin_file_name': '3300052084.tar.gz',
                    'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                    'remote_purge_days': None,
                    'transaction_id': 1,
                    'transfer_mode': 0,
                    'user_save_till': None,
                    'validate_mode': 0}],
          'record_count': 1,
          'return_count': 100},
         ),
        ('args',
         [
             [
                 {'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                  'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
                  'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                  'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
                  'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
                  'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
                  'created_dt': datetime(2022, 5, 9, 6, 45),
                  'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                  'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                  'transfer_mode': 0, 'file_status': 'BACKUP_READY', 'dt_to_purge': None}
             ],
         ],
         ['3300052084.tar.gz'],
         {},
         [call.execute('select f.*, c.status as file_status, STR_TO_DATE(GREATEST(ifnull(date_add(created_dt, interval local_purge_days day),"0000-00-00 00:00:00"),ifnull(user_save_till,"0000-00-00 00:00:00")),"%%Y-%%m-%%d %%T") as dt_to_purge from file f left join file_status_cv c on c.file_status_id=f.file_status_id  where file_name=%s limit 500', ['3300052084.tar.gz'])],
         [
             {'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
              'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
              'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
              'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
              'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
              'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
              'created_dt': datetime(2022, 5, 9, 6, 45),
              'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'file_status': 'BACKUP_READY', 'dt_to_purge': None}
         ],
         ),
    ])
    def test_Tape_get_files(self, _description, sql_responses, args, kwargs, expected_calls, expected):
        self.cursor.fetchall.side_effect = sql_responses

        self.assertEqual(self.tape.get_files(args, kwargs), expected)
        for c in expected_calls:
            self.assertIn(c, self.cursor.mock_calls)

    def test_Tape_get_latestfile(self):
        record = {'file_id': 14512785, 'transaction_id': 1, 'file_name': '3300052084.tar.gz',
                  'file_path': '/global/dna/dm_archive/img/submissions/268439', 'origin_file_name': '3300052084.tar.gz',
                  'origin_file_path': '/global/dna/projectdirs/microbial/img_web_data_ava/download',
                  'file_size': 3323992, 'file_date': datetime(2022, 5, 9, 6, 41, 30), 'file_owner': 'img',
                  'file_group': 'img', 'file_permissions': '0100644', 'local_purge_days': 2,
                  'md5sum': '588d01941eafad0769608c3ca873f3f2', 'file_status_id': 6,
                  'created_dt': datetime(2022, 5, 9, 6, 45),
                  'modified_dt': datetime(2022, 5, 9, 6, 47, 25), 'validate_mode': 0, 'user_save_till': None,
                  'metadata_id': '62791a11c2c506c5afdfce76', 'auto_uncompress': 0, 'remote_purge_days': None,
                  'transfer_mode': 0, 'file_status': 'BACKUP_READY', 'dt_to_purge': None}
        self.cursor.fetchall.side_effect = [[record]]
        self.assertEqual(self.tape.get_latestfile(None, {'file': '/global/dna/dm_archive/img/submissions/268439'}),
                         record)
        self.cursor.execute.assert_called_with(
            'select * from file where (file_name=%s and file_path=%s) or (origin_file_name=%s and origin_file_path=%s) order by file_id desc limit 1',
            ['268439', '/global/dna/dm_archive/img/submissions', '268439', '/global/dna/dm_archive/img/submissions'])

    def test_Tape_post_service(self):
        self.cursor.lastrowid = 100
        request = {'service': 'foobar', 'submited_dt': None, 'started_dt': '2022-05-08 9:54:15',
                   'ended_dt': None, 'seconds_to_run': 0, 'last_heartbeat': datetime(2022, 5, 9, 7, 0, 14),
                   'available_threads': 5, 'hostname': 'dtn03.nersc.gov',
                   'tasks': 'ingest,delete,purge,put,copy,md5,tar', 'division': 'jgi'}

        self.assertEqual(self.tape.post_service(None, request), {'service_id': 100})
        self.assertIn(
            call.execute(
                'insert into service ( service, started_dt, seconds_to_run, last_heartbeat, available_threads, hostname, tasks) values (%s,%s,%s,%s,%s,%s,%s)',
                ['foobar', '2022-05-08 9:54:15', 0, datetime(2022, 5, 9, 7, 0, 14), 5, 'dtn03.nersc.gov',
                 'ingest,delete,purge,put,copy,md5,tar']
            ),
            self.cursor.mock_calls)

    def test_Tape_put_service(self):
        self.cursor.lastrowid = 1
        request = {'last_heartbeat': datetime(2022, 5, 9, 7, 0, 14), 'hostname': 'dtn03.nersc.gov',
                   'available_threads': 5, 'tasks': 'ingest,delete,purge,put,copy,md5,tar',
                   'started_dt': '2022-05-08 9:54:15',
                   'service_id': 3423,
                   'seconds_to_run': 0,
                   'submited_dt': None,
                   'ended_dt': None,
                   }

        self.tape.put_service([3423], request)

        self.assertIn(
            call.execute(
                'update service set  last_heartbeat=%s, hostname=%s, available_threads=%s, tasks=%s, started_dt=%s, service_id=%s, seconds_to_run=%s where service_id=3423',
                [datetime(2022, 5, 9, 7, 0, 14), 'dtn03.nersc.gov', 5, 'ingest,delete,purge,put,copy,md5,tar',
                 '2022-05-08 9:54:15', 3423, 0]),
            self.cursor.mock_calls)

    def test_Tape_get_service(self):
        record = {'service_id': 3423, 'submited_dt': None, 'started_dt': datetime(2022, 5, 8, 9, 54, 15),
                  'ended_dt': None, 'seconds_to_run': 0, 'last_heartbeat': datetime(2022, 5, 9, 7, 0, 14),
                  'available_threads': 5, 'hostname': 'dtn03.nersc.gov',
                  'tasks': 'ingest,delete,purge,put,copy,md5,tar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_service([3423], None), record)
        self.cursor.execute.assert_called_with('select * from service where service_id=%s limit 500', [3423])

    def test_Tape_get_services(self):
        record = {'service_id': 3423, 'submited_dt': None, 'started_dt': datetime(2022, 5, 8, 9, 54, 15),
                  'ended_dt': None, 'seconds_to_run': 0, 'last_heartbeat': datetime(2022, 5, 9, 7, 0, 14),
                  'available_threads': 5, 'hostname': 'dtn03.nersc.gov',
                  'tasks': 'ingest,delete,purge,put,copy,md5,tar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_services(None, {}), [record])
        self.cursor.execute.assert_called_with('select * from service limit 500')

    def test_Tape_get_currentservices(self):
        record = {'service_id': 3407, 'submited_dt': None, 'started_dt': datetime(2022, 4, 28, 18, 44, 4),
                  'ended_dt': None, 'seconds_to_run': 0, 'last_heartbeat': datetime(2022, 5, 2, 9, 8, 53),
                  'available_threads': 5, 'hostname': 'dtn03.nersc.gov',
                  'tasks': 'ingest,delete,purge,put,copy,md5,tar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_currentservices(None, None), [record])
        self.cursor.execute.assert_called_with(
            'select * from service where ended_dt is null and (last_heartbeat is not null and last_heartbeat>date_sub(now(), interval 5 minute) or (last_heartbeat is null and submited_dt> date_sub(now(), interval 5 hour))) limit 500')

    def test_Tape_get_tarrecord(self):
        record = {'tar_record_id': 224967, 'root_path': '/global/dna/dm_archive/',
                  'remote_path': '/home/projects/dm_archive/root/000/224/224967.tar'}
        self.cursor.fetchall.side_effect = [[record]]

        self.assertEqual(self.tape.get_tarrecord([224967], None), record)
        self.cursor.execute.assert_called_with('select * from tar_record record where tar_record_id=%s limit 500',
                                               [224967])

    def test_Tape_put_tar(self):
        self.tape.put_tar([224967], {'root_path': '/global/dna/dm_archive/', 'tar_record_id': 224967,
                                     'remote_path': '/home/projects/dm_archive/root/000/224/224967.tar'})

        self.cursor.execute.assert_called_with(
            'update tar_record set  root_path=%s, tar_record_id=%s, remote_path=%s where tar_record_id=224967',
            ['/global/dna/dm_archive/', 224967, '/home/projects/dm_archive/root/000/224/224967.tar'])

    @parameterized.expand([
        ('backup_ready', 6,
         [
             [{'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa', 'local_purge_days': 0, 'file_id': 14509150,
               'file_size': 143528, 'validate_mode': 0, 'backup_record_id': 18821766, 'service': 1,
               'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379', 'md5sum': None, 'division': 'jgi'}],
         ]
         ),
        ('copy_ready', 2,
         [
             [{'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
               'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
               'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
               'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': 2,
               'created_dt': datetime(2022, 5, 9, 3, 28, 40),
               'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
               'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
               'transfer_mode': 0, 'file_status': 'COPY_FAILED', 'dt_to_purge': None, 'division': 'jgi'}],
             [{'backup_record_id': 18821766, 'file_id': 14509150, 'service': 1, 'remote_file_name': None,
               'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 1, 'md5sum': None,
               'dt_modified': datetime(2022, 5, 9, 3, 28, 39), 'dt_to_release': None}],
             [{'file_status_history_id': 140855179, 'file_id': 14509150, 'file_status_id': 1,
               'dt_begin': datetime(2022, 5, 9, 3, 28, 39),
               'dt_end': datetime(2022, 5, 9, 3, 28, 39)}],
         ]),
        ('tar_ready', 14,
         [
             [{'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
               'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
               'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
               'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': 14,
               'created_dt': datetime(2022, 5, 9, 3, 28, 40),
               'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
               'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
               'transfer_mode': 0, 'file_status': 'COPY_FAILED', 'dt_to_purge': None, 'division': 'jgi'}],
             [{'backup_record_id': 18821766, 'file_id': 14509150, 'service': 1, 'remote_file_name': None,
               'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 1, 'md5sum': None,
               'dt_modified': datetime(2022, 5, 9, 3, 28, 39), 'dt_to_release': None}],
             [{'file_status_history_id': 140855179, 'file_id': 14509150, 'file_status_id': 1,
               'dt_begin': datetime(2022, 5, 9, 3, 28, 39),
               'dt_end': datetime(2022, 5, 9, 3, 28, 39)}],
         ]),
        ('copy_complete', 4,
         [
             [{'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa', 'local_purge_days': 0, 'file_id': 14509150,
               'file_size': 143528, 'validate_mode': 0, 'backup_record_id': 18821766, 'service': 1,
               'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379', 'md5sum': None, 'division': 'jgi'}],
             [{'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa', 'local_purge_days': 0, 'file_id': 14509150,
               'file_size': 143528, 'validate_mode': 0, 'backup_record_id': 18821766, 'service': 1,
               'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379', 'md5sum': None, 'division': 'jgi'}],
             [{'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa', 'local_purge_days': 0, 'file_id': 14509150,
               'file_size': 143528, 'validate_mode': 0, 'backup_record_id': 18821766, 'service': 1,
               'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379', 'md5sum': None, 'division': 'jgi'}],
         ]),
        ('tar_complete', 16,
         [
             [{'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa', 'local_purge_days': 0, 'file_id': 14509150,
               'file_size': 143528, 'validate_mode': 0, 'backup_record_id': 18821766, 'service': 1,
               'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379', 'md5sum': None, 'division': 'jgi'}],
             [{'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
               'file_path': '/global/dna/dm_archive/img/submissions/268379',
               'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
               'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
               'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
               'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': 16,
               'created_dt': datetime(2022, 5, 9, 3, 28, 40),
               'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
               'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
               'transfer_mode': 0, 'file_status': 'COPY_FAILED', 'dt_to_purge': None, 'division': 'jgi'}],
             [{'backup_record_id': 18821766, 'file_id': 14509150, 'service': 1, 'remote_file_name': None,
               'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 1, 'md5sum': None,
               'dt_modified': datetime(2022, 5, 9, 3, 28, 39), 'dt_to_release': None}],
             [{'file_status_history_id': 140855179, 'file_id': 14509150, 'file_status_id': 1,
               'dt_begin': datetime(2022, 5, 9, 3, 28, 39),
               'dt_end': datetime(2022, 5, 9, 3, 28, 39)}],
         ])
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_put_file(self, _description, file_status_id, sql_responses, restserver):
        self.cursor.lastrowid = 1
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        request = {'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
                   'file_path': '/global/dna/dm_archive/img/submissions/268379',
                   'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
                   'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
                   'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
                   'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': file_status_id,
                   'created_dt': datetime(2022, 5, 9, 3, 28, 40),
                   'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
                   'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
                   'transfer_mode': 0}
        self.cursor.execute.return_value = 1
        self.cursor.fetchall.side_effect = sql_responses

        self.assertEqual(self.tape.put_file([14509150], request), 1)

    def test_Tape_md5_selected(self):
        self.tape.md5_selected({'md5_queue_id': 431849})

        self.cursor.execute.assert_called_with('update md5_queue set queue_status_id=%s where md5_queue_id=%s',
                                               (2, 431849))

    @parameterized.expand([
        ('metadata_records',
         [{'file_path': '/global/dna/dm_archive/img/submissions/268379'}],
         {'file_id': 14509150, 'file_path': '/global/dna/dm_archive/img/submissions/268379'},
         [call.execute('update file set file_status_id=%s where file_id=%s', (15, 14509150))]),
        ('no_metadata_records',
         [],
         None,
         [call.execute('update file set file_status_id=%s where file_id=%s', (15, 14509150)),
          call.execute('update file set file_status_id=%s where file_id=%s', (11, 14509150))]),
    ])
    @patch('tape.restful.RestServer')
    @patch('tape.time.sleep')
    def test_Tape_tar_selected(self, _description, metadata_response, expected, expected_calls, sleep, restserver):
        server = Mock()
        server.run_method.return_value = metadata_response
        restserver.Instance.return_value = server

        self.assertEqual(self.tape.tar_selected({'file_id': 14509150}), expected)
        for c in expected_calls:
            self.assertIn(c, self.cursor.mock_calls)

    def test_Tape_copy_selected(self):
        self.tape.copy_selected({'file_id': 14509150})

        self.cursor.execute.assert_called_with('update file set file_status_id=%s where file_id=%s', (3, 14509150))

    def test_Tape_transfer_selected(self):
        self.tape.transfer_selected({'records': [{'backup_record_id': 3001}]})

        self.assertIn(
            call.execute('update backup_record set backup_record_status_id=3 where backup_record_id in (3001)', ()),
            self.cursor.mock_calls)
        self.assertIn(call.execute(
            'update file f left join backup_record b on b.file_id=f.file_id set file_status_id=7 where backup_record_id in (3001)',
            ()), self.cursor.mock_calls)

    def test_Tape_pull_selected(self):
        pull_queue_records = [{'pull_queue_id': 11527020, 'file_id': 123}]
        info = [{'pull_queue_id': 11527020, 'volume': 'AG8142', 'position_a': 142, 'position_b': 0,
                 'requestor': 'foo@bar.com', 'priority': 6, 'file_permissions': '0100640',
                 'file_path': '/global/dna/dm_archive/img/submissions/254666', 'service': 1,
                 'file_name': 'Ga0466400_contigs.fna', 'remote_file_path': '.',
                 'remote_file_name': 'Ga0466400_contigs.fna.17376984', 'backup_record_id': 17376984,
                 'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar', 'tar_record_id': 392751,
                 'division': 'jgi'}]
        self.cursor.fetchall.side_effect = [pull_queue_records, info]
        expected_sql_calls = [
            call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (2, 11527020)),
            call.execute('update file set file_status_id = %s where file_id = %s', (12, 123)),
            call.execute(
                'select q.pull_queue_id, q.volume, q.position_a, q.position_b, q.requestor, q.priority, f.file_permissions, f.file_path, b.service, f.file_name, b.remote_file_path, b.remote_file_name, b.backup_record_id, t.remote_path, b.tar_record_id, f.division from file f join pull_queue q on f.file_id = q.file_id left join backup_record b on f.file_id = b.file_id and b.service = %s left join tar_record t on t.tar_record_id = b.tar_record_id where q.queue_status_id = %s and q.volume = %s',
                [1, 2, 'AG8142']),
        ]

        self.assertEqual(self.tape.pull_selected('AG8142', 'jgi', 1), info)
        for c in expected_sql_calls:
            self.assertIn(c, self.cursor.mock_calls)

    @parameterized.expand([
        ('in_progress', 2, 12),
        ('complete', 3, 13),
    ])
    @patch('tape.restful.RestServer')
    def test_Tape_put_pull(self, _description, queue_status_id, file_status_id, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        record = {'priority': 6,
                  'queue_status_id': queue_status_id,
                  'pull_queue_id': 11527020,
                  'file_id': 13368043,
                  'requestor': 'foo@bar.com',
                  'volume': 'AG8142',
                  'position_b': 0,
                  'tar_record_id': 392751,
                  'dt_modified': datetime(2022, 5, 9, 6, 56, 33),
                  'position_a': 142}
        self.cursor.fetchall.side_effect = [[{'file_id': 13368043}]]

        self.tape.put_pull([11527020], record)

        self.assertIn(
            call.execute(
                'update pull_queue set  priority=%s, queue_status_id=%s, pull_queue_id=%s, file_id=%s, requestor=%s, volume=%s, position_b=%s, tar_record_id=%s, dt_modified=%s, position_a=%s where pull_queue_id=11527020',
                [6, queue_status_id, 11527020, 13368043, 'foo@bar.com', 'AG8142', 0, 392751,
                 datetime(2022, 5, 9, 6, 56, 33),
                 142]),
            self.cursor.mock_calls)
        self.assertIn(call.execute('update file set  file_status_id=%s where file_id=13368043', [file_status_id]),
                      self.cursor.mock_calls)

    def test_Tape_get_heartbeat(self):
        prep_queue = Mock()
        pull_queue = Mock()
        prep_queue.get_pending_tasks_count.return_value = 100
        pull_queue.get_pending_tasks_count.return_value = 200
        self.tape.divisions['jgi'].prep_queue = prep_queue
        self.tape.divisions['jgi'].pull_queue = pull_queue
        self.tape.use_db_prep_tasks = True
        self.tape.use_db_pull_tasks = True

        self.assertEqual(self.tape.get_heartbeat(['jgi', 2], None),
                         {'copy': {'currently_running': 0, 'file_size': 2265588, 'record_count': 2},
                          'delete': {'currently_running': 0, 'file_size': 0, 'record_count': 0},
                          'ingest': {'currently_running': 0, 'file_size': 7506, 'record_count': 2},
                          'md5': {'currently_running': 0, 'file_size': 202827386880, 'record_count': 1},
                          'prep': {'record_count': 100},
                          'pull': {'record_count': 200},
                          'purge': {'currently_running': 0, 'file_size': 0, 'record_count': 0},
                          'put': {'currently_running': 0, 'file_size': 0, 'record_count': 0},
                          'tar': {'currently_running': 0, 'file_size': 602653198, 'record_count': 2}})
        self.cursor.execute.assert_called_with('update service set  last_heartbeat=now() where service_id=2', [])

    @parameterized.expand([
        ('not_in_resources_gone', False),
        ('in_resources_gone', True),
    ])
    @patch('tape.datetime')
    def test_Tape_post_resourceoffline(self, _description, in_resources_gone, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        if in_resources_gone:
            self.tape.resources_gone['foo'] = {}

        self.tape.post_resourceoffline(None, {'resource': 'foo', 'service_id': 2})

        self.assertEqual(self.tape.resources_gone,
                         {'foo': {2: datetime(2000, 1, 2, 3, 4, 5)}})

    def test_Tape_post_resourceonline(self):
        self.tape.resources_gone['foo'] = {2: ''}

        self.tape.post_resourceonline(None, {'resource': 'foo', 'service_id': 2})

        self.assertEqual(len(self.tape.resources_gone), 0)

    def test_Tape_get_folders(self):
        self.cursor.fetchall.side_effect = [
            [{'file_path': '/global/dna/dm_archive/img/submissions/10000'}],
            [{'file_name': '1074260.scaffolds.cov', 'file_path': '/global/dna/dm_archive/img/submissions',
              'file_size': 141742514}],
        ]

        self.assertEqual(self.tape.get_folders(['global', 'dna', 'dm_archive', 'img', 'submissions'], None),
                         [{'file_path': '/global/dna/dm_archive/img/submissions/10000', 'is_dir': True},
                          {'file_name': '1074260.scaffolds.cov',
                           'file_path': '/global/dna/dm_archive/img/submissions',
                           'file_size': 141742514}])

    @patch('tape.restful.RestServer')
    def test_Tape_put_savefile(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.cursor.fetchall.side_effect = [
            [{'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
              'file_path': '/global/dna/dm_archive/img/submissions/268379',
              'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
              'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
              'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
              'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': 5,
              'created_dt': datetime(2022, 5, 9, 3, 28, 40),
              'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0}],
            [{'file_id': 14509150, 'transaction_id': 1, 'file_name': 'Ga0536210_prodigal_proteins.faa',
              'file_path': '/global/dna/dm_archive/img/submissions/268379',
              'origin_file_name': 'Ga0536210_prodigal_proteins.faa',
              'origin_file_path': '/global/cfs/cdirs/img/annotated_submissions/268379', 'file_size': 143528,
              'file_date': datetime(2022, 5, 6, 22, 48, 40), 'file_owner': 'gbp', 'file_group': 'img',
              'file_permissions': '0100664', 'local_purge_days': 2, 'md5sum': None, 'file_status_id': 16,
              'created_dt': datetime(2022, 5, 9, 3, 28, 40),
              'modified_dt': datetime(2022, 5, 9, 3, 44, 35), 'validate_mode': 0, 'user_save_till': None,
              'metadata_id': '6278ec89c2c506c5afdfb211', 'auto_uncompress': 0, 'remote_purge_days': None,
              'transfer_mode': 0, 'file_status': 'COPY_FAILED', 'dt_to_purge': None}],
            [{'backup_record_id': 18821766, 'file_id': 14509150, 'service': 1, 'remote_file_name': None,
              'remote_file_path': None, 'tar_record_id': None, 'backup_record_status_id': 1, 'md5sum': None,
              'dt_modified': datetime(2022, 5, 9, 3, 28, 39), 'dt_to_release': None}],
            [{'file_status_history_id': 140855179, 'file_id': 14509150, 'file_status_id': 1,
              'dt_begin': datetime(2022, 5, 9, 3, 28, 39),
              'dt_end': datetime(2022, 5, 9, 3, 28, 39)}],
        ]

        self.tape.put_savefile(None, {'days': 10,
                                      'file': '/global/dna/dm_archive/img/submissions/268379/Ga0536210_prodigal_proteins.faa',
                                      })

        self.assertIn(call.execute(
            'update file set user_save_till=date_add(now(), interval %s day) where file_id=%s and (user_save_till is null or user_save_till< date_add(now(), interval %s day))',
            (10, 14509150, 10)), self.cursor.mock_calls)

    def test_Tape_post_current_quota(self):
        self.tape.post_current_quota(None, {'quotas': [{"fs": "archive",
                                                        "space_used": "1000.00B",
                                                        "space_quota": "2000.00B",
                                                        "pace_perc": "50.0%"}]})

        self.assertEqual(self.tape.quota_used, 1000)

    @patch('tape.datetime')
    def test_Tape_get_diskusage(self, datetime_mock):
        datetime_mock.datetime.today.return_value = datetime(2000, 1, 2, 3, 4, 5)
        del self.tape.disk_usage['date_updated']

        self.assertEqual(self.tape.get_diskusage(None, None), {'bytes_free': 49999990,
                                                               'bytes_used': 50000010,
                                                               'disk_reserve': 10,
                                                               'files': 10,
                                                               'disk_usage_files': 50,
                                                               'files_restoring': 2,
                                                               'disk_usage_files_restoring': 10,
                                                               'disk_usage_other': 49999960,
                                                               'dna_free_tb': 4.999999e-05})

    def test_Tape_add_all_to_queue_by_feature(self):
        records = [{'path': '/path/to/local/my_file_1.txt'}, {'path': '/path/to/remote/my_file_2.txt'},
                   {'path': '/path/to/remote/my_file_2.txt', 'source': 'bar'}]
        queue = Mock()

        self.tape._add_all_to_queue_by_feature(queue, records, lambda r: r.get('path'))

        for c in [call.add({'path': '/path/to/local/my_file_1.txt'}),
                  call.add({'path': '/path/to/remote/my_file_2.txt'}, ['foo'], False),
                  call.add({'path': '/path/to/remote/my_file_2.txt'}, ['foo'], False)]:
            self.assertIn(c, queue.mock_calls)

    @parameterized.expand([
        ('not_remote', '/path/to/local/my_file.txt', {'foo': 'bar'}, call.add({'foo': 'bar'})),
        ('remote_by_file_path', '/path/to/remote/my_file.txt', {'foo': 'bar'}, call.add({'foo': 'bar'}, ['foo'], False)),
        ('remote_by_file_source', '/path/to/remote/my_file.txt', {'foo': 'bar', 'source': 'bar'}, call.add({'foo': 'bar', 'source': 'bar'}, ['bar'], False)),
    ])
    def test_Tape_add_to_queue_by_feature(self, _description, file_path, record, expected_add_call):
        queue = Mock()

        self.tape._add_to_queue_by_feature(queue, file_path, record)

        self.assertIn(expected_add_call, queue.mock_calls)

    @parameterized.expand([
        ('origin_file_path',
         {'origin_file_path': '/path/to/origin_file_path', 'origin_file_name': 'origin_file_name.txt',
          'file_path': '/path/to/file_path', 'file_name': 'file_name.txt',
          'local_purge_days': 2},
         '/path/to/origin_file_path',
         ),
        ('origin_file_path_0_local_purge_days',
         {'origin_file_path': '/path/to/origin_file_path', 'origin_file_name': 'origin_file_name.txt',
          'file_path': '/path/to/file_path', 'file_name': 'file_name.txt',
          'local_purge_days': 0},
         '/path/to/file_path'
         ),
        ('file_path',
         {'origin_file_path': None, 'origin_file_name': None,
          'file_path': '/path/to/file_path', 'file_name': 'file_name.txt',
          'local_purge_days': 2},
         '/path/to/file_path'
         ),
    ])
    def test_Tape_get_tar_record_path(self, _description, record, expected):
        self.assertEqual(self.tape._get_tar_record_path(record), expected)

    def test_Tape_get_registered_egress_requests(self):
        expected = [{
            'egress_id': 1, 'requestor': 'foobar', 'source': 'my_source', 'file_id': 2, 'file_name': 'file_name.txt',
            'file_path': '/path/to/file_path', 'file_status_id': 4
        }]
        self.cursor.fetchall.return_value = [{
            'egress_id': 1, 'requestor': 'foobar', 'source': 'my_source', 'file_id': 2, 'file_name': 'file_name.txt',
            'file_path': '/path/to/file_path', 'file_status_id': 4
        }]

        self.assertEqual(self.tape.get_registered_egress_requests(['my_source'], {}), expected)
        self.assertIn(call.execute(
            'select e.egress_id, e.requestor, e.source, e.file_id, f.file_name, f.file_path, f.file_status_id from egress e inner join file f using(file_id) where e.egress_status_id=%s and f.file_status_id in (%s, %s, %s, %s, %s) and e.source=%s limit 500',
            [1, 4, 6, 7, 8, 13, 'my_source']), self.cursor.mock_calls)

    def test_Tape_get_egress_requests(self):
        expected = [{
            'egress_id': 1, 'egress_status_id': 3, 'requestor': 'foobar', 'source': 'my_source', 'file_id': 2
        }]
        self.cursor.fetchall.return_value = [{
            'egress_id': 1, 'egress_status_id': 3, 'requestor': 'foobar', 'source': 'my_source', 'file_id': 2
        }]

        self.assertEqual(self.tape.get_egress_requests(['my_source', 2], {}), expected)
        self.assertIn(call.execute(
            'select e.egress_id, e.egress_status_id, e.requestor, e.source, e.file_id from egress e where e.source=%s and e.file_id=%s limit 500',
            ['my_source', [2]]), self.cursor.mock_calls)

    def test_Tape_put_egress_request(self):
        self.tape.put_egress_request([1], {'egress_status_id': 2, 'bytes_transferred': 1000})

        self.assertIn(
            call.execute('update egress set  egress_status_id=%s, bytes_transferred=%s where egress_id=1', [2, 1000]),
            self.cursor.mock_calls)

    def test_Tape_get_dm_archive_roots(self):
        self.assertEqual({'nersc': '/path/to/archive', 'bar': '/path/to/bar/dm_archive'},
                         self.tape.get_dm_archive_roots(None, None))

    def test_Tape_get_backup_service_feature_name(self):
        self.assertEqual(self.tape._get_backup_service_feature_name(2), 'hsi_2')

    @parameterized.expand([
        ('missing_backup_services_to_feature_name', {}),
        ('missing_service_id_mapping', {'backup_services_to_feature_name': {}}),
    ])
    def test_Tape_get_backup_service_feature_name_configuration_error(self, _description, config):
        self.tape.config = SimpleNamespace(**config)
        self.assertRaises(tape.ConfigurationException, self.tape._get_backup_service_feature_name, 2)

    def test_Tape_get_backup_services(self):
        self.cursor.fetchall.side_effect = [
            [{'backup_service_id': 1, 'name': 'archive', 'server': 'archive.nersc.gov',
              'default_path': '/home/projects/dm_archive/root', 'type': 'HPSS'}],
        ]

        self.assertEqual(self.tape._get_backup_services(), {1: {'backup_service_id': 1,
                                                                'default_path': '/home/projects/dm_archive/root',
                                                                'name': 'archive',
                                                                'server': 'archive.nersc.gov',
                                                                'type': 'HPSS'}})

    @parameterized.expand([
        ('existing_and_new_services_creates_backup_record_for_new_updates_updates_file_status_from_backup_complete',
         8,
         {'services': [2, 4]},
         {'backup_record_ids': [1000]},
         [call.execute('insert into backup_record ( service, file_id, backup_record_status_id) values (%s,%s,%s)',
                       [4, 1, 2]),
          call.execute('update file set  file_status_id=%s where file_id=1', [6])],
         ),
        ('existing_and_new_services_creates_backup_record_for_new_updates_updates_file_status_from_restored',
         13,
         {'services': [2, 4]},
         {'backup_record_ids': [1000]},
         [call.execute('insert into backup_record ( service, file_id, backup_record_status_id) values (%s,%s,%s)',
                       [4, 1, 2]),
          call.execute('update file set  file_status_id=%s where file_id=1', [6])],
         ),
        ('existing_and_new_services_creates_backup_record_for_new_updates_does_not_update_file_status_from_backup_in_progress',
         7,
         {'services': [2, 4]},
         {'backup_record_ids': [1000]},
         [call.execute('insert into backup_record ( service, file_id, backup_record_status_id) values (%s,%s,%s)',
                       [4, 1, 2])],
         ),
        ('existing_services_does_not_create_backup_record_or_update_file_status',
         8,
         {'services': [2]},
         {'backup_record_ids': []},
         [],
         ),
    ])
    def test_Tape_post_backup_service_for_file(self, _description, file_status_id, request, expected, expected_calls):
        self.cursor.lastrowid = 1000
        self.cursor.fetchall.side_effect = [
            [{'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_size': 1000, 'file_status_id': file_status_id,
              'division': 'jgi'}],
            [{'service': 2}],
        ]
        self.tape.backup_services[4] = {'backup_service_id': 4, 'name': 'emsl', 'server': None,
                                        'default_path': '/archive/svc-jgi-archive/data', 'type': 'globus',
                                        'division': 'jgi'}

        self.assertEqual(self.tape.post_backup_service_for_file([1], request), expected)
        for c in expected_calls:
            self.assertIn(c, self.cursor.mock_calls)

    @parameterized.expand([
        ('file_not_found',
         [[]],
         {'services': [2]},
         ),
        ('invalid_file_status_id',
         [[{'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_size': 1000,
            'file_status_id': 2}]],
         {'services': [2]},
         ),
        ('invalid_backup_service',
         [[{'file_name': 'my_file.txt', 'file_path': '/path/to', 'file_size': 1000,
            'file_status_id': 6}],
          [{'backup_service_id': 4, 'name': 'emsl', 'server': None,
            'default_path': '/archive/svc-jgi-archive/data', 'type': 'globus'}]],
         {'services': [3]},
         ),
    ])
    def test_Tape_post_backup_service_for_file_invalid_request_raises_HttpException(self, _description, sql_responses,
                                                                                    request):
        self.cursor.fetchall.side_effect = sql_responses

        self.assertRaises(common.HttpException, self.tape.post_backup_service_for_file, [1], request)

    @parameterized.expand([
        ('unsupported_feature', ['not_supported'], [], {}, None, {}, [], [3]),
        ('no_volume_locks_priority_not_in_enabled_queue',
         ['foo'],
         [[{'pull_queue_id': 1, 'volume': 'volume_a', 'priority': 0}],
          [{'pull_queue_id': 1, 'file_id': 123}],
          [{'pull_queue_id': 1, 'volume': 'volume_a', 'position_a': 142, 'position_b': 0,
            'requestor': 'foo@bar.com', 'priority': 0, 'file_permissions': '0100640',
            'file_path': '/global/dna/dm_archive/img/submissions/254666', 'service': 1,
            'file_name': 'Ga0466400_contigs.fna', 'remote_file_path': '.',
            'remote_file_name': 'Ga0466400_contigs.fna.17376984', 'backup_record_id': 17376984,
            'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar', 'tar_record_id': 392751}
           ]],
         {},
         {'data': [{'backup_record_id': 17376984,
                    'file_name': 'Ga0466400_contigs.fna',
                    'file_path': '/global/dna/dm_archive/img/submissions/254666',
                    'file_permissions': '0100640',
                    'position_a': 142,
                    'position_b': 0,
                    'priority': 0,
                    'pull_queue_id': 1,
                    'remote_file_name': 'Ga0466400_contigs.fna.17376984',
                    'remote_file_path': '.',
                    'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar',
                    'requestor': 'foo@bar.com',
                    'service': 1,
                    'tar_record_id': 392751,
                    'volume': 'volume_a'}],
          'uses_resources': ['foo', 'bar']},
         {'volume_a': {'locked': datetime(2000, 1, 2, 3, 4, 5), 'pull_queue_id': 1}},
         [call.execute(
             'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in (%s, %s, %s) order by priority, pull_queue_id limit 1',
             [1, 'jgi', 0, 1, 3])],
         [3],
         ),
        ('volume_locks_priority_not_in_enabled_queue',
         ['foo'],
         [[{'pull_queue_id': 1, 'volume': 'volume_a', 'priority': 0}],
          [{'pull_queue_id': 1, 'file_id': 123}],
          [{'pull_queue_id': 1, 'volume': 'volume_a', 'position_a': 142, 'position_b': 0,
            'requestor': 'foo@bar.com', 'priority': 0, 'file_permissions': '0100640',
            'file_path': '/global/dna/dm_archive/img/submissions/254666', 'service': 1,
            'file_name': 'Ga0466400_contigs.fna', 'remote_file_path': '.',
            'remote_file_name': 'Ga0466400_contigs.fna.17376984', 'backup_record_id': 17376984,
            'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar', 'tar_record_id': 392751}
           ]],
         {'volume_b': {'locked': datetime(2000, 1, 2, 3, 4, 5),
                       'pull_queue_id': 2}},
         {'data': [{'backup_record_id': 17376984,
                    'file_name': 'Ga0466400_contigs.fna',
                    'file_path': '/global/dna/dm_archive/img/submissions/254666',
                    'file_permissions': '0100640',
                    'position_a': 142,
                    'position_b': 0,
                    'priority': 0,
                    'pull_queue_id': 1,
                    'remote_file_name': 'Ga0466400_contigs.fna.17376984',
                    'remote_file_path': '.',
                    'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar',
                    'requestor': 'foo@bar.com',
                    'service': 1,
                    'tar_record_id': 392751,
                    'volume': 'volume_a'}],
          'uses_resources': ['foo', 'bar']},
         {'volume_a': {'locked': datetime(2000, 1, 2, 3, 4, 5),
                       'pull_queue_id': 1},
          'volume_b': {'locked': datetime(2000, 1, 2, 3, 4, 5),
                       'pull_queue_id': 2}},
         [call.execute(
             'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in (%s, %s, %s) and volume not in (%s) order by priority, pull_queue_id limit 1',
             [1, 'jgi', 0, 1, 3, 'volume_b'])],
         [3],
         ),
        ('no_volume_locks_priority_in_enabled_queue_remaining_tasks_by_priority_empty',
         ['foo'],
         [[{'pull_queue_id': 1, 'volume': 'volume_a', 'priority': 3}],
          [{'pull_queue_id': 1, 'file_id': 123}],
          [{'pull_queue_id': 1, 'volume': 'volume_a', 'position_a': 142, 'position_b': 0,
            'requestor': 'foo@bar.com', 'priority': 3, 'file_permissions': '0100640',
            'file_path': '/global/dna/dm_archive/img/submissions/254666', 'service': 1,
            'file_name': 'Ga0466400_contigs.fna', 'remote_file_path': '.',
            'remote_file_name': 'Ga0466400_contigs.fna.17376984', 'backup_record_id': 17376984,
            'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar', 'tar_record_id': 392751}
           ],
          [{'cnt': 0}]],
         {},
         {'data': [{'backup_record_id': 17376984,
                    'file_name': 'Ga0466400_contigs.fna',
                    'file_path': '/global/dna/dm_archive/img/submissions/254666',
                    'file_permissions': '0100640',
                    'position_a': 142,
                    'position_b': 0,
                    'priority': 3,
                    'pull_queue_id': 1,
                    'remote_file_name': 'Ga0466400_contigs.fna.17376984',
                    'remote_file_path': '.',
                    'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar',
                    'requestor': 'foo@bar.com',
                    'service': 1,
                    'tar_record_id': 392751,
                    'volume': 'volume_a'}],
          'uses_resources': ['foo', 'bar']},
         {'volume_a': {'locked': datetime(2000, 1, 2, 3, 4, 5),
                       'pull_queue_id': 1}},
         [call.execute(
             'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in (%s, %s, %s) order by priority, pull_queue_id limit 1',
             [1, 'jgi', 0, 1, 3]),
          call.execute('select count(*) as cnt from pull_queue join file using(file_id) where queue_status_id = %s and priority = %s and division = %s limit 500',
                       [1, 3, 'jgi'])],
         []
         ),
        ('no_volume_locks_priority_in_enabled_queue_remaining_tasks_by_priority_not_empty',
         ['foo'],
         [[{'pull_queue_id': 1, 'volume': 'volume_a', 'priority': 3}],
          [{'pull_queue_id': 1, 'file_id': 123}],
          [{'pull_queue_id': 1, 'volume': 'volume_a', 'position_a': 142, 'position_b': 0,
            'requestor': 'foo@bar.com', 'priority': 3, 'file_permissions': '0100640',
            'file_path': '/global/dna/dm_archive/img/submissions/254666', 'service': 1,
            'file_name': 'Ga0466400_contigs.fna', 'remote_file_path': '.',
            'remote_file_name': 'Ga0466400_contigs.fna.17376984', 'backup_record_id': 17376984,
            'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar', 'tar_record_id': 392751}
           ],
          [{'cnt': 1}]],
         {},
         {'data': [{'backup_record_id': 17376984,
                    'file_name': 'Ga0466400_contigs.fna',
                    'file_path': '/global/dna/dm_archive/img/submissions/254666',
                    'file_permissions': '0100640',
                    'position_a': 142,
                    'position_b': 0,
                    'priority': 3,
                    'pull_queue_id': 1,
                    'remote_file_name': 'Ga0466400_contigs.fna.17376984',
                    'remote_file_path': '.',
                    'remote_path': '/home/projects/dm_archive/root/000/392/392751.tar',
                    'requestor': 'foo@bar.com',
                    'service': 1,
                    'tar_record_id': 392751,
                    'volume': 'volume_a'}],
          'uses_resources': ['foo', 'bar']},
         {'volume_a': {'locked': datetime(2000, 1, 2, 3, 4, 5),
                       'pull_queue_id': 1}},
         [call.execute(
             'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in (%s, %s, %s) order by priority, pull_queue_id limit 1',
             [1, 'jgi', 0, 1, 3]),
          call.execute('select count(*) as cnt from pull_queue join file using(file_id) where queue_status_id = %s and priority = %s and division = %s limit 500',
                       [1, 3, 'jgi'])],
         [3],
         ),
        ('no_volume_locks_priority_not_in_enabled_queue_no_tasks_by_volume',
         ['foo'],
         [[]],
         {},
         None,
         {},
         [call.execute(
             'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in (%s, %s, %s) order by priority, pull_queue_id limit 1',
             [1, 'jgi', 0, 1, 3])],
         [3],
         ),
    ])
    @patch('tape.datetime')
    def test_Tape_PullQueue_next(self, _description, available_features, sql_responses, volume_locks, expected,
                                 expected_volume_locks, expected_sql_calls, expected_enabled_queues, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        self.cursor.fetchall.side_effect = sql_responses
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo', 'bar'])
        pull_queue.enabled_queues.append(3)
        pull_queue.volume_locks = volume_locks

        self.assertEqual(pull_queue.next(available_features), expected)
        self.assertEqual(pull_queue.volume_locks, expected_volume_locks)
        self.assertEqual(pull_queue.enabled_queues, expected_enabled_queues)
        for c in expected_sql_calls:
            self.assertIn(c, self.cursor.mock_calls)

    def test_Tape_PullQueue_init_locks(self):
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo', 'bar'])

        pull_queue.init_locks([{'volume': 'volume_a'}, {'volume': 'volume_b'}])

        self.assertEqual({'volume_a': 1, 'volume_b': 1}, pull_queue.volume_locks)

    def test_Tape_PullQueue_clear_lock(self):
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo', 'bar'])
        pull_queue.volume_locks = {'volume_a': {'pull_queue_id': 1, 'locked': datetime(2000, 1, 2, 3, 4, 5)},
                                   'volume_b': {'pull_queue_id': 2, 'locked': datetime(2000, 1, 2, 3, 4, 5)}}

        pull_queue.clear_lock('volume_a')

        self.assertEqual({'volume_b': {'pull_queue_id': 2, 'locked': datetime(2000, 1, 2, 3, 4, 5)}},
                         pull_queue.volume_locks)

    def test_Tape_PullQueue_enable_short(self):
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo', 'bar'])

        pull_queue.enable_short()

        self.assertEqual(pull_queue.enabled_queues, [2, 3])

    def test_Tape_PullQueue_enable_long(self):
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo', 'bar'])

        pull_queue.enable_long()

        self.assertEqual(pull_queue.enabled_queues, [4, 5, 6, 7])

    def test_Tape_PullQueue_get_pending_tasks_count(self):
        self.cursor.fetchall.side_effect = [[{'cnt': 100}]]
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo'])

        self.assertEqual(pull_queue.get_pending_tasks_count(), 100)

    def test_Tape_PullQueue_delete_pending_tasks_for_file(self):
        pull_queue = Tape.PullQueue('pull', self.tape, 'jgi', 1, ['foo'])

        pull_queue.delete_pending_tasks_for_file(123)

        self.assertIn(call.execute('delete from pull_queue where file_id = %s and queue_status_id = %s', (123, 1)),
                      self.cursor.mock_calls)

    @parameterized.expand([
        ('unsupported_feature', ['not_supported'], [], None, []),
        ('no_pending_prep_requests',
         ['foo'],
         [[]],
         None,
         [call.execute(
             'select pull_queue_id from pull_queue p join backup_record b using(file_id) where volume is null and b.service = %s and queue_status_id = %s limit %s',
             [1, 1, 1000])]),
        ('pending_prep_requests',
         ['foo'],
         [[{'pull_queue_id': 1}, {'pull_queue_id': 2}],
          [{'pull_queue_id': 1, 'tar_record_id': 1000, 'remote_path': '/path/to/remote/1000.tar',
            'remote_file_path': '.', 'remote_file_name': 'my_file_1.tar', 'service': 1},
           {'pull_queue_id': 2, 'tar_record_id': None, 'remote_path': None,
            'remote_file_path': '/path/to/remote', 'remote_file_name': 'my_file_2.gz', 'service': 1}]],
         {'data': [{'pull_queue_id': 1,
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
          'uses_resources': ['foo', 'bar']},
         [call.execute(
             'select pull_queue_id from pull_queue p join backup_record b using(file_id) where volume is null and b.service = %s and queue_status_id = %s limit %s',
             [1, 1, 1000]),
          call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (7, 1)),
          call.execute('update pull_queue set queue_status_id = %s where pull_queue_id = %s', (7, 2)),
          call.execute(
              'select p.pull_queue_id, t.tar_record_id, t.remote_path, b.remote_file_path, b.remote_file_name, b.service from pull_queue p left join tar_record t on p.tar_record_id = t.tar_record_id join backup_record b using(file_id) where b.service = %s and p.pull_queue_id in (%s,%s)',
              [1, 1, 2])]),
    ])
    @patch('tape.datetime')
    def test_Tape_PrepQueue_next(self, _description, available_features, sql_responses, expected, expected_sql_calls,
                                 datetime_mock):
        datetime_mock.datetime.now.return_value = datetime(2000, 1, 2, 3, 4, 5)
        self.cursor.fetchall.side_effect = sql_responses
        prep_queue = Tape.PrepQueue('prep', self.tape, 1, ['foo', 'bar'])

        self.assertEqual(prep_queue.next(available_features), expected)
        for c in expected_sql_calls:
            self.assertIn(c, self.cursor.mock_calls)

    def test_Tape_PrepQueue_get_pending_tasks_count(self):
        self.cursor.fetchall.side_effect = [[{'cnt': 100}]]
        prep_queue = Tape.PrepQueue('prep', self.tape, ['foo'], 1)

        self.assertEqual(prep_queue.get_pending_tasks_count(), 100)


if __name__ == '__main__':
    unittest.main()
