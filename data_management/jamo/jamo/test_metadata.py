import collections
import os
import time
import unittest
import metadata
import datetime
import urllib
from collections import OrderedDict
from parameterized import parameterized
from metadata import QueueHash, Metadata
from bson.objectid import ObjectId
from lapinpy import common
from unittest.mock import patch, Mock, MagicMock, call
from types import SimpleNamespace


@patch('metadata.time.sleep', new=MagicMock())
class TestMetadata(unittest.TestCase):

    def setUp(self):
        self.__initialize()

    @patch('pymongo.MongoClient')
    def __initialize(self, mongo_client_mock):
        config = SimpleNamespace(**{
            'mongoserver': 'mongoserver',
            'mongo_user': 'mongo_user',
            'mongo_pass': 'mongo_pass',
            'meta_db': 'meta_db',
            'mongo_options': 'mongo_options',
            'dm_archive_root': '/archive/root',
            'metadata_search_options_basic': ['foo', 'bar'],
            'publishing_flags': ['sra'],
            'search_fields': {'File name': 'file_name'},
            'dm_archive_root_by_division': {'jgi': 'path/to/jgi/dm_archive'},
        })
        self.db = MagicMock()
        self.db.__getitem__.return_value = self.db
        self.client = MagicMock()
        self.client.__getitem__.return_value = self.db
        mongo_client_mock.return_value = self.client
        self.cursor = MagicMock()
        records = [iter([{'map': {'files': {'use': False}, 'library_name': {'extract': True}, 'physical_run_unit_id': {'new_key': 'illumina_physical_run_unit_id', 'extract': True}, 'parent_sdm_seq_unit_id': {'extract': True}, 'gls_physical_run_unit_id': {'extract': True}, 'history': {'use': False}}, 'url': 'https://sdm.jgi.doe.gov/api/illumina/sdmsequnit2/{{illumina_sdm_seq_unit_id}}', 'key': 'illumina_sdm_seq_unit_id', 'owner': 'sdm', 'identifier': 'sdm_seq_unit', '_id': ObjectId('51cb5c3d067c0175c171a987')}]),
                   iter([{'owner': 'sdm', 'callback': '/api/tape/grouprestore', 'name': 'Restore files', '_id': ObjectId('52169c3a067c010551524fe4')}]),
                   iter([{'relative_root': '/global/dna/dm_archive/sdm', 'user': 'sdm', '_id': ObjectId('5226cea6067c016931c7fb91')}]),
                   iter([])]
        self.cursor.__iter__.side_effect = records
        self.cursor.__getitem__.return_value = self.cursor
        self.db.find.return_value = self.cursor
        self.db.with_options.return_value = self.db
        self.db.count_documents.return_value = 1
        self.metadata = Metadata(config)
        self.metadata.cleanThread.cancel()
        self.cursor.__iter__.side_effect = None

    def test_processservice(self):
        @metadata.processservice(name='foo', description='foobar', typ='my_type', template='my_template')
        def func():
            return 'foo'

        self.assertIn(
            {'method': func, 'type': 'my_type', 'name': 'foo', 'template': 'my_template', 'description': 'foobar'},
            metadata.processservices)

    def test_QueueHash_get_item_found(self):
        queue_hash = QueueHash(10)
        queue_hash.hash = {'foo': 'bar'}

        self.assertEqual(queue_hash['foo'], 'bar')

    def test_QueueHash_get_item_not_found(self):
        queue_hash = QueueHash(10)
        queue_hash.hash = {'foo': 'bar'}

        self.assertRaises(KeyError, queue_hash.__getitem__, 'baz')

    @parameterized.expand([
        ('found', 'foo', True),
        ('found', 'baz', False),
    ])
    def test_QueueHash_contains(self, _description, value, found):
        queue_hash = QueueHash(10)
        queue_hash.hash = {'foo': 'bar'}

        self.assertEqual(value in queue_hash, found)

    def test_QueueHash_set_item(self):
        queue_hash = QueueHash(10)

        queue_hash['foo'] = 'bar'

        self.assertEqual(queue_hash.hash.get('foo'), 'bar')

    def test_QueueHash_clear(self):
        queue_hash = QueueHash(10)
        queue_hash.hash = {'foo': 'bar'}

        queue_hash.clear()

        self.assertEqual(len(queue_hash.hash), 0)

    def test_Metadata_doneloading(self):
        @metadata.processservice(name='foo', description='foobar', typ='my_type', template='my_template')
        def func():
            return 'foo'
        func.address = 'http://foobar.com'

        self.metadata.doneloading()

        self.assertIn({'name': 'foo', 'template': 'my_template', 'address': 'http://foobar.com', 'type': 'my_type',
                       'description': 'foobar'}, metadata.processservices)

    def test_Metadata_createDecisionTree(self):
        self.cursor.__iter__.return_value = iter([
            {'group': 'auto', 'description': 'auto-created from file viral_minimal_draft.yaml',
             'url': 'http://127.0.0.1:8080/api/jira/createjira/viral_minimal_draft', 'Enabled': True,
             'filter': {'metadata>sequencing_project>actual_sequencing_product_name': {'#in': ['Viral Minimal Draft']},
                        'metadata>illumina_physical_run_id': {'#exists': True},
                        'metadata>sow_segment>scientific_program': 'Microbial', 'user': 'rqc',
                        'metadata>fastq_type': 'filtered', 'metadata>sow_segment>account_purpose': 'Programmatic',
                        'metadata>sow_segment>sow_item_type': 'Fragment'}, 'user': 'jira', 'type': 'metadata',
             'name': 'viral_minimal_draft', '_id': ObjectId('60b05fe4c399d4ad32fceb31')}
        ])

        self.metadata.createDecisionTree()

        self.assertEqual(self.metadata.subscriptionMap, {'viral_minimal_draft': 'http://127.0.0.1:8080/api/jira/createjira/viral_minimal_draft'})
        self.assertEqual(repr(self.metadata.subscriptionTree),
                         "metadata.sequencing_project.actual_sequencing_product_name,{'Viral Minimal Draft': metadata.illumina_physical_run_id,{},[({'$exists': True}, metadata.sow_segment.scientific_program,{'Microbial': user,{'rqc': metadata.fastq_type,{'filtered': metadata.sow_segment.account_purpose,{'Programmatic': metadata.sow_segment.sow_item_type,{'Fragment': ['viral_minimal_draft']},[]},[]},[]},[]},[])]},[]")

    def test_Metadata_shutdown(self):
        self.metadata.shutdown()

    @patch('metadata.threading')
    def test_Metadata_addEvent(self, thread_mock):
        thread_mock.Thread.return_value = thread_mock

        self.metadata.addEvent('add', {'foo': 'bar'})

        thread_mock.start.assert_called()

    @patch('metadata.restful.RestServer')
    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_post_importfromtape(self, datetime_mock, curl, restserver):
        server = Mock()
        server.run_method.return_value = {'file_name': 'file.tar',
                                          'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                                          'file_id': 14509150,
                                          'file_owner': 'sdm'}
        restserver.Instance.return_value = server
        curl.get.return_value = {'foo': 'bar'}
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.save.return_value = '5fab1aca47675a20c853bc10'

        self.assertEqual(self.metadata.post_importfromtape(None, {
            'file_id': 14509150,
            'metadata': {'illumina_sdm_seq_unit_id': 'foo'},
            'file_type': 'tar',
            'user': 'sdm',
            'group': 'sdm_group',
        }), {'metadata_id': '5fab1aca47675a20c853bc10'})
        self.db.save.assert_called_with({'file_type': 'tar', 'file_name': 'file.tar', 'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'file_id': 14509150, 'file_owner': 'sdm', 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'group': 'sdm_group', 'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata': {'sdm_seq_unit': {'foo': 'bar'}, 'illumina_sdm_seq_unit_id': 'foo'}, 'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79', 'user': 'sdm'})

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_file(self, restserver):
        record = {'file_name': 'file.tar',
                  'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                  'file_id': 14509150,
                  'file_owner': 'sdm',
                  'status': 'old',
                  'metadata_id': '5fab1aca47675a20c853bc10',
                  '_id': '51d45d9e067c014cd6e88f61'}
        server = Mock()
        server.run_method.return_value = record
        restserver.Instance.return_value = server
        self.cursor.__iter__.return_value = iter([record])
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.post_file(None, {'file': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/file.tar',
                                                        'inputs': ['5fab1aca47675a20c853bc10'],
                                                        'file_type': 'tar',
                                                        'metadata': {'foo': 'bar'},
                                                        '__auth': {'user': 'sdm', 'group': 'sdm_group'},
                                                        'source': 'my_source'}),
                         {'metadata_id': '5fab1aca47675a20c853bc10'})
        self.assertIn(call.run_method('tape', 'post_file',
                                      file='/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79/file.tar',
                                      inputs=[ObjectId('5fab1aca47675a20c853bc10')], file_type='tar',
                                      __auth={'user': 'sdm', 'group': 'sdm_group'}, source='my_source',
                                      call_source='file'), server.mock_calls)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_folder(self, restserver):
        record = {
            'status': 'old',
            'metadata_id': '5fab1aca47675a20c853bc10',
            '_id': '51d45d9e067c014cd6e88f61',
        }
        server = Mock()
        server.run_method.return_value = record
        restserver.Instance.return_value = server
        self.cursor.__iter__.return_value = iter([record])
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.post_folder(None, {
            'index': True,
            'inputs': ['5fab1aca47675a20c853bc10'],
            'folder': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
            'file_type': 'tar folder',
            'metadata': {'foo': 'bar'},
            'ignore': ['ignored_folder'],
            'extract': [{'path': 'archive_individually', 'metadata': {'bar': 'baz'}, 'file_type': 'tar'}],
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
            'source': 'my_source',
        }), {'metadata_id': '5fab1aca47675a20c853bc10'})
        self.assertIn(call.run_method('tape', 'post_file', index=True, inputs=['5fab1aca47675a20c853bc10'],
                                      folder='/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                                      file_type='tar folder', ignore=['ignored_folder'], extract=[
            {'path': 'archive_individually', 'metadata': {'bar': 'baz'}, 'file_type': 'tar'}],
            __auth={'user': 'sdm', 'group': 'sdm_group'}, source='my_source',
            call_source='folder',
            file='/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
            auto_uncompress=False), server.mock_calls)

    @parameterized.expand([
        ('old', {'status': 'old', 'metadata_id': '5fab1aca47675a20c853bc10', '_id': '51d45d9e067c014cd6e88f61'},
         {'metadata': {'foo': 'bar'}, '__auth': {'user': 'sdm', 'group': 'sdm_group', 'division': 'jgi'},
          'replace_with_null': True, 'staging_path': '/path/to/staging', 'file': 'hpss://path/to/file',
          'destination': '/path/to/destination', 'file_type': 'tar'},
         {'metadata_id': '5fab1aca47675a20c853bc10'},
         call.update({'_id': {'$in': [ObjectId('51d45d9e067c014cd6e88f61')]}}, {
             '$set': {'file_type': 'tar', 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                      'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata': {'foo': 'bar'}}},
                     multi=True)),
        ('new_destination_starts_with_slash',
         {'status': 'new', 'metadata_id': '5fab1aca47675a20c853bc10', '_id': '51d45d9e067c014cd6e88f61',
          'file_id': 14509150, 'file_ingest_id': 64439, 'request_count': 1},
         {'metadata': {'foo': 'bar'}, '__auth': {'user': 'sdm', 'group': 'sdm_group', 'division': 'jgi'},
          'replace_with_null': True, 'file': '/path/to/file', 'destination': '/path/to/destination', 'file_type': 'tar',
          'inputs': ['5fab1aca47675a20c853bc10']}, {'metadata_id': '5327394649607a1be0059511'},
         call.save({'inputs': ['5fab1aca47675a20c853bc10'], 'file_type': 'tar',
                    'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'file_id': 14509150,
                    'file': '/path/to/file',
                    'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    '_id': '5327394649607a1be0059511', 'group': 'sdm_group',
                    'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    'destination': '/path/to/destination', 'file_ingest_id': 64439,
                    '__auth': {'group': 'sdm_group', 'user': 'sdm', 'division': 'jgi'}, 'request_count': 2,
                    'ingest_id': '5327394649607a1be0059511', 'metadata': {'foo': 'bar'},
                    'user': 'sdm', 'division': 'jgi'})),
        ('new_destination_does_not_start_with_slash',
         {'status': 'new', 'metadata_id': '5fab1aca47675a20c853bc10', '_id': '51d45d9e067c014cd6e88f61',
          'file_id': None, 'file_ingest_id': 64439, 'request_count': 1
          },
         {'metadata': {'foo': 'bar'}, '__auth': {'user': 'sdm', 'group': 'sdm_group', 'division': 'jgi'},
          'replace_with_null': True, 'file': '/path/to/file', 'destination': 'path/to/destination/', 'file_type': 'tar',
          'inputs': ['5fab1aca47675a20c853bc10']},
         {'metadata_id': '5327394649607a1be0059511'},
         call.save({'inputs': ['5fab1aca47675a20c853bc10'], 'file_type': 'tar',
                    'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    'file_status': 'REGISTERED-INGEST', 'file_id': -64439,
                    'file': '/path/to/file',
                    'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    '_id': '5327394649607a1be0059511', 'group': 'sdm_group',
                    'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    'destination': 'path/to/destination/', 'file_ingest_id': 64439,
                    '__auth': {'group': 'sdm_group', 'user': 'sdm', 'division': 'jgi'}, 'request_count': 2,
                    'ingest_id': '5327394649607a1be0059511', 'metadata': {'foo': 'bar'},
                    'user': 'sdm', 'division': 'jgi'})),
    ])
    @patch('metadata.restful.RestServer')
    @patch('lapinpy.mongorestful.datetime')
    @patch('metadata.datetime')
    def test_Metadata_ingest_data(self, _description, record, kwargs, expected, expected_db_update_call,
                                  datetime_mongorestful, datetime_metadata, restserver):
        server = Mock()
        server.run_method.return_value = record
        restserver.Instance.return_value = server
        self.cursor.__iter__.return_value = iter([record])
        datetime_mongorestful.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        datetime_metadata.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.save.return_value = '5327394649607a1be0059511'
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.ingest_data(None, kwargs), expected)
        self.assertIn(expected_db_update_call, self.db.mock_calls)

    @parameterized.expand([
        ('old',
         {
             'metadata': {'foo': 'bar'},
             'file_type': 'tar',
             'metadata_id': '5fab1aca47675a20c853bc10',
         },
         {'file_name': 'file.tar',
          'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
          'file_id': 14509150,
          'file_owner': 'sdm',
          'status': 'old',
          'metadata_id': '5fab1aca47675a20c853bc10',
          'metadata': {'bar': 'baz'},
          '_id': '51d45d9e067c014cd6e88f61'},
         {
             '_metadata_ingest_id': '5327394649607a1be0059511',
             '_status': 'old',
         },
         {'metadata_id': '5fab1aca47675a20c853bc10'},
         call.update({'_id': {'$in': [ObjectId('51d45d9e067c014cd6e88f61')]}}, {
             '$set': {'file_type': 'tar', 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                      'metadata': {'foo': 'bar', 'bar': 'baz'}}}, multi=True)),
        ('folder_str_type',
         {
             'metadata': {'foo': 'bar'},
             'file_type': 'tar',
             'metadata_modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
             'user': 'sdm',
             'group': 'sdm_group',
             'division': 'jgi',
             'inputs': ['5fab1aca47675a20c853bc10'],
         },
         {'file_name': 'file.tar',
          'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
          'file_id': 14509150,
          'file_size': 100000,
          'file_permissions': '0100644',
          'file_group': 'genome',
          'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
          'file_owner': 'sdm',
          'status': 'new',
          'file_status_id': 10,
          'metadata_id': '5fab1aca47675a20c853bc10',
          'metadata': {'bar': 'baz'},
          '_id': '51d45d9e067c014cd6e88f61'},
         {
             '_metadata_ingest_id': '5327394649607a1be0059511',
             '_status': 'new',
             'local_purge_days': 5,
             'metadata_id': '5fab1aca47675a20c853bc10',
             '_is_folder': True,
             'file_name': 'file.tar',
             'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
             'file_id': 14509150,
             'file_size': 100000,
             'file_permissions': '0100644',
             'file_group': 'genome',
             'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
             'file_owner': 'sdm',
             'file_status': 'new',
             'file_status_id': 10,
         },
         {'metadata_id': '5327394649607a1be0059511'},
         call.save(
             {'inputs': ['5fab1aca47675a20c853bc10'], 'file_type': ['folder', 'tar'], 'file_name': 'file.tar',
              'added_date': datetime.datetime(1999, 1, 2, 3, 4, 5), 'file_permissions': '0100644', 'file_status': 'new',
              'user': 'sdm', 'file_size': 100000, 'dt_to_purge': datetime.datetime(1999, 1, 7, 3, 4, 5),
              'file_group': 'genome', 'file_owner': 'sdm', 'modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
              'group': 'sdm_group', 'division': 'jgi', 'file_status_id': 10,
              'metadata_modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
              'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5), 'file_id': 14509150,
              '_id': '5327394649607a1be0059511',
              'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
              'metadata': {'foo': 'bar', 'bar': 'baz'}})
         ),
        ('folder_list_type',
         {
             'metadata': {'foo': 'bar'},
             'file_type': ['tar'],
             'metadata_modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
             'user': 'sdm',
             'group': 'sdm_group',
             'division': 'jgi',
             'inputs': ['5fab1aca47675a20c853bc10'],
         },
         {'file_name': 'file.tar',
          'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
          'file_id': 14509150,
          'file_size': 100000,
          'file_permissions': '0100644',
          'file_group': 'genome',
          'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
          'file_owner': 'sdm',
          'status': 'new',
          'file_status_id': 10,
          'metadata_id': '5fab1aca47675a20c853bc10',
          'metadata': {'bar': 'baz'},
          '_id': '51d45d9e067c014cd6e88f61'
          },
         {
             '_metadata_ingest_id': '5327394649607a1be0059511',
             '_status': 'new',
             'local_purge_days': 5,
             'metadata_id': '5fab1aca47675a20c853bc10',
             '_is_folder': True,
             'file_name': 'file.tar',
             'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
             'file_id': 14509150,
             'file_size': 100000,
             'file_permissions': '0100644',
             'file_group': 'genome',
             'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
             'file_owner': 'sdm',
             'file_status': 'new',
             'file_status_id': 10,
         },
         {'metadata_id': '5327394649607a1be0059511'},
         call.save(
             {'inputs': ['5fab1aca47675a20c853bc10'], 'file_type': ['tar', 'folder'], 'file_name': 'file.tar',
              'added_date': datetime.datetime(1999, 1, 2, 3, 4, 5), 'file_permissions': '0100644', 'file_status': 'new',
              'user': 'sdm', 'file_size': 100000, 'dt_to_purge': datetime.datetime(1999, 1, 7, 3, 4, 5),
              'file_group': 'genome', 'file_owner': 'sdm', 'modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
              'group': 'sdm_group', 'division': 'jgi', 'file_status_id': 10,
              'metadata_modified_date': datetime.datetime(1999, 1, 2, 3, 4, 5),
              'file_date': datetime.datetime(1999, 1, 2, 3, 4, 5), 'file_id': 14509150,
              '_id': '5327394649607a1be0059511',
              'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
              'metadata': {'foo': 'bar', 'bar': 'baz'}})
         ),
    ])
    @patch('metadata.restful.RestServer')
    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_post_file_ingest(self, _description, ingest_record, file_record, kwargs, expected,
                                       expected_db_update_call, datetime_mock, restserver):
        db_return_values = [iter([ingest_record]), iter([file_record]), iter([file_record])]
        self.cursor.__iter__.side_effect = db_return_values
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.save.return_value = '5327394649607a1be0059511'
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.post_file_ingest(None, kwargs), expected)
        self.assertIn(expected_db_update_call, self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_put_file(self, datetime_mock):
        record = {
            'user': 'sdm',
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.put_file(None, {
            'id': '5327394649607a1be0059511',
            'data': {'foo': 'bar'},
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
            'permissions': ['admin'],
        })

        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}},
                                  {'$set': {'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'foo': 'bar'}},
                                  multi=True),
                      self.db.mock_calls)

    @parameterized.expand([
        ('admin', {'permissions': ['admin']}),
        ('file_owner', {'__auth': {'user': 'sdm', 'group': 'sdm_group'}}),
    ])
    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_put_filemetadata(self, _description, extra_kwargs, datetime_mock):
        record = {
            'user': 'sdm',
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        kwargs = {
            'id': '5327394649607a1be0059511',
            'metadata': {'foo': 'bar'},
        }
        kwargs.update(extra_kwargs)
        self.cursor.__iter__.side_effect = lambda: iter([record])
        self.db.update.return_value = {'nModified': 1}
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.assertEqual(self.metadata.put_filemetadata(None, kwargs), {'n': 1, 'nModified': 1, 'ok': 1.0})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata.foo': 'bar'}}, multi=True),
            self.db.mock_calls)

    def test_Metadata_put_filemetadata_not_admin_or_owner(self):
        record = {
            'user': 'sdm',
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertRaises(common.HttpException, self.metadata.put_filemetadata, None, {
            'id': '5327394649607a1be0059511',
            'metadata': {'foo': 'bar'},
            '__auth': {'user': 'foo', 'group': 'sdm_group'}
        })

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_put_filesuper(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.put_filesuper(None, {
            'query': {'_id': '5327394649607a1be0059511'},
            'data': {'foo': 'bar'}
        })

        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'foo': 'bar'}}, multi=True),
            self.db.mock_calls)

    @unittest.skip('This method does not work, will always throw an exception')
    def test_Metadata_delete_update(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])

        self.metadata.delete_update(['5327394649607a1be0059511'], {
            '__auth': {'user': 'sdm', 'group': 'sdm_group'}
        })

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_put_update(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.put_update(['5327394649607a1be0059511'], {
            'foo': 'bar',
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
        }), {'n': 1, 'nModified': 1, 'ok': 1.0})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata.sdm_group.foo': 'bar'}},
            multi=True),
            self.db.mock_calls)

    @patch('metadata.datetime')
    def test_Metadata_ingest_retry_records(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        datetime_mock.timedelta.return_value = datetime.datetime(2000, 1, 2, 3, 3, 5)

        self.assertEqual(self.metadata.ingest_retry_records(None, None), [record])
        self.assertIn(call.find({'added_date': {'$lt': datetime.timedelta(0, 60)}, 'file_id': {'$lt': 0}}),
                      self.db.mock_calls)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_ingest_retry(self, restserver):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_id': -1000,
        }
        self.cursor.__iter__.return_value = iter([record])
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.post_ingest_retry(None, None),
                         {'processed': 1})
        self.assertIn(call.run_method('tape', 'put_file_ingest_retry', 1000),
                      server.mock_calls)

    @parameterized.expand([
        ('str_key_str_value', 'illumina_sdm_seq_unit_id', {'illumina_sdm_seq_unit_id': 'bar'}),
        ('list_key_str_value', ['illumina_sdm_seq_unit_id'], {'illumina_sdm_seq_unit_id': 'bar'}),
        ('list_key_list_value', ['illumina_sdm_seq_unit_id'], {'illumina_sdm_seq_unit_id': ['bar']}),
    ])
    def test_Metadata_processStores(self, _description, metadata_store_key, kwargs):
        self.metadata.stores.get('illumina_sdm_seq_unit_id')[0]['key'] = metadata_store_key
        self.metadata.store_cache = {'sdm_seq_unit/bar': {'foo': 'bar'}}

        new_metadata, all_failed = self.metadata.processStores(kwargs)

        self.assertEqual(new_metadata, {'sdm_seq_unit': {'foo': 'bar'}})

    @patch('metadata.curl')
    def test_Metadata_processStores_curl_error(self, curl):
        curl.get.side_effect = urllib.error.URLError('Error')

        new_metadata, all_failed = self.metadata.processStores({'illumina_sdm_seq_unit_id': 'bar'})

        self.assertEqual([{'processed': ['illumina_sdm_seq_unit_id'], 'dictionary': OrderedDict([('illumina_sdm_seq_unit_id', 'bar')])}],
                         all_failed)

    @parameterized.expand([
        ('map_name_with_gt',
         'illumina_sdm_seq_unit_id',
         collections.OrderedDict([('illumina_sdm_seq_unit_id', 'bar')]),
         {'sdm_seq_unit/bar': [{'foo': 'bar', 'foobar': {'bar': 'baz', 'bar2': 'baz2'}}],
          'sdm_seq_unit/baz': [{'foo3': 'bar3'}]},
         {'foobar>bar': {'extract': ['bar']}},
         {'bar': 'baz',
          'sdm_seq_unit': {'bar': 'baz', 'foo': 'bar', 'foobar': {'bar2': 'baz2'}}},
         ['illumina_sdm_seq_unit_id', 'bar'],
         'sdm_seq_unit',
         None,
         ),
        ('cached',
         'illumina_sdm_seq_unit_id',
         collections.OrderedDict([('illumina_sdm_seq_unit_id', 'bar')]),
         {'sdm_seq_unit.foobar/bar': [{'foo': 'bar', 'foobar': {'bar': 'baz', 'bar2': 'baz2'}}]},
         {},
         {'sdm_seq_unit': {'foobar': {'foo': 'bar',
                                      'foobar': {'bar': 'baz', 'bar2': 'baz2'}}}},
         ['illumina_sdm_seq_unit_id'],
         'sdm_seq_unit.foobar',
         None,
         ),
        ('not_cached',
         'illumina_sdm_seq_unit_id',
         collections.OrderedDict([('illumina_sdm_seq_unit_id', 'bar')]),
         [],
         {},
         {'sdm_seq_unit': {'foo': 'bar'}},
         ['illumina_sdm_seq_unit_id'],
         'sdm_seq_unit',
         [{'foo': 'bar'}],
         ),
        ('composite_key',
         ['bar', 'foo'],
         collections.OrderedDict([('illumina_sdm_seq_unit_id', 123), ('bar', 'bar1')]),
         [],
         {'bar': {'extract': True}, 'foo': {'extract': True}},
         {'bar': 'bar1', 'foo': 'foo1', 'sdm_seq_unit': {'bar': 'bar1', 'foo': 'foo1'}},
         ['illumina_sdm_seq_unit_id', 'bar'],
         'sdm_seq_unit',
         [{'foo': 'foo1', 'bar': 'bar1'}, {}],
         ),
    ])
    @patch('metadata.curl')
    def test_Metadata_processStore(self, _description, key, key_values, store_cache, store_map_extra, expected,
                                   expected_already_processed, identifier, curl_responses, curl):
        already_processed = []
        extracted_keys = {}
        original_doc = {}

        if curl_responses:
            curl.get.side_effect = curl_responses
        else:
            self.metadata.store_cache = store_cache
        store = self.metadata.stores.get('illumina_sdm_seq_unit_id')[0]
        store['flatten'] = True
        store['conform_keys'] = True
        self.metadata.stores['bar'] = [store]
        store['map'].update(store_map_extra)
        store['identifier'] = identifier
        store['key'] = key

        self.assertEqual(self.metadata.processStore(key_values, store, already_processed,
                                                    extracted_keys, original_doc), expected)
        self.assertEqual(already_processed, expected_already_processed)

    @parameterized.expand([
        ('str_value', 'bar', ('foo', 'bar')),
        ('dict_value', {'bar': 'baz'}, ('foo', {'bar': 'baz'})),
        ('list_value', ['bar', 'baz'], ('foo', ['bar', 'baz'])),
    ])
    def test_Metadata_conform(self, _description, value, expected):
        self.assertEqual(self.metadata.conform('foo', value), expected)

    @parameterized.expand([
        ('default_name', {'foo': {'default': 'default_value'}}, {}, {'foo': 'default_value'}),
        ('kwargs_name', {'foo': {'required': False, 'type': 'string'}}, {'foo': 'kwargs_name'}, {'foo': 'kwargs_name'})
    ])
    def test_Metadata_parseInputs(self, _description, inputs, kwargs, expected):
        self.assertEqual(self.metadata.parseInputs(inputs, kwargs), expected)

    def test_Metadata_parseInputs_missing_required_field(self):
        self.assertRaises(common.ValidationError, self.metadata.parseInputs,
                          {'foo': {'required': True, 'type': 'string'}}, {})

    @parameterized.expand([
        ('string', 1, '1'),
        ('number', '1', 1),
        ('bool', 'TRUE', True),
        ('list', ['foo'], ['foo']),
        ('list:int', '1,2,3', [1, 2, 3]),
    ])
    def test_Metadata_parseType(self, type, value, expected):
        self.assertEqual(self.metadata.parseType(value, type), expected)

    def test_Metadata_parseType_int_string_contains_non_ints(self):
        self.assertRaises(common.ValidationError, self.metadata.parseType, '1,2,a', 'list:int')

    @parameterized.expand([
        ('str', {'#foo': '#bar'}, {'foo': 'bar.baz', 'bar': 'baz'}, {'bar.baz': 'baz'}),
        ('dict', {'#foo': {'bar': 'baz'}}, {'foo': 'bar.baz'}, {'bar.baz': {'bar': 'baz'}}),
        ('list', {'foo': ['#bar', 'baz']}, {'bar': 'bar1'}, {'foo': ['bar1', 'baz']}),
    ])
    def test_Metadata_parseQuery(self, _description, query, variables, expected):
        self.assertEqual(self.metadata.parseQuery(query, variables), expected)

    def test_Metadata_getResults(self):
        self.assertEqual(self.metadata.getResults({'foo': 'foo1', 'bar': {'baz': 'baz1'}}, {'foo', 'bar>baz'}),
                         {'baz': 'baz1', 'foo': 'foo1'})

    @parameterized.expand([
        ('no_args',
         [],
         {
             'requestor': 'sdm',
             '_id': '5327394649607a1be0059511',
         },
         [{
             '_id': ObjectId('5327394649607a1be0059511'),
             'name': 'foobar',
             'foo': 'bar',
         }],
         call.find({'requestor': 'sdm', '_id': ObjectId('5327394649607a1be0059511')}),
         ),
        ('test',
         ['Restore files', 'test'],
         {
             'requestor': 'sdm',
             '_id': '5327394649607a1be0059511',
             'foo': 'bar',
             '_page': 1
         },
         [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}],
         call.find({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}})
         ),
        ('no_test',
         ['Restore files'],
         {
             'requestor': 'sdm',
             '_id': '5327394649607a1be0059511',
             'foo': 'bar',
             '_page': 1
         },
         [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}],
         call.find({'requestor': 'sdm'})
         ),
    ])
    def test_Metadata_get_query(self, _description, args, kwargs, expected, expected_db_call):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'bar',
        }
        self.cursor.__iter__.return_value = iter([record])

        service = self.metadata.process_services['Restore files']
        service['inputs'] = {'requestor': {'required': False, 'type': 'string'}}
        service['query'] = {'requestor': '#requestor'}
        service['test_data'] = ['5327394649607a1be0059511']
        service['return'] = ['name']

        self.assertEqual(self.metadata.get_query(args, kwargs), expected)
        self.assertIn(expected_db_call, self.db.mock_calls)

    @patch('lapinpy.mongorestful.random')
    def test_Metadata_pagequery(self, random_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
            'bar': datetime.datetime(2022, 1, 2),
        }
        self.cursor.__iter__.return_value = iter([record])
        random_mock.choice.return_value = 'A'
        expected = {'cursor_id': 'A' * 10,
                    'end': 1,
                    'fields': ['foo'],
                    'record_count': 1,
                    'records': [{'_id': ObjectId('5327394649607a1be0059511'),
                                 'bar': datetime.datetime(2022, 1, 3, 0, 0),
                                 'foo': 'foo1_str',
                                 'name': 'foobar'}],
                    'start': 1,
                    'timeout': 540}

        self.assertEqual(self.metadata.pagequery('file',
                                                 {'inputs': ['5327394649607a1be0059511']}, ['foo'], 100, ['foo', 1],
                                                 {'foo': '{{value}}_str',
                                                  'bar': lambda x, y: y + datetime.timedelta(1)},
                                                 {'bar': {'type': 'date'}},
                                                 True), expected)
        self.assertIn(call.find({'inputs': [ObjectId('5327394649607a1be0059511')]}, ['foo']), self.db.mock_calls)

    def test_Metadata_query(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.query('file', inputs=['5327394649607a1be0059511']),
                         [record])
        self.assertIn(call.find({'inputs': [ObjectId('5327394649607a1be0059511')]}), self.db.mock_calls)

    @parameterized.expand([
        ('with_fields',
         {
             'fields': ['foo'],
             'query': {'_id': '5327394649607a1be0059511'},
             'flatten': True,
             'requestor': 'sdm',
         },
         {'cursor_id': 'A' * 10,
          'end': 1,
          'fields': ['foo'],
          'record_count': 1,
          'records': [{'_id': ObjectId('5327394649607a1be0059511'),
                       'foo': 'foo1',
                       'name': 'foobar'}],
          'start': 1,
          'timeout': 540},
         ),
        ('without_fields',
         {
             'query': 'select foo where _id = 5327394649607a1be0059511',
             'flatten': True,
             'requestor': 'sdm',
             'cltool': True,
         },
         {'cursor_id': 'A' * 10,
          'end': 1,
          'fields': ['foo'],
          'record_count': 1,
          'records': [{'_id': ObjectId('5327394649607a1be0059511'),
                       'foo': 'foo1',
                       'name': 'foobar'}],
          'start': 1,
          'timeout': 540},
         ),
    ])
    @patch('lapinpy.mongorestful.random')
    def test_Metadata_post_pagequery(self, _description, kwargs, expected, random_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
        }
        self.cursor.__iter__.return_value = iter([record])
        random_mock.choice.return_value = 'A'

        self.assertEqual(self.metadata.post_pagequery(None, kwargs), expected)

    @patch('lapinpy.mongorestful.random')
    def test_Metadata_post_portalquery(self, random_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
        }
        self.cursor.__iter__.return_value = iter([record])
        random_mock.choice.return_value = 'A'

        self.assertEqual(self.metadata.post_portalquery(None, {
            'fields': ['foo'],
            'query': {'_id': '5327394649607a1be0059511'}}),
            {'cursor_id': 'A' * 10,
             'end': 1,
             'fields': ['foo'],
             'record_count': 1,
             'records': [{'_id': ObjectId('5327394649607a1be0059511'),
                          'foo': 'foo1',
                          'name': 'foobar'}],
             'start': 1,
             'timeout': 540})

    @parameterized.expand([
        ('with_args',
         ['Restore files'],
         {'_id': '5327394649607a1be0059511'},
         [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}]
         ),
        ('no_args',
         [],
         {'_id': '5327394649607a1be0059511',
          'requestor': 'sdm',
          'fields': ['foo']
          },
         [{'_id': ObjectId('5327394649607a1be0059511'),
           'foo': 'foo1',
           'name': 'foobar'}]
         ),
    ])
    def test_Metadata_post_query(self, _description, args, kwargs, expected):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
        }
        self.cursor.__iter__.return_value = iter([record])
        service = self.metadata.process_services['Restore files']
        service['return'] = ['name']

        self.assertEqual(self.metadata.post_query(args, kwargs), expected)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_queryfilesaved(self, restserver):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
            'file_name': 'file.tar',
            'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
            'file_id': 14509150
        }
        self.cursor.__iter__.return_value = iter([record])
        server = Mock()
        server.run_method.return_value = True
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.post_queryfilesaved([],
                                                           {'query': {'_id': '5327394649607a1be0059511'}}),
                         [{'_id': ObjectId('5327394649607a1be0059511'),
                           'file_id': 14509150,
                           'file_name': 'file.tar',
                           'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                           'file_safe_in_jamo': True}])

    @parameterized.expand([
        ('no_limit_passed_defaults_to_all', {'_id': ObjectId('5327394649607a1be0059511')},
         call.find().__getitem__(slice(0, 1000, None))),
        ('limit_passed', {'_id': ObjectId('5327394649607a1be0059511'), 'limit': 50},
         call.find().__getitem__(slice(0, 50, None))),
    ])
    @patch('metadata.restful.RestServer')
    def test_Metadata_post_queryfilesafe(self, _description, kwargs, expected_query_call, restserver_mock):
        records = [
            {
                '_id': ObjectId('5327394649607a1be0059511'),
                'name': 'foobar',
                'foo': 'foo1',
                'file_name': 'file.tar',
                'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                'file_id': 14509150
            },
            {
                '_id': ObjectId('5327394649607a1be0059512'),
                'name': 'foobar2',
                'foo': 'foo2',
                'file_name': 'file2.tar',
                'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                'file_id': 14509152
            },
        ]
        self.cursor.__iter__.return_value = iter(records)
        # We set a fake document count to check `limit` requests.
        self.db.count_documents.return_value = 1000
        server = Mock()
        server.run_method.return_value = True
        restserver_mock.Instance.return_value = server

        self.assertEqual(self.metadata.post_queryfilesafe([], kwargs),
                         [{'_id': ObjectId('5327394649607a1be0059511'),
                           'file_id': 14509150,
                           'file_name': 'file.tar',
                           'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                           'file_safe_in_jamo': True},
                          {'_id': ObjectId('5327394649607a1be0059512'),
                           'file_id': 14509152,
                           'file_name': 'file2.tar',
                           'file_path': '/global/dna/shared/rqc/pipelines/filter/archive/03/14/72/79',
                           'file_safe_in_jamo': True}])
        # Verify that we're requesting records from 0 to expected `limit`.
        self.assertIn(expected_query_call, self.db.mock_calls)

    def test_Metadata_post_checkdata(self):
        self.metadata.stores.get('illumina_sdm_seq_unit_id')[0]['key'] = 'illumina_sdm_seq_unit_id'
        self.metadata.store_cache = {'sdm_seq_unit/bar': {'foo': 'bar'}}

        self.assertEqual(self.metadata.post_checkdata(None, {'illumina_sdm_seq_unit_id': 'bar'}),
                         {'sdm_seq_unit': {'foo': 'bar'}})

    def test_Metadata_post_filetype(self):
        kwargs = {'file_type': 'tar', 'description': 'Tar file'}
        self.db.insert.return_value = kwargs

        self.assertEqual(self.metadata.post_filetype(None, kwargs),
                         "{'file_type': 'tar', 'description': 'Tar file'}")

    def test_Metadata_get_file(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'foo': 'foo1',
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_file(['5327394649607a1be0059511'], None),
                         record)

    @parameterized.expand([
        ('existing_service',
         {
             '__auth': {'user': 'sdm', 'group': 'sdm'},
             'key': 'illumina_sdm_seq_unit_id',
             'url': 'http://foobar.com/datastore/{{value}}',
             'identifier': 'sdm_seq_unit',
         },
         ObjectId('51cb5c3d067c0175c171a987'),
         call.save({'_id': ObjectId('51cb5c3d067c0175c171a987'), 'url': 'http://foobar.com/datastore/{{value}}',
                    'key': 'illumina_sdm_seq_unit_id', 'owner': 'sdm', 'identifier': 'sdm_seq_unit'})
         ),
        ('non_existing_service_key',
         {
             '__auth': {'user': 'sdm', 'group': 'sdm'},
             'key': 'foo',
             'url': 'http://foobar.com/datastore/{{value}}',
             'identifier': 'bar',
         },
         '51cb5c3d067c0175c171a987',
         call.save(
             {'url': 'http://foobar.com/datastore/{{value}}', 'owner': 'sdm', 'identifier': 'bar', 'key': 'foo'})
         ),
        ('existing_service_key',
         {
             '__auth': {'user': 'sdm', 'group': 'sdm'},
             'key': 'illumina_sdm_seq_unit_id',
             'url': 'http://foobar.com/datastore/{{value}}',
             'identifier': 'bar',
         },
         '51cb5c3d067c0175c171a987',
         call.save(
             {'url': 'http://foobar.com/datastore/{{value}}', 'owner': 'sdm', 'identifier': 'bar',
              'key': 'illumina_sdm_seq_unit_id'})
         ),
    ])
    def test_Metadata_post_datastore(self, _description, kwargs, expected, expected_db_call):
        self.db.save.return_value = ObjectId('51cb5c3d067c0175c171a987')

        self.assertEqual(self.metadata.post_datastore(None, kwargs), expected)
        self.assertIn(expected_db_call, self.db.mock_calls)

    @unittest.skip('Method does not work')
    def test_Metadata_post_dataservice(self):
        self.db.save.return_value = ObjectId('51cb5c3d067c0175c171a987')

        self.assertEqual(self.metadata.post_dataservice(None, {
            'name': 'new_service',
            'callback': '/api/newservice',
        }), '51cb5c3d067c0175c171a987')

    @parameterized.expand([
        ('non_matching_identifier',
         {
             'key': 'illumina_sdm_seq_unit_id',
             'identifier': 'foo',
             '_id': '51cb5c3d067c0175c171a987'
         },
         ),
        ('matching_identifier',
         {
             'key': 'illumina_sdm_seq_unit_id',
             'identifier': 'sdm_seq_unit',
             '_id': '51cb5c3d067c0175c171a987'
         }
         ),
    ])
    def test_Metadata_post_removedatastore(self, _description, kwargs):
        self.metadata.post_removedatastore(None, kwargs)
        self.assertIn(call.remove({'_id': ObjectId('51cb5c3d067c0175c171a987')}), self.db.mock_calls)

    def test_Metadata_get_datastore(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_datastore(['51cb5c3d067c0175c171a987'], None),
                         record)
        self.assertIn(call.find({'identifier': '51cb5c3d067c0175c171a987'}),
                      self.db.mock_calls)

    @parameterized.expand([
        ('kwargs',
         {'_id': '5327394649607a1be0059511'},
         call.find({'_id': ObjectId('5327394649607a1be0059511')})
         ),
        ('no_kwargs',
         {},
         call.find({})
         ),
    ])
    def test_Metadata_get_datastores(self, _description, kwargs, expected_db_call):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_datastores(None, kwargs), [record])
        self.assertIn(expected_db_call, self.db.mock_calls)

    @parameterized.expand([
        ('enabled', 'True'),
        ('disabled', 'False'),
    ])
    def test_Metadata_post_togglesubscriptions(self, _description, enabled):
        self.cursor.__iter__.return_value = iter([
            {'group': 'auto', 'description': 'auto-created from file single_viral_sort.yaml',
             'url': 'http://127.0.0.1:8080/api/jira/createjira/single_viral_sort', 'Enabled': True, 'filter': {
                    'metadata>sequencing_project>actual_sequencing_product_name': {'#in': ['Viral Single Particle Sort']},
                    'metadata>illumina_physical_run_id': {'#exists': True},
                    'metadata>sow_segment>scientific_program': 'Microbial', 'user': 'rqc',
                    'metadata>fastq_type': 'filtered',
                    'metadata>sow_segment>account_purpose': {'#in': ['Programmatic', 'QC']}}, 'user': 'jira',
             'type': 'metadata', 'name': 'single_viral_sort', '_id': ObjectId('5fab1aca47675a20c853bc10')}])

        self.metadata.post_togglesubscriptions(None, {
            'enabled': enabled,
            '_id': '5fab1aca47675a20c853bc10'
        })

        self.assertIn(
            call.update({'_id': ObjectId('5fab1aca47675a20c853bc10')}, {'$set': {'Enabled': enabled != 'True'}}, multi=True),
            self.db.mock_calls)

    @parameterized.expand([
        ('queryResults',
         {
             'queryResults': True,
             'query': '_id = 5fab1aca47675a20c853bc10',
             'fields': ['name'],
             'page': 1,
         },
         {'return_count': 1, 'record_count': 1,
          'data': [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}]}
         ),
        ('kwargs',
         {
             '_id': '5327394649607a1be0059511'
         },
         [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}],
         ),
        ('no_kwargs',
         {
             '_id': '5327394649607a1be0059511'
         },
         [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}],
         ),
    ])
    def test_Metadata_get_subscriptions_menu(self, _description, kwargs, expected):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.cursor.__iter__.return_value = iter([record])
        self.cursor.skip.return_value = self.cursor
        self.cursor.limit.return_value = self.cursor

        self.assertEqual(self.metadata.get_subscriptions_menu(None, kwargs), expected)

    @patch('metadata.restful.RestServer')
    def test_Metadata_delete_file(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server

        self.metadata.delete_file(None, {'file': '/path/to/file'})
        self.assertIn(call.remove({'file_name': 'file', 'file_path': '/path/to'}), self.db.mock_calls)
        self.assertIn(call.run_method('tape', 'delete_file', file='/path/to/file'), server.mock_calls)

    def test_Metadata_get_search(self):
        self.assertEqual(self.metadata.get_search(None, None),
                         {'fields': ['foo', 'bar'],
                          'services': {'Restore files': {'_id': ObjectId('52169c3a067c010551524fe4'),
                                                         'callback': '/api/tape/grouprestore',
                                                         'name': 'Restore files',
                                                         'owner': 'sdm'}}})

    @parameterized.expand([
        ('less_than_11_hours', datetime.datetime(2000, 1, 2, 3, 4, 5), '03:04 am'),
        ('less_than_365_days', datetime.datetime(1999, 11, 2, 3, 4, 5), 'Nov 2'),
        ('greater_than_365_days', datetime.datetime(1998, 11, 2, 3, 4, 5), '02/11/1998')
    ])
    @patch('metadata.datetime')
    def test_Metadata_getSimpleDateString(self, _description, date, expected, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        datetime_mock.timedelta.side_effect = lambda **x: datetime.timedelta(**x)

        self.assertEqual(self.metadata.getSimpleDateString(date), expected)

    @parameterized.expand([
        ('no_args',
         [],
         {'start': 1, 'record_count': 1, 'end': 1, 'cursor_id': 'A' * 10, 'records': [
             {'file_name': 'foobar.tar', 'added_date': '02/01/2000', 'selected': False, 'desc': '/path/to'}]}
         ),
        ('args',
         ['foo'],
         {'cursor_id': 'foo',
          'end': 1,
          'record_count': 1,
          'records': [{'added_date': '02/01/2000',
                       'desc': '/path/to',
                       'file_name': 'foobar.tar',
                       'selected': False}],
          'start': 6}
         ),
    ])
    @patch('lapinpy.mongorestful.random')
    def test_Metadata_post_search(self, _description, args, expected, random_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_name': 'foobar.tar',
            'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'file_path': '/path/to',
        }
        self.cursor.__iter__.return_value = iter([record])
        random_mock.choice.return_value = 'A'
        session_data = {'last_accessed': datetime.datetime(2022, 2, 1),
                        'timeout': 10, 'cursor': self.cursor,
                        'return_count': 5, 'end': 5,
                        'record_count': 1, 'cursor_id': 'foo',
                        'flatten': True}
        self.metadata.cursors = {'foo': session_data}

        self.assertEqual(self.metadata.post_search(args, {
            'query': '_id = 5327394649607a1be0059511'
        }), expected)

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_add_update(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_name': 'foobar.tar',
            'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'file_path': '/path/to',
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.add_update(({'_id': '5327394649607a1be0059511'}, {'file_name': 'bar.tar'}), None)

        # Wait for update thread to complete execution
        for i in range(5):
            if self.metadata.updateThreadRunning:
                time.sleep(0.1)
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'file_name': 'bar.tar', 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)}}, multi=True),
            self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_runUpdate_file_update(self, datetime_mock):
        event = 'file_update'
        data = ({'_id': '5327394649607a1be0059511'}, {'file_name': 'bar.tar'})
        expected_db_call = call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'file_name': 'bar.tar', 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)}}, multi=True)

        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_name': 'foobar.tar',
            'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'file_path': '/path/to',
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.events.append((event, data))

        self.metadata.runUpdate()

        self.assertIn(expected_db_call, self.db.mock_calls)

    @patch('metadata.datetime')
    @patch('lapinpy.mongorestful.datetime')
    @patch('metadata.curl')
    def test_Metadata_runUpdate_add(self, curl, datetime_metadata, datetime_mongorestful):
        event = 'add'
        data = {'metadata': {'foo': 'bar'}, '_id': '5327394649607a1be0059512'}
        expected_db_call = call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     '_subscriptions.single_viral_sort.called_new': datetime.datetime(2000, 1, 2, 3, 4, 5)}},
            multi=True)

        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_name': 'foobar.tar',
            'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'file_path': '/path/to',
        }
        datetime_metadata.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        datetime_mongorestful.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.cursor.__iter__.side_effect = [iter([
            {'group': 'auto', 'description': 'auto-created from file single_viral_sort.yaml',
             'url': 'http://127.0.0.1:8080/api/jira/createjira/single_viral_sort', 'Enabled': True, 'filter': {
                    'metadata>foo': {'#in': ['bar']}}, 'user': 'jira',
             'type': 'metadata', 'name': 'single_viral_sort', '_id': ObjectId('5fab1aca47675a20c853bc10')}]),
            iter([record])]
        self.metadata.createDecisionTree()
        self.metadata.events.append((event, data))

        self.metadata.runUpdate()

        self.assertIn(expected_db_call, self.db.mock_calls)

    @patch('metadata.curl')
    def test_Metadata_post_importproduction(self, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'file_name': 'foobar.tar',
            'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'file_path': '/path/to',
        }
        self.cursor.__iter__.return_value = iter([record])
        curl.get.return_value = record

        self.metadata.post_importproduction(None, {
            'files': ['5327394649607a1be0059511'],
        })

        self.assertIn(call.save({'file_name': 'foobar.tar', 'added_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                 '_id': ObjectId('5327394649607a1be0059511'), 'file_path': '/path/to'}),
                      self.db.mock_calls)

    @parameterized.expand([
        ('get_file',
         [iter([{
             '_id': ObjectId('5327394649607a1be0059511'),
             '_subscriptions': {'foo': {'called_new': True}},
         }])],
         None,
         ),
        ('curl',
         [iter([None])],
         {
             '_id': ObjectId('5327394649607a1be0059511'),
             '_subscriptions': {'foo': {'called_new': True}},
         },
         ),
    ])
    @patch('metadata.curl')
    @patch('metadata.threading')
    def test_Metadata_post_testsubscription(self, _description, db_return_values, curl_return_value, threading, curl):
        expected = (
            'add', {'_subscriptions': {'foo': {'called_new': True}}, '_id': ObjectId('5327394649607a1be0059511')})

        self.cursor.__iter__.side_effect = db_return_values
        if curl_return_value:
            curl.get.return_value = curl_return_value

        self.metadata.post_testsubscription(None, {
            'files': ['5327394649607a1be0059511'],
            'override': True,
        })

        self.assertIn(expected, self.metadata.events)

    @patch('metadata.threading')
    def test_Metadata_startEventThread(self, threading):
        self.metadata.startEventThread()

        self.assertIn(call.Thread(target=self.metadata.runUpdate), threading.mock_calls)
        self.assertIn(call.Thread().start(), threading.mock_calls)

    def test_Metadata_get_processservices(self):
        self.cursor.__iter__.return_value = iter([{'owner': 'sdm', 'callback': '/api/ncbi/files', 'name': 'NCBI Project', '_id': ObjectId('5327394649607a1be0059511')}])

        self.assertEqual(self.metadata.get_processservices(None, None),
                         [{'owner': 'sdm', 'callback': '/api/ncbi/files', 'name': 'NCBI Project', '_id': ObjectId('5327394649607a1be0059511')}])
        self.db.__getitem__.assert_called_with('process_services')

    def test_Metadata_post_processservice(self):
        self.db.save.return_value = '5327394649607a1be0059511'

        self.assertEqual(self.metadata.post_processservice(None, {
            '__auth': {'group': 'sdm'},
            'callback': '/api/tape/grouprestore',
            'name': 'Restore files',
        }), '5327394649607a1be0059511')

        self.metadata.db.save.assert_called_with({'owner': 'sdm', 'callback': '/api/tape/grouprestore', '_id': ObjectId('5327394649607a1be0059511'), 'name': 'Restore files'})

    def test_Metadata_post_user(self):
        expected = {'_id': ObjectId('5226cea6067c016931c7fb91'), 'relative_root': 'sdm/files', 'user': 'sdm'}

        self.assertEqual(self.metadata.post_user(None, {'user': 'sdm', 'relative_root': 'sdm/files'}),
                         expected)
        self.assertEqual(self.metadata.userSettings.get('sdm'), expected)

    def test_Metadata_get_users(self):
        users = [{'relative_root': '/global/dna/dm_archive/sdm', 'user': 'sdm', '_id': ObjectId('5226cea6067c016931c7fb91')}]
        self.cursor.__iter__.return_value = iter(users)

        self.assertEqual(self.metadata.get_users(None, None), users)
        self.db.__getitem__.assert_called_with('user')

    def test_Metadata_safeMerge(self):
        origin = {'foo': {'bar': 'baz'}}
        new = {'foo': {'bar': 'foobar'}}
        replace_with_null = True

        self.assertEqual(self.metadata.safeMerge(origin, new, replace_with_null), 1)
        self.assertEqual(origin, {'foo': {'bar': 'foobar'}})

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_post_registerupdate(self, datetime_mock, curl):
        files = [{'_id': ObjectId('51d45d9e067c014cd6e88f61'),
                  'metadata': {'foo': 'bar', 'file_owner': 'sdm',
                               'illumina_sdm_seq_unit_id': {'sdm_seq_unit': {'foo': 'bar'}}}}]
        self.cursor.__iter__.return_value = iter(files)
        self.metadata.stores.get('illumina_sdm_seq_unit_id')[0]['key'] = 'illumina_sdm_seq_unit_id'
        curl.get.return_value = {'foo': 'bar'}
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.assertEqual(self.metadata.post_registerupdate(None, {
            'where': {'_id': '51d45d9e067c014cd6e88f61'},
            'keep': ['file_owner', 'illumina_sdm_seq_unit_id']
        }), 'processed 1 records, modified 1 records')
        self.assertIn(call.save({'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                 'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                 '_id': ObjectId('51d45d9e067c014cd6e88f61'),
                                 'metadata': {'file_owner': 'sdm', 'foo': 'bar', 'sdm_seq_unit': {'foo': 'bar'},
                                              'illumina_sdm_seq_unit_id': {'sdm_seq_unit': {'foo': 'bar'}}}}),
                      self.db.mock_calls)

    def test_Metadata_get_keys_dictionary(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_keys_dictionary(None, None), [record])

    def test_Metadata_put_update_keys(self):
        self.assertEqual(self.metadata.put_update_keys(None, {
            'data': {'foo': 'bar'},
            '_id': '5327394649607a1be0059511',
        }), {'status': 'ok'})
        self.assertIn(call.update({'_id': '5327394649607a1be0059511'}, {'$set': {'foo': 'bar'}}),
                      self.db.mock_calls)

    def test_Metadata_get_check_keys(self):
        self.assertEqual(self.metadata.get_check_keys(None, {
            '_id': '5327394649607a1be0059511',
        }), 1)

    def test_Metadata_get_parent_keys(self):
        record = {
            '_id': 'foo.bar',
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_parent_keys(None, None),
                         {'bar': 1})

    def test_Metadata_get_keys(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_keys(['5327394649607a1be0059511'], None),
                         [record])

    @parameterized.expand([
        ('ids_in_kwargs',
         {'ids': '5327394649607a1be0059511,5327394649607a1be0059512'},
         ),
        ('ids_not_in_kwargs',
         {},
         )
    ])
    def test_Metadata_get_files(self, _description, kwargs):
        records = [{'_id': ObjectId('5327394649607a1be0059511')},
                   {'_id': ObjectId('5327394649607a1be0059512')}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertEqual(self.metadata.get_files(None, kwargs), records)

    def test_Metadata_exchangeKeys(self):
        data = [{'foo': 'foo1'}, {'bar': 'bar1'}]
        what_to = {'foo': 'new_foo', 'bar': 'new_bar'}

        self.assertEqual(self.metadata.exchangeKeys(data, what_to),
                         [{'new_foo': 'foo1'}, {'new_bar': 'bar1'}])

    @parameterized.expand([
        ('record_found',
         {
             '_id': ObjectId('5327394649607a1be0059511'),
             'user': 'sdm'
         },
         {'subscription_id': ObjectId('5327394649607a1be0059511')},
         call.save(
             {'name': 'foobar', 'url': 'http://foobar.com', 'Enabled': True, 'filter': {}, 'user': 'sdm',
              '_id': ObjectId('5327394649607a1be0059511'), 'type': 'metadata', 'description': 'Some description'})
         ),
        ('record_not_found',
         None,
         {'subscription_id': '5327394649607a1be0059511'},
         call.save(
             {'name': 'foobar', 'url': 'http://foobar.com', 'Enabled': True, 'filter': {}, 'user': 'sdm',
              'type': 'metadata', 'description': 'Some description'})
         ),
    ])
    def test_Metadata_post_subscription(self, _description, record, expected, expected_db_call):
        self.db.subscriptions.find_one.return_value = record
        self.cursor.__iter__.return_value = iter([
            {'group': 'auto', 'description': 'auto-created from file single_viral_sort.yaml',
                'url': 'http://127.0.0.1:8080/api/jira/createjira/single_viral_sort', 'Enabled': True, 'filter': {
                    'metadata>sequencing_project>actual_sequencing_product_name': {'#in': ['Viral Single Particle Sort']},
                    'metadata>illumina_physical_run_id': {'#exists': True},
                    'metadata>sow_segment>scientific_program': 'Microbial', 'user': 'rqc',
                    'metadata>fastq_type': 'filtered',
                    'metadata>sow_segment>account_purpose': {'#in': ['Programmatic', 'QC']}}, 'user': 'jira',
             'type': 'metadata', 'name': 'single_viral_sort', '_id': ObjectId('5fab1aca47675a20c853bc10')}])
        self.db.save.return_value = '5327394649607a1be0059511'

        self.assertEqual(self.metadata.post_subscription(None, {
            'name': 'foobar',
            'description': 'Some description',
            'filter': {},
            'url': 'http://foobar.com',
            'user': 'sdm',
        }), expected)
        self.assertIn(expected_db_call, self.db.mock_calls)

    def test_Metadata_get_subscriptions(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'parent>child': 'foobar',
            '#foo': 'bar'
        }
        self.cursor.__iter__.return_value = iter([record])

        self.assertEqual(self.metadata.get_subscriptions(None, {
            '_id': '5327394649607a1be0059511'
        }), [{'$foo': 'bar',
              '_id': ObjectId('5327394649607a1be0059511'),
              'parent.child': 'foobar'}])

    @patch('metadata.uuid')
    @patch('metadata.datetime')
    def test_Metadata_post_duid(self, datetime_mock, uuid):
        uuid.uuid1.return_value = 'UUID'
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.assertEqual(self.metadata.post_duid(None, {
            'files': ['5327394649607a1be0059511']
        }), {'duid': 'UUID',
             'url': 'http://genome.jgi-psf.org/pages/dynamicOrganismDownload.jsf?organism=duid&duid=UUID'})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)},
            '$push': {'metadata.portal.duid': 'UUID'}}, multi=True),
            self.db.mock_calls)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_projectfiles(self, restserver):
        files = [{'file_name': 'file2.tar'},
                 {'file_name': 'file1.tar'}]
        server = Mock()
        server.run_method.return_value = files
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.post_projectfiles(None, {
            'identifier': '_id',
            'values': ['foo', 'bar'],
            'fields': ['foo', 'bar'],
        }), {'files': [{'file_name': 'file1.tar'}, {'file_name': 'file2.tar'}]})

    @patch('metadata.restful.RestServer')
    def test_Metadata_get_projectfiles(self, restserver):
        files = [
            {'group': 'sdm', 'file_type': ['fastq.gz', 'fastq'], 'file_name': '1604.1.1458.fastq.gz', 'file_id': 35392,
             'metadata': {'library_name': 'HYNW', 'portal': {'display_location': ['Raw Data']}},
             '_id': ObjectId('51d45d9e067c014cd6e88f61')}]
        server = Mock()
        server.run_method.side_effect = [files,
                                         {'set_portal': True}]
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.get_projectfiles(None, {
            'identifier': '_id',
            'values': '51d45d9e067c014cd6e88f61',
            'user': 'sdm',
        }), {'display_location_cv': [],
             'files': [{'_id': ObjectId('51d45d9e067c014cd6e88f61'),
                        'file_id': 35392,
                        'file_name': '1604.1.1458.fastq.gz',
                        'file_type': ['fastq.gz', 'fastq'],
                        'group': 'sdm',
                        'metadata': {'library_name': 'HYNW',
                                     'portal': {'display_location': ['Raw Data']}}}],
             'perms': 1,
             'publishing_flags': ['sra']})

    @parameterized.expand([
        ('int', 1, 1),
        ('int_str', '1', 1),
        ('float', 1.0, 1.0),
        ('float_str', '1.0', 1.0),
        ('str', 'foo', 'foo'),
    ])
    def test_Metadata_checkNumeric(self, _description, value, expected):

        self.assertEqual(self.metadata.checkNumeric(value), expected)

    def test_Metadata_get_distributionproperties(self):
        self.assertEqual(self.metadata.get_distributionproperties(None, None),
                         {'display_location_cv': [],
                          'publishing_flags': ['sra'],
                          'search_fields': {'File name': 'file_name'}})

    @patch('metadata.datetime')
    def test_Metadata_put_portallocation(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'metadata': {'portal': {'display_location': ['old', 'path', 'to']}},
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.put_portallocation(['5327394649607a1be0059511'], {
            'path': ['new', 'path', 'to'],
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
        })

        self.assertIn(call.update({'_id': ObjectId('5327394649607a1be0059511')}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'metadata.portal.display_location': ['new', 'path', 'to'],
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)}, '$push': {
                '__update_publish_to': {'on': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                        'display_location': {'to': 'new/path/to', 'from': 'old/path/to'},
                                        'user': 'sdm'}}}, multi=True),
            self.db.mock_calls)

    @patch('metadata.datetime')
    def test_Metadata_delete_portallocation(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'metadata': {'portal': {'display_location': ['path', 'to']}},
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.delete_portallocation(['5327394649607a1be0059511'], {
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
        })

        self.assertIn(call.update({'_id': ObjectId('5327394649607a1be0059511')}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)},
            '$unset': {'metadata.portal.display_location': ''}, '$push': {
                '__update_publish_to': {'on': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                        'display_location': {'to': '', 'from': 'path/to'}, 'user': 'sdm'}}},
            multi=True),
            self.db.mock_calls)

    @parameterized.expand([
        ('update',
         {
             '_id': ObjectId('5327394649607a1be0059511'),
             'metadata': {'portal': {'display_location': ['path', 'to']}, 'publish_to': ['foo', 'bar']},
         },
         {
             'flags': 'foo',
             '__auth': {'user': 'sdm', 'group': 'sdm_group'},
         },
         call.update({'_id': ObjectId('5327394649607a1be0059511')}, {
             '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                      'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata.publish_to': ['foo']},
             '$push': {'__update_publish_to': {'on': datetime.datetime(2000, 1, 2, 3, 4, 5), 'user': 'sdm',
                                               'publish_to': ['-bar']}}}, multi=True),
         None,
         ),
        ('remove',
         {
             '_id': ObjectId('5327394649607a1be0059511'),
             'metadata': {'portal': {'display_location': ['path', 'to']}, 'publish_to': ['foo']},
         },
         {
             'flags': 'foo',
             '__auth': {'user': 'sdm', 'group': 'sdm_group'},
             'operation': 'remove',
         },
         None,
         call.run_method('metadata', 'delete_publishingflags',
                         ObjectId('5327394649607a1be0059511'),
                         __auth={'group': 'sdm', 'user': 'sdm'}, permissions=['admin']),
         ),
        ('add',
         {
             '_id': ObjectId('5327394649607a1be0059511'),
             'metadata': {'portal': {'display_location': ['path', 'to']}, 'publish_to': ['foo']},
         },
         {
             'flags': 'bar',
             '__auth': {'user': 'sdm', 'group': 'sdm_group'},
             'operation': 'add',
         },
         call.update({'_id': ObjectId('5327394649607a1be0059511')}, {
             '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                      'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5), 'metadata.publish_to': ['foo', 'bar']},
             '$push': {'__update_publish_to': {'on': datetime.datetime(2000, 1, 2, 3, 4, 5), 'user': 'sdm',
                                               'publish_to': ['+bar']}}}, multi=True),
         None,
         ),
    ])
    @patch('metadata.restful.RestServer')
    @patch('metadata.datetime')
    def test_Metadata_put_publishingflags(self, _description, record, kwargs, expected_db_call, expected_internal_call,
                                          datetime_mock, restserver):
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server

        self.metadata.put_publishingflags(['5327394649607a1be0059511'], kwargs)

        if expected_db_call:
            self.assertIn(expected_db_call, self.db.mock_calls)
        if expected_internal_call:
            self.assertIn(expected_internal_call, server.mock_calls)

    @patch('metadata.datetime')
    def test_Metadata_delete_publishingflags(self, datetime_mock):
        record = {'_id': ObjectId('5327394649607a1be0059511'),
                  'metadata': {'portal': {'display_location': ['path', 'to']}, 'publish_to': ['foo', 'bar']}}
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)

        self.metadata.delete_publishingflags(['5327394649607a1be0059511'], {
            '__auth': {'user': 'sdm', 'group': 'sdm_group'},
        })

        self.assertIn(
            call.update({'_id': ObjectId('5327394649607a1be0059511')}, {
                '$unset': {'metadata.publish_to': ''},
                '$set': {
                    'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                    'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)},
                '$push': {'__update_publish_to': {'user': 'sdm', 'on': datetime.datetime(2000, 1, 2, 3, 4, 5),
                                                  'publish_to': ['-foo', '-bar']}}}, multi=True),
            self.db.mock_calls)

    def test_Metadata_put_safeupdate(self):
        self.metadata.put_safeupdate(['5327394649607a1be0059511'], {'foo': 'bar'})

    def test_Metadata_get_search2(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.cursor.__iter__.return_value = iter([record])
        self.cursor.skip.return_value = self.cursor
        self.cursor.limit.return_value = self.cursor

        self.assertEqual(self.metadata.get_search2(None, {
            'queryResults': True,
            'query': '_id = 5fab1aca47675a20c853bc10',
            'fields': ['name'],
            'page': 1,
        }), {'data': [{'_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar'}],
             'record_count': 1,
             'return_count': 1})

    def test_Metadata_get_htmltemplate(self):
        self.metadata.location = os.path.dirname(os.path.abspath(__file__))

        self.assertEqual(self.metadata.get_htmltemplate(['keys.html'], None),
                         b'{%for item in data%}\n{{item._id}}<br>\n{%endfor%}\n')

    def test_Metadata_get_distinct(self):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.db.distinct.return_value = [record]

        self.assertEqual(self.metadata.get_distinct(['5327394649607a1be0059511'], None),
                         [record])

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_put_portalpath(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.put_portalpath(['5327394649607a1be0059511'], {
            'foo': 'bar'
        }), {'nModified': 1, 'ok': 1.0, 'n': 1})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'metadata.portal.display_location.foo': 'bar'},
            '$addToSet': {'metadata.portal.identifier': {'$each': ['foo']}}}, multi=True),
            self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_delete_portalpath(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.delete_portalpath(['5327394649607a1be0059511', 'foo'], None),
                         {'n': 1, 'nModified': 1, 'ok': 1.0})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)},
            '$unset': {'metadata.portal.display_location.foo': ''}}, multi=True),
            self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Metadata_post_safeupdate(self, datetime_mock):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
        }
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.metadata.post_safeupdate(None, {
            'permissions': ['admin'],
            'query': {'_id': '5327394649607a1be0059511'},
            'update': {'foo': 'bar'}
        }), {'n': 1, 'nModified': 1, 'ok': 1.0})
        self.assertIn(call.update({'_id': {'$in': [ObjectId('5327394649607a1be0059511')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5),
                     'modified_date': datetime.datetime(2000, 1, 2, 3, 4, 5)}, 'foo': 'bar'}, multi=True),
            self.db.mock_calls)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_delete(self, restserver):
        meta_data = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
            'file_id': 14509150,
        }
        delete_data = {
            'tape_records': 1,
            'tape_data': {'foo': 'bar'},

        }
        server = Mock()
        server.run_method.side_effect = [[meta_data], delete_data]
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.post_delete(None, {
            'query': {'_id': '5327394649607a1be0059511'}
        }), {'file_records': 1, 'tape_records': 1})
        self.assertIn(call.remove({'_id': ObjectId('5327394649607a1be0059511')}),
                      self.db.mock_calls)
        self.assertIn(call.save(
            {'name': 'foobar', '_tape_data': {'foo': 'bar'}, 'file_status': 'PURGED', 'file_id': 14509150,
             '_id': ObjectId('5327394649607a1be0059511'),
             'metadata': {'portal': {'display_location': {'foo': 'bar'}}}}), self.db.mock_calls)

    @patch('metadata.restful.RestServer')
    def test_Metadata_post_undelete(self, restserver):
        meta_data = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
            'file_id': 14509150,
            '_tape_data': {'foo': 'bar'},
        }
        undelete_data = {
            'tape_records': 1,
            'tape_data': {'foo': 'bar'},
        }
        server = Mock()
        server.run_method.side_effect = [[meta_data], undelete_data]
        restserver.Instance.return_value = server

        self.assertEqual(self.metadata.post_undelete(None, {
            'query': {'_id': '5327394649607a1be0059511'}
        }), {'file_records': 1, 'tape_records': 1})
        self.assertIn(call.remove({'_id': ObjectId('5327394649607a1be0059511')}),
                      self.db.mock_calls)
        self.assertIn(call.save({'file_id': 14509150, '_id': ObjectId('5327394649607a1be0059511'), 'name': 'foobar',
                                 'metadata': {'portal': {'display_location': {'foo': 'bar'}}}}),
                      self.db.mock_calls)

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_queue_wip_metadata_refresh(self, datetime_mock, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059512'),
            'key_name': 'metadata.sample_id',
            'key_value': 2,
            'keep': ['gls_physical_run_unit_id', 'library_name'],
            'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'source_process': 'wip'
        }
        curl.get.side_effect = [[2], [3]]
        core = Mock()
        core.getSetting.return_value = '2000-01-02'
        self.metadata.core = core
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.today.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.metadata.appname = 'some_app'
        self.metadata.config.wip_updates = [{'service': 'https://foo_service', 'entity': 'foo_entity',
                                             'keep': ['gls_physical_run_unit_id', 'library_name'],
                                             'key': 'metadata.sample_id'},
                                            {'service': 'https://bar_service', 'entity': 'bar_entity',
                                             'keep': ['sequencing_project_id'],
                                             'key': 'metadata.sequencing_project_id'}]

        self.metadata.queue_wip_metadata_refresh()

        # Only the second metadata refresh should be written to the DB as the first one already exists in the DB.
        self.assertIn(call.insert(
            {'key_name': 'metadata.sequencing_project_id', 'key_value': 3, 'keep': ['sequencing_project_id'],
             'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5), 'source_process': 'wip'}), self.db.mock_calls)
        self.assertIn(call.saveSetting('some_app', 'wip_update', '2000-01-02'), core.mock_calls)

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_queue_dus_metadata_refresh(self, datetime_mock, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059512'),
            'key_name': 'metadata.library_name',
            'key_value': 'ABDT',
            'keep': ['library_name'],
            'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'source_process': 'dus'
        }
        records_to_update = [
            [{'library-stock-id': 21, 'library-name': 'ABDU', 'data-utilization-status': 'Unrestricted',
              'last-modified-date': '2023-06-16T09:34:58.913946', 'sequencing-projects': [1000124]}],
            [{'sequencing-project-id': 1470028, 'data-utilization-status': 'Restricted',
              'last-modified-date': '2023-09-11T15:30:44.422784'}]]
        curl.get.side_effect = records_to_update
        core = Mock()
        core.getSetting.return_value = '2000-01-02'
        self.metadata.core = core
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.today.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.metadata.appname = 'some_app'
        self.metadata.config.dus_updates = {'services': [{'entity': 'libraries',
                                                          'keep': ['library_name'],
                                                          'key': 'metadata.library_name',
                                                          'entity_key': 'library-name'},
                                                         {'entity': 'sequencing-projects',
                                                          'keep': ['sequencing_project_id'],
                                                          'key': 'metadata.sequencing_project_id',
                                                          'entity_key': 'sequencing-project-id'}
                                                         ],
                                            'url': 'http://foobar.com/{{entity}}/{{date}}'}

        self.metadata.queue_dus_metadata_refresh()

        # Only the second metadata refresh should be written to the DB as the first one already exists in the DB.
        self.assertIn(call.insert(
            {'key_name': 'metadata.sequencing_project_id', 'key_value': 1470028, 'keep': ['sequencing_project_id'],
             'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5), 'source_process': 'dus'}), self.db.mock_calls)
        self.assertIn(call.saveSetting('some_app', 'dus_update', '2000-01-02'), core.mock_calls)

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_queue_mycocosm_metadata_refresh(self, datetime_mock, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059512'),
            'key_name': 'metadata.mycocosm_portal_id',
            'key_value': "portal_id_1",
            'keep': ['mycocosm_portal_id'],
            'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5),
            'source_process': 'mycocosm'
        }
        records_to_update = [[
            {
                'portal_id': 'portal_id_1',
                'parent': 'fungal-program-annotated-genome',
                'deleted': False,
                'last_updated': '2023-02-17T15:01:33.133'
            },
            {
                'portal_id': 'portal_id_2',
                'parent': 'fungal-program-annotated-genome',
                'deleted': False,
                'last_updated': '2023-02-17T15:03:59.359'
            }
        ]]
        curl.get.side_effect = records_to_update
        core = Mock()
        core.getSetting.return_value = '2000-01-02'
        self.metadata.core = core
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.today.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.metadata.appname = 'some_app'
        self.metadata.config.mycocosm_updates = [{'service': 'https://foo_service',
                                                  'keep': ['mycocosm_portal_id'],
                                                  'key': 'metadata.mycocosm_portal_id'}]

        self.metadata.queue_mycocosm_metadata_refresh()

        # Only the second metadata refresh should be written to the DB as the first one already exists in the DB.
        self.assertIn(call.insert(
            {'key_name': 'metadata.mycocosm_portal_id', 'key_value': 'portal_id_2', 'keep': ['mycocosm_portal_id'],
             'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5), 'source_process': 'mycocosm'}), self.db.mock_calls)
        self.assertIn(call.saveSetting('some_app', 'mycocosm_update', '2000-01-02'), core.mock_calls)

    def test_Metadata_update_queued_metadata_refresh(self):
        records = [
            {
                '_id': ObjectId('5327394649607a1be0059512'),
                'key_name': 'metadata.sample_id',
                'key_value': 2,
                'keep': ['gls_physical_run_unit_id', 'library_name'],
                'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5),
                'source_process': 'wip'
            },
            {
                '_id': ObjectId('5327394649607a1be0059513'),
                'key_name': 'metadata.sequencing_project_id',
                'key_value': 3,
                'keep': ['sequencing_project_id'],
                'dt_modified': datetime.datetime(2000, 1, 2, 3, 4, 5),
                'source_process': 'wip'
            },
        ]
        self.cursor.__iter__.return_value = iter(records)
        # We set a fake document count to verify that the request is made for all documents.
        self.db.count_documents.return_value = 1000
        expected_db_calls = [call.remove({'_id': ObjectId('5327394649607a1be0059512')}),
                             call.remove({'_id': ObjectId('5327394649607a1be0059513')}),
                             # Verify that we're requesting records from 0 to total number of records (1000 in this
                             #  case) from DB.
                             call.find().__getitem__(slice(0, 1000, None))]

        self.metadata.update_queued_metadata_refresh()

        for c in expected_db_calls:
            self.assertIn(c, self.db.mock_calls)

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_refresh_wip_data(self, datetime_mock, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
        }
        curl.get.return_value = ['5327394649607a1be0059511']
        core = Mock()
        core.getSetting.return_value = '2000-01-02'
        self.metadata.core = core
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.today.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.metadata.appname = 'some_app'
        self.metadata.config.wip_updates = [{'service': 'foo_service', 'entity': 'foo_entity',
                                             'keep': ['file_owner', 'illumina_sdm_seq_unit_id'],
                                             'key': 'illumina_sdm_seq_unit_id'}]

        self.metadata.refresh_wip_data()

        self.assertIn(call.saveSetting('some_app', 'wip_update', '2000-01-02'),
                      core.mock_calls)

    @patch('metadata.curl')
    @patch('metadata.datetime')
    def test_Metadata_refresh_dus_data(self, datetime_mock, curl):
        record = {
            '_id': ObjectId('5327394649607a1be0059511'),
            'name': 'foobar',
            'metadata': {'portal': {'display_location': {'foo': 'bar'}}},
            'entity_key': '5327394649607a1be0059512'
        }
        curl.get.return_value = [record]
        core = Mock()
        core.getSetting.return_value = '2000-01-02'
        self.metadata.core = core
        self.cursor.__iter__.return_value = iter([record])
        datetime_mock.datetime.today.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
        self.metadata.appname = 'some_app'
        self.metadata.config.dus_updates = {'services': [{'service': 'foo_service', 'entity': 'foo_entity',
                                                          'keep': ['file_owner', 'illumina_sdm_seq_unit_id'],
                                                          'key': 'illumina_sdm_seq_unit_id',
                                                          'entity_key': 'entity_key'}],
                                            'url': 'http://foobar.com/{{entity}}/{{date}}'}

        self.metadata.refresh_dus_data()

        self.assertIn(call.saveSetting('some_app', 'dus_update', '2000-01-02'),
                      core.mock_calls)

    @parameterized.expand([
        ('conform_false', False, {'foo': {'extract': True}, 'bar': {}, 'baz': {'new_key': 'baz-new', 'extract': True},
                                  'foobar': {'extract': True, 'skip_extract_if_exists': 'foobar'},
                                  'foz>boz>moz': {'extract': True}},
         {'baz-new': 'baz1', 'foo': 'foo1', 'moz': 'moz1'}),
        ('conform_true', True, {'foo': {'extract': True}, 'bar': {}, 'baz': {'new_key': 'baz-new', 'extract': True},
                                'foobar': {'extract': True, 'skip_extract_if_exists': 'foobar'},
                                'foz>boz>moz': {'extract': True}},
         {'baz_new': 'baz1', 'foo': 'foo1', 'moz': 'moz1'}),
    ])
    def test_Metadata_extract_keys(self, _description, conform, store_map, expected):
        data = {'foo': 'foo1', 'bar': 'bar1', 'baz': 'baz1', 'foobar': 'foobar1', 'foz': {'boz': {'moz': 'moz1'}}}
        original_doc = {'foobar': 'foobar2'}

        self.assertEqual(self.metadata._extract_keys(data, store_map, conform, original_doc), expected)


if __name__ == '__main__':
    unittest.main()
