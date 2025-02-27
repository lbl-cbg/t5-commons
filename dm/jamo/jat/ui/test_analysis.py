import unittest
import analysis
import os
from parameterized import parameterized
import datetime
from lapinpy import common
from bson.objectid import ObjectId
from unittest.mock import patch, Mock, MagicMock, call
from tempfile import TemporaryDirectory
from types import SimpleNamespace


class TestAnalysis(unittest.TestCase):

    def setUp(self):
        self.__initialize()

    @patch('pymongo.MongoClient')
    def __initialize(self, mongo_client):
        config = SimpleNamespace(**{
            'mongoserver': 'mongoserver',
            'mongo_user': 'mongo_user',
            'mongo_pass': 'mongo_pass',
            'meta_db': 'meta_db',
            'instance_type': 'prod',
            'from_address': 'me@foobar.com',
            'to_address': 'you@foobar.com',
            'url': 'http://address/to/foo',
            'publishing_flags': ['Foo', 'Bar'],
            'display_location_cv': ['Raw data'],
            'division': {
                'jgi': {'jat_key_name': 'AUTO'}},
        })

        self.cursor = MagicMock()
        self.cursor.skip.return_value = self.cursor
        self.cursor.limit.return_value = self.cursor
        self.cursor.__getitem__.return_value = self.cursor
        self.db = MagicMock()
        self.db.find.return_value = self.cursor
        self.db.__getitem__.return_value = self.db
        self.db.count_documents.return_value = 0
        self.core = Mock()
        mongo_client.return_value = self.db
        self.analysis = analysis.Analysis(config)
        self.analysis.db = self.db
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        self.analysis.location = self.temp_dir.name
        self.analysis.repo_location = self.analysis.location
        self.analysis.core = self.core
        self.analysis.appname = 'my_app'
        self.curl = Mock()
        self.analysis.bb_curl = self.curl

    def tearDown(self):
        del self.db.count_documents
        self.temp_dir.cleanup()

    @parameterized.expand([
        ('string', 'foo', True),
        ('number', 1.0, True),
        ('boolean', False, True),
        ('boolean', False, True),
        ('list:number', 1, True),
        ('list:string', ['foo', 'bar'], True),
        ('unknown', False, False),
    ])
    def test_checkType(self, type, value, expected):
        self.assertEqual(analysis.checkType(type, value), expected)

    def test_log(self):
        @analysis.log
        def func(*args, **kwargs):
            return 'foo'

        self.assertEqual(func(), 'foo')

    @parameterized.expand([
        ('string', 'string', 'foo', ('foo', True)),
        ('number_float', 'number', '1.0', (1.0, True)),
        ('number_int', 'number', '1', (1, True)),
        ('boolean_true', 'boolean', 'True', (True, True)),
        ('boolean_false', 'boolean', 'False', (False, True)),
        ('boolean_none', 'boolean', None, (None, False)),
        ('list_str_list', 'list:number', '1,2,3', ([1, 2, 3], True)),
        ('list_str', 'list:string', 'foo', ('foo', True)),
        ('unknown', 'unknown', 'foo', (None, False)),
    ])
    def test_convertType(self, _description, type, value, expected):
        self.assertEqual(analysis.convertType(type, value), expected)

    def test_check_keys(self):
        known_keys = {'bar'}
        doc = {'foo': 'foo1',
               'bar': 'bar1',
               }
        extra_keys = {}
        file = 'my_file'
        expected = ["warning: Metadata key 'foo' for output file 'my_file' not found in analysis template"]

        self.assertEqual(analysis.check_keys(known_keys, doc, extra_keys, file),
                         expected)

    def test_process_template_data(self):
        template = {
            'required_metadata_keys': [{'key': 'foo'}],
            'outputs': [{'label': 'my_label', 'tags': ['tag1'], 'required': True,
                         'required_metadata_keys': [{'key': 'baz'}, {'key': 'foo'}],
                         'metadata': {'bar': 'bar2'},
                         'file': 'my_file'}]
        }
        template_data = {
            'metadata': {'foo': 'foo1',
                         'bar': 'bar1'},
            'baz': 'baz1,',
            'outputs': [{}, {'label': 'some_label'}, {'label': 'my_label', 'metadata': {'foo': 'foo2'}}]
        }
        expected = {'outputs': [{'required': True, 'required_metadata_keys': [{'key': 'baz'}, {'key': 'foo'}],
                                 'metadata': {'foo': 'foo2', 'bar': 'bar2'}, 'tags': ['tag1'], 'label': 'my_label'}],
                    'inputs': [], 'warnings': ["warning: Metadata key 'bar' not found in analysis template",
                                               "warning: Output file 'None' does not have a label tag in the submission file",
                                               "warning: Output file 'None' does not have a matching label of 'some_label' in the analysis template",
                                               "warning: Metadata key 'bar' for output file 'None' not found in analysis template",
                                               'warning: You have Metadata keys that are not defined in the template.  Processing will continue for now.  In a future version of jat, your import will be aborted.',
                                               'warning: You have outputs with invalid labels.  Processing will continue for now.  In a future version of jat, your import will be aborted.'],
                    'options': {'baz': 'baz1,'}, 'metadata': {'foo': 'foo1', 'bar': 'bar1'}}

        self.assertEqual(analysis.process_template_data(template, template_data), expected)

    def test_process_template(self):
        kwargs = {
            'outputs': [{'label': 'my_label', 'file': 'my_file'}],
            'template': {'outputs': [{'label': 'my_label'}]}
        }

        analysis.process_template(kwargs)

        self.assertEqual(kwargs.get('outputs'), [{'label': 'my_label', 'file': 'my_file'}])

    def test_process_template_failed_validation(self):
        kwargs = {
            'outputs': [{'label': 'my_label',
                         'default_metadata_values': {'foo': 'foo1'},
                         'required_metadata_keys': [{'key': 'bar'}, {'key': 'baz', 'type': 'str'}],
                         'metadata': {'baz': 1}}],
            'template': {'outputs': [{'label': 'my_label'},
                                     {'label': 'other_label_1', 'file_name': os.path.basename(__file__)},
                                     {'label': 'other_label_2', 'required': True},
                                     {'label': 'other_label_2', 'required': False}],
                         'default_metadata_values': {'foo1': 'foo2'},
                         'required_metadata_keys': [{'key': 'blah'}, {'key': 'bar1', 'type': 'number'}]},
            'location': os.path.abspath(os.path.dirname(__file__)),
            'metadata': {'bar1': 'bar2'}
        }

        with self.assertRaises(common.HttpException) as cm:
            analysis.process_template(kwargs)

        # Verify 400 exception
        self.assertEqual(cm.exception.code, 400)
        self.assertEqual(cm.exception.message, ['error: cannot submit this analysis, encountered the following errors:',
                                                'metadata key blah was not found in the analysis metadata',
                                                'metadata key bar1 found in the analysis metadata had the wrong type. Was str expected number',
                                                'missing required file of type other_label_2',
                                                'The following errors were encountered for file type my_label:',
                                                'file location not found', 'required metadata key bar not found',
                                                'metadata key baz has the wrong type. Got int, expected str'])

    def test_Analysis_sendEmail(self):
        self.analysis.sendEmail('foo@bar.com', 'Some subject', 'Hello world', 'me@foobar.com', ['attachment'],
                                'you@foobar.com', 'some_key', ['me_too@foobar.com'], ['me_too_hidden@foobar.com'],
                                'text/html')

        self.assertIn(call.insert(
            {'content': 'Hello world', 'to': 'foo@bar.com', 'from': 'me@foobar.com', 'attachments': ['attachment'],
             'key': 'some_key', 'email_status': 'pending', 'cc': ['me_too@foobar.com'], 'mime': 'text/html',
             'bcc': ['me_too_hidden@foobar.com'], 'subject': 'Some subject'}), self.db.mock_calls)

    @parameterized.expand([
        ('greater_equal_to_jat_key_dir_switch', 'foo-367700', 'analyses-36'),
        ('less_than__to_jat_key_dir_switch', 'foo-1', 'analyses'),
    ])
    def test_Analysis_get_analyses_dir(self, _description, jat_key, expected):
        self.assertEqual(self.analysis.get_analyses_dir(jat_key), expected)

    @patch('analysis.datetime')
    @patch('analysis.sdmlogger')
    def test_Analysis_onstartup(self, sdmlogger_mock, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)
        cv_keys_dir = f'{self.analysis.location}/cv/keys'
        os.makedirs(cv_keys_dir)
        with open(f'{cv_keys_dir}/tags.yaml', 'w') as f:
            f.write('-\n value: viral\n description: ""')
        with open(f'{cv_keys_dir}/output_tags.yaml', 'w') as f:
            f.write('-\n value: readme_file\n description: ""')
        with open(f'{cv_keys_dir}/release_to.yaml', 'w') as f:
            f.write('-\n value: img\n description: the identifier for indicating that a release is releasable to IMG')
        macros_dir = f'{self.analysis.location}/macros'
        os.makedirs(macros_dir)
        with open(f'{macros_dir}/filtered_fastq_info.yaml', 'w') as f:
            f.write(
                'name: filtered_fastq_info\ndescription: pre-defined metadata for filtered fastqs\nrequired_metadata_keys:\n- description: Filter product type\n  key: filter_product_type\n  type: string')
        templates_dir = f'{self.analysis.location}/templates'
        os.makedirs(templates_dir)
        with open(f'{templates_dir}/gold_sigs.yaml', 'w') as f:
            f.write(
                'name: gold_sigs\ndescription: Specifications for submitting sig report to JAMO\nrequired_metadata_keys:\n- description: the Analysis Project ID\n  key: analysis_project_id\n  type: number\ntags:\n- sigs_report\noutputs:\n- label: sigs_report\n  description: A text report summarizing SIG fields\n  required: true\n  required_metadata_keys:\n  - macro: file_info\n  default_metadata_values:\n    file_format: text\n  tags:\n    - report')
        tag_templates_dir = f'{self.analysis.location}/tag_templates'
        os.makedirs(tag_templates_dir)
        with open(f'{tag_templates_dir}/fastq.yaml', 'w') as f:
            f.write(
                'description: a sequence file in fastq format\nrequired_metadata_keys:\n  - key: fastq_type\n    description: the type of fastq file, can be pooled\n    type: "list:str"')
        sample_calls = [
            call.save({'name': 'filtered_fastq_info', 'description': 'pre-defined metadata for filtered fastqs',
                       'required_metadata_keys': [
                           {'description': 'Filter product type', 'key': 'filter_product_type', 'type': 'string',
                            'required': True}], 'user': 'auto', 'group': 'users'}),
            call.save({'name': 'gold_sigs', 'description': 'Specifications for submitting sig report to JAMO',
                       'required_metadata_keys': [
                           {'description': 'the Analysis Project ID', 'key': 'analysis_project_id', 'type': 'number',
                            'required': True}], 'tags': ['sigs_report'],
                       'outputs': [
                           {'label': 'sigs_report', 'description': 'A text report summarizing SIG fields',
                            'required': True,
                            'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                            'default_metadata_values': {'file_format': 'text'}, 'tags': ['report']}],
                       'user': 'auto',
                       'group': 'users', 'md5': 'e71635e1f59af571b4248596321b3cfd',
                       'last_modified': datetime.datetime(2022, 1, 2, 0, 0)}),
            call.save({'description': 'a sequence file in fastq format', 'required_metadata_keys': [
                {'key': 'fastq_type', 'description': 'the type of fastq file, can be pooled', 'type': 'list:str',
                 'required': True}], 'user': 'auto', 'group': 'users', 'name': 'fastq',
                'md5': '22ca2b950281a3d8f83a8be16f28b0d0',
                       'last_modified': datetime.datetime(2022, 1, 2, 0, 0)}),
        ]

        self.analysis.onstartup()

        self.assertEqual(self.analysis.cv, {'output_tags': {'readme_file': ''},
                                            'release_to': {'img': 'the identifier for indicating that a release is '
                                                                  'releasable to IMG'},
                                            'tags': {'viral': ''}})
        for c in sample_calls:
            self.assertIn(c, self.db.mock_calls)
        self.assertIn(call.sendEmail('you@foobar.com', 'Template Change: gold_sigs',
                                     'The gold_sigs template has been changed in production.\n\nGo to http://address/to/foo/my_app/distributionproperties/gold_sigs to update Portal Display Location',
                                     fromAddress='me@foobar.com', replyTo=None), sdmlogger_mock.mock_calls)

    @patch('analysis.datetime')
    @patch('analysis.sdmlogger')
    def test_Analysis_post_reloadAll(self, sdmlogger_mock, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)
        cv_keys_dir = f'{self.analysis.location}/cv/keys'
        os.makedirs(cv_keys_dir)
        with open(f'{cv_keys_dir}/tags.yaml', 'w') as f:
            f.write('-\n value: viral\n description: ""')
        with open(f'{cv_keys_dir}/output_tags.yaml', 'w') as f:
            f.write('-\n value: readme_file\n description: ""')
        with open(f'{cv_keys_dir}/release_to.yaml', 'w') as f:
            f.write('-\n value: img\n description: the identifier for indicating that a release is releasable to IMG')
        macros_dir = f'{self.analysis.location}/macros'
        os.makedirs(macros_dir)
        with open(f'{macros_dir}/filtered_fastq_info.yaml', 'w') as f:
            f.write(
                'name: filtered_fastq_info\ndescription: pre-defined metadata for filtered fastqs\nrequired_metadata_keys:\n- description: Filter product type\n  key: filter_product_type\n  type: string')
        templates_dir = f'{self.analysis.location}/templates'
        os.makedirs(templates_dir)
        with open(f'{templates_dir}/gold_sigs.yaml', 'w') as f:
            f.write(
                'name: gold_sigs\ndescription: Specifications for submitting sig report to JAMO\nrequired_metadata_keys:\n- description: the Analysis Project ID\n  key: analysis_project_id\n  type: number\ntags:\n- sigs_report\noutputs:\n- label: sigs_report\n  description: A text report summarizing SIG fields\n  required: true\n  required_metadata_keys:\n  - macro: file_info\n  default_metadata_values:\n    file_format: text\n  tags:\n    - report')
        tag_templates_dir = f'{self.analysis.location}/tag_templates'
        os.makedirs(tag_templates_dir)
        with open(f'{tag_templates_dir}/fastq.yaml', 'w') as f:
            f.write(
                'description: a sequence file in fastq format\nrequired_metadata_keys:\n  - key: fastq_type\n    description: the type of fastq file, can be pooled\n    type: "list:str"')
        sample_calls = [
            call.save({'name': 'filtered_fastq_info', 'description': 'pre-defined metadata for filtered fastqs',
                       'required_metadata_keys': [
                           {'description': 'Filter product type', 'key': 'filter_product_type', 'type': 'string',
                            'required': True}], 'user': 'auto', 'group': 'users'}),
            call.save({'name': 'gold_sigs', 'description': 'Specifications for submitting sig report to JAMO',
                       'required_metadata_keys': [
                           {'description': 'the Analysis Project ID', 'key': 'analysis_project_id', 'type': 'number',
                            'required': True}], 'tags': ['sigs_report'],
                       'outputs': [
                           {'label': 'sigs_report', 'description': 'A text report summarizing SIG fields',
                            'required': True,
                            'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                            'default_metadata_values': {'file_format': 'text'}, 'tags': ['report']}],
                       'user': 'auto',
                       'group': 'users', 'md5': 'e71635e1f59af571b4248596321b3cfd',
                       'last_modified': datetime.datetime(2022, 1, 2, 0, 0)}),
            call.save({'description': 'a sequence file in fastq format', 'required_metadata_keys': [
                {'key': 'fastq_type', 'description': 'the type of fastq file, can be pooled', 'type': 'list:str',
                 'required': True}], 'user': 'auto', 'group': 'users', 'name': 'fastq',
                'md5': '22ca2b950281a3d8f83a8be16f28b0d0', 'last_modified': datetime.datetime(2022, 1, 2, 0, 0)}),
        ]

        self.analysis.post_reloadAll(None, None)

        self.assertEqual(self.analysis.cv, {'output_tags': {'readme_file': ''},
                                            'release_to': {'img': 'the identifier for indicating that a release is '
                                                                  'releasable to IMG'},
                                            'tags': {'viral': ''}})
        for c in sample_calls:
            self.assertIn(c, self.db.mock_calls)
        self.assertIn(call.sendEmail('you@foobar.com', 'Template Change: gold_sigs',
                                     'The gold_sigs template has been changed in production.\n\nGo to http://address/to/foo/my_app/distributionproperties/gold_sigs to update Portal Display Location',
                                     fromAddress='me@foobar.com', replyTo=None), sdmlogger_mock.mock_calls)

    def test_Analysis_getAliasOn(self):
        self.core.getSetting.return_value = 500

        self.assertEqual(self.analysis.getAliasOn(), 500)
        self.assertIn(call.getSetting('my_app', 'alias', 1000), self.core.mock_calls)

    def test_Analysis_getNextAlias(self):
        self.core.getSetting.return_value = 500

        self.assertEqual(self.analysis.getNextAlias(), 501)
        self.assertIn(call.saveSetting('my_app', 'alias', 501), self.core.mock_calls)

    @patch('analysis.datetime')
    def test_Analysis_post_analysis(self, datetime_mock):
        kwargs = {
            '__auth': {'user': 'foobar'},
            'key': 'some_key',
            'location': '/path/to/location',
        }
        self.cursor.__iter__.return_value = iter([])
        self.db.count_documents.return_value = 0
        self.db.save.return_value = '52a03153f28749549db94c10'
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.post_analysis(None, kwargs),
                         {'analysis_id': '52a03153f28749549db94c10', 'location': '/path/to/location'})

    def test_Analysis_get_myanalyses(self):
        kwargs = {
            '__auth': {'user': 'foobar'},
            '_id': '52a03153f28749549db94c10',
        }
        records = [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertEqual(self.analysis.get_myanalyses(None, kwargs),
                         [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}])
        self.assertIn(call.find({'_id': ObjectId('52a03153f28749549db94c10'), 'user': 'foobar'}), self.db.mock_calls)

    def test_Analysis_get_analyses(self):
        kwargs = {
            '_id': '52a03153f28749549db94c10',
        }
        records = [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertEqual(self.analysis.get_analyses(None, kwargs), [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}])
        self.assertIn(call.find({'_id': ObjectId('52a03153f28749549db94c10')}), self.db.mock_calls)

    @patch('lapinpy.mongorestful.random')
    def test_Analysis_get_ranalyses(self, random_mock):
        records = [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)
        random_mock.choice.return_value = 'A'

        self.assertEqual(self.analysis.get_ranalyses(None, None),
                         {'record_count': 1, 'end': 1, 'records': [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}],
                          'fields': None, 'start': 1, 'timeout': 540, 'cursor_id': 'AAAAAAAAAA'})

    def test_Analysis_post_deletereviewanalysis(self):
        self.analysis.post_deletereviewanalysis(None, {'user': 'foo', 'key': 'some_key'})

        self.assertIn(call.remove({'options.reviewer': 'foo@lbl.gov', 'key': 'some_key'}), self.db.mock_calls)

    @patch('analysis.restful.RestServer')
    @patch('analysis.datetime')
    def test_Analysis_post_releaseanalysis(self, datetime_mock, restserver):
        kwargs = {
            'key': 'some_key',
            'user': 'foo',
            'group': 'sdm',
            'outputs': [
                {'metadata': {'file_format': 'txt'}, 'file': '/path/to/my_file.txt', 'label': 'my_label',
                 'metadata_id': '52a03153f28749549db94c14'}
            ],
            'location': '/path/to',
        }
        self.db.find_one.return_value = {
            'options': {
                'skip_folder': False,
                'reviewer': 'foo@lbl.gov',
                'send_email': True,
                'email': {'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                          'reply_to': 'reply@foo.com',
                          'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                          'subject': 'My subject',
                          'attachments': ['my_label']}
            },
            'template': 'my_template',
            'key': 'key-1000',
            'outputs': [{
                'metadata': {'foo': 'foo1'},
                'label': 'my_label',
                'file': '/path/to/my_file',
                'tags': 'fastq',
            }],
            'metadata': {'bar': 'bar1', 'portal': {'display_location': ['Raw Data']}, 'jat_key': 'some_jat_key'},
            'inputs': ['62680104f21e5a14d08d8366'],
            'location': '/path/to/my_file.txt',
        }
        records = [
            {'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm', 'email': {'to': ['foobar2@foo.com']}}]
        self.cursor.__iter__.return_value = iter(records)
        server = Mock()
        server.run_method.side_effect = [
            {'metadata_id': '52a03153f28749549db94c11'},
            {'metadata_id': '52a03153f28749549db94c12'},
        ]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.post_releaseanalysis(None, kwargs)

        for c in [call.insert(
                {'content': {'files': ['52a03153f28749549db94c14'], 'strings': u'\nHello world\nBye world'},
                 'to': ['foobar1@foo.com'], 'from': 'me@foobar.com', 'attachments': ['52a03153f28749549db94c14'],
                 'key': 'some_key', 'email_status': 'pending', 'cc': ['cc@foo.com'], 'mime': 'plain',
                 'bcc': ['bcc@foo.com'], 'subject': u'My subject'}),
            call.save({'status': 'Released', 'inputs': ['62680104f21e5a14d08d8366'], 'group': 'sdm',
                       'template': 'my_template', 'modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                       'outputs': [
                           {'tags': 'fastq', 'label': 'my_label', 'metadata_id': '52a03153f28749549db94c11',
                            'file': '/path/to/my_file', 'metadata': {'foo': 'foo1'}}], 'user': 'foo',
                       'added_date': datetime.datetime(2022, 1, 2, 0, 0), 'location': '/path/to/my_file.txt',
                       'key': 'key-1000', 'metadata_id': '52a03153f28749549db94c12',
                       'options': {'send_email': True, 'skip_folder': False},
                       'metadata': {'portal': {'display_location': [u'Raw Data']}, 'bar': 'bar1'}})]:
            self.assertIn(c, self.db.mock_calls)

    def test_Analysis_get_review(self):
        self.db.find_one.return_value = {'options': {'reviewer': 'foo@lbl.gov'}}

        self.assertEqual(self.analysis.get_review(['foobar'], {'user': 'foo'}),
                         {'options': {'reviewer': 'foo@lbl.gov'}})

    @parameterized.expand([
        ('files', ['jat-key', 'files'], [{'file_name': 'my_file.txt',
                                          'file_path': '/path/to/',
                                          'file_status': 'PURGED',
                                          'file_type': 'txt',
                                          'label': 'my_label',
                                          'metadata_id': '52a03153f28749549db94c12'}]),
        ('no_files', ['jat-key'], {'_id': '52a03153f28749549db94c10',
                                   'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c12'}]}),
    ])
    @patch('analysis.restful.RestServer')
    def test_Analysis_get_analysis(self, _description, args, expected, restserver):
        records = [{'_id': '52a03153f28749549db94c10',
                    'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c12'}]}]
        self.cursor.__iter__.return_value = iter(records)
        server = Mock()
        server.run_method.return_value = [
            {'file_name': 'my_file.txt', 'file_path': '/path/to/', 'file_status': 'PURGED',
             'file_type': 'txt',
             '_id': '52a03153f28749549db94c12'}]
        restserver.Instance.return_value = server

        self.assertEqual(self.analysis.get_analysis(args, None), expected)

    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_put_metadata2(self, datetime_mock):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.return_value = iter(records)
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.put_metadata2(['my_key'], {'foo': 'foo1', 'bar': 'bar1'})

        self.assertIn(call.update({'_id': {'$in': [ObjectId('52a03153f28749549db94c10')]}}, {
            '$set': {'modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'metadata.foo': 'foo1',
                     'metadata.bar': 'bar1'}}, multi=True), self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_put_analysis(self, datetime_mock):
        self.cursor.__iter__.return_value = iter([{'_id': '52a03153f28749549db94c10'}])
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.put_analysis(['52a03153f28749549db94c10'], {'__auth': {'user': 'foo'}, 'foo': 'bar'})

        self.assertIn(call.update({'_id': {'$in': [ObjectId('52a03153f28749549db94c10')]}},
                                  {'$set': {'modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'foo': 'bar'}},
                                  multi=True), self.db.mock_calls)

    @parameterized.expand([
        ('user', 'foo', 'foo'),
        ('group', 'bar', 'sdm'),
    ])
    @patch('analysis.datetime')
    def test_Analysis_post_tagtemplate(self, _description, user, group, datetime_mock):
        records = [{'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm'}]
        self.cursor.__iter__.return_value = iter(records)
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.post_tagtemplate(None, {
            'name': 'foobar',
            'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc',
            'description': 'Some description',
            'default_metadata_values': {'foo': 'foo1', 'bar': 'bar1'},
            'required_metadata_keys': [{"macro": "file_info", "required": True}],
            'user': user,
            'group': group
        })

        self.assertIn(call.save({'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                 '_id': ObjectId('52a03153f28749549db94c10'), 'description': 'Some description',
                                 'default_metadata_values': {'foo': 'foo1', 'bar': 'bar1'},
                                 'last_modified': datetime.datetime(2022, 1, 2, 0, 0), 'user': user, 'group': group,
                                 'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc', 'name': 'foobar'}), self.db.mock_calls)

    def test_Analysis_post_tagtemplate_not_user_or_group_raises_exception(self):
        records = [{'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm'}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertRaises(common.HttpException, self.analysis.post_tagtemplate, None, {
            'name': 'foobar',
            'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc',
            'description': 'Some description',
            'default_metadata_values': {'foo': 'foo1', 'bar': 'bar1'},
            'required_metadata_keys': [{"macro": "file_info", "required": True}],
            'user': 'bar',
            'group': 'bar'
        })

    @parameterized.expand([
        ('update', 'foo', 'foo',
         [{'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm',
           'outputs': [{'label': 'length_histo'}, {'label': 'my_label'}]}],
         [call.save(
             {'_id': ObjectId('52a03153f28749549db94c10'), 'description': 'Some description', 'tags': ['my_tag'],
              'outputs': [{'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': True, 'label': 'length_histo'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'},
                           'required': True, 'label': 'length_plot'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': True, 'label': 'mirna_detected'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': False, 'label': 'mirna_expressed'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'},
                           'required': True, 'label': 'mirna_results'}],
              'last_modified': datetime.datetime(2022, 1, 2, 0, 0), 'user': 'foo', 'group': 'foo',
              'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc', 'name': 'foo'})]),
        ('update_group', 'bar', 'sdm',
         [{'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm',
           'outputs': [{'label': 'length_histo'}, {'label': 'my_label'}]}],
         [call.save(
             {'_id': ObjectId('52a03153f28749549db94c10'), 'description': 'Some description', 'tags': ['my_tag'],
              'outputs': [{'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': True, 'label': 'length_histo'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'},
                           'required': True, 'label': 'length_plot'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': True, 'label': 'mirna_detected'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                           'required': False, 'label': 'mirna_expressed'},
                          {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                           'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
                           'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'},
                           'required': True, 'label': 'mirna_results'}],
              'last_modified': datetime.datetime(2022, 1, 2, 0, 0), 'user': 'bar', 'group': 'sdm',
              'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc', 'name': 'foo'})]),
        ('new', 'foo', 'sdm', [],
         [call.save({'description': 'Some description', 'tags': ['my_tag'], 'outputs': [
             {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
              'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
              'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': True,
              'label': 'length_histo'}, {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                         'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                                         'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'},
                                         'required': True, 'label': 'length_plot'},
             {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
              'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
              'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': True,
              'label': 'mirna_detected'}, {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                           'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                                           'tags': ['report'], 'default_metadata_values': {'file_format': 'text'},
                                           'required': False, 'label': 'mirna_expressed'},
             {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
              'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
              'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'}, 'required': True,
              'label': 'mirna_results'}], 'last_modified': datetime.datetime(2022, 1, 2, 0, 0), 'user': 'foo',
             'group': 'sdm', 'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc', 'name': 'foo'})]
         ),
    ])
    @patch('analysis.datetime')
    @patch('analysis.sdmlogger')
    def test_Analysis_post_template(self, _description, user, group, records, expected_db_calls, sdmlogger_mock,
                                    datetime_mock):
        self.cursor.__iter__.return_value = iter(records)
        self.analysis.cv['tags'] = {'my_tag': {}}
        self.analysis.cv['outputs.tags'] = {'report': {}}
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.post_template(None, {
            'name': 'foo',
            'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc',
            'description': 'Some description',
            'tags': ['my_tag'],
            'outputs': [{'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': True,
                         'label': 'length_histo'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'}, 'required': True,
                         'label': 'length_plot'}, {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                                   'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
                                                   'tags': ['report'],
                                                   'default_metadata_values': {'file_format': 'text'}, 'required': True,
                                                   'label': 'mirna_detected'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': False,
                         'label': 'mirna_expressed'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'}, 'required': True,
                         'label': 'mirna_results'}],
            'user': user, 'group': group
        })

        for c in expected_db_calls:
            self.assertIn(c, self.db.mock_calls)
        self.assertIn(call.sendEmail('you@foobar.com', 'Template Change: foo',
                                     'The foo template has been changed in production.\n\nGo to http://address/to/foo/my_app/distributionproperties/foo to update Portal Display Location',
                                     fromAddress='me@foobar.com', replyTo=None), sdmlogger_mock.mock_calls)

    def test_Analysis_post_template_update_not_user_or_group_raises_exception(self):
        records = [{'_id': '52a03153f28749549db94c10', 'user': 'foo', 'group': 'sdm',
                    'outputs': [{'label': 'length_histo'}, {'label': 'my_label'}]}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertRaises(common.HttpException, self.analysis.post_template, None, {
            'name': 'foo',
            'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc',
            'description': 'Some description',
            'tags': ['my_tag'],
            'outputs': [{'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': True,
                         'label': 'length_histo'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'}, 'required': True,
                         'label': 'length_plot'}, {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                                   'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
                                                   'tags': ['report'],
                                                   'default_metadata_values': {'file_format': 'text'}, 'required': True,
                                                   'label': 'mirna_detected'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': False,
                         'label': 'mirna_expressed'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'}, 'required': True,
                         'label': 'mirna_results'}],
            'user': 'bar', 'group': 'bar'
        })

    def test_Analysis_update_distribution_properties(self):
        records_analysis_publishingflags = [{'outputs': {'foo': 'foo1', 'baz': 'baz1'}}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [
            iter(records_analysis_publishingflags),
            iter(records_analysis_plocations)
        ]
        self.analysis.__update_distribution_properties__({
            'name': 'my_template',
            'outputs': [{'label': 'foo'}, {'label': 'bar'}]
        })

        for c in [call.save({'foo': 'foo1'}), call.save({'bar': 'bar1'})]:
            self.assertIn(c, self.db.mock_calls)

    @patch('analysis.sdmlogger')
    def test_Analysis_notify_template_change(self, sdmlogger_mock):
        self.analysis.__notify_template_change__({
            'name': 'foo',
            'email': {
                'reply_to': 'reply_to@foobar.com'
            }
        })

        self.assertIn(call.sendEmail('you@foobar.com', 'Template Change: foo',
                                     'The foo template has been changed in production.\n\nGo to http://address/to/foo/my_app/distributionproperties/foo to update Portal Display Location',
                                     fromAddress='me@foobar.com', replyTo='reply_to@foobar.com'),
                      sdmlogger_mock.mock_calls)

    def test_Analysis_post_validatetemplate(self):
        self.analysis.cv['tags'] = {'my_tag_1': {}}
        self.analysis.cv['outputs.tags'] = {'report': {}}
        kwargs = {
            'name': 'foo',
            'md5': '7c6ecf91dd9fb9c52eba5f68911dc5bc',
            'description': 'Some description',
            'tags': ['my_tag_1', 'my_tag_2'],
            'outputs': [{'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_histogram.txt file containing read length histogram for library AAAA',
                         'tags': ['deport'], 'default_metadata_values': {'file_format': 'text'}, 'required': True,
                         'label': 'length_histo'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_read_length_distribution.pdf file containing plot of read length distribution for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'pdf'}, 'required': True,
                         'label': 'length_plot'}, {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                                   'description': 'A AAAA_miRNAs_detected.csv file containing known/novel miRNAs detected for library AAAA',
                                                   'tags': ['report'],
                                                   'default_metadata_values': {'file_format': 'text'}, 'required': True,
                                                   'label': 'mirna_detected'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_miRNAs_expressed.csv file containing known miRNAs expressed for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'text'}, 'required': False,
                         'label': 'mirna_expressed'},
                        {'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                         'description': 'A AAAA_results.tar.gz file containing smRNA analysis results for library AAAA',
                         'tags': ['report'], 'default_metadata_values': {'file_format': 'gzipped'}, 'required': True,
                         'label': 'mirna_results'}],
            'user': 'foo',
        }
        self.assertEqual(["Analysis level tag: 'my_tag_2' is not valid",
                          "Output level tag: 'deport' on label: 'length_histo' is not valid"],
                         self.analysis.post_validatetemplate(None, kwargs))

    @parameterized.expand([
        ('queryResults',
         {'queryResults': True, 'query': {'_id': '52a03153f28749549db94c10'}, 'fields': ['foo', 'bar'], 'page': 1},
         {'data': [{'bar': 'bar1', 'foo': 'foo1'}], 'record_count': 1, 'return_count': 1}
         ),
        ('no_queryResults', {}, [{'bar': 'bar1', 'foo': 'foo1'}]
         )
    ])
    def test_Analysis_get_templates(self, _description, kwargs, expected):
        records = [{'foo': 'foo1', 'bar': 'bar1'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.analysis.get_templates(None, kwargs), expected)

    def test_Analysis_get_emails(self):
        records = [{'foo': 'foo1', 'bar': 'bar1'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.analysis.get_emails(None, {'foo': 'bar'}), [{'foo': 'foo1', 'bar': 'bar1'}])
        self.assertIn(call.find({'foo': 'bar'}), self.db.mock_calls)

    def test_Analysis_post_emailstatus(self):
        self.analysis.post_emailstatus(None, {
            'what': {'_id': '52a03153f28749549db94c10'},
            'data': {'foo': 'bar'}
        })

        self.assertIn(call.update({'_id': ObjectId('52a03153f28749549db94c10')}, {'$set': {'foo': 'bar'}}, multi=True),
                      self.db.mock_calls)

    @parameterized.expand([
        ('has_template', [{'foo': 'bar', 'user': 'foobar', 'group': 'sdm', '_id': '52a03153f28749549db94c10'}],
         {'foo': 'bar'}),
        ('no_template', [], {'foo': {'bar': 1}},
         call.get('berkeleylab/jgi-jat/src/templates/templates/my_template.yaml', output='raw'))
    ])
    def test_Analysis_get_template(self, _description, records, expected, expected_curl_call=None):
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)
        self.curl.get.return_value = 'foo:\n  bar: 1'

        self.assertEqual(self.analysis.get_template(['templates/my_template'], None), expected)
        if expected_curl_call:
            self.assertIn(expected_curl_call, self.curl.mock_calls)

    @parameterized.expand([
        ('macro_count_gt_1', [{'name': 'foo'}, {'name': 'bar'}],
         {'defined_in': 'macro', 'key': 'some_key', 'location': 'output', 'templates': ['foo_template']}),
        ('macro_count_eq_1', [{'name': 'foo'}],
         {'defined_in': 'macro', 'key': 'some_key', 'location': 'output', 'templates': ['foo_template']}),
        ('no_macro', [],
         {'defined_in': 'template', 'key': 'some_key', 'location': 'output', 'templates': ['foo_template']}),
    ])
    def test_Analysis_get_keylocations(self, _description, records_macro, expected):
        records_templates_1 = []
        records_templates_2 = [{'name': 'foo_template'}]
        self.cursor.__iter__.side_effect = [iter(records_macro), iter(records_templates_1), iter(records_templates_2)]
        self.db.count_documents.side_effect = [len(records_macro), len(records_templates_1), len(records_templates_2)]

        self.assertEqual(self.analysis.get_keylocations(['some_key'], None), expected)

    def test_Analysis_get_resolvedtemplate(self):
        records_analysis_templates = [{'foo': 'foobar',
                                       'required_metadata_keys': ['foo',
                                                                  {'macro': 'bar'},
                                                                  {'baz': 'baz1'}],
                                       'outputs': [
                                           {
                                               'required_metadata_keys': ['blah'],
                                               'label': 'foo',
                                           },
                                           {
                                               'label': 'my_label',
                                               'default_metadata_values': {'publish_to': 'foo_publish'}
                                           }]
                                       }]
        records_analysis_publishingflags = [{'outputs': {'foo': 'bar'}, 'template_flags': 'foo_flags'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_templates), iter(records_analysis_publishingflags)]
        self.db.count_documents.side_effect = [len(records_analysis_templates), len(records_analysis_publishingflags)]
        self.analysis.macros = {'foo': {'required_metadata_keys': ['foo1']},
                                'bar': {'required_metadata_keys': ['bar1']},
                                'blah': {'required_metadata_keys': ['blah1']}}

        self.assertEqual(self.analysis.get_resolvedtemplate(['my_namespace/some_key'], None),
                         {'foo': 'foobar',
                          'outputs': [{'default_metadata_values': {'publish_to': 'bar'},
                                       'label': 'foo',
                                       'required_metadata_keys': ['blah1']},
                                      {'default_metadata_values': {}, 'label': 'my_label'}],
                          'required_metadata_keys': ['foo1', 'bar1', {'baz': 'baz1'}]}
                         )

    def test_Analysis_post_output(self):
        self.analysis.post_output(['52a03153f28749549db94c10'], {
            '__auth': {'user': 'foobar'},
            'foo': 'bar',
            'file': 'my_file.txt',
            'file_type': 'txt',
        })

        self.assertIn(call.find({'_id': ObjectId('52a03153f28749549db94c10'), 'user': 'foobar'}), self.db.mock_calls)

    def test_Analysis_post_releaseto(self):
        records = [{'group': 'sdm', 'options': {'release_to': ['bar']}}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertEqual(self.analysis.post_releaseto([], {'key': 'some_key', 'group': 'sdm_2',
                                                           'permissions': ['admin'], 'set': ['foo'], 'unset': ['bar']}),
                         {'Status': 'OK. Successfully set the release_to flags.'})
        self.assertIn(call.save({'group': 'sdm', 'options': {'release_to': ['foo']}}), self.db.mock_calls)

    @parameterized.expand([
        ('admin', 'admin', 'admin', ['admin']),
        ('user', 'bar', 'bar', []),
        ('group', 'foo', 'sdm', []),
    ])
    @patch('lapinpy.mongorestful.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_put_import(self, _description, user, group, permissions, restserver, datetime_mock):
        kwargs = {
            'user': user,
            'group': group,
            'permissions': permissions,
            'metadata_id': '52a03153f28749549db94c12',
            'template': 'my_template',
            'publish': True,
            'metadata': {'bar': 'bar1', 'baz': 'baz1', 'portal': {'display_location': ['Raw Data']}},
            'inputs': ['/foo/bar', 'baz'],
            'send_email': True,
            'release_to': ['foobar'],
            'email': {'to': ['foobar2@foo.com']},
            'outputs': [
                {'label': 'my_label', 'file': '/path/to/my_file.txt'}
            ],
        }

        records_analysis = [
            {'_id': '52a03153f28749549db94c10', 'user': 'bar', 'group': 'sdm',
             'metadata_id': '52a03153f28749549db94c12', 'template': 'my_template', 'metadata': {'foo': 'foo1'},
             'inputs': ['baz'], 'options': {}, 'location': '/path/to',
             'outputs': [
                 {'file': '/path/to/my_file.txt',
                  'label': 'my_label',
                  'metadata': {},
                  'metadata_id': '52a03153f28749549db94c13'},
             ]}]
        records_analysis_2 = [
            {'_id': '52a03153f28749549db94c10', 'user': 'bar', 'group': 'sdm',
             'metadata_id': '52a03153f28749549db94c12', 'template': 'my_template', 'metadata': {'foo': 'foo2'},
             'inputs': ['baz'], 'options': {}, 'location': '/path/to',
             'outputs': [
                 {'file': '/path/to/my_file.txt',
                  'label': 'my_label',
                  'metadata': {},
                  'metadata_id': '52a03153f28749549db94c13'},
             ]}]
        records_analysis_template = [{'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}, {'key': 'portal'}],
                                      'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo'}]}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c13'}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_file = [{'_id': '52a03153f28749549db94c14',
                         'metadata': {'template_name': 'my_template'}}]
        records_file_2 = [{'_id': '52a03153f28749549db94c14',
                           'metadata': {'template_name': 'my_template'}, 'foo': 'foo2'}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis),
                                            iter(records_analysis_template),
                                            iter(records_analysis_publishingflags),
                                            iter(records_file),
                                            iter(records_analysis_template),
                                            iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations),
                                            iter(records_file_2),
                                            iter(records_analysis_2)]
        server = Mock()
        server.run_method.side_effect = [{'metadata_id': '52a03153f28749549db94c11'},
                                         {'_id': '52a03153f28749549db94c14'},
                                         [{'_id': '52a03153f28749549db94c14'}],
                                         None,
                                         None,
                                         None,
                                         None]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.put_import(['my_key'], kwargs),
                         {'warnings': ["warning: Metadata key 'baz' not found in analysis template",
                                       "warning: Metadata key 'baz' for output file 'my_file.txt' not found in analysis template",
                                       'warning: You have Metadata keys that are not defined in the template.  Processing will continue for now.  In a future version of jat, your update will be aborted.']}
                         )
        for c in [call.save({'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                             'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c14',
                             'metadata': {'template_name': 'my_template', 'jat_publish_flag': True}}),
                  call.save({'inputs': ['52a03153f28749549db94c11', 'baz'], 'outputs': [
                      {'metadata_id': '52a03153f28749549db94c13',
                       'metadata': {'baz': 'baz1', 'bar': 'bar1', 'portal': {'display_location': ['Raw Data']}},
                       'file': '/path/to/my_file.txt', 'label': 'my_label'}], 'user': 'bar', 'group': 'sdm',
                      'modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'publish': True,
                'location': '/path/to', 'template': 'my_template',
                            'metadata_id': '52a03153f28749549db94c12', '_id': '52a03153f28749549db94c10',
                            'options': {'send_email': True, 'release_to': ['foobar'],
                                        'email': {'to': ['foobar2@foo.com']}},
                            'metadata': {'bar': 'bar1', 'foo': 'foo1', 'baz': 'baz1',
                                         'portal': {'display_location': ['Raw Data']}}})]:
            self.assertIn(c, self.db.mock_calls)

    def test_Analysis_put_import_not_admin_or_user_or_group_raises_exception(self):
        kwargs = {
            'user': 'foo',
            'group': 'foo',
            'permissions': [],
            'metadata_id': '52a03153f28749549db94c12',
            'template': 'my_template',
            'publish': True,
            'metadata': {'bar': 'bar1', 'baz': 'baz1', 'portal': {'display_location': ['Raw Data']}},
            'inputs': ['/foo/bar', 'baz'],
            'send_email': True,
            'release_to': ['foobar'],
            'email': {'to': ['foobar2@foo.com']},
            'outputs': [
                {'label': 'my_label', 'file': '/path/to/my_file.txt'}
            ],
        }
        records_analysis = [
            {'_id': '52a03153f28749549db94c10', 'user': 'bar', 'group': 'sdm',
             'metadata_id': '52a03153f28749549db94c12', 'template': 'my_template', 'metadata': {'foo': 'foo1'},
             'inputs': ['baz'], 'options': {}, 'location': '/path/to',
             'outputs': [
                 {'file': '/path/to/my_file.txt',
                  'label': 'my_label',
                  'metadata': {},
                  'metadata_id': '52a03153f28749549db94c13'},
             ]}]
        self.cursor.__iter__.side_effect = [iter(records_analysis)]

        self.assertRaises(common.HttpException, self.analysis.put_import, ['my_key'], kwargs)

    @patch('lapinpy.mongorestful.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_post_analysisupdate(self, restserver, datetime_mock):
        records_analysis = [
            {'_id': '52a03153f28749549db94c10', 'user': 'bar', 'group': 'sdm',
             'metadata_id': '52a03153f28749549db94c12', 'template': 'my_template',
             'metadata': {'foo': 'foo1'}, 'inputs': ['baz'], 'options': {}, 'location': '/path/to',
             'outputs': [
                 {'file': '/path/to/my_file.txt',
                  'label': 'my_label',
                  'metadata': {},
                  'metadata_id': '52a03153f28749549db94c13'},
             ]}]
        records_analysis_2 = [
            {'_id': '52a03153f28749549db94c10', 'user': 'bar', 'group': 'sdm',
             'metadata_id': '52a03153f28749549db94c12', 'template': 'my_template', 'metadata': {'foo': 'foo2'},
             'inputs': ['baz'], 'options': {}, 'location': '/path/to',
             'outputs': [
                 {'file': '/path/to/my_file.txt',
                  'label': 'my_label',
                  'metadata': {},
                  'metadata_id': '52a03153f28749549db94c13'},
             ]}]
        records_analysis_template = [{'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}, {'key': 'portal'}],
                                      'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo'}]}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c13'}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_file = [{'_id': '52a03153f28749549db94c14',
                         'metadata': {'template_name': 'my_template'}}]
        records_file_2 = [{'_id': '52a03153f28749549db94c14',
                           'metadata': {'template_name': 'my_template'}, 'foo': 'foo2'}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis),
                                            iter(records_analysis_template),
                                            iter(records_analysis_publishingflags),
                                            iter(records_file),
                                            iter(records_analysis_template),
                                            iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations),
                                            iter(records_file_2),
                                            iter(records_analysis_2)]
        server = Mock()
        server.run_method.side_effect = [[{'_id': '52a03153f28749549db94c14'}],
                                         None,
                                         None,
                                         None,
                                         None]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.post_analysisupdate(None, {
            'jat_key': 'jat_key-1',
            'template_data': {'outputs': [
                {'label': 'my_label', 'file': '/path/to/my_file.txt',
                 'metadata': {'bar': 'bar1', 'portal': {'display_location': ['Raw Data']}},
                 }],
                'metadata': {'bar': 'bar1'}},
            'user': 'foo',
            'group': 'sdm',
            'permissions': ['admin'],
        }), {})
        for c in [call.save({'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c14', 'metadata': {'template_name': 'my_template', 'jat_publish_flag': True}}),
                  call.save({'inputs': [], 'outputs': [{'metadata_id': '52a03153f28749549db94c13', 'metadata': {'bar': 'bar1', 'portal': {'display_location': ['Raw Data']}}, 'file': '/path/to/my_file.txt', 'label': 'my_label'}], 'user': 'bar', 'group': 'sdm', 'modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'publish': True, 'location': '/path/to', 'template': 'my_template', 'metadata_id': '52a03153f28749549db94c12', '_id': '52a03153f28749549db94c10', 'options': {}, 'metadata': {'foo': 'foo1', 'bar': 'bar1'}})]:
            self.assertIn(c, self.db.mock_calls)

    def test_Analysis_get_distinct(self):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.db.distinct.return_value = records

        self.assertEqual(self.analysis.get_distinct(['52a03153f28749549db94c10'], None),
                         [{'_id': '52a03153f28749549db94c10'}])
        self.assertIn(call.distinct('52a03153f28749549db94c10', filter={'52a03153f28749549db94c10': {'$ne': None}}), self.db.mock_calls)

    @parameterized.expand([
        ('publish', True,
         call.save({'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c10', 'metadata': {'template_name': 'my_template', 'jat_publish_flag': True}})),
        ('unpublish', False,
         call.save({'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c10', 'metadata': {'template_name': 'my_template', 'jat_publish_flag': False}})),
    ])
    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_update_publish(self, _description, publish, expected_db_call, datetime_mock):
        key = 'jat_key-1'
        keep_defined = True
        user = 'foo'

        records_file = [{'_id': '52a03153f28749549db94c10',
                         'metadata': {'template_name': 'my_template'}}]
        records_file_2 = [{'_id': '52a03153f28749549db94c10',
                           'metadata': {'template_name': 'my_template2'}}]
        records_templates = [
            {'outputs': [{'label': 'my_label'}, {'label': 'my_label2'}], 'user': 'foobar', 'group': 'sdm',
             '_id': '52a03153f28749549db94c10'}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_file), iter(records_templates),
                                            iter(records_analysis_publishingflags), iter(records_analysis_plocations),
                                            iter(records_file_2)]
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.update_publish(key, publish, keep_defined, user)

        self.assertIn(expected_db_call, self.db.mock_calls)

    @parameterized.expand([
        ('metadata_id_found', '/path/to/my_file.txt', '52a03153f28749549db94c10'),
        ('metadata_id_not_found', '/path/to/my_file_2.txt', None),
    ])
    def test_Analysis_get_metadata_id_for_file(self, _description, path, expected):
        outputs = [{'file': 'my_file.txt', 'metadata_id': '52a03153f28749549db94c10'}]
        base = '/path/to'

        self.assertEqual(self.analysis._get_metadata_id_for_file(outputs, path, base),
                         expected)

    @patch('analysis.restful.RestServer')
    def test_Analysis_importAnalysis_with_reviewer(self, restserver):
        server = Mock()
        server.run_method.side_effect = [{'_id': '52a03153f28749549db94c10'},
                                         {'_id': '52a03153f28749549db94c11', 'metadata_id': '52a03153f28749549db94c11'},
                                         {'_id': '52a03153f28749549db94c12', 'metadata_id': '52a03153f28749549db94c12'},
                                         ]
        restserver.Instance.return_value = server
        records_analysis_template = [{'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}, {'key': 'portal'}],
                                      'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo'}]}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c13'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_template)]
        self.core.getSetting.return_value = 1
        self.db.save.return_value = '52a03153f28749549db94c13'

        self.assertEqual(self.analysis.import_analysis({
            'skip_folder': False, 'location': '/path/to',
            'options': {'reviewer': 'reviewer_foo', 'publish': True},
            'outputs': [{'metadata': {'publish_to': 'publish_foo', 'file_format': 'txt'}, 'file': 'my_file.txt',
                         'label': 'my_label'}],
            'inputs': ['52a03153f28749549db94c10', 12345, 'foo'],
            'template': 'my_template',
            'metadata': {},
            'division': 'jgi',
        }), {'analysis_id': '52a03153f28749549db94c13',
             'jat_key': 'AUTO-2',
             'location': '/path/to'})
        for c in [call.insert({'content': 'You can review the release here: http://address/to/foo/my_app/review/AUTO-2',
                               'to': 'reviewer_foo', 'from': 'sdm@localhost', 'attachments': [], 'key': 'AUTO-2',
                               'email_status': 'pending', 'cc': [], 'mime': 'plain', 'bcc': [],
                               'subject': 'A release is available for you to review'}),
                  call.save({'status': 'Under Review',
                             'inputs': ['52a03153f28749549db94c10', '52a03153f28749549db94c11',
                                        '52a03153f28749549db94c12'], 'key': 'AUTO-2',
                             'outputs': [
                                 {'label': 'my_label', 'file': '/path/to/my_file.txt',
                                  'metadata': {'publish_to': 'publish_foo', 'file_format': 'txt'}}], 'publish': True,
                             'location': '/path/to', 'template': 'my_template',
                             'metadata': {'template_name': 'my_template', 'jat_key': 'AUTO-2'},
                             'options': {'reviewer': 'reviewer_foo'}, 'skip_folder': False,
                             'division': 'jgi'})]:
            self.assertIn(c, self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    @patch('analysis.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_importAnalysis_without_reviewer(self, restserver, datetime_mock_analysis, datetime_mock_lapinpy):
        server = Mock()
        server.run_method.side_effect = [{'_id': '52a03153f28749549db94c10'},
                                         {'_id': '52a03153f28749549db94c11', 'metadata_id': '52a03153f28749549db94c11'},
                                         {'_id': '52a03153f28749549db94c12', 'metadata_id': '52a03153f28749549db94c12'},
                                         {'_id': '52a03153f28749549db94c14', 'metadata_id': '52a03153f28749549db94c14'},
                                         {'_id': '52a03153f28749549db94c16', 'metadata_id': '52a03153f28749549db94c16'},
                                         {'foo': 'foo1', 'bar': 'bar1'}
                                         ]
        restserver.Instance.return_value = server
        records_analysis_template = [{'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}, {'key': 'portal'}],
                                      'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo'}],
                                                   'file': 'my_file.txt'}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
                                      'email': {
                                          'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                                          'reply_to': 'reply@foo.com',
                                          'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                                          'subject': 'My subject',
                                          'attachments': ['my_label']}}]
        records_analysis_template_2 = [{'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}, {'key': 'portal'}],
                                        'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo'}],
                                                     'file': 'my_file.txt'}],
                                        'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
                                        'email': {
                                            'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                                            'reply_to': 'reply@foo.com',
                                            'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                                            'subject': 'My subject',
                                            'attachments': ['my_label']}}]
        records_file = [{'_id': '52a03153f28749549db94c16',
                         'metadata': {'template_name': 'my_template'}}]
        records_file_2 = [{'_id': '52a03153f28749549db94c16',
                           'metadata': {'template_name': 'my_template2'}}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_template), iter(records_file),
                                            iter(records_analysis_template_2), iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations), iter(records_file_2)]
        self.core.getSetting.return_value = 1
        self.db.save.return_value = '52a03153f28749549db94c13'
        datetime_mock_analysis.datetime.now.return_value = datetime.datetime(2022, 1, 2)
        datetime_mock_lapinpy.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.import_analysis({
            'skip_folder': False, 'location': '/path/to',
            'options': {'publish': True, 'send_email': True, 'email': {'reply_to': 'reply@foo.com'}},
            'outputs': [{'metadata': {'publish_to': 'publish_foo', 'file_format': 'txt',
                                      'portal': {'display_location': ['Raw Data']}},
                         'file': 'my_file.txt',
                         'label': 'my_label',
                         'tags': ['my_tag']}],
            'inputs': ['52a03153f28749549db94c10', 12345, 'foo'],
            'template': 'my_template',
            'metadata': {},
            'user': 'foo',
            'group': 'sdm',
            'division': 'jgi',
        }), {'analysis_id': '52a03153f28749549db94c13',
             'jat_key': 'AUTO-2',
             'location': '/path/to'})
        for c in [call.save({'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                             'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c16',
                             'metadata': {'template_name': 'my_template', 'jat_publish_flag': True}}),
                  call.save({'status': 'Released', 'inputs': ['52a03153f28749549db94c10', '52a03153f28749549db94c11',
                                                              '52a03153f28749549db94c12'], 'outputs': [
                      {'tags': ['my_tag'], 'label': 'my_label', 'metadata_id': '52a03153f28749549db94c14',
                       'file': '/path/to/my_file.txt',
                       'metadata': {'portal': {'display_location': [u'Raw Data']}, 'publish_to': 'publish_foo',
                                    'file_format': 'txt'}}], 'added_date': datetime.datetime(2022, 1, 2, 0, 0),
                      'user': 'foo', 'key': 'AUTO-2', 'modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                      'group': 'sdm', 'publish': True,
                      'email': {'attachments': ['my_label'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                                'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                                'to': ['foobar1@foo.com'], 'reply_to': 'reply@foo.com', 'subject': 'My subject'},
                      'location': '/path/to', 'template': 'my_template',
                      'metadata_id': '52a03153f28749549db94c16', 'skip_folder': False,
                      'options': {'send_email': True},
                      'metadata': {'template_name': 'my_template', 'foo': 'foo1', 'bar': 'bar1'},
                      'division': 'jgi'})]:
            self.assertIn(c, self.db.mock_calls)

    @patch('analysis.restful.RestServer')
    def test_Analysis_post_analysisimport(self, restserver):
        records_analysis_template = [
            {'required_metadata_keys': [{'key': 'foo', 'type': 'string', 'required': False},
                                        {'key': 'bar', 'type': 'string', 'required': False},
                                        {'key': 'portal', 'type': 'string', 'required': False}],
             'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],

                          'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},

                          'tags': ['my_tag'], 'file': 'my_file.txt'}],
             'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
             'email': {
                 'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                 'reply_to': 'reply@foo.com',
                 'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                 'subject': 'My subject',
                 'attachments': ['my_label']}}]
        records_analysis_template_2 = [
            {'required_metadata_keys': [{'key': 'foo', 'type': 'string', 'required': False},
                                        {'key': 'bar', 'type': 'string', 'required': False},
                                        {'key': 'portal', 'type': 'string', 'required': False}],
             'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],

                          'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},

                          'tags': ['my_tag'], 'file': 'my_file.txt'}],
             'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
             'email': {
                 'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                 'reply_to': 'reply@foo.com',
                 'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                 'subject': 'My subject',
                 'attachments': ['my_label']}}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_file = [{'_id': '52a03153f28749549db94c16',
                         'metadata': {'template_name': 'my_template'}}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_template), iter(records_analysis_publishingflags),
                                            iter(records_analysis_template), iter(records_file),
                                            iter(records_analysis_template_2), iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations), iter(records_file)]
        self.core.getSetting.return_value = 1
        server = Mock()
        server.run_method.side_effect = [{'_id': '52a03153f28749549db94c10', 'metadata_id': '52a03153f28749549db94c10'},
                                         {'_id': '52a03153f28749549db94c11', 'metadata_id': '52a03153f28749549db94c11'},
                                         {'_id': '52a03153f28749549db94c12', 'metadata_id': '52a03153f28749549db94c12'},
                                         ]
        restserver.Instance.return_value = server
        self.db.save.return_value = '52a03153f28749549db94c17'

        self.assertEqual(self.analysis.post_analysisimport(None, {
            'template_name': 'my_template',
            'template_data': {'outputs': [{'label': 'my_label', 'tags': ['my_tag'], 'file': 'my_file.txt',
                                           'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},
                                           }]},
            'location': '/path/to/my_folder',
            'skip_folder': False,
            'user': 'foo',
            'group': 'sdm',
            'division': 'jgi',
            'metadata': {'foo': 'foo1'},
            'source': 'my_source',
        }), {'analysis_id': '52a03153f28749549db94c17',
             'jat_key': 'AUTO-2',
             'location': '/path/to/my_folder',
             'warnings': []})
        for c in [call.run_method('metadata', 'post_file',
                                  metadata={'template_name': 'my_template', 'jat_key': 'AUTO-2', 'foo': 'foo1',
                                            'bar': 'bar1', 'portal': 'my_portal', 'jat_label': 'my_label',
                                            'jat_publish_flag': True}, destination='analyses/AUTO-2/',
                                  file='/path/to/my_folder/my_file.txt', file_type=['my_tag'],
                                  __auth={'user': 'foo', 'group': 'sdm', 'division': 'jgi'}, inputs=[],
                                  source='my_source'),
                  call.run_method('metadata', 'post_folder',
                                  metadata={'template_name': 'my_template', '_id': '52a03153f28749549db94c12',
                                            'metadata_id': '52a03153f28749549db94c12'},
                                  destination='analyses/AUTO-2/my_folder.AUTO-2', local_purge_days=2,
                                  ignore=['my_file.txt'], folder='/path/to/my_folder', file_type='analysis',
                                  __auth={'user': 'foo', 'group': 'sdm', 'division': 'jgi'}, source='my_source')]:
            self.assertIn(c, server.mock_calls)

    @parameterized.expand([
        ('require_datacenter_source', True),
        ('warning_datacenter_source', False),
    ])
    @patch('analysis.restful.RestServer')
    def test_Analysis_post_analysisimport_missing_source_parameter(self, _description, require_datacenter_source,
                                                                   restserver_mock):
        self.analysis.config.require_datacenter_source = require_datacenter_source
        records_analysis_template = [
            {'required_metadata_keys': [{'key': 'foo', 'type': 'string', 'required': False},
                                        {'key': 'bar', 'type': 'string', 'required': False},
                                        {'key': 'portal', 'type': 'string', 'required': False}],
             'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],

                          'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},

                          'tags': ['my_tag'], 'file': 'my_file.txt'}],
             'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
             'email': {
                 'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                 'reply_to': 'reply@foo.com',
                 'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                 'subject': 'My subject',
                 'attachments': ['my_label']}}]
        records_analysis_template_2 = [
            {'required_metadata_keys': [{'key': 'foo', 'type': 'string', 'required': False},
                                        {'key': 'bar', 'type': 'string', 'required': False},
                                        {'key': 'portal', 'type': 'string', 'required': False}],
             'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],

                          'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},

                          'tags': ['my_tag'], 'file': 'my_file.txt'}],
             'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c15',
             'email': {
                 'content': [{'file': 'my_label'}, 'Hello world', {'string': 'Bye world'}],
                 'reply_to': 'reply@foo.com',
                 'to': ['foobar1@foo.com'], 'cc': ['cc@foo.com'], 'bcc': ['bcc@foo.com'],
                 'subject': 'My subject',
                 'attachments': ['my_label']}}]
        records_analysis_publishingflags = [{'outputs': {},
                                             'template_flags': ['template_flag']}]
        records_file = [{'_id': '52a03153f28749549db94c16',
                         'metadata': {'template_name': 'my_template'}}]
        records_analysis_plocations = [{'outputs': {'bar': 'bar1', 'baz': 'baz1'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_template), iter(records_analysis_publishingflags),
                                            iter(records_analysis_template), iter(records_file),
                                            iter(records_analysis_template_2), iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations), iter(records_file)]
        self.core.getSetting.return_value = 1
        server = Mock()
        server.run_method.side_effect = [{'_id': '52a03153f28749549db94c10', 'metadata_id': '52a03153f28749549db94c10'},
                                         {'_id': '52a03153f28749549db94c11', 'metadata_id': '52a03153f28749549db94c11'},
                                         {'_id': '52a03153f28749549db94c12', 'metadata_id': '52a03153f28749549db94c12'},
                                         ]
        restserver_mock.Instance.return_value = server
        self.db.save.return_value = '52a03153f28749549db94c17'
        request = {'template_name': 'my_template',
                   'template_data': {'outputs': [
                       {'label': 'my_label', 'tags': ['my_tag'], 'file': 'my_file.txt',
                        'metadata': {'foo': 'foo1', 'bar': 'bar1', 'portal': 'my_portal'},
                        }]},
                   'location': '/path/to/my_file.txt',
                   'user': 'foo',
                   'group': 'sdm',
                   'division': 'jgi',
                   }

        if require_datacenter_source:
            self.assertRaises(common.HttpException, self.analysis.post_analysisimport, None, request)
        else:
            self.assertEqual({'analysis_id': '52a03153f28749549db94c17',
                              'jat_key': 'AUTO-2',
                              'location': '/path/to/my_file.txt',
                              'warnings': ['Requests without data center `source` parameter are deprecated. '
                                           'It will become a REQUIRED parameter. Please update your calls '
                                           'to pass the parameter']},
                             self.analysis.post_analysisimport(None, request))

    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_put_metadata(self, datetime_mock):
        records_analysis = [{'_id': '52a03153f28749549db94c10'}]
        records_file = [{'_id': '52a03153f28749549db94c11'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis), iter(records_file)]
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.put_metadata(['jat-key'], {
            '__auth': {'group': 'sdm'},
            'metadata': {'key': '52a03153f28749549db94c10', 'foo': 'bar'},
        })

        self.assertIn(call.update({'_id': {'$in': [ObjectId('52a03153f28749549db94c11')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                     'modified_date': datetime.datetime(2022, 1, 2, 0, 0),
                     'metadata.sdm.key': '52a03153f28749549db94c10', 'metadata.sdm.foo': 'bar'}}, multi=True),
            self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_post_safeupdate(self, datetime_mock):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.update.return_value = {'nModified': 1}
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.post_safeupdate(None, {
            'permissions': ['admin'],
            'query': {'_id': '52a03153f28749549db94c10'},
            'update': {'foo': 'bar'}
        }), {'nModified': 1, 'ok': 1.0, 'n': 1})

        self.assertIn(call.update({'_id': {'$in': [ObjectId('52a03153f28749549db94c10')]}},
                                  {'$set': {'modified_date': datetime.datetime(2022, 1, 2, 0, 0)}, 'foo': 'bar'},
                                  multi=True), self.db.mock_calls)

    def test_Analysis_post_macro(self):
        self.analysis.macros = {'my_macro': {'_id': '52a03153f28749549db94c10', 'user': 'foo'}}

        self.analysis.post_macro(['my_macro'], {
            'user': 'foo',
            'name': 'my_macro',
            'description': 'My macro',
            'required_metadata_keys': [{
                'description': 'Key description',
                'key': 'my_key',
                'type': 'string',
                'required': True,
            }],
        })

        self.assertIn(call.save({'required_metadata_keys': [
            {'required': True, 'type': 'string', 'description': 'Key description', 'key': 'my_key'}],
            'description': 'My macro', '_id': '52a03153f28749549db94c10', 'user': 'foo',
            'name': 'my_macro'}), self.db.mock_calls)

    def test_Analysis_get_macros(self):
        records = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [{'key': 'foo'}, {'key': 'bar'}]}]
        self.cursor.__iter__.return_value = iter(records)

        self.assertEqual(self.analysis.get_macros(None, None),
                         [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': 'bar, foo'}])

    def test_Analysis_get_macro(self):
        records = [{'_id': '52a03153f28749549db94c10', 'name': 'my_macro'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.analysis.get_macro(['my_macro'], None),
                         {'_id': '52a03153f28749549db94c10', 'name': 'my_macro'})
        self.assertIn(call.find({'name': 'my_macro'}), self.db.mock_calls)

    def test_Analysis_post_query(self):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.analysis.post_query(None, {'modified_date': '2022-01-02'}),
                         [{'_id': '52a03153f28749549db94c10'}])
        self.assertIn(call.find({'modified_date': datetime.datetime(2022, 1, 2, 0, 0)}), self.db.mock_calls)

    def test_Analysis_get_query(self):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.analysis.get_query(None, {'_id': '52a03153f28749549db94c10'}),
                         [{'_id': '52a03153f28749549db94c10'}])
        self.assertIn(call.find({'_id': ObjectId('52a03153f28749549db94c10')}), self.db.mock_calls)

    def test_Analysis_post_subscription(self):
        self.analysis.post_subscription(None, {'url': '/path/to/url', 'filter': {'_id': '52a03153f28749549db94c10'}})

    def test_Analysis_loadAllMacros(self):
        macros_dir = f'{self.analysis.location}/macros'
        os.makedirs(macros_dir)
        with open(f'{macros_dir}/filtered_fastq_info.yaml', 'w') as f:
            f.write(
                'name: filtered_fastq_info\ndescription: pre-defined metadata for filtered fastqs\nrequired_metadata_keys:\n- description: Filter product type\n  key: filter_product_type\n  type: string')

        self.analysis.loadAllMacros()

        # Check a sample of macros
        self.assertIn('filtered_fastq_info', self.analysis.macros.keys())

    def test_Analysis_loadMacro(self):
        macros_dir = f'{self.analysis.location}/macros'
        os.makedirs(macros_dir)
        with open(f'{macros_dir}/filtered_fastq_info.yaml', 'w') as f:
            f.write(
                'name: filtered_fastq_info\ndescription: pre-defined metadata for filtered fastqs\nrequired_metadata_keys:\n- description: Filter product type\n  key: filter_product_type\n  type: string')
        data = {'user': 'auto', 'required_metadata_keys': [
            {'required': True, 'type': 'string', 'description': 'Filter product type', 'key': 'filter_product_type'}],
            'group': 'users', 'name': 'filtered_fastq_info',
            'description': 'pre-defined metadata for filtered fastqs'}

        self.analysis.loadMacro('filtered_fastq_info')

        self.assertEqual(self.analysis.macros.get('filtered_fastq_info'), data)
        self.assertIn(call.save(data), self.db.mock_calls)

    def test_Analysis_loadAllCvs(self):
        cv_keys_dir = f'{self.analysis.location}/cv/keys'
        os.makedirs(cv_keys_dir)
        with open(f'{cv_keys_dir}/tags.yaml', 'w') as f:
            f.write('-\n value: viral\n description: ""')

        self.analysis.loadAllCvs()

        # Check a sample of macros
        self.assertIn('tags', self.analysis.cv.keys())

    def test_Analysis_loadCv(self):
        cv_keys_dir = f'{self.analysis.location}/cv/keys'
        os.makedirs(cv_keys_dir)
        with open(f'{cv_keys_dir}/tags.yaml', 'w') as f:
            f.write('-\n value: viral\n description: "viral_description"')

        self.analysis.loadCv('tags')

        self.assertEqual(self.analysis.cv.get('tags'), {'viral': 'viral_description'})

    def test_Analysis_get_cv(self):
        self.analysis.cv = {'foo': {'bar': 'baz'}}

        self.assertEqual(self.analysis.get_cv(None, None), {'foo': {'bar': 'baz'}})

    @patch('analysis.sdmlogger')
    def test_Analysis_loadAllTemplates(self, sdmlogger_mock):
        templates_dir = f'{self.analysis.location}/templates'
        os.makedirs(templates_dir)
        with open(f'{templates_dir}/gold_sigs.yaml', 'w') as f:
            f.write(
                'name: gold_sigs\ndescription: Specifications for submitting sig report to JAMO\nrequired_metadata_keys:\n- description: the Analysis Project ID\n  key: analysis_project_id\n  type: number\ntags:\n- sigs_report\noutputs:\n- label: sigs_report\n  description: A text report summarizing SIG fields\n  required: true\n  required_metadata_keys:\n  - macro: file_info\n  default_metadata_values:\n    file_format: text\n  tags:\n  - report')

        self.analysis.loadAllTemplates()

        self.assertIn(call.remove({'user': 'auto', 'name': {'$nin': ['gold_sigs']}}), self.db.mock_calls)

    def test_Analysis_loadAllTagTemplates(self):
        tag_templates_dir = f'{self.analysis.location}/tag_templates'
        os.makedirs(tag_templates_dir)
        with open(f'{tag_templates_dir}/fastq.yaml', 'w') as f:
            f.write(
                'description: a sequence file in fastq format\nrequired_metadata_keys:\n  - key: fastq_type\n    description: the type of fastq file, can be pooled\n    type: "list:str"')

        self.analysis.loadAllTagTemplates()

        self.assertIn(call.remove({'user': 'auto', 'name': {'$nin': ['fastq']}}), self.db.mock_calls)

    @patch('analysis.datetime')
    def test_Analysis_loadTagTemplate(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)
        self.analysis.repo_location = self.analysis.location
        tag_templates_dir = f'{self.analysis.location}/tag_templates'
        os.makedirs(tag_templates_dir)
        with open(f'{tag_templates_dir}/fastq.yaml', 'w') as f:
            f.write(
                'description: a sequence file in fastq format\nrequired_metadata_keys:\n  - key: fastq_type\n    description: the type of fastq file, can be pooled\n    type: "list:str"')

        self.analysis.loadTagTemplate('fastq')

        self.assertIn(call.save({'required_metadata_keys': [
            {'required': True, 'type': 'list:str', 'description': 'the type of fastq file, can be pooled',
             'key': 'fastq_type'}],
            'group': 'users', 'description': 'a sequence file in fastq format', 'name': 'fastq',
            'last_modified': datetime.datetime(2022, 1, 2, 0, 0), 'user': 'auto',
            'md5': '22ca2b950281a3d8f83a8be16f28b0d0'}), self.db.mock_calls)

    @patch('analysis.datetime')
    @patch('analysis.sdmlogger')
    def test_Analysis_loadTemplate(self, sdmlogger_mock, datetime_mock):
        self.analysis.repo_location = self.analysis.location
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)
        templates_dir = f'{self.analysis.location}/templates'
        os.makedirs(templates_dir)

        with open(f'{templates_dir}/microbial_raw_single_minimal.yaml', 'w') as f:
            f.write(
                'name: microbial_raw_single_minimal\ndescription: Specifications for submitting Raw Microbial Single Cell Minimal Draft Assembly to JAMO\ntags:\n- microbial\n- minimal\n- single_cell\n- production\n- raw\nrequired_metadata_keys:\n- macro: production_release\n- description: any comments that the analyst wants to include with the analysis\n  key: analysis_comments\n  type: string\n  required: false\noutputs:\n- description: A txt report summarizing the released data\n  label: text_report\n  required_metadata_keys:\n    - macro: file_info\n  default_metadata_values:\n    file_format: txt\n  tags:\n  - report')

        self.analysis.loadTemplate('microbial_raw_single_minimal')

        self.assertIn(call.save({'name': 'microbial_raw_single_minimal',
                                 'description': 'Specifications for submitting Raw Microbial Single Cell Minimal Draft Assembly to JAMO',
                                 'tags': ['microbial', 'minimal', 'single_cell', 'production', 'raw'],
                                 'required_metadata_keys': [{'macro': 'production_release', 'required': True}, {
                                     'description': 'any comments that the analyst wants to include with the analysis',
                                     'key': 'analysis_comments', 'type': 'string', 'required': False}],
                                 'outputs': [
                                     {'description': 'A txt report summarizing the released data',
                                      'label': 'text_report',
                                      'required_metadata_keys': [{'macro': 'file_info', 'required': True}],
                                      'default_metadata_values': {'file_format': 'txt'},
                                      'tags': ['report'], 'required': True}],
                                 'user': 'auto', 'group': 'users', 'md5': 'a55b891be05d8134d16a8765be32accf',
                                 'last_modified': datetime.datetime(2022, 1, 2, 0, 0)}),
                      self.db.mock_calls)

    @parameterized.expand([
        ('fields', {
            'fields': ['foo', 'bar'],
            'query': {'_id': '52a03153f28749549db94c10'},
            'requestor': 'foobar',
            'source': 'analysis',
            'cltool': True,
        },
            {'cursor_id': 'A' * 10,
             'end': 1,
             'fields': ['foo', 'bar'],
             'record_count': 1,
             'records': [{'_id': '52a03153f28749549db94c10',
                          'bar': 'bar1',
                          'foo': 'foo1'}],
             'start': 1,
             'timeout': 540}
        ),
        ('no_fields', {
            'query': 'select foo, bar where _id = 52a03153f28749549db94c10',
            'requestor': 'foobar',
            'source': 'analysis',
        },
            {'cursor_id': 'A' * 10,
             'end': 1,
             'fields': ['foo', 'bar', 'file_name'],
             'record_count': 1,
             'records': [{'_id': '52a03153f28749549db94c10',
                          'bar': 'bar1',
                          'foo': 'foo1'}],
             'start': 1,
             'timeout': 540}
        ),
    ])
    @patch('lapinpy.mongorestful.random.choice')
    def test_Analysis_post_pagequery(self, _description, kwargs, expected, random_mock):
        records = [{'_id': '52a03153f28749549db94c10', 'foo': 'foo1', 'bar': 'bar1'}]
        self.cursor.__iter__.return_value = iter(records)
        self.db.count_documents.return_value = len(records)
        random_mock.return_value = 'A'

        self.assertEqual(self.analysis.post_pagequery(None, kwargs), expected)

    @parameterized.expand([
        ('flags_and_plocs', [{'outputs': {'my_label': ['publish1']}}],
         [{'outputs': {'my_label': 'Raw Data', 'my_label2': 'portal'}}],
         {'display_location_cv': ['Raw data'],
          'outputs': {'my_label': {'display_location': 'Raw Data',
                                   'publish_to': ['publish1']},
                      'my_label2': {'display_location': 'portal', 'publish_to': []}},
          'publishing_flags': ['Foo', 'Bar']}
         ),
        ('no_flags_and_plocs', [], [],
         {'display_location_cv': ['Raw data'],
          'outputs': {'my_label': {'display_location': '', 'publish_to': []},
                      'my_label2': {'display_location': '', 'publish_to': []}},
          'publishing_flags': ['Foo', 'Bar']}
         ),
    ])
    def test_Analysis_get_distributionproperties(self, _description, records_analysis_publishingflags,
                                                 records_analysis_plocations, expected):
        records_templates = [
            {'outputs': [{'label': 'my_label'}, {'label': 'my_label2'}], 'user': 'foobar', 'group': 'sdm',
             '_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.side_effect = [iter(records_templates), iter(records_analysis_publishingflags),
                                            iter(records_analysis_plocations)]
        self.db.count_documents.side_effect = [len(records_templates), len(records_analysis_publishingflags),
                                               len(records_analysis_plocations)]

        self.assertEqual(self.analysis.get_distributionproperties(['my_template'], None),
                         expected)

    @parameterized.expand([
        ('plocs', [{'outputs': {'my_label2': 'Raw Data'}}], {'my_label2': 'Raw Data', 'my_label': ''}),
        ('no_plots', [], {'my_label': '', 'my_label2': 'Raw data'}),
    ])
    def test_Analysis_get_portallocations(self, _description, records_analysis_plocations, expected):
        records_templates = [{'outputs': [{'label': 'my_label'}, {'label': 'my_label2',
                                                                  'default_metadata_values.portal.display_location': [
                                                                      'Raw data']}],
                              'user': 'foobar', 'group': 'sdm', '_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_plocations), iter(records_templates)]

        self.assertEqual(self.analysis.get_portallocations(['my_template'], None),
                         expected)

    @parameterized.expand([
        ('no_errors', {
            'XXredirect_internalXX': True,
            'my_label': 'Raw Data',
            'my_label2': 'portal',
        },
            {'update': "{'nModified': 1}"}),
        ('errors', {
            'XXredirect_internalXX': True,
            'my_label': 'Raw Data',
            'my_label2': 'portal{{foo}',
        },
            {'errors': [{'output': 'my_label2',
                         'type': 'bad macro',
                         'value': 'portal{{foo}'}],
             'update': "{'nModified': 1}"}),
    ])
    @patch('analysis.restful.RestServer')
    def test_Analysis_put_portallocations(self, _description, kwargs, expected, restserver):
        records_analysis_plocations = [{'outputs': {'my_label': 'Raw Data', 'my_label2': 'Raw Data'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis_plocations)]
        server = Mock()
        server.run_method.side_effect = [
            {'metadata_id': '52a03153f28749549db94c11'},
        ]
        restserver.Instance.return_value = server
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.analysis.put_portallocations(['my_template'], kwargs), expected)

    @parameterized.expand([
        ('no_macros', 'portal', {'num_files_updated': 0},
         [call.run_method('metadata', 'put_filesuper', data={'metadata.portal.display_location': ['portal']},
                          query={'metadata.jat_label': 'my_label', '__update_publish_to': {'$exists': False},
                                 'metadata.template_name': 'my_template'})]),
        ('macros', 'portal_{_id}', {'num_files_updated': 1},
         [call.run_method('metadata', 'post_pagequery', fields=['_id'],
                          query={'metadata.jat_label': 'my_label', '__update_publish_to': {'$exists': False},
                                 'metadata.template_name': 'my_template'}),
          call.run_method('metadata', 'put_file',
                          data={'metadata.portal.display_location': [u'portal_52a03153f28749549db94c11']},
                          id='52a03153f28749549db94c11')]),
        ('error', 'portal_{{_id}', {'errors': [{'bad macro': 'portal_{{_id}'}]}, []),
    ])
    @patch('analysis.restful.RestServer')
    def test_Analysis_updateFilePortalLocations(self, _description, new_location, expected, expected_metadata_calls,
                                                restserver):
        template = 'my_template'
        output = 'my_label'
        server = Mock()
        server.run_method.side_effect = [
            [{'_id': '52a03153f28749549db94c11'}],
            [{'_id': '52a03153f28749549db94c12'}],
        ]
        restserver.Instance.return_value = server

        self.assertEqual(self.analysis.update_file_portal_locations(template, output, new_location), expected)
        for c in expected_metadata_calls:
            self.assertIn(c, server.mock_calls)

    @parameterized.expand([
        ('publishingflags_in_db', [{'outputs': {'my_label': ['publish1']}, 'template_flags': 'my_template_flags'}],
         [], {'my_label': ['publish1'], 'template_flags': 'my_template_flags'}),
        ('publishingflags_not_in_db', [],
         [{'outputs': [{'label': 'my_label'}], 'user': 'foobar', 'group': 'sdm', '_id': '52a03153f28749549db94c10'}],
         {'my_label': [], 'template_flags': []}),
    ])
    def test_Analysis_get_publishingflags(self, _description, records_analysis_publishingflags, records_templates,
                                          expected):
        self.cursor.__iter__.side_effect = [iter(records_analysis_publishingflags), iter(records_templates)]

        self.assertEqual(self.analysis.get_publishingflags(['my_template'], None),
                         expected)

    def test_Analysis_put_publishingflags(self):
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.analysis.put_publishingflags(['my_template'], {
            'XXredirect_internalXX': True,
            'my_label': ['Raw Data'],
            'my_label2': [],
        }), "{'nModified': 1}")
        self.assertIn(call.update({'template': 'my_template'}, {'$set': {'outputs': {'my_label': ['Raw Data']}, 'template_flags': ['Raw Data']}}, multi=True, upsert=True),
                      self.db.mock_calls)

    def test_Analysis_delete_publishingflags(self):
        records = [{'outputs': {'my_label': ['publish1']}, 'template_flags': 'my_template_flags'}]
        self.cursor.__iter__.side_effect = [iter(records)]
        self.db.update.return_value = {'nModified': 1}

        self.assertEqual(self.analysis.delete_publishingflags(['my_template'], {}),
                         "{'nModified': 1}")
        self.assertIn(call.update({'template': 'my_template'}, {'$set': {'outputs': {'my_label': []}}}, multi=True, upsert=True), self.db.mock_calls)

    def test_Analysis_extractMacros(self):
        self.assertEqual(self.analysis.extractMacros('foo{bar}baz{foobar}'), ['bar', 'foobar'])

    @patch('analysis.restful.RestServer')
    def test_Analysis_post_delete(self, restserver):
        server = Mock()
        server.run_method.side_effect = [
            [{'_id': '52a03153f28749549db94c10', 'key': 'jat-key'}],
            {'_id': '52a03153f28749549db94c10',
             'file_records': 1,
             'tape_records': 2},
        ]
        restserver.Instance.return_value = server

        self.assertEqual(self.analysis.post_delete(None, {
            'query': {'_id': '52a03153f28749549db94c10'},
        }), {'analysis_records': 1, 'file_records': 1, 'tape_records': 2})
        for c in [call.save({'_id': '52a03153f28749549db94c10', 'key': 'jat-key'}),
                  call.remove({'_id': ObjectId('52a03153f28749549db94c10')})]:
            self.assertIn(c, self.db.mock_calls)

    @patch('analysis.restful.RestServer')
    def test_Analysis_post_undelete(self, restserver):
        server = Mock()
        server.run_method.side_effect = [
            [{'_id': '52a03153f28749549db94c10', 'key': 'jat-key'}],
            {'_id': '52a03153f28749549db94c10',
             'file_records': 1,
             'tape_records': 2},
        ]
        restserver.Instance.return_value = server

        self.assertEqual(self.analysis.post_undelete(None, {
            'query': {'_id': '52a03153f28749549db94c10'},
        }), {'analysis_records': 1, 'file_records': 1, 'tape_records': 2})
        for c in [call.save({'_id': '52a03153f28749549db94c10', 'key': 'jat-key'}),
                  call.remove({'_id': ObjectId('52a03153f28749549db94c10')})]:
            self.assertIn(c, self.db.mock_calls)

    @patch('lapinpy.mongorestful.random.choice')
    def test_Analysis_post_tags(self, random_mock):
        records = [{'_id': '52a03153f28749549db94c10'}]
        self.cursor.__iter__.side_effect = [iter(records)]
        self.db.count_documents.return_value = len(records)
        random_mock.return_value = 'A'

        self.assertEqual(self.analysis.post_tags(None, {
            'return_count': 10,
            'query': {'_id': '52a03153f28749549db94c10'},
            'fields': ['_id']
        }), {'cursor_id': 'A' * 10,
             'end': 1,
             'fields': ['_id'],
             'record_count': 1,
             'records': [{'_id': '52a03153f28749549db94c10'}],
             'start': 1,
             'timeout': 540})

    def test_Analysis_get_tag(self):
        self.db.find_one.return_value = {'_id': '52a03153f28749549db94c10'}

        self.assertEqual(self.analysis.get_tag(['my_tag'], None), {'_id': '52a03153f28749549db94c10'})

    @patch('analysis.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_post_importfile(self, restserver, datetime_mock):
        records = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}]}]
        self.cursor.__iter__.side_effect = [iter(records)]
        server = Mock()
        server.run_method.side_effect = [
            {'_id': '52a03153f28749549db94c11'},
        ]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2020, 1, 2)

        self.assertEqual(self.analysis.post_importfile(None, {
            'metadata': {'foo': 'foo1'},
            'tags': ['my_tag'],
            'file': 'my_file',
            'tape_options': {'tape_opt': 'opt1'},
            'user': 'foo',
            'group': 'sdm',
            'division': 'jgi',
            'source': 'my_source',
        }), {'_id': '52a03153f28749549db94c11', 'warnings': []})
        self.assertIn(call.run_method('metadata', 'post_file',
                                      __auth={'group': 'sdm', 'user': 'foo', 'division': 'jgi'},
                                      destination='file_imports/2020/1/', file='my_file', file_type=['my_tag'],
                                      metadata={'foo': 'foo1'}, tape_opt='opt1', source='my_source'),
                      server.mock_calls)

    @patch('analysis.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_post_importfile_missing_source_parameter(self, restserver, datetime_mock):
        require_datacenter_source = False

        self.analysis.config.require_datacenter_source = require_datacenter_source
        records = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}]}]
        self.cursor.__iter__.side_effect = [iter(records)]
        server = Mock()
        server.run_method.side_effect = [
            {'_id': '52a03153f28749549db94c11'},
        ]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2020, 1, 2)
        request = {
            'metadata': {'foo': 'foo1'},
            'tags': ['my_tag'],
            'file': 'my_file',
            'tape_options': {'tape_opt': 'opt1'},
            'user': 'foo',
            'group': 'sdm',
        }

        if require_datacenter_source:
            self.assertRaises(common.HttpException, self.analysis.post_importfile, None, request)
        else:
            self.assertEqual(self.analysis.post_importfile(None, request),
                             {'_id': '52a03153f28749549db94c11',
                              'warnings': ['Requests without data center `source` parameter are deprecated. '
                                           'It will become a REQUIRED parameter. Please update your calls '
                                           'to pass the parameter']})

    def test_Analysis_safeMerge(self):
        origin = {'foo': {'bar': 'baz'}}
        new = {'foo': {'bar': 'foobar'}}
        replace_with_null = True

        self.assertEqual(self.analysis.safeMerge(origin, new, replace_with_null), 1)
        self.assertEqual(origin, {'foo': {'bar': 'foobar'}})

    @patch('analysis.datetime')
    @patch('analysis.restful.RestServer')
    def test_Analysis_post_registerupdate(self, restserver, datetime_mock):
        records = [{'_id': '52a03153f28749549db94c10', 'metadata': {'foo': 'foo1'}}]
        self.cursor.__iter__.side_effect = [iter(records)]
        server = Mock()
        server.run_method.side_effect = [
            {'_id': '52a03153f28749549db94c10', 'metadata': {'bar': 'bar1'}},
        ]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.post_registerupdate(None, {
            'where': {'_id': '52a03153f28749549db94c10'},
            'keep': ['foo'],
        }), 'processed 1 records, modified 1 records')
        self.assertIn(call.save({'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': ObjectId('52a03153f28749549db94c10'), 'metadata': {'foo': 'foo1', 'metadata': {'bar': 'bar1'}, '_id': '52a03153f28749549db94c10'}}), self.db.mock_calls)

    def test_Analysis_post_validatetags(self):
        records = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}]}]
        self.cursor.__iter__.side_effect = [iter(records)]

        self.assertEqual(self.analysis.post_validatetags(None, {
            'metadata': {'foo': 'foo1'},
            'tags': ['my_tags'],
        }), {'foo': 'foo1'})

    def test_Analysis_get_templatesmetadata(self):
        records = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}]}]
        self.cursor.__iter__.side_effect = [iter(records)]

        self.assertEqual(self.analysis.get_templatesmetadata(['my_template'], None),
                         ([{'key': 'foo', 'type': 'string'}], []))

    def test_Analysis_checkMetadata(self):
        templates = [{'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [
            {'key': 'foo', 'type': 'string', 'required': True},
            {'key': 'bar', 'type': 'string', 'required': True},
            {'key': 'baz', 'type': 'number', 'required': True},
            {'key': 'boz', 'type': 'number', 'required': True},
            {'key': 'foz', 'type': 'number', 'required': 'true', 'options': [1, 3, 5]},
            {'key': 'faz', 'type': 'number', 'required': 'true'},
            {'key': 'far', 'type': 'number', 'required': 'true', 'options': [1, 3, 5]},
        ]}]
        metadata = {'foo': 'foo1', 'baz': '1', 'boz': 'two', 'foz': '2', 'faz': 'three', 'far': [2]}
        expected = ["missing required field 'bar' ", "wrong type found for key:'boz' should have the type:'number' ", " incorect value passed for key: 'foz' the allowed values are: '1,3,5' ", "wrong type found for key:'faz' should have the type:'number' ", "wrong type found for key:'far' should have the type:'number' ", " incorect value passed for key: 'far' the allowed values are: '1,3,5' "]

        self.assertEqual(self.analysis.checkMetadata(templates, metadata), expected)

    def test_Analysis_condenseMetadata(self):
        templates = [
            {'_id': '52a03153f28749549db94c10', 'required_metadata_keys': [
                {'key': 'foo', 'type': 'string', 'required': True},
                {'key': 'bar', 'type': 'string'},
                {'key': 'baz', 'type': 'string', 'required': False},
                {'key': 'boo', 'type': 'string', 'required': 'true'},
            ]},
            {'_id': '52a03153f28749549db94c11', 'required_metadata_keys': [
                {'key': 'foo', 'type': 'string', 'required': True},
                {'key': 'bar', 'type': 'string'},
                {'key': 'baz', 'type': 'string', 'required': 'true'},
                {'key': 'boo', 'type': 'string', 'required': 'true'},
            ]},
            {'_id': '52a03153f28749549db94c12', 'required_metadata_keys': [
                {'key': 'foo', 'type': 'string', 'required': True},
                {'key': 'bar', 'type': 'string'},
                {'key': 'foo', 'type': 'string', 'required': 'true'},
            ]}
        ]

        self.assertEqual(self.analysis.condenseMetadata(templates),
                         ([{'key': 'bar', 'type': 'string'}, {'key': 'baz', 'required': False, 'type': 'string'}, {'key': 'boo', 'required': False, 'type': 'string'}, {'key': 'foo', 'required': True, 'type': 'string'}], [[{'key': 'baz', 'required': 'true', 'type': 'string'}, {'key': 'boo', 'required': 'true', 'type': 'string'}]]))

    @parameterized.expand([
        ('admin', ['admin'], 'admin', 'admin'),
        ('user', ['analysis_update'], 'foo', 'foo'),
        ('group', ['analysis_update'], 'bar', 'sdm'),
    ])
    @patch('analysis.restful.RestServer')
    @patch('analysis.datetime')
    def test_Analysis_post_addfile(self, _description, permissions, user, group, datetime_mock, restserver):
        kwargs = {
            'file': '/path/to/my_file_new.txt',
            'metadata': {'foo': 'foo1', 'portal': {'display_location': ['Raw data']}},
            'permissions': permissions,
            'user': user,
            'group': group,
            'division': 'jgi',
            'source': 'my_source',
        }
        expected_db_call = call.update({'key': 'jat_key-1'},
                                       {'$set': {'modified_date': datetime.datetime(2022, 1, 2, 0, 0)}, '$push': {
                                           'outputs': {'description': 'My description', 'tags': ['my_tags'],
                                                       'label': 'my_label', 'file': '/path/to/my_file_new.txt',
                                                       'metadata_id': '52a03153f28749549db94c14',
                                                       'metadata': {'foo': 'foo1',
                                                                    'portal': {'display_location': [u'Raw data']}}}}},
                                       multi=True)
        expected_metadata_call = call.run_method('metadata', 'post_file',
                                                 __auth={'group': group, 'user': user, 'division': 'jgi'},
                                                 destination='analyses/jat_key-1/', file='/path/to/my_file_new.txt',
                                                 file_type=['my_tags'], source='my_source',
                                                 metadata={'bar': 'bar1', 'foo': 'foo1',
                                                           'portal': {'display_location': [u'Raw data']},
                                                           'jat_key': 'jat_key-1', 'jat_label': 'my_label'})
        records_analysis = [{'_id': '52a03153f28749549db94c10',
                             'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c13',
                                          'file': 'my_file.txt'}],
                             'template': 'my_template',
                             'metadata': {'foo': 'foo2'},
                             'user': 'foo', 'group': 'sdm'}]
        records_analysis_template = [{'required_metadata_keys': [{'key': 'portal'}],
                                      'outputs': [{'label': 'my_label',
                                                   'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],
                                                   'tags': ['my_tags'], 'description': 'My description',
                                                   'default_metadata_values': {'bar': 'bar1'}}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c11'}]
        records_analysis_publishingflags = [{'outputs': {'foo': 'bar'}, 'template_flags': 'foo_flags'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis), iter(records_analysis_template),
                                            iter(records_analysis_publishingflags)]
        server = Mock()
        server.run_method.side_effect = [
            {'metadata_id': '52a03153f28749549db94c14'},
        ]
        restserver.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.analysis.post_addfile(['jat_key-1', 'my_label'], kwargs)

        self.assertIn(expected_db_call, self.db.mock_calls)
        self.assertIn(expected_metadata_call, server.mock_calls)

    def test_Analysis_post_addfile_not_admin_or_user_or_group_raises_exception(self):
        kwargs = {
            'file': '/path/to/my_file_new.txt',
            'metadata': {'foo': 'foo1', 'portal': {'display_location': ['Raw data']}},
            'permissions': ['analysis_update'],
            'user': 'foo',
            'group': 'foo',
            'source': 'my_source',
        }
        records_analysis = [{'_id': '52a03153f28749549db94c10',
                             'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c13', 'file': 'my_file.txt'}],
                             'template': 'my_template',
                             'metadata': {'foo': 'foo2'},
                             'user': 'bar', 'group': 'sdm'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis)]

        self.assertRaises(common.HttpException, self.analysis.post_addfile, ['jat_key-1', 'my_label'], kwargs)

    @parameterized.expand([
        ('require_datacenter_source', True),
        ('warning_datacenter_source', False),
    ])
    @patch('analysis.restful.RestServer')
    @patch('analysis.datetime')
    def test_Analysis_post_addfile_missing_source_parameter(self, _description, require_datacenter_source,
                                                            datetime_mock, restserver_mock):
        self.analysis.config.require_datacenter_source = require_datacenter_source
        kwargs = {
            'file': '/path/to/my_file_new.txt',
            'metadata': {'foo': 'foo1', 'portal': {'display_location': ['Raw data']}},
            'permissions': ['admin'],
            'user': 'admin',
            'group': 'admin',
        }
        records_analysis = [{'_id': '52a03153f28749549db94c10',
                             'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c13', 'file': 'my_file.txt'}],
                             'template': 'my_template',
                             'metadata': {'foo': 'foo2'},
                             'user': 'foo', 'group': 'sdm'}]
        records_analysis_template = [{'required_metadata_keys': [{'key': 'portal'}],
                                      'outputs': [{'label': 'my_label', 'required_metadata_keys': [{'key': 'foo', 'type': 'string'}],
                                                   'tags': ['my_tags'], 'description': 'My description',
                                                   'default_metadata_values': {'bar': 'bar1'}}],
                                      'user': 'foo', 'group': 'sdm', '_id': '52a03153f28749549db94c11'}]
        records_analysis_publishingflags = [{'outputs': {'foo': 'bar'}, 'template_flags': 'foo_flags'}]
        self.cursor.__iter__.side_effect = [iter(records_analysis), iter(records_analysis_template),
                                            iter(records_analysis_publishingflags)]
        server = Mock()
        server.run_method.side_effect = [
            {'metadata_id': '52a03153f28749549db94c14'},
        ]
        restserver_mock.Instance.return_value = server
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        if require_datacenter_source:
            self.assertRaises(common.HttpException, self.analysis.post_addfile, ['jat_key-1', 'my_label'], kwargs)
        else:
            self.assertEqual(self.analysis.post_addfile(['jat_key-1', 'my_label'], kwargs),
                             {'warnings': ['Requests without data center `source` parameter are deprecated. '
                                           'It will become a REQUIRED parameter. Please update your calls '
                                           'to pass the parameter']})

    @patch('analysis.datetime')
    def test_Analysis_put_publish(self, datetime_mock):
        records_analysis = [{'_id': '52a03153f28749549db94c10',
                             'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c13', 'file': 'my_file.txt'}],
                             'template': 'my_template',
                             'metadata': {'foo': 'foo1'},
                             'group': 'sdm', 'publish': False}]
        records_file = [{'_id': '52a03153f28749549db94c14',
                         'metadata': {'template_name': 'my_template'}}]
        records_template = [
            {'outputs': [{'label': 'my_label'}], 'user': 'foobar', 'group': 'sdm', '_id': '52a03153f28749549db94c10'}]
        records_analysis_publishingflags = [{'outputs': {'foo': 'bar'}, 'template_flags': 'foo_flags'}]
        records_analysis_plocations = [{'outputs': {'my_label': 'Raw Data'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis), iter(records_file), iter(records_template),
                                            iter(records_analysis_publishingflags), iter(records_analysis_plocations),
                                            iter(records_file)]
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.put_publish(['jat_key-1'], {
            '__auth': {'user': 'foo', 'group': 'sdm'},
            'permissions': ['admin'],
        }), {'Status': 'OK. Successfully set the publish flag.'})
        self.assertIn(call.update({'key': 'jat_key-1'}, {'$set': {'modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'publish': True}}, multi=True), self.db.mock_calls)

    @patch('lapinpy.mongorestful.datetime')
    def test_Analysis_put_unpublishfile(self, datetime_mock):
        records_analysis = [{'_id': '52a03153f28749549db94c10',
                             'outputs': [{'label': 'my_label', 'metadata_id': '52a03153f28749549db94c13', 'file': 'my_file.txt'}],
                             'template': 'my_template',
                             'metadata': {'foo': 'foo1'},
                             'group': 'sdm', 'publish': False}]
        records_file = [{'_id': '52a03153f28749549db94c14',
                         'metadata': {'template_name': 'my_template'}}]
        records_file_2 = [{'_id': '52a03153f28749549db94c14',
                           'metadata': {'template_name': 'my_template_2'}}]
        self.cursor.__iter__.side_effect = [iter(records_analysis), iter(records_file), iter(records_file_2)]
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        self.assertEqual(self.analysis.put_unpublishfile(['jat_key-1'], {
            'unpublish': True,
            'file_name': 'my_file.txt',
            'replaced_by': 'my_file_2.txt',
            '__auth': {'user': 'foo', 'group': 'sdm'},
            'permissions': ['admin'],
        }), {'Status': 'OK. Successfully set the obsolete flag.'})
        self.assertIn(call.save({'modified_date': datetime.datetime(2022, 1, 2, 0, 0), '_id': '52a03153f28749549db94c14', 'metadata_modified_date': datetime.datetime(2022, 1, 2, 0, 0), 'obsolete': True, 'replaced_by': 'my_file_2.txt', 'metadata': {'template_name': 'my_template', 'jat_publish_flag': False}}), self.db.mock_calls)

    def test_Analysis_get_keys(self):
        records = [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}]
        self.cursor.__iter__.side_effect = [iter(records)]

        self.assertEqual(self.analysis.get_keys(['52a03153f28749549db94c10'], {}),
                         [{'_id': '52a03153f28749549db94c10', 'foo': 'bar'}])

    @parameterized.expand([
        ('require_datacenter_source', True),
        ('warning_datacenter_source', False),
    ])
    def test_Analysis_validate_datacenter_source(self, _description, require_datacenter_source):
        self.analysis.config.require_datacenter_source = require_datacenter_source

        if require_datacenter_source:
            self.assertRaises(common.HttpException, self.analysis._validate_datacenter_source, None)
        else:
            self.assertEqual(self.analysis._validate_datacenter_source(None),
                             'Requests without data center `source` parameter are deprecated. It will become a REQUIRED parameter. Please update your calls to pass the parameter')

    def test_unpublish_file(self):
        records = [{'_id': '52a03153f28749549db94c10',
                    'metadata': {'template_name': 'my_template'}}]
        self.cursor.__iter__.side_effect = [iter(records)]
        output = {
            'metadata': {
                'publish_to': 'publish_foo',
                'portal': {'display_location': ['Raw Data']},
                'foo': 'bar',
            },
            '__update_publish_to': 'update_publish_foo',
        }

        analysis.unpublish_file(output)

        self.assertEqual(output, {'metadata': {'foo': 'bar', 'jat_publish_flag': False}})

    @parameterized.expand([
        ('dist_props',
         {'metadata': {'jat_label': 'my_jat_label'}, 'location': 'my_location'},
         {'my_jat_label': {'publish_to': 'publish_foo', 'display_location': '{location}'}},
         [],
         {'location': 'my_location', 'metadata': {'portal': {'display_location': [u'my_location']}, 'publish_to': 'publish_foo', 'jat_publish_flag': True, 'jat_label': 'my_jat_label'}}),
        ('no_dist_props',
         {'metadata': {'jat_label': 'my_jat_label', 'publish_to': 'publish_foo', 'portal': {'display_location': ['Raw data']}}, 'location': 'my_location'},
         {'my_jat_label': {'publish_to': '', 'display_location': ''}},
         [],
         {'location': 'my_location', 'metadata': {'jat_label': 'my_jat_label', 'jat_publish_flag': True, 'portal': {}}}
         ),
        ('keep_defined',
         {'metadata': {'jat_label': 'my_jat_label', 'publish_to': 'publish_foo', 'portal': {'display_location': ['Raw data']}}, 'location': 'my_location', '_id': '52a03153f28749549db94c10'},
         {},
         ['52a03153f28749549db94c10'],
         {'__update_publish_to': [{'display_location': {'from': '', 'to': 'Raw data'}, 'on': datetime.datetime(2022, 1, 2, 0, 0), 'user': 'foo'}], '_id': '52a03153f28749549db94c10', 'location': 'my_location', 'metadata': {'jat_label': 'my_jat_label', 'jat_publish_flag': True, 'portal': {'display_location': ['Raw data']}, 'publish_to': 'publish_foo'}}
         ),
    ])
    @patch('analysis.datetime')
    def test_publish_file(self, _description, output, dist_props, keep_defined, expected, datetime_mock):

        user = 'foo'
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 1, 2)

        analysis.publish_file(output, dist_props, keep_defined, user)

        self.assertEqual(output, expected)

    def test_eval_string(self):
        self.assertEqual(analysis.eval_string('foo:{foo}', {'foo': 'bar'}),
                         'foo:bar')

    def test_get_value(self):
        self.assertEqual(analysis.get_value({'foo': {'bar': 'baz'}}, 'foo.bar'),
                         'baz')

    @parameterized.expand([
        ({'added_date': datetime.datetime(2022, 1, 2, 0, 0)}, 'FY2022 Q2'),
        ({'added_date': datetime.datetime(2022, 2, 2, 0, 0)}, 'FY2022 Q2'),
        ({'added_date': datetime.datetime(2022, 3, 2, 0, 0)}, 'FY2022 Q2'),
        ({'added_date': datetime.datetime(2022, 4, 2, 0, 0)}, 'FY2022 Q3'),
        ({'added_date': datetime.datetime(2022, 5, 2, 0, 0)}, 'FY2022 Q3'),
        ({'added_date': datetime.datetime(2022, 6, 2, 0, 0)}, 'FY2022 Q3'),
        ({'added_date': datetime.datetime(2022, 7, 2, 0, 0)}, 'FY2022 Q4'),
        ({'added_date': datetime.datetime(2022, 8, 2, 0, 0)}, 'FY2022 Q4'),
        ({'added_date': datetime.datetime(2022, 9, 2, 0, 0)}, 'FY2022 Q4'),
        ({'added_date': datetime.datetime(2022, 10, 2, 0, 0)}, 'FY2023 Q1'),
        ({'added_date': datetime.datetime(2022, 11, 2, 0, 0)}, 'FY2023 Q1'),
        ({'added_date': datetime.datetime(2022, 12, 2, 0, 0)}, 'FY2023 Q1'),
        ({'foo': 'val'}, None)
    ])
    def test_get_quarter(self, args, expected):
        self.assertEqual(analysis._get_quarter(args, None), expected)

    def test_get_publish(self):
        self.assertEqual(analysis._get_publish({'publish': True}, 'foo'), 'True')
        self.assertEqual(analysis._get_publish({'publish': False}, 'foo'), None)
        self.assertEqual(analysis._get_publish({'foo': 'var'}, 'foo'), None)


if __name__ == '__main__':
    unittest.main()
