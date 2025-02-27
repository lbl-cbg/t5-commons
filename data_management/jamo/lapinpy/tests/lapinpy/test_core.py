import unittest
from lapinpy import common
from lapinpy.core import Core
from parameterized import parameterized
import datetime
import os
import sys
from lapinpy import core
from lapinpy import mysqlrestful
from lapinpy import restful
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, MagicMock, call, PropertyMock
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, MagicMock, call, PropertyMock
    from builtins import range
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestCore(unittest.TestCase):

    def setUp(self):
        self.__setUp()

    @patch.object(mysqlrestful, 'pymysql')
    @patch.object(core, 'MongoRestful')
    def __setUp(self, mysql, _mongo):
        self.config = MagicMock()
        self.restserver = MagicMock()
        self.connection = Mock()
        mysql.connect.return_value = self.connection
        self.connect = Mock()
        self.connection.return_value = self.connect
        self.cursor = Mock()
        self.connection.cursor.return_value = self.cursor
        self.core = Core(self.config)
        self.core.restserver = self.restserver
        self.core.connections = [self.connection]
        self.cursor.lastrowid = 1

    # Helper function for validating mock SQL calls since the orderings are handled differently between PY2 and PY3
    def _assertIn_sql_calls(self, py2_call, py3_call):
        try:
            self.assertIn(py2_call, self.connection.mock_calls)
        except Exception:
            self.assertIn(py3_call, self.connection.mock_calls)

    @parameterized.expand([
        ('less', '1.2.3', '1.2.4', -1),
        ('equal', '1.2.3', '1.2.3', 0),
        ('greater', '1.2.4', '1.2.3', 1),
    ])
    def test_compareVersions(self, _description, v1, v2, expected):
        self.assertEqual(core.compareVersions(v1, v2), expected)

    @patch.object(core, 'getpass')
    def test_Core_get_runningas(self, getpass):
        getpass.getuser.return_value = 'foo'

        self.assertEqual(self.core.get_runningas(None, None), {'user': 'foo'})

    def test_Core_put_permission_tree(self):
        record = {
            'parent': 'admin',
            'child': 'kbase',
        }
        self.cursor.fetchall.return_value = [record]

        self.core.put_permission_tree(None, None)

        self.assertEqual(self.core.permission_tree, {'admin': ['admin', ['kbase']], 'kbase': ['kbase']})

    @parameterized.expand([
        ('no_config_modules', []),
        ('config_modules', [{'name': 'foo', 'path': '/path/to/foo'}]),
    ])
    def test_Core_get_modules(self, _description, config_modules):
        if config_modules:
            self.config.modules_to_load = config_modules
        else:
            del self.config.modules_to_load
        record = {
            'name': 'core',
            'path': '/path/to/core'
        }
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_modules(), [record] + config_modules)
        self.assertIn(call.cursor().execute('select * from modules limit 500'), self.connection.mock_calls)

    @parameterized.expand([
        ('update', [], [{'name': 'core', 'path': '/path/to/core'}],
         call.cursor().execute('update modules set path=%s where name=%s', ('/path/to/new/location/core', 'core'))),
        ('insert', [], [],
         call.cursor().execute('insert into modules values (%s,%s)', ('core', '/path/to/new/location/core'))),
        ('module_in_config_is_skipped', [{'name': 'core', 'path': '/path/to/core'}], [],
         None),
    ])
    def test_Core_addModule(self, _description, config_modules, records, expected_sql_call):
        self.config.modules_to_load = config_modules
        self.cursor.fetchall.return_value = records
        module = Mock()
        module.appname = 'core'
        module.location = '/path/to/new/location/core'

        self.core.addModule(module)

        if expected_sql_call:
            self.assertIn(expected_sql_call, self.connection.mock_calls)
        else:
            self.assertEqual(len(self.connection.mock_calls), 0)

    @parameterized.expand([
        ('existing_app_and_permissions',
         [[{'id': 1, 'name': 'sdm', 'token': '091FCVUS4HHUI1EKHVGBQV7OI29ZFYW2',
          'created': datetime.datetime(2014, 9, 5, 5, 4, 12)}]],
         'admin',
         'admin',
         {'token': '091FCVUS4HHUI1EKHVGBQV7OI29ZFYW2', 'created': datetime.datetime(2014, 9, 5, 5, 4, 12), 'id': 1,
          'permissions': [{'application_id': 1, 'permission': 'admin'}], 'name': 'sdm'},
         []
         ),
        ('new_app',
         [[],
          [{'id': 1, 'name': 'sdm', 'token': '091FCVUS4HHUI1EKHVGBQV7OI29ZFYW2',
            'created': datetime.datetime(2014, 9, 5, 5, 4, 12)}]],
         'admin',
         'admin',
         {'token': 'A' * 32, 'created': datetime.datetime(2014, 9, 5, 5, 4, 12), 'id': 1,
          'permissions': [{'application_id': 1, 'permission': 'admin'}], 'name': 'sdm'},
         [call.cursor().execute('insert into application values (null, %s ,%s)', ('core', 'core')),
          call.cursor().execute('insert into application_tokens (application_id, token) values (%s, %s)',
                                (1, 'A' * 32))]
         ),
        ('new_permission',
         [
             [],
             [{'id': 1, 'name': 'sdm', 'token': '091FCVUS4HHUI1EKHVGBQV7OI29ZFYW2',
               'created': datetime.datetime(2014, 9, 5, 5, 4, 12)}],
             [{'application': 1, 'name': 'admin'}, {'application': 1, 'name': 'pacbio_admin'}],
             [{'name': 'admin'}],
             [{'id': 1, 'application': 1, 'permission': 4}],
         ],
         'admin',
         'pacbio_admin',
         {'created': datetime.datetime(2014, 9, 5, 5, 4, 12),
          'id': 1,
          'name': 'sdm',
          'permissions': [{'application_id': 1, 'permission': 'admin'},
                          {'application_id': 1, 'permission': 'pacbio_admin'}],
          'token': 'A' * 32},
         [call.cursor().execute('insert into application values (null, %s ,%s)', ('core', 'core')),
          call.cursor().execute('insert into application_tokens (application_id, token) values (%s, %s)',
                                (1, 'A' * 32))]
         ),
    ])
    @patch.object(core, 'random')
    def test_Core_generate_apptoken(self, _description, records, permission, request_permission, expected, expected_sql_calls, random):
        permission_record_1 = {'application': 1, 'name': permission}
        permission_record_2 = {'name': permission}
        records.append([permission_record_1])
        records.append([permission_record_2])
        random.choice.return_value = 'A'

        self.cursor.fetchall.side_effect = records
        self.core.permission_tree = {'admin': ['admin', ['kbase']], 'kbase': ['kbase']}

        self.assertEqual(self.core.generate_apptoken('core', [request_permission]),
                         expected)
        for sql_call in expected_sql_calls:
            self.assertIn(sql_call, self.connection.mock_calls)

    def test_Core_post_togglemethodcron(self):
        foo = Mock()
        bar = Mock()
        foo.__func__ = PropertyMock()
        bar.__func__ = PropertyMock()
        foo.__func__.enabled = False
        bar.__func__.enabled = False
        foo.enabled = False
        bar.enabled = False
        foo.__name__ = 'foo'
        bar.__name__ = 'bar'
        cron_methods = Mock()
        cron_methods.cronMethods = [foo, bar]
        self.restserver.apps.__getitem__.return_value = cron_methods

        self.core.post_togglemethodcron(None, {'app': 'lapinpy', 'method_name': 'foo'})

        self.assertEqual(foo.__func__.enabled, True)
        self.assertEqual(bar.__func__.enabled, False)

    def test_Core_post_triggercron(self):
        foo = Mock()
        bar = Mock()
        foo.__name__ = 'foo'
        bar.__name__ = 'bar'
        cron_methods = Mock()
        cron_methods.cronMethods = [foo, bar]
        self.restserver.apps.__getitem__.return_value = cron_methods

        self.core.post_triggercron(None, {'app': 'lapinpy', 'method_name': 'foo'})

        self.assertIn(call.run_cron_method(foo, True), self.restserver.mock_calls)

    def test_Core_get_crons(self):
        foo = Mock()
        foo.__name__ = 'foo'
        foo.nextEvent = datetime.datetime(1, 2, 3)
        foo.lastRan = datetime.datetime(1, 2, 2)
        foo.enabled = True
        foo.cron = ['*', '0', '*', '*', '*']
        cron_methods = Mock()
        cron_methods.cronMethods = [foo]
        self.restserver.apps = {'foo': cron_methods}

        self.assertEqual(self.core.get_crons(None, None),
                         [{'last_ran': datetime.datetime(1, 2, 2, 0, 0), 'parameters': '* : 0 : * : * : *',
                           'app': 'foo', 'enabled': True, 'method_name': 'foo',
                           'next_event': datetime.datetime(1, 2, 3, 0, 0)}])

    def test_Core_get_permissions(self):
        record = {'id': 1, 'name': 'admin'}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_permissions(None, None), [record])
        self.assertIn(call.cursor().execute('select * from permission limit 500'),
                      self.connection.mock_calls)

    def test_Core_post_permission(self):
        self.cursor.execute.return_value = 1

        self.assertEqual(self.core.post_permission(None, {'name': 'foo'}), 1)
        self.assertEqual(self.core.permissions.get('1'), 'foo')
        self.assertIn(call.cursor().execute('insert into permission ( name) values (%s)', ['foo']),
                      self.connection.mock_calls)

    @parameterized.expand([
        ('found', {'admin': ['admin', ['kbase']], 'kbase': ['kbase']}, ['admin', 'kbase']),
        ('not_found', {}, ['admin']),
    ])
    def test_Core_get_childpermissions(self, _description, permission_tree, expected):
        self.core.permission_tree = permission_tree
        self.assertEqual(self.core.get_childpermissions(['admin'], None),
                         expected)

    def test_Core_get_whoami(self):
        self.cursor.fetchall.return_value = [{'name': 'admin'}]

        self.assertEqual(self.core.get_whoami(None, {'__auth': {'user': 'my_user'}}),
                         {'permissions': ['admin'], 'user': 'my_user'})

    def test_Core_get_permissions_from_user_token(self):
        self.core.permission_tree = {'admin': ['admin', ['kbase']], 'kbase': ['kbase']}
        self.cursor.fetchall.return_value = [{'name': 'admin'}]

        self.assertEqual(self.core.get_permissions_from_user_token('token'),
                         ['admin', 'kbase'])

    def test_Core_get_removeapppermission(self):
        self.cursor.fetchall.side_effect = [
            [{'id': 1}],
            [{'id': 2, 'application': 1, 'permission': 1}],
        ]

        self.core.get_removeapppermission(None, {'permission': 'admin', 'application_id': 1})

        self.assertIn(
            call.cursor().execute('delete from application_permissions where application=%s and permission=%s', (1, 1)),
            self.connection.mock_calls)

    def test_Core_get_apppermissions2(self):
        self.cursor.fetchall.return_value = [{'application': 1, 'name': 'admin'}]

        self.assertEqual(self.core.get_apppermissions2(['token'], None),
                         [{'application_id': 1, 'permission': 'admin'}])

    def test_Core_get_apppermissions(self):
        self.cursor.fetchall.return_value = [{'name': 'admin'}]

        self.assertEqual(self.core.get_apppermissions(['token'], None), ['admin'])

    @parameterized.expand([
        ('new_permission',
         [[{'id': 1}], [], ],
         call.cursor().execute(
             'insert into application_permissions (application, permission) values (%s, %s)', ('app_id', 1))
         ),
        ('existing_permission',
         [[{'id': 1}], [{'id': 1, 'application': 1, 'permission': 1}], ],
         None),
    ])
    def test_Core_post_apppermission(self, _description, records, expected_sql_call):
        self.cursor.fetchall.side_effect = records

        self.core.post_apppermission(None, {'permission': 'admin', 'id': 'app_id'})

        if expected_sql_call:
            self.assertIn(expected_sql_call, self.connection.mock_calls)

    @parameterized.expand([
        ('new_permission',
         [[{'id': 1}], [], ],
         call.cursor().execute('insert into user_permissions (user_id, permission) values (%s, %s)', ('user_id', 1))),
        ('existing_permission',
         [[{'id': 1}], [{'id': 1, 'user_id': 1, 'permission': 1}], ],
         None),
    ])
    def test_Core_post_userpermission(self, _description, records, expected_sql_call):
        self.cursor.fetchall.side_effect = records

        self.core.post_userpermission(None, {'permission': 'admin', 'user_id': 'user_id'})

        if expected_sql_call:
            self.assertIn(expected_sql_call, self.connection.mock_calls)

    def test_Core_get_app(self):
        record = {'id': 1, 'name': 'sdm', 'token': 'some_token',
                  'created': datetime.datetime(2014, 9, 5, 5, 4, 12)}
        self.cursor.fetchall.side_effect = [[record],
                                            [{'application': 1, 'name': 'admin'}]]

        self.assertEqual(self.core.get_app(['my_app'], None),
                         {'created': datetime.datetime(2014, 9, 5, 5, 4, 12),
                          'id': 1,
                          'name': 'sdm',
                          'permissions': [{'application_id': 1, 'permission': 'admin'}],
                          'token': 'some_token'})

    def test_Core_get_user(self):
        record = {'user_id': 1, 'email': 'foo@lbl.gov', 'name': 'Foo Bar', 'group': 'sdm'}
        self.cursor.fetchall.side_effect = [[record],
                                            [{'name': 'admin', 'user_id': 1}]]

        self.assertEqual(self.core.get_user(['foo'], None), record)

    def test_Core_get_self(self):
        self.assertEqual(self.core.get_self(None, {'__auth': {'user': 'my_user'}}), {'user': 'my_user'})

    def test_Core_get_users(self):
        record = {'user_id': 1, 'email': 'foo@lbl.gov', 'name': 'Foo Bar', 'group': 'sdm'}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_users(None, {}), [record])

    def test_Core_get_user_from_id(self):
        record = {'id': 1, 'user_id': 1, 'token': 'some_token', 'email': 'foo@lbl.gov', 'name': 'Foo Bar',
                  'group': 'sdm'}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_user_from_id('some_token'), [record])

    def test_Core_post_chart(self):
        self.core.post_chart(None, {
            'chart': '/api/tape/status',
            'wrapper': {'dataSourceUrl': '/api/tape/status?tq=di'}
        })

        self.assertIn(
            call.cursor().execute('insert into chart_conf (url, conf) values (%s,%s) on duplicate key update conf=%s', (
                '/api/tape/status', {'dataSourceUrl': '/api/tape/status?tq=di'},
                {'dataSourceUrl': '/api/tape/status?tq=di'})),
            self.connection.mock_calls)
        self.assertEqual(self.core.chart_mappings.get('/api/tape/status'),
                         {'dataSourceUrl': '/api/tape/status?tq=di'})

    def test_Core_get_settings(self):
        settings = Mock()
        settings.entries.foo_db = 'database'
        settings.entries.foo_user = 'foo'
        settings.entries.foo_pass = 'pass'
        settings.entries.app_name = 'lapinpy'
        settings.entries.shared = {'app_name': 'lapinpy'}
        config_manager = Mock()
        config_manager.get_settings.return_value = settings
        self.restserver.configManager = config_manager

        self.assertEqual(self.core.get_settings([], None), settings.entries)

    def test_Core_get_applications(self):
        app = Mock()
        app.location = '/path/to/app'
        app.cron_enabled = False
        app.auto_reload = True
        app.last_modified = datetime.datetime(2000, 1, 2, 3, 4)
        app.loaded_time = datetime.datetime(2000, 1, 2, 4, 4)
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_applications(None, None),
                         [{'auto_reload': True,
                           'cron_enabled': False,
                           'file_location': '/path/to/app',
                           'last_modified': datetime.datetime(2000, 1, 2, 3, 4),
                           'loaded': datetime.datetime(2000, 1, 2, 4, 4),
                           'name': 'foo'}])

    def test_Core_get_applicationcron(self):
        foo = Mock()
        foo.__name__ = 'foo'
        foo.enabled = False
        foo.lastRan = datetime.datetime(2000, 1, 2, 5, 4)
        foo.nextEvent = datetime.datetime(2000, 1, 2, 6, 4)
        foo.lastJobName = 'last_job'
        foo.cron = ['*', '0', '*', '*', '*']
        app = Mock()
        app.cron_enabled = False
        app.cronMethods = [foo]
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_applicationcron(['foo'], None),
                         [{'app': 'foo',
                           'enabled': False,
                           'last_job_name': 'last_job',
                           'last_ran': datetime.datetime(2000, 1, 2, 5, 4),
                           'method_name': 'foo',
                           'next_event': datetime.datetime(2000, 1, 2, 6, 4),
                           'parameters': '* : 0 : * : * : *'}])

    def test_Core_post_reloadapp(self):
        foo = Mock()
        foo.__name__ = 'foo'
        foo.enabled = False
        foo.lastRan = datetime.datetime(2000, 1, 2, 5, 4)
        foo.nextEvent = datetime.datetime(2000, 1, 2, 6, 4)
        foo.lastJobName = 'last_job'
        foo.cron = ['*', '0', '*', '*', '*']
        app = Mock()
        app.cron_enabled = False
        app.cronMethods = [foo]
        app.location = '/path/to/app'
        self.restserver.apps = {'foo': app}
        self.restserver.reloadApp.return_value = {'foo': 'bar'}

        self.assertEqual(self.core.post_reloadapp(None, {'name': 'foo'}),
                         {'foo': 'bar'})
        self.assertIn(call.reloadApp('/path/to/app/foo.py'), self.restserver.mock_calls)

    def cron_method(self):
        pass

    def test_Core_post_togglecron(self):
        app = Mock()
        app.cron_enabled = False
        app.cronMethods = [self.cron_method]
        self.restserver.apps = {'foo': app}

        self.core.post_togglecron(None, {'name': 'foo'})

        self.assertEqual(app.cron_enabled, True)
        self.assertEqual(self.cron_method.__func__.enabled, True)

    def test_Core_get_applicationimports(self):
        app = Mock()
        app.loaded_imports = {'core': '/path/to/core'}
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_applicationimports(['foo'], None),
                         [{'library': 'core', 'path': '/path/to/core'}])

    def test_Core_get_applicationjobs(self):
        record = {'job_id': 1234, 'record_id': None}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_applicationjobs(['my_pipeline'], None), [record])

    def test_Core_get_application(self):
        record = {'job_id': 1234, 'record_id': None}
        self.cursor.fetchall.return_value = [record]
        foo = Mock()
        foo.__name__ = 'foo'
        foo.enabled = False
        foo.lastRan = datetime.datetime(2000, 1, 2, 5, 4)
        foo.nextEvent = datetime.datetime(2000, 1, 2, 6, 4)
        foo.lastJobName = 'last_job'
        foo.cron = ['*', '0', '*', '*', '*']
        app = Mock()
        app.cron_enabled = False
        app.cronMethods = [foo]
        app.location = '/path/to/app'
        app.loaded_imports = {'core': '/path/to/core'}
        app.auto_reload = True
        app.loaded_time = datetime.datetime(2000, 1, 2, 3, 4)
        app.last_modified = datetime.datetime(2000, 1, 2, 2, 4)
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_application(['foo'], None),
                         {'auto_reload': True,
                          'cron_enabled': False,
                          'cron_events': [{'app': 'foo',
                                           'enabled': False,
                                           'last_job_name': 'last_job',
                                           'last_ran': datetime.datetime(2000, 1, 2, 5, 4),
                                           'method_name': 'foo',
                                           'next_event': datetime.datetime(2000, 1, 2, 6, 4),
                                           'parameters': '* : 0 : * : * : *'}],
                          'file_location': '/path/to/app',
                          'imports': [{'library': 'core', 'path': '/path/to/core'}],
                          'jobs': [{'job_id': 1234, 'record_id': None}],
                          'last_modified': datetime.datetime(2000, 1, 2, 2, 4),
                          'loaded': datetime.datetime(2000, 1, 2, 3, 4),
                          'name': 'foo'})

    def test_Core_post_job(self):
        self.core.post_job(None, {
            'status': 'Submitted',
            'job_name': 'my job',
            'job_path': '/path/to/job',
            'platform': 'localhost',
            'minutes': 5,
            'cores': 1,
            'pipeline': 'illumina',
            'process': 'my process',
            'process_id': 456,
            'record_id': '789',
            'record_id_type': 'int'
        })

        self._assertIn_sql_calls(
            call.cursor().execute(
                'insert into job ( status, submitted_date, pipeline, process, job_path, record_id_type, process_id, platform, record_id, cores, minutes, job_name) values (%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                ['Submitted', 'illumina', 'my process', '/path/to/job', 'int', 456, 'localhost', '789', 1, 5, 'my job']),
            call.cursor().execute(
                'insert into job ( status, submitted_date, job_name, job_path, process_id, platform, minutes, cores, pipeline, process, record_id, record_id_type) values (%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                ['Submitted', 'my job', '/path/to/job', 456, 'localhost', 5, 1, 'illumina', 'my process', '789', 'int']))

    def test_Core_get_jobs(self):
        record = {'job_id': 1234, 'record_id': None, 'pipeline': 'illumina'}
        self.config.job_monitor = ['illumina']
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_jobs(None, {'tq': ''}), [record])

    def test_Core_get_jobs2(self):
        record = {'job_id': 1234, 'record_id': None}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_jobs2(None, {
            'job_id': 1234,
            'record_id': None,
        }), [record])
        self._assertIn_sql_calls(
            call.cursor().execute(
                'select * from job where (record_id is null or record_id = "") and job_id = 1234 order by job_id desc limit 500'),
            call.cursor().execute(
                'select * from job where job_id = 1234 and (record_id is null or record_id = "") order by job_id desc limit 500')
        )

    @parameterized.expand([
        ('submitted', {'job_id': 1234, 'status': 'Started'},
         call.cursor().execute('update job set  started_date=now(), status=%s where job_id=1234',
                               ['Started'])),
        ('ended', {'job_id': 1234, 'status': 'Finished'},
         call.cursor().execute('update job set  ended_date=now(), status=%s where job_id=1234',
                               ['Finished'])),
    ])
    def test_Core_put_job(self, _description, kwargs, expected_sql_call):
        self.core.put_job(None, kwargs)

        self.assertIn(expected_sql_call, self.connection.mock_calls)

    def test_Core_get_job(self):
        record = {'job_id': 1234, 'record_id': None}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_job([1234], None), record)

    @parameterized.expand([
        ('lost', 'All finished',
         call.cursor().execute(
             'update job set status="Lost", exit_code = -1, ended_date=now() where process_id = 5678', ())
         )
    ])
    @patch('lapinpy.oauth2.OAuth2Session')
    def test_Core_monitorjobs(self, _description, output, expected_sql_call, session):
        record = {'job_id': 1234, 'status': 'Started', 'process_id': 5678}

        self.cursor.fetchall.return_value = [record]
        self.config.job_platform = 'localhost'
        self.config.job_monitor = ['illumina']
        self.config.api_url = '/api/core/'
        self.config.use_slurm = False
        mock_session = Mock()
        post_response = Mock()
        post_response.json.return_value = {'task_id': '1234'}
        get_response = Mock()

        get_response.json.return_value = {'status': 'Finished', 'result': '{"output": "%s"}' % output}
        mock_session.post.return_value = post_response
        mock_session.get.return_value = get_response
        session.return_value = mock_session

        self.core.monitorjobs()

        self.assertIn(expected_sql_call, self.connection.mock_calls)

    def test_Core_appForm(self):
        self.assertEqual(self.core.appForm(None, None),
                         '<form method="post" action="/api/core/app"><div>Application:</div><input id="name" name="name" class="element text medium" type="text" maxlength="255" value=""/> <input id="saveForm" class="button_text" type="submit" name="submit" value="Submit"/></form>')

    def test_Core_get_status(self):
        app = Mock()
        app.__test__ = lambda: 'Success'
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_status(None, None), {'foo': 'Success'})

    @parameterized.expand([
        ('match', [{'value': '1.0.4'}], '1.0.4'),
        ('default', [], '1.0.0'),
    ])
    def test_Core_getSetting(self, _description, records, expected):
        self.cursor.fetchall.return_value = records

        self.assertEqual(self.core.getSetting('core', 'version', '1.0.0'), expected)

    @parameterized.expand([
        ('match', [{'value': '1.0.4'}],
         call.cursor().execute('update setting set value = %s where application=%s and setting=%s',
                               ('1.0.4', 'core', 'version'))),
        ('default', [],
         call.cursor().execute('insert into setting (application,setting,value) values (%s,%s,%s)',
                               ('core', 'version', '1.0.4'))),
    ])
    def test_Core_saveSetting(self, _description, records, expected_sql_call):
        self.cursor.fetchall.return_value = records

        self.core.saveSetting('core', 'version', '1.0.4')

        self.assertIn(expected_sql_call, self.connection.mock_calls)

    @parameterized.expand([
        ('success', 200, {'foo': 'bar'}),
        ('failure', 500, None),
    ])
    @patch('lapinpy.oauth2.OAuth2Session')
    def test_Core_fetch_nersc_user_info(self, _description, status_code, expected, session):
        response = Mock()
        response.status_code = status_code
        response.json.return_value = {'foo': 'bar'}
        response.reason = 'Internal Error'
        mock_session = Mock()
        mock_session.get.return_value = response
        session.return_value = mock_session
        self.config.api_url = '/api/core'

        self.assertEqual(self.core.fetch_nersc_user_info('foo'), expected)

    @patch.object(core, 'sdmlogger')
    def test_Core_post_sendmail(self, sdmlogger):
        self.core.post_sendmail(None, {'to': 'foo@foobar.com', 'subject': 'Some subject', 'body': 'Some content'})

        self.assertIn(call.sendEmail('foo@foobar.com', 'Some subject', 'Some content'), sdmlogger.mock_calls)

    @patch.object(core, 'random')
    @patch.object(core, 'sdmlogger')
    @patch.object(core, 'start_session')
    def test_post_associate_nersc_user_not_found_uses_oauth_flow(self, start_session, email, random):
        kwargs = {'token': 'request_token', 'user': 'my_user'}
        self.restserver.get_authorization_url.return_value = 'http://authentication/url'
        # Make random.choice always return 'A'
        random.choice.return_value = 'A'
        self.config.api_url = 'http://path/to/sf/api/'
        session = Mock()
        start_session.return_value = session
        response = Mock()
        session.get.return_value = response
        response.status_code = 404
        response.reason = 'User not found'

        self.core.post_associate(None, kwargs)

        # Verify request token cached (oauth callback token will use the patched `random.choice`)
        self.assertEqual(self.core.tempTokens, {'A' * 10: 'request_token'})
        # Verify email sent
        self.assertIn(call.sendEmail('my_user@nersc.gov', 'Validate your jamo account',
                                     'In order for you to use jat on the command line you need to associate your user account with your ui account. \nPlease click here: http://authentication/url to validate your account'),
                      email.mock_calls)

    @patch.object(core, 'random')
    @patch.object(core, 'sdmlogger')
    @patch.object(core, 'start_session')
    @patch('lapinpy.mysqlrestful.MySQLRestful.query')
    @patch('lapinpy.mysqlrestful.MySQLRestful.modify')
    def test_post_associate_nersc_user_found_already_exists(self, modify, query, start_session, email, random):
        kwargs = {'token': 'request_token', 'user': 'my_user'}
        self.restserver.get_authorization_url.return_value = 'http://authentication/url'
        # Make random.choice always return 'A'
        random.choice.return_value = 'A'
        query.return_value = {'user_id': 10}
        self.config.api_url = 'http://path/to/sf/api/'
        session = Mock()
        start_session.return_value = session
        response = Mock()
        session.get.return_value = response
        response.status_code = 200
        response.json.return_value = {
            'firstname': 'John',
            'lastname': 'Doe',
            'email': 'johndoe@foo.com',
        }

        self.core.post_associate(None, kwargs)

        # Verify request token not cached
        self.assertEqual(len(self.core.tempTokens), 0)
        # Verify email not sent
        email.assert_not_called()
        # Verify token association written to database
        modify.assert_called_with('insert into user_tokens values (null,%s,%s)', 10, 'A' * 32)

    @patch.object(core, 'random')
    @patch.object(core, 'sdmlogger')
    @patch.object(core, 'start_session')
    @patch('lapinpy.mysqlrestful.MySQLRestful.query')
    @patch('lapinpy.mysqlrestful.MySQLRestful.modify')
    def test_post_associate_nersc_user_found_new_user_created(self, modify, query, start_session, email, random):
        kwargs = {'token': 'request_token', 'user': 'my_user'}
        self.restserver.get_authorization_url.return_value = 'http://authentication/url'
        # Make random.choice always return 'A'
        random.choice.return_value = 'A'
        query.side_effect = ({}, ({'user_id': 10},), {})
        self.config.api_url = 'http://path/to/sf/api/'
        session = Mock()
        start_session.return_value = session
        response = Mock()
        session.get.return_value = response
        response.status_code = 200
        response.json.return_value = {
            'firstname': 'John',
            'lastname': 'Doe',
            'email': 'johndoe@foo.com',
        }

        self.core.post_associate(None, kwargs)

        # Verify request token not cached
        self.assertEqual(len(self.core.tempTokens), 0)
        # Verify email not sent
        email.assert_not_called()
        # Verify user written to database
        self.assertIn(call('insert into user values (null,%s,%s,null)', 'johndoe@foo.com', 'John Doe'),
                      modify.mock_calls)
        # Verify token association written to database
        self.assertIn(call('insert into user_tokens values (null,%s,%s)', 10, 'A' * 32), modify.mock_calls)
        modify.assert_called_with('insert into user_tokens values (null,%s,%s)', 10, 'A' * 32)

    def test_get_associate(self):
        args = ('oauth_callback_token', )
        kwargs = {
            '__auth': {'user': 'my_user'}
        }
        self.core.tempTokens = {'oauth_callback_token': 'request_token'}

        self.core.get_associate(args, kwargs)

        # Verify removal of oauth token from cache
        self.assertNotIn('oauth_callback_token', self.core.tempTokens)
        # Verify username added to request token cache for creating a token when fetching
        self.assertEqual(self.core.token_reservations.get('request_token'), 'my_user')

    def test_get_associate_oauth_token_not_in_cache_raises_400(self):
        args = ('invalid_token', )
        kwargs = {
            '__auth': {'user': 'my_user'}
        }

        with self.assertRaises(common.HttpException) as cm:
            self.core.get_associate(args, kwargs)

        # Verify 400 exception
        self.assertEqual(cm.exception.code, 400)
        self.assertEqual(cm.exception.message, 'You have gone to an expired page sorry')

    @patch('lapinpy.mysqlrestful.MySQLRestful.query')
    @patch('lapinpy.mysqlrestful.MySQLRestful.modify')
    @patch.object(core, 'random')
    def test_get_reserved_token(self, random, modify, query):
        kwargs = {'token': 'request_token'}
        self.core.token_reservations = {'request_token': 'my_user'}
        query.return_value = {'user_id': 10}
        # Make random.choice always return 'A'
        random.choice.return_value = 'A'

        token = self.core.get_reserved_token(None, kwargs)

        # Verify that the user token is returned (32 characters long)
        self.assertEqual(token, 'A' * 32)
        # Verify removal of request token from cache
        self.assertNotIn('request_token', self.core.token_reservations)
        # Verify token association written to database
        modify.assert_called_with('insert into user_tokens values (null,%s,%s)', 10, 'A' * 32)

    def test_get_reserved_token_reservation_token_not_in_cache_raises_400(self):
        kwargs = {}

        with self.assertRaises(common.HttpException) as cm:
            self.core.get_reserved_token(None, kwargs)

        # Verify 400 exception
        self.assertEqual(cm.exception.code, 400)
        self.assertEqual(cm.exception.message, 'You are using an expired token sorry')

    def test_Core_put_user(self):
        self.core.put_user(None, {'group': 'sdm', '__auth': {'user': 'foo'}})

        self.assertIn(call.cursor().execute('update user set `group`=%s where email=%s', ('sdm', 'foo@lbl.gov')),
                      self.connection.mock_calls)

    def test_Core_get_howami(self):
        app = Mock()
        app.get_howami = lambda x, y: 'Success'
        self.restserver.apps = {'foo': app}

        self.assertEqual(self.core.get_howami(None, None), {'foo': 'Success'})

    def test_Core_get_htmltemplate(self):
        self.assertTrue(len(self.core.get_htmltemplate(['applications.html'], None)))

    def test_Core_get_jsscript(self):
        self.assertTrue(
            len(self.core.get_jsscript(
                ['{}/../../scripts/acute.select.js'.format(os.path.dirname(os.path.abspath(__file__)))], None)))

    @parameterized.expand([
        ('int', 1, True),
        ('float', 1.0, True),
        ('str_int', '1', True),
        ('str_float', '1.0', True),
        ('bool', True, True),
        ('str_non_digit', 'foo', False)
    ])
    def test_Core_is_numeric_for_table(self, _description, value, expected):
        self.assertEqual(self.core.is_numeric_for_table(value), expected)

    @patch.object(restful, 'RestServer')
    def test_Core_generate_data_from_func(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server

        self.assertEqual(self.core.generate_data_from_func(['foo'], {'db_address': '127.0.0.1/db/address'}),
                         {'foo': 'bar'})
        self.assertIn(call.run_method('127.0.0.1', 'get_db', 'foo', 'address', db_address='127.0.0.1/db/address'),
                      server.mock_calls)

    @parameterized.expand([
        ('and', 'foo = foo1 and bar = bar1', [{'foo': 'foo1', 'bar': 'bar1'}], [{'foo': 'foo1', 'bar': 'bar1'}, {'foo': 5}, {'foo': True}]),
        ('in', "foo in 'foo1', 'foo2'", [{'foo': 'foo1', 'bar': 'bar1'}, {'foo': 'foo2'}], [{'foo': 'foo1', 'bar': 'bar1'}, {'foo': 'foo2'}, {'foo': True}]),
        ('like', "foo like 'foo1'", [{'foo': 'foo1', 'bar': 'bar1'}], [{'foo': 'foo1', 'bar': 'bar1'}, {'foo': 5}, {'foo': True}]),
        ('bool', "foo = true", [{'foo': True}], [{'foo': 'foo1', 'bar': 'bar1'}, {'foo': 5}, {'foo': True}]),
        ('float', "foo = 5", [{'foo': 5}], [{'foo': 4}, {'foo': 5}, {'foo': 6}]),
    ])
    def test_Core_filter_data(self, _description, query, expected, data):
        self.assertEqual(
            self.core.filter_data(data, None, {'query': query}), expected)

    @parameterized.expand([
        ('asc', {'sort': 'foo asc'}, [{'foo': 1}, {'foo': 2}, {'foo': 3}]),
        ('desc', {'sort': 'foo desc'}, [{'foo': 3}, {'foo': 2}, {'foo': 1}]),
    ])
    def test_Core_sort_data(self, _description, kwargs, expected):
        self.assertEqual(self.core.sort_data([{'foo': 3}, {'foo': 1}, {'foo': 2}], None, kwargs), expected)

    def test_Core_page_data(self):
        data = list(range(1, 11))

        self.assertEqual(self.core.page_data(data, len(data), 5, {'page': 2}), [7, 8, 9, 10])

    def test_Core_process_data(self):
        data = [{'foo': 'foo1'}, {'foo': 'foo2'}, {'foo': 'foo3'}]

        self.assertEqual(self.core.process_data(data, None, {'return_count': 2, 'query': "foo = foo1"}),
                         {'data': [{'foo': 'foo1'}], 'record_count': 1, 'return_count': 2})

    @parameterized.expand([
        ('generated_data',
         {'db_address': '127.0.0.1/db/address',
          'id_field': 'foo',
          'columns': [['foo', {'type': 'string'}],
                      ['bar', {'type': 'string'}]],
          'multi_select': None,
          },
         {'first_row': None,
          'last_page': 2,
          'tbody': '<tr data-id="foo1" data-name="foo"><td>foo1</td><td>bar1</td></tr>',
          'total': 10,
          'total_formatted': '10'},
         {},
         ),
        ('existing_data',
         {'db_address': '127.0.0.1/db/address',
          'id_field': 'foo',
          'columns': [],
          'multi_select': None,
          'data': [{'foo': 'foo1', 'bar': 'bar1', 'baz': 'baz1'}],
          'record_count': 10, 'return_count': 5,
          },
         {'first_row': {'bar': 'bar1', 'baz': 'baz1', 'foo': 'foo1'},
          'last_page': 1,
          'tbody': '<tr data-id="foo1" data-name="foo"><td>baz1</td><td>foo1</td><td>bar1</td></tr>',
          'total': 1,
          'total_formatted': '1'},
         {'first_row': {'bar': 'bar1', 'baz': 'baz1', 'foo': 'foo1'},
          'last_page': 1,
          'tbody': '<tr data-id="foo1" '
                   'data-name="foo"><td>foo1</td><td>bar1</td><td>baz1</td></tr>',
          'total': 1,
          'total_formatted': '1'},
         ),
    ])
    @patch.object(restful, 'RestServer')
    def test_Core_post_queryResults_dataChange(self, _description, kwargs, expected_py2, expected_py3, restserver):
        data = [{'foo': 'foo1', 'bar': 'bar1', 'baz': 'baz1'}]
        server = Mock()
        server.run_method.return_value = {'data': data,
                                          'record_count': 10, 'return_count': 5}
        restserver.Instance.return_value = server

        actual = self.core.post_queryResults_dataChange(['foo'], kwargs)

        try:
            self.assertEqual(actual, expected_py2)
        except AssertionError as e:  # noqa: F841
            self.assertEqual(actual, expected_py3)

    def test_Core_post_userquery(self):
        self.core.temp_mongo.postUserQuery.return_value = {'foo': 'bar'}

        self.assertEqual(self.core.post_userquery(None, {'foo': 'bar'}),
                         {'foo': 'bar'})
        self.assertIn(call.postUserQuery(None, {'foo': 'bar'}),
                      self.core.temp_mongo.mock_calls)

    def test_Core_get_python_version(self):
        expected = {'python_version': '{}.{}.{}'.format(sys.version_info[0],
                                                        sys.version_info[1],
                                                        sys.version_info[2])}

        self.assertEqual(self.core.get_python_version(None, None),
                         expected)

    def test_Core_get_userinfo_from_user_token(self):
        record = {'user': 'foo@lbl.gov', 'group': 'sdm', 'division': 'jgi'}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_userinfo_from_user_token('user_token'),
                         {'user': 'foo', 'group': 'sdm', 'division': 'jgi'})
        self.assertIn(call.cursor().execute(
            'select email as user, `group`, division from user_tokens t left join user u on u.user_id=t.user_id where token=%s limit 500',
            ['user_token']), self.connection.mock_calls)

    def test_Core_get_appinfo_from_token(self):
        record = {'user': 'foo_app', 'group': 'sdm', 'division': 'jgi'}
        self.cursor.fetchall.return_value = [record]

        self.assertEqual(self.core.get_appinfo_from_token('app_token'),
                         {'user': 'foo_app', 'group': 'sdm', 'division': 'jgi'})
        self.assertIn(call.cursor().execute(
            'select name as user, `group`, division from application_tokens t left join application a on a.id=t.application_id where token=%s limit 500',
            ['app_token']), self.connection.mock_calls)


if __name__ == '__main__':
    unittest.main()
