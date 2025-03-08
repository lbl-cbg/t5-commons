import unittest
import jamo_common
import datetime
import tempfile
from parameterized import parameterized
from bson.objectid import ObjectId

try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock
    from backports.tempfile import TemporaryDirectory
    from builtins import str
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestJamoCommon(unittest.TestCase):

    def test_expose(self):
        @jamo_common.expose('My description', 'my_name')
        def func(*args, **kwargs):
            pass

        self.assertEqual(func.expose, True)
        self.assertEqual(func.name, 'my_name')
        self.assertEqual(func.description, 'My description')

    @parameterized.expand([
        ('contains_equals', ['foo = "bar"'], "'foo =' ' \"bar\"'"),
        ('pass_through', ['foo like "bar%"'], "'foo like \"bar%\"'"),
        ('mongo', ['{"_id": "my_id"}'], '{"_id": "my_id"}'),
    ])
    def test_parse_jamo_query(self, _description, args, expected):
        self.assertEqual(jamo_common.parse_jamo_query(args), expected)

    @parameterized.expand([
        ('str', '2022-01-02', datetime.datetime(2022, 1, 2, 0, 0)),
        ('list', ['2022-01-02'], [datetime.datetime(2022, 1, 2, 0, 0)]),
        ('dict', {'modified_date': '2022-01-02', '$eq': '2022-01-03', 'last_accessed': '2022-01-04'},
         {'$eq': datetime.datetime(2022, 1, 3, 0, 0), 'last_accessed': '2022-01-04',
          'modified_date': datetime.datetime(2022, 1, 2, 0, 0)}
         ),
    ])
    def test_convert_dates(self, _description, query, expected):
        self.assertEqual(jamo_common.convert_dates(query), expected)

    def test_ArgRunner_call(self):
        def func(args):
            self.assertEqual(args, ['baz=baz1', 'foo=foo1', 'bar=bar1'])

        arg_runner = jamo_common.ArgRunner('runner')
        arg_runner.methods = ['my_method']
        arg_runner.my_method = func

        with TemporaryDirectory(suffix='tmp') as temp_dir:
            with open('{}/args.txt'.format(temp_dir), 'w') as f:
                f.writelines(['foo=foo1\n', 'bar=bar1\n'])
                f.flush()
                arg_runner(['my_method', 'baz=baz1', '-f', '{}/args.txt'.format(temp_dir)])

    @patch('jamo_common.difflib.get_close_matches')
    def test_ArgRunner_call_error(self, difflib):
        arg_runner = jamo_common.ArgRunner('runner')
        arg_runner.methods = ['my_method']
        difflib.return_value = ['my_method']

        self.assertRaises(SystemExit, arg_runner, ['my_method_2', 'foo=foo1'])

    @parameterized.expand([
        ('str', str('foo: bar')),
        ('dict', {'foo': 'bar'}),
    ])
    @patch('jamo_common.subprocess.call')
    def test_editYaml_jsonO_arg(self, _description, jsonO, subprocess_mock):
        def func(*args, **kwargs):
            yaml_file = args[0].split(' ')[-1]
            with open(yaml_file, 'w') as f:
                f.write('{foo: baz}')
                f.flush()

        expected = {'foo': 'baz'}
        fileLoc = None
        subprocess_mock.side_effect = func

        self.assertEqual(jamo_common.editYaml(jsonO, fileLoc), expected)

    @patch('jamo_common.subprocess.call')
    def test_editYaml_jsonO_fileLoc(self, subprocess_mock):
        def func(*args, **kwargs):
            yaml_file = args[0].split(' ')[-1]
            with open(yaml_file, 'w') as f:
                f.write('foo: baz')
                f.flush()

        expected = {'foo': 'baz'}
        jsonO = None
        subprocess_mock.side_effect = func

        with tempfile.NamedTemporaryFile('w') as f:
            f.write('foo: bar')
            f.flush()
            self.assertEqual(jamo_common.editYaml(jsonO, f.name), expected)

    def test_replaceKeys(self):
        self.assertEqual(jamo_common.replaceKeys({'foo': 'bar'}, 'hello: {foo}'),
                         'hello: bar')

    @parameterized.expand([
        ('str', 'hello: {foo}', 'hello: bar'),
        ('list', ['hello: {foo}'], ['hello: bar']),
        ('dict', {'data': 'hello: {foo}'}, {'data': 'hello: bar'}),
    ])
    def test_replaceAllValues(self, _description, obj, expected):
        self.assertEqual(jamo_common.replaceAllValues({'foo': 'bar'}, obj), expected)

    def test_evalString(self):
        self.assertEqual(jamo_common.evalString('foo:{foo}', {'foo': 'bar'}),
                         'foo:bar')

    def test_getValue(self):
        self.assertEqual(jamo_common.getValue({'foo': {'bar': 'baz'}}, 'foo.bar'),
                         'baz')

    @parameterized.expand([
        ('str', str('{"foo": "bar"}')),
        ('dict', {'foo': 'bar'}),
    ])
    @patch('jamo_common.subprocess.call')
    def test_editJson_jsonO_arg(self, _description, jsonO, subprocess_mock):
        def func(*args, **kwargs):
            yaml_file = args[0].split(' ')[-1]
            with open(yaml_file, 'w') as f:
                f.write('{"foo": "baz"}')
                f.flush()

        expected = {'foo': 'baz'}
        fileLoc = None
        subprocess_mock.side_effect = func

        self.assertEqual(jamo_common.editJson(jsonO, fileLoc), expected)

    @patch('jamo_common.subprocess.call')
    def test_editJson_jsonO_fileLoc(self, subprocess_mock):
        def func(*args, **kwargs):
            yaml_file = args[0].split(' ')[-1]
            with open(yaml_file, 'w') as f:
                f.write('{"foo": "baz"}')
                f.flush()

        expected = {'foo': 'baz'}
        jsonO = None
        subprocess_mock.side_effect = func

        with tempfile.NamedTemporaryFile('w') as f:
            f.write('{"foo": "bar"}')
            f.flush()
            self.assertEqual(jamo_common.editJson(jsonO, f.name), expected)

    @parameterized.expand([
        ('dict', {'foo': 'bar'}, jamo_common.CustomDict),
        ('list', ['foo', 'bar'], jamo_common.CustomList),
        ('str', str('foo'), str)
    ])
    def test_customtransform(self, _description, data, expected_type):
        self.assertTrue(isinstance(jamo_common.customtransform(data), expected_type))

    def test_prepend(self):
        def func(*args, **kwargs):
            self.assertEqual(args, (1, 2))
            self.assertEqual(kwargs, {'foo': 'foo1', 'bar': 'bar1'})

        jamo_common.prepend(1, foo='foo1')(func)(2, bar='bar1')

    def test_CustomDict_delitem(self):
        custom_dict = jamo_common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        del custom_dict['bar']

        self.assertEqual(custom_dict.dic, {'foo': 'foo1'})

    @parameterized.expand([
        ('no_leftovers', {'foo': 'foo1', 'bar': 'bar1'}, 'foo', 'foo1'),
        ('leftovers', {'foo': {'bar': 'bar1'}}, 'foo.bar', 'bar1'),
    ])
    def test_CustomDict_getitem(self, _description, data, key, expected):
        custom_dict = jamo_common.CustomDict(data)

        self.assertEqual(custom_dict[key], expected)

    def test_CustomDict_setitem(self):
        custom_dict = jamo_common.CustomDict({})

        custom_dict['foo.bar'] = 'bar1'

        self.assertEqual(custom_dict.dic, {'foo': {'bar': 'bar1'}})

    def test_CustomDict_get(self):
        custom_dict = jamo_common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.get('foo'), 'foo1')

    def test_CustomDict_caller(self):
        def func(*args, **kwargs):
            return 'OK'

        custom_dict = jamo_common.CustomDict({'foo': 'foo1', 'bar': 'bar1'},
                                             method=lambda *x, **y: x)
        custom_dict.on = 'my_func'
        custom_dict.methods = {'my_func': func}

        self.assertEqual(custom_dict._CustomDict__caller('foo', bar='bar1'), 'OK')

    def test_CustomDict_getattr(self):
        custom_dict = jamo_common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(getattr(custom_dict, '__class__'), jamo_common.CustomDict)
        self.assertEqual(getattr(custom_dict, 'foo'), 'foo1')

    def test_CustomDict_contains(self):
        custom_dict = jamo_common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual('foo' in custom_dict, True)

    def test_CustomList_getattr(self):
        custom_list = jamo_common.CustomList(['foo', 'bar'])

        self.assertEqual(getattr(custom_list, '__len__')(), 2)

    def test_CustomList_getitem(self):
        data = [['foo', 'bar']]
        item = 0
        expected = 'foo'

        custom_list = jamo_common.CustomList(data)

        self.assertEqual(custom_list[item], expected)

    def test_CustomList_iter(self):
        custom_list = jamo_common.CustomList(['foo', 'bar'])

        iterator = custom_list.__iter__()

        self.assertEqual(next(iterator), 'foo')
        self.assertEqual(next(iterator), 'bar')
        self.assertRaises(StopIteration, next, iterator)

    @parameterized.expand([
        ('str', 'foobar', ['foobar']),
        ('int_str', '1', [1]),
        ('bool_str', 'True', [True]),
        ('str_with_backslash', 'foo\\bar', ['foobar']),
        ('nested_str', 'foo(bar(baz))', [('foo', [('bar', ['baz'])])]),
    ])
    def test_tokenize(self, _description, string, expected):
        self.assertEqual(jamo_common.tokenize(string), expected)

    @parameterized.expand([
        ('str', '62791a11c2c506c5afdfce76', ObjectId('62791a11c2c506c5afdfce76')),
        ('str_not_oid', 'not_oid', 'not_oid'),
        ('list', ['62791a11c2c506c5afdfce76'], [ObjectId('62791a11c2c506c5afdfce76')]),
        ('dict', {'_id': '62791a11c2c506c5afdfce76'}, {'_id': ObjectId('62791a11c2c506c5afdfce76')})
    ])
    def test_convertToOID(self, _description, obj, expected):
        self.assertEqual(jamo_common.convertToOID(obj), expected)

    @parameterized.expand([
        ('str', 'foo', 'foo'),
        ('list', ['foo', 'bar'], 'foo bar'),
        ('dict', {'foo': 'bar'}, "{'foo': 'bar'}"),
    ])
    def test_stringify_tokens(self, _description, tokens, expected):
        self.assertEqual(jamo_common.stringify_tokens(tokens), expected)

    def test_toMongoSet(self):
        tokens = '_id = 62791a11c2c506c5afdfce76, foo = bar'

        self.assertEqual(jamo_common.toMongoSet(tokens), {'$set': {'_id': '62791a11c2c506c5afdfce76', 'foo': 'bar'}})

    @parameterized.expand([
        ('equal', '_id = 62791a11c2c506c5afdfce76', {'_id': ObjectId('62791a11c2c506c5afdfce76')}),
        ('in', '_id in (62791a11c2c506c5afdfce76)', {'_id': {'$in': [ObjectId('62791a11c2c506c5afdfce76')]}}),
        ('and_or', '_id = 62791a11c2c506c5afdfce76 and (foo = foo1 or bar = bar1 or (baz = baz1 and foobar = foobar1))',
         {'$and': [{'$or': [{'foo': 'foo1'}, {'bar': 'bar1'}, {'baz': 'baz1', 'foobar': 'foobar1'}]}],
          '_id': ObjectId('62791a11c2c506c5afdfce76')}),
        ('date', 'modified_date = 2020-01-02', {'modified_date': datetime.datetime(2020, 1, 2, 0, 0)}),
        ('notequal_exists', 'file_type != "folder" and obsolete exists false',
         {'file_type': {'$ne': 'folder'}, 'obsolete': {'$exists': False}}),
        ('like', 'foo like %abc%', {'foo': {'$options': 'i', '$regex': '.*abc.*'}})
    ])
    def test_toMongoObj(self, _description, tokens, expected):
        self.assertEqual(jamo_common.toMongoObj(tokens), expected)

    @parameterized.expand([
        ('list', ['1', '2', '3'], [1, 2, 3]),
        ('single_value', '1', 1),
    ])
    def test_toInt(self, _description, args, expected):
        self.assertEqual(jamo_common.toInt(args), expected)

    @parameterized.expand([
        ('list', ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77'],
         [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]),
        ('single_value', '62791a11c2c506c5afdfce76', ObjectId('62791a11c2c506c5afdfce76')),
    ])
    def test_toObjectId(self, _description, args, expected):
        self.assertEqual(jamo_common.toObjectId(args), expected)

    def test_openFile(self):
        with tempfile.NamedTemporaryFile('w') as f:
            f.write('Hello world\n')
            f.flush()

            self.assertEqual(jamo_common.openFile([f.name]), ['Hello world'])

    @patch('jamo_common.sys.stdin')
    def test_stdin(self, stdin_mock):
        stdin_mock.readlines.return_value = ['Hello world\n']

        self.assertEqual(jamo_common.stdin(None), ['Hello world'])

    def test_detokenize(self):
        token = [[('int', '5'), ('int', '6')], ('oid', '62791a11c2c506c5afdfce76')]
        expected = [5, 6, ObjectId('62791a11c2c506c5afdfce76')]

        self.assertEqual(jamo_common.detokenize(token), expected)

    @parameterized.expand([
        ('json', ['{"_id": "62791a11c2c506c5afdfce76"}'], [{u'_id': u'62791a11c2c506c5afdfce76'}]),
        ('tokens_count_gt_60',
         ['_id = 62791a11c2c506c5afdfce76', 'and', 'foo in int({})'.format(', '.join(str(i) for i in range(1, 63)))],
         [{'foo': {
             '$in': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                     28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]},
             '_id': ObjectId('62791a11c2c506c5afdfce76')},
             {'foo': {'$in': [51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62]},
              '_id': ObjectId('62791a11c2c506c5afdfce76')}]),
        ('tokens_count_lt_60', ['_id = 62791a11c2c506c5afdfce76', 'and', 'foo in int(1, 2, 3)'],
         [{'_id': ObjectId('62791a11c2c506c5afdfce76'), 'foo': {'$in': [1, 2, 3]}}])
    ])
    def test_getQueries(self, _description, args, expected):
        self.assertEqual(jamo_common.getQueries(args), expected)

    @parameterized.expand([
        ('json', '{"_id": "62791a11c2c506c5afdfce76"}', [{u'_id': u'62791a11c2c506c5afdfce76'}]),
        ('list', ['_id = 62791a11c2c506c5afdfce76', 'and', 'foo in int(1, 2, 3)'],
         {'_id': ObjectId('62791a11c2c506c5afdfce76'), 'foo': {'$in': [1, 2, 3]}}),
    ])
    def test_getQuery(self, _description, args, expected):
        self.assertEqual(jamo_common.getQuery(args), expected)

    @parameterized.expand([
        ('regex_true', 'foo', {'$regex': 'f(o|a)o'}, True),
        ('regex_false', 'foo', {'$regex': 'f(u|a)o'}, False),
        ('lt_true', 1, {'$lt': 5}, True),
        ('lt_false', 10, {'$lt': 5}, False),
        ('lte_true', 1, {'$lte': 5}, True),
        ('lte_false', 10, {'$lte': 5}, False),
        ('gt_true', 10, {'$gt': 5}, True),
        ('gt_false', 5, {'$gt': 10}, False),
        ('gte_true', 10, {'$gte': 5}, True),
        ('gte_false', 5, {'$gte': 10}, False),
        ('ne_true', 10, {'$ne': 5}, True),
        ('ne_false', 5, {'$ne': 5}, False),
        ('in_true', 'foo', {'$in': {'foo': 'bar'}}, True),
        ('in_false', 'foo', {'$in': {'bar': 'baz'}}, False),
        ('nin_true', 'foo', {'$nin': {'bar': 'baz'}}, True),
        ('nin_false', 'foo', {'$nin': {'foo': 'bar'}}, False),
        ('exists_true', 'foo', {'$exists': True}, True),
        ('exists_false', None, {'$exists': True}, False),
        ('value_list_condition_str', ['foo', 'bar'], 'foo', True),
        ('value_list_condition_list', ['foo', 'bar'], ['foo', 'bar'], True),
    ])
    def test_checkKey(self, _description, value, condition, expected):
        self.assertEqual(jamo_common.checkKey(value, condition), expected)

    @parameterized.expand([
        ('or',
         {'$or': {'_id': '62791a11c2c506c5afdfce76'}},
         True,
         ),
        ('and',
         {'$and': {'_id': '62791a11c2c506c5afdfce76'}},
         True),
        ('not',
         {'$not': {'_id': '62791a11c2c506c5afdfce76'}},
         False,
         ),
        ('nor',
         {'$nor': {'_id': '62791a11c2c506c5afdfce76'}},
         False,
         ),
    ])
    def test_checkMongoQuery(self, _description, query, expected):
        data = {'_id': '62791a11c2c506c5afdfce76'}
        self.assertEqual(jamo_common.checkMongoQuery(data, query), expected)

    def test_PageList_getitem(self):
        page = {'cursor_id': 'some_cursor_id',
                'record_count': 3,
                'records': [{'_id': ObjectId('5327394649607a1be0059511')},
                            {'_id': ObjectId('5327394649607a1be0059512')},
                            {'_id': ObjectId('5327394649607a1be0059513')}
                            ]}

        page_list = jamo_common.PageList(page, None, 'my_service')

        self.assertEqual(page_list[1].dic, {'_id': ObjectId('5327394649607a1be0059512')})

    def test_PageList_iter(self):
        curl = Mock()
        page = {'cursor_id': 'some_cursor_id',
                'record_count': 4,
                'records': [{'_id': ObjectId('5327394649607a1be0059511')},
                            {'_id': ObjectId('5327394649607a1be0059512')},
                            {'_id': ObjectId('5327394649607a1be0059513')}
                            ]}
        curl.get.side_effect = [{'cursor_id': 'some_cursor_id',
                                 'record_count': 4,
                                 'records': [{'_id': ObjectId('5327394649607a1be0059514')},
                                             ]},
                                {'cursor_id': 'some_cursor_id',
                                 'record_count': 0,
                                 'records': []}
                                ]

        page_list = jamo_common.PageList(page, curl, 'my_service')

        entries = [i.dic for i in iter(page_list)]
        self.assertEqual(entries, [{'_id': ObjectId('5327394649607a1be0059511')},
                                   {'_id': ObjectId('5327394649607a1be0059512')},
                                   {'_id': ObjectId('5327394649607a1be0059513')},
                                   {'_id': ObjectId('5327394649607a1be0059514')}])

    def test_PageList_len(self):
        page = {'cursor_id': 'some_cursor_id',
                'record_count': 3,
                'records': [{'_id': ObjectId('5327394649607a1be0059511')},
                            {'_id': ObjectId('5327394649607a1be0059512')},
                            {'_id': ObjectId('5327394649607a1be0059513')}
                            ]}

        page_list = jamo_common.PageList(page, None, 'my_service')

        self.assertEqual(len(page_list), 3)

    @parameterized.expand([
        ('proposal', {'proposal': {'default_project_manager': {'email_address': 'baz'}}}, 'baz'),
        ('sequencing_project', {'sequencing_project': {'project_manager_cid': 1}}, 'foo'),
        ('project_collaborators', {'project_collaborators': {'project_manager_cid': 1}}, 'foo'),
        ('sequencing_project_manager_id', {'sow_segment': {'sequencing_project_manager_id': 2}}, 'bar'),
    ])
    def test_JiraUsers_get_pm_username_from_file_metadata(self, _description, metadata, expected):
        jira_users = jamo_common.JiraUsers()
        jira_users.user_map = {1: 'foo', 2: 'bar'}

        self.assertEqual(jira_users.get_pm_username_from_file_metadata(metadata), expected)

    def test_JiraUsers_set_users_templates(self):
        data = {'baz': 'baz1'}
        metadata = {'sequencing_project': {'project_manager_cid': 1}}

        jira_users = jamo_common.JiraUsers()
        jira_users.user_map = {1: 'foo', 2: 'bar'}

        self.assertEqual(jira_users.set_users_templates(data, metadata), ({'baz': 'baz1'}, ['foo']))

    def test_JiraUsers_set_watchers_pacbio(self):
        jira_users = jamo_common.JiraUsers()

        self.assertEqual(jira_users.set_watchers_pacbio(['baz1, baz2'], 'bar'), ['baz1, baz2', 'bar'])

    def test_JiraUsers_set_watchers_templates(self):
        data = {'watchers': ['2', 'baz2'], 'foobar': 'foobar1'}
        metadata = {'sequencing_project': {'project_manager_cid': 1}}

        jira_users = jamo_common.JiraUsers()
        jira_users.user_map = {1: 'foo', 2: 'bar'}

        self.assertEqual(jira_users.set_watchers_templates(data, metadata), ({'foobar': 'foobar1'}, ['bar', 'baz2', 'foo']))

    def test_JiraUsers_set_assignee_in_jira_data(self):
        data = {'fields': {'assignee': {'name': '2'}}}

        jira_users = jamo_common.JiraUsers()
        jira_users.user_map = {1: 'foo', 2: 'bar'}

        self.assertEqual(jira_users.set_assignee_in_jira_data(data), {'fields': {'assignee': {'name': 'bar'}}})

    @parameterized.expand([
        ('get_pm_from_metadata', {'sequencing_project': {'project_manager_cid': 2}}, ['baz1', 'baz2', 'bar']),
        ('no_get_pm_from_metadata', None, ['baz1', 'baz2', 'foo']),
    ])
    def test_JiraUsers_add_pm_to_watchers(self, _description, metadata, expected):
        watchers = ['baz1', 'baz2']

        jira_users = jamo_common.JiraUsers()
        jira_users.user_map = {1: 'foo', 2: 'bar'}

        self.assertEqual(jira_users.add_pm_to_watchers(watchers, 'foo', metadata), expected)


if __name__ == '__main__':
    unittest.main()
