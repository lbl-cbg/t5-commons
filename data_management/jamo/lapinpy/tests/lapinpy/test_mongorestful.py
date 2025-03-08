import unittest
from lapinpy import mongorestful, common
from lapinpy.mongorestful import MongoRestful
from bson.objectid import ObjectId
from parameterized import parameterized
from pymongo import ReadPreference
from pymongo.read_preferences import Nearest
import datetime
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, MagicMock, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, MagicMock, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestMongoRestful(unittest.TestCase):

    def setUp(self):
        self.db = Mock()
        self.db_map = {'file': self.db, 'queries': self.db}
        self.mongorestful = MongoRestful('host', 'user', 'password', 'db', {'appname': 'foobar'})
        self.mongorestful.db = self.db_map
        self.mongorestful.cleanThread.cancel()

    @parameterized.expand([
        ('string', '62791a11c2c506c5afdfce76', ObjectId('62791a11c2c506c5afdfce76')),
        ('string_object_id', "ObjectId('62791a11c2c506c5afdfce76')", ObjectId('62791a11c2c506c5afdfce76')),
        ('invalid_string', '7df78ad8902', '7df78ad8902'),
        ('list', ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77'], [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]),
        ('dict', {'foo': '62791a11c2c506c5afdfce76', 'bar': '62791a11c2c506c5afdfce77'},
         {'foo': ObjectId('62791a11c2c506c5afdfce76'), 'bar': ObjectId('62791a11c2c506c5afdfce77')}),
    ])
    def test_convertToOID(self, _description, arg, expected):
        self.assertEqual(mongorestful.convertToOID(arg), expected)

    @parameterized.expand([
        ('key_not_in_dict', {'foo': 'bar'}, 'bar.baz', 'foo', True),
        ('key_in_dict_matches', {'foo': {'bar': {'baz': 1}}}, 'foo.bar', {'baz': 1}, False),
        ('key_in_dict_no_matches', {'foo': {'bar': {'baz': 1}}}, 'foo.bar', {'baz': 2}, True),
        ('key_not_in_list', [{'foo': 'bar'}], '0', 'foo', True),
        ('key_in_list', [{'foo': 'bar'}], '0', {'foo': 'bar'}, False),
        ('doc_not_list_or_dict', 1, '0', 2, True),
    ])
    def test_set_key(self, _description, doc, key, value, expected):
        self.assertEqual(mongorestful.set_key(doc, key, value), expected)

    @parameterized.expand([
        ('key_not_in_dict', {'foo': 'bar'}, 'bar.baz', False),
        ('key_in_dict', {'foo': {'bar': {'baz': 1}}}, 'foo.bar', True),
        ('key_not_in_list', [{'foo': 'bar'}], '0', True),
        ('key_in_list', [{'foo': 'bar'}], '0', True),
        ('doc_not_list_or_dict', 1, '0', False),
    ])
    def test_unset_key(self, _description, doc, key, expected):
        self.assertEqual(mongorestful.unset_key(doc, key, None), expected)

    @parameterized.expand([
        ('key_not_in_dict', {'foo': 'bar'}, 'bar.baz', 'bar', False),
        ('key_in_dict', {'foo': {'bar': {'baz': 1}}}, 'foo.bar', 'bar', True),
        ('key_in_dict_no_matches', {'foo': {'bar': {'baz': 1}}}, 'foo', 'bar', True),
        ('key_and_value_match', {'foo': {'bar': {'baz': 1}}}, 'foo', 'foo', False),
    ])
    def test_rename_key(self, _description, doc, key, value, expected):
        self.assertEqual(mongorestful.rename_key(doc, key, value), expected)

    def test_push_value(self):
        self.assertEqual(mongorestful.push_value(None, None, None), True)

    @parameterized.expand([
        ('key_not_in_dict', {'foo': {'bar': 1}}, 'foo.baz', 'bar', True),
        ('subdoc_not_list', {'foo': 1}, 'foo', 'bar', False),
        ('value_in_dict', {'foo': [1]}, 'foo', 1, False),
        ('value_not_in_dict', {'foo': [1]}, 'foo', 2, True),
        ('value_in_list', [['foo']], '0', 'foo', False),
        ('value_not_in_list', [['foo']], '0', 'bar', True),
        ('value_dict_found', {'foo': [1]}, 'foo', {'$each': [1]}, False),
        ('value_dict_not_found', {'foo': [1]}, 'foo', {'$each': [2]}, True),
        ('doc_not_list_or_dict', 1, '0', 1, False),
    ])
    def test_add_to_set(self, __description, doc, key, value, expected):
        self.assertEqual(mongorestful.add_to_set(doc, key, value), expected)

    def test_MongoRestful_save(self):
        self.db.save.return_value = '62791a11c2c506c5afdfce76'

        self.assertEqual(self.mongorestful.save('file', {'foo': 'bar'}), '62791a11c2c506c5afdfce76')
        self.db.save.assert_called_with({'foo': 'bar'})

    @parameterized.expand([
        ('file_matching_metadata',
         {'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2021, 12, 12), 'metadata': {'foo': 'bar'}},
         {'metadata_modified_date': datetime.datetime(2022, 1, 1, 0, 0), 'modified_date': datetime.datetime(2022, 2, 2, 0, 0), '_id': '62791a11c2c506c5afdfce76', 'metadata': {'foo': 'bar'}},
         [{'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2022, 1, 1), 'metadata': {'foo': 'bar'}, 'foobar': 1}]),
        ('file_non_matching_metadata',
         {'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2021, 12, 12), 'metadata': {'foo': 'baz'}},
         {'metadata_modified_date': datetime.datetime(2022, 2, 2, 0, 0),
          'modified_date': datetime.datetime(2022, 2, 2, 0, 0), '_id': '62791a11c2c506c5afdfce76', 'metadata': {'foo': 'baz'}},
         [{'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2022, 1, 1), 'metadata': {'foo': 'bar'},
           'foobar': 1}]),
        ('file_doc_data_equal',
         {'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2021, 12, 12), 'metadata': {'foo': 'bar'}},
         None,
         [{'_id': '62791a11c2c506c5afdfce76', 'metadata_modified_date': datetime.datetime(2022, 12, 12), 'metadata': {'foo': 'bar'}}]),
    ])
    @patch.object(mongorestful, 'datetime')
    def test_MongoRestful_smartSave(self, _description, data, expected_db_save_call, records, datetime_mock):
        cursor = MagicMock()
        cursor.__iter__.return_value = iter(records)
        cursor.__getitem__.return_value = cursor
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 2, 2)
        self.db.find.return_value = cursor
        self.db.save.return_value = '62791a11c2c506c5afdfce76'
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.mongorestful.smartSave('file', data),
                         '62791a11c2c506c5afdfce76')
        if expected_db_save_call:
            self.db.save.assert_called_with(expected_db_save_call)
        else:
            self.db.save.assert_not_called()

    def test_MongoRestful_stop(self):
        clean_thread = Mock()
        client = Mock()
        self.mongorestful.cleanThread = clean_thread
        self.mongorestful.client = client

        self.mongorestful.stop()

        clean_thread.cancel.assert_called()
        client.close.assert_called()

    def test_MongoRestful_get_howami(self):
        client = Mock()
        client.alive.return_value = True
        self.mongorestful.client = client

        self.assertEqual(self.mongorestful.get_howami(None, None), {'mongo_connection_alive': True})

    @patch.object(mongorestful, 'datetime')
    def test_MongoRestful_cleanCursors(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 2, 2)
        cursor_1 = Mock()
        cursor_2 = Mock()
        self.mongorestful.cursors = {'cursor_1': {'last_accessed': datetime.datetime(2022, 2, 1),
                                                  'timeout': 10, 'cursor': cursor_1},
                                     'cursor_2': {'last_accessed': datetime.datetime(2022, 2, 2),
                                                  'timeout': 10, 'cursor': cursor_2}}

        self.mongorestful.cleanCursors()

        # cursor_1 has been deleted
        cursor_1.close.assert_called()
        # cursor_2 still exists
        cursor_2.close.assert_not_called()
        self.assertEqual(self.mongorestful.cursors, {'cursor_2': {'last_accessed': datetime.datetime(2022, 2, 2),
                                                                  'timeout': 10, 'cursor': cursor_2}})

    def test_MongoRestful_find(self):
        self.db.find.return_value = [{'foo': 'foo1'}, {'bar': 'bar1'}]

        self.assertEqual(self.mongorestful.find('file', {'_id': '62791a11c2c506c5afdfce76'}, foo='bar'), [{'foo': 'foo1'}, {'bar': 'bar1'}])
        self.db.find.assert_called_with({'_id': ObjectId('62791a11c2c506c5afdfce76')}, foo='bar')

    def test_MongoRestful_update(self):
        self.db.update.return_value = '62791a11c2c506c5afdfce76'

        self.assertEqual(self.mongorestful.update('file', {'_id': '62791a11c2c506c5afdfce76'}, {'foo': 'bar'}),
                         '62791a11c2c506c5afdfce76')
        self.db.update.assert_called_with({'_id': ObjectId('62791a11c2c506c5afdfce76')}, {'foo': 'bar'}, multi=True)

    def test_MongoRestful_remove(self):
        self.mongorestful.remove('file', {'_id': '62791a11c2c506c5afdfce76'})

        self.db.remove.assert_called_with({'_id': ObjectId('62791a11c2c506c5afdfce76')})

    @parameterized.expand([
        ('string_db_return', '62791a11c2c506c5afdfce76'),
        ('list_db_return', ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77']),
    ])
    def test_MongoRestful_insert(self, _description, insert_return_value):
        self.db.insert.return_value = insert_return_value

        self.assertEqual(self.mongorestful.insert('file', {'foo': 'bar'}), insert_return_value)
        self.db.insert.assert_called_with({'foo': 'bar'})

    @patch.object(mongorestful, 'random')
    def test_MongoRestful_getRandomId(self, random_mock):
        random_mock.choice.return_value = 'A'

        self.assertEqual(self.mongorestful.getRandomId(5), 'A' * 5)

    @parameterized.expand([
        ('no_more_records', 10, False),
        ('more_records', 15, True),
    ])
    @patch.object(mongorestful, 'datetime')
    def test_MongoRestful_get_nextpage(self, _description, record_count, more_records, datetime_mock):
        expected = {'cursor_id': 'cursor_1',
                    'end': 10,
                    'record_count': record_count,
                    'records': [{'_id': '62791a11c2c506c5afdfce76', 'bar': 'bar1_func', 'foo': 'foo1_str'}],
                    'start': 6}

        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1'}]
        cursor = MagicMock()
        cursor.count.return_value = len(records)
        cursor.__iter__.return_value = iter(records)
        session_data = {'last_accessed': datetime.datetime(2022, 2, 1),
                        'timeout': 10, 'cursor': cursor,
                        'return_count': 5, 'end': 5,
                        'record_count': record_count, 'cursor_id': 'cursor_1',
                        'flatten': True,
                        'modifiers': {'foo': '{{value}}_str',
                                      'bar': lambda x, y: '{}_func'.format(y)}}
        self.mongorestful.cursors = {'cursor_1': session_data}
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 5, 5)

        self.assertEqual(self.mongorestful.get_nextpage(['cursor_1'], None), expected)
        if more_records:
            self.assertEqual(session_data.get('last_accessed'), datetime.datetime(2022, 5, 5))
            cursor.close.assert_not_called()
        else:
            cursor.close.assert_called()

    def test_MongoRestful_post_page(self):
        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1'},
                   {'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo2', 'bar': 'bar1'}]
        cursor = MagicMock()
        cursor.skip.return_value = cursor
        cursor.limit.return_value = cursor
        cursor.__iter__.return_value = iter(records)
        self.db.find.return_value = cursor
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.mongorestful.post_page(None, {'record_count': 10, 'start': 0, 'query': {'bar': 'bar1'},
                                                            'fields': ['foo', 'bar'], 'flatten': True, 'sort': ['bar'],
                                                            'collection': 'file'}), {'end': 9,
                                                                                     'record_count': 2,
                                                                                     'records': [{'_id': '62791a11c2c506c5afdfce76',
                                                                                                  'bar': 'bar1',
                                                                                                  'foo': 'foo1'},
                                                                                                 {'_id': '62791a11c2c506c5afdfce77',
                                                                                                  'bar': 'bar1',
                                                                                                  'foo': 'foo2'}],
                                                                                     'start': 0})

    @parameterized.expand([
        ('first_level', {'foo': 'bar'}, 'foo', 'bar'),
        ('second_level', {'foo': {'bar': 'baz'}}, 'foo.bar', 'baz'),
        ('not_found', {'foo': 'bar'}, 'baz', None),
    ])
    def test_MongoRestful_getvalue(self, _description, doc, field, expected):
        self.assertEqual(self.mongorestful._MongoRestful__getvalue(doc, field), expected)

    @parameterized.expand([
        ('first_level', {'foo': 'bar'}, 'foo', 'baz', {'foo': 'baz'}),
        ('second_level', {'foo': {'bar': 'baz'}}, 'foo.bar', 'foobar', {'foo': {'bar': 'foobar'}}),
    ])
    def test_MongoRestful_setvalue(self, _description, doc, field, value, expected):
        self.mongorestful._MongoRestful__setvalue(doc, field, value)

        self.assertEqual(doc, expected)

    @parameterized.expand([
        ('oid', 'oid', '62791a11c2c506c5afdfce76', ObjectId('62791a11c2c506c5afdfce76')),
        ('date', 'date', '2022-01-01', datetime.datetime(2022, 1, 1, 0, 0)),
        ('list', 'oid', ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77'], [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]),
        ('dict', 'oid', {'foo': '62791a11c2c506c5afdfce76', 'bar': '62791a11c2c506c5afdfce77'},
         {'foo': ObjectId('62791a11c2c506c5afdfce76'), 'bar': ObjectId('62791a11c2c506c5afdfce77')}),
        ('no_change', 'int', 5, 5)
    ])
    def test_MongoRestful_encode_value(self, _description, to_type, obj, expected):
        self.assertEqual(self.mongorestful.encode_value(obj, to_type), expected)

    @parameterized.expand([
        ('key_in_key_types', {'foo': '62791a11c2c506c5afdfce76', 'bar': '2022-01-01', 'baz': 'foobar'},
         {'foo': 'oid', 'bar': 'date'},
         {'bar': datetime.datetime(2022, 1, 1, 0, 0), 'foo': ObjectId('62791a11c2c506c5afdfce76'),
          'baz': 'foobar'}
         ),
        ('dict_value', {'foo': {'bar': '2022-01-01'}},
         {'bar': 'date'},
         {'foo': {'bar': datetime.datetime(2022, 1, 1, 0, 0)}}),
    ])
    def test_MongoRestful_encode_values(self, _description, obj, key_types, expected):
        self.assertEqual(self.mongorestful.encode_values(obj, key_types), expected)

    def test_MongoRestful_getUserQueries(self):
        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1', 'page': 'metadata-search2'},
                   {'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo2', 'bar': 'bar1', 'page': 'metadata-search2'}]
        cursor = MagicMock()
        cursor.__iter__.return_value = iter(records)
        cursor.__getitem__.return_value = cursor
        self.db.find.return_value = cursor
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.mongorestful.getUserQueries(None, {'user': 'foo'}, 'metadata-search2'),
                         records)
        self.db.find.assert_called_with({'page': 'metadata-search2', 'user': 'foo'})

    @parameterized.expand([
        ('remove_query', True),
        ('save', False)
    ])
    def test_MongoRestful_postUserQuery(self, _description, remove_query):
        kwargs = {'remove_query': remove_query, '_id': '62791a11c2c506c5afdfce76'}

        self.mongorestful.postUserQuery(None, kwargs)
        if remove_query:
            self.db.remove.assert_called_with({'_id': ObjectId('62791a11c2c506c5afdfce76')})
        else:
            self.db.save.assert_called_with({'_id': '62791a11c2c506c5afdfce76'})

    def test_MongoRestful_update_csv_list(self):
        self.assertEqual(self.mongorestful.update_csv_list([{'foo': 'foo1'}, {'bar': ['bar1', 'bar2']}]),
                         [{'foo': 'foo1'}, {'bar': ['"bar1,bar2"']}])

    @parameterized.expand([
        ('dict_query', {'query': {'bar': 'bar1', '_id': {'$in': ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77']}}, 'return_count': 10, 'sort': 'foo desc',
                        'fields': ['foo', 'bar'], 'page': 1},
         [{'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo2', 'bar': 'bar1'},
          {'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1'}],
         {'data': [{'_id': '62791a11c2c506c5afdfce77', 'bar': 'bar1', 'foo': 'foo2'},
                   {'_id': '62791a11c2c506c5afdfce76', 'bar': 'bar1', 'foo': 'foo1'}],
          'record_count': 2,
          'return_count': 2},
         call({'_id': {'$in': [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]},
               'bar': 'bar1'}, ['foo', 'bar'], sort=[('foo', -1)])),
        ('str_query', {'query': "bar = bar1 _id in ('62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77')", 'sort': 'foo asc',
                       'fields': ['foo', 'bar'], 'page': 1},
         [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1'},
          {'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo2', 'bar': 'bar1'}],
         {'data': [{'_id': '62791a11c2c506c5afdfce76', 'bar': 'bar1', 'foo': 'foo1'},
                   {'_id': '62791a11c2c506c5afdfce77', 'bar': 'bar1', 'foo': 'foo2'}],
          'record_count': 2,
          'return_count': 2},
         call({'bar': 'bar1',
               '_id': {'$in': [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]}},
              ['foo', 'bar'], sort=[('foo', 1)])
         ),
    ])
    def test_MongoRestful_queryResults_dataChange(self, _description, parameters, records, expected, find_db_call):
        cursor = MagicMock()
        cursor.skip.return_value = cursor
        cursor.limit.return_value = cursor
        cursor.__iter__.return_value = records
        self.db.find.return_value = cursor
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.mongorestful.queryResults_dataChange(parameters, 'file'), expected)
        self.assertIn(find_db_call, self.db.find.mock_calls)

    @parameterized.expand([
        ('str_what', "bar = 2022-01-02 _id in ('62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77')"),
        ('dict_what', {'bar': '2022-01-02', '_id': {'$in': ['62791a11c2c506c5afdfce76', '62791a11c2c506c5afdfce77']}}),
    ])
    @patch.object(mongorestful, 'random')
    def test_MongoRestful_pagequery(self, _description, what, random_mock):
        select = ['foo', 'bar']
        return_count = 50
        sort = ['foo', 1]
        modifiers = {'foo': '{{value}}_str',
                     'bar': lambda x, y: y + datetime.timedelta(1)}
        key_map = {'bar': {'type': 'date'}}
        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': datetime.datetime(2022, 1, 2)},
                   {'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo2', 'bar': datetime.datetime(2022, 1, 2)}]
        expected = {'cursor_id': 'A' * 10,
                    'end': 2,
                    'fields': ['foo', 'bar'],
                    'record_count': 2,
                    'records': [{'_id': '62791a11c2c506c5afdfce76',
                                 'bar': datetime.datetime(2022, 1, 3, 0, 0),
                                 'foo': 'foo1_str'},
                                {'_id': '62791a11c2c506c5afdfce77',
                                 'bar': datetime.datetime(2022, 1, 3, 0, 0),
                                 'foo': 'foo2_str'}],
                    'start': 1,
                    'timeout': 540}
        find_db_call = call({'_id': {'$in': [ObjectId('62791a11c2c506c5afdfce76'), ObjectId('62791a11c2c506c5afdfce77')]},
                             'bar': datetime.datetime(2022, 1, 2, 0, 0)}, ['foo', 'bar'])
        random_mock.choice.return_value = 'A'
        cursor = MagicMock()
        self.db.count_documents.return_value = len(records)
        cursor.__iter__.return_value = records
        self.db.find.return_value = cursor

        self.assertEqual(self.mongorestful.pagequery('file', what, select, return_count, sort, modifiers, key_map, True, 540),
                         expected)
        self.assertIn(find_db_call, self.db.find.mock_calls)

    def test_MongoRestful_flatten(self):
        self.assertEqual(self.mongorestful.flatten({'foo': {'bar': {'baz': 1}, 'foobar': True}}),
                         {'foo.bar.baz': 1, 'foo.foobar': True})

    def test_MongoRestful_findOne(self):
        self.db.find_one.return_value = {'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': datetime.datetime(2022, 1, 2)}

        self.assertEqual(self.mongorestful.findOne('file', _id='62791a11c2c506c5afdfce76'),
                         {'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': datetime.datetime(2022, 1, 2)})
        self.db.find_one.assert_called_with({'_id': '62791a11c2c506c5afdfce76'})

    @parameterized.expand([
        ('no_tqx_no_limit', {'_page': 1},
         [call.with_options(read_preference=Nearest(tag_sets=None, max_staleness=-1, hedge=None)),
          call.find({'foo': 'foo1'}),
          call.find().__getitem__(slice(0, 2, None))]),
        ('no_tqx_limit_set_to_1', {'_page': 1, 'limit': 1},
         [call.with_options(read_preference=Nearest(tag_sets=None, max_staleness=-1, hedge=None)),
          call.find({'foo': 'foo1'}),
          call.find().__getitem__(slice(0, 1, None))]),
        ('no_tqx_limit_set_to_none', {'limit': None},
         [call.with_options(read_preference=Nearest(tag_sets=None, max_staleness=-1, hedge=None)),
          call.find({'foo': 'foo1'}),
          call.find().__getitem__(slice(0, 2, None))]),
        ('tqx', {'tqx': '', 'tq': 'limit 10', 'sort': ['bar', 1]},
         [call.with_options(read_preference=Nearest(tag_sets=None, max_staleness=-1, hedge=None)),
          call.find({'foo': 'foo1'}),
          call.find().sort([('bar', 1)])])
    ])
    def test_MongoRestful_query(self, _description, extra_kwargs, expected_db_calls):
        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1'},
                   {'_id': '62791a11c2c506c5afdfce77', 'foo': 'foo1', 'bar': 'bar2'}]
        cursor = MagicMock()
        cursor.sort.return_value = cursor
        cursor.__iter__.return_value = iter(records)
        cursor.__getitem__.return_value = cursor
        self.db.with_options.return_value = self.db
        self.db.find.return_value = cursor
        self.db.count_documents.return_value = len(records)

        self.assertEqual(self.mongorestful.query('file', foo='foo1', _read_preference=ReadPreference.NEAREST,
                                                 **extra_kwargs),
                         [{'_id': '62791a11c2c506c5afdfce76', 'bar': 'bar1', 'foo': 'foo1'},
                          {'_id': '62791a11c2c506c5afdfce77', 'bar': 'bar2', 'foo': 'foo1'}])
        for db_call in expected_db_calls:
            self.assertIn(db_call, self.db.mock_calls)

    @parameterized.expand([
        ('no_tqx_page_with_limit_none', {'_page': 1, 'limit': None}),
        ('no_tqx_page_less_than_0', {'_page': 0}),
    ])
    def test_MongoRestful_query_http_exceptions(self, _description, kwargs):
        self.assertRaises(common.HttpException, self.mongorestful.query, 'file', **kwargs)

    def test_MongoRestful_post_approveupdate(self):
        self.mongorestful.post_approveupdate(['62791a11c2c506c5afdfce77'], {})

    @patch.object(mongorestful, 'datetime')
    def test_MongoRestful_smartUpdate(self, datetime_mock):
        what = {'_id': '62791a11c2c506c5afdfce76'}
        update = {'$set': {'metadata': {'foo': 'baz'}}, 'foo': 'foo2'}
        expected_db_call = call.update({'_id': {'$in': [ObjectId('62791a11c2c506c5afdfce76')]}}, {
            '$set': {'metadata_modified_date': datetime.datetime(2022, 2, 2, 0, 0),
                     'modified_date': datetime.datetime(2022, 2, 2, 0, 0),
                     'metadata': {'foo': 'baz'}}, 'foo': 'foo2'}, multi=True)

        records = [{'_id': '62791a11c2c506c5afdfce76', 'foo': 'foo1', 'bar': 'bar1', 'metadata': {'foo': 'bar'}}]
        cursor = MagicMock()
        cursor.__iter__.return_value = iter(records)
        self.db.find.return_value = cursor
        self.db.update.return_value = {'nModified': 1}
        datetime_mock.datetime.now.return_value = datetime.datetime(2022, 2, 2)

        self.assertEqual(self.mongorestful.smartUpdate('file', what, update), {'n': 1, 'nModified': 1, 'ok': 1.0})
        self.assertIn(expected_db_call, self.db.mock_calls)

    def test_MongoRestful_exchangeKeys(self):
        data = [{'foo': 'foo1'}, {'foo': 'foo2'}]
        expected = [{'baz': 'foo1'}, {'baz': 'foo2'}]

        self.assertEqual(self.mongorestful.exchangeKeys(data, {'foo': 'baz'}), expected)


if __name__ == '__main__':
    unittest.main()
