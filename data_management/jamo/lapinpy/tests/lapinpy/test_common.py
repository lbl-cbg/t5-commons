import unittest
from lapinpy import common
import datetime
from parameterized import parameterized
from bson.objectid import ObjectId


class TestCommon(unittest.TestCase):

    # Helper function for validating dict equality since the orderings are handled differently between PY2 and PY3
    def _assertEqual(self, actual, py2_data, py3_data):
        try:
            self.assertEqual(actual, py2_data)
        except Exception:
            self.assertEqual(actual, py3_data)

    def test_ValidationError(self):
        exception = Exception('foo')

        self.assertEqual(common.ValidationError(exception).error, exception)

    @parameterized.expand([
        ('str', 'Not found', '404:Not found'),
        ('list', ['Not found', 'May have been deleted'], '404:Not found,May have been deleted'),
    ])
    def test_HttpException(self, _description, message, expected):
        exception = common.HttpException(404, message)

        self.assertEqual(exception.code, 404)
        self.assertEqual(str(exception), expected)

    @parameterized.expand([
        ('dict', {'foo': 'bar'}, common.CustomDict),
        ('list', ['foo', 'bar'], common.CustomList),
        ('str', 'foo', str)
    ])
    def test_customtransform(self, _description, data, expected_type):
        self.assertTrue(isinstance(common.customtransform(data), expected_type))

    def test_prepend(self):
        def func(*args, **kwargs):
            self.assertEqual(args, (1, 2))
            self.assertEqual(kwargs, {'foo': 'foo1', 'bar': 'bar1'})

        common.prepend(1, foo='foo1')(func)(2, bar='bar1')

    def test_copy_args(self):
        @common.copy_args
        def func(self, name1, name2, name3='foo3', *args, **kwargs):
            pass

        func(self, 'foo1', name2='foo2')

        self.assertEqual(self.name1, 'foo1')
        self.assertEqual(self.name2, 'foo2')
        self.assertEqual(self.name3, 'foo3')

    @parameterized.expand([
        ('simple_key', {'foo': 'bar'}, 'foo', 'bar'),
        ('child_key_in_dict', {'foo': {'bar': 'baz'}}, 'foo.bar', 'baz'),
        ('child_key_not_in_dict', {'foo': {'bar.baz': 'bar'}}, 'foo.bar.baz', 'bar'),
        ('value_dict', {'foo': {'bar': 'bar1'}}, 'foo', common.CustomDict({'bar': 'bar1'})),
    ])
    def test_CustomDict_get_item(self, _description, data, key, expected):
        custom_dict = common.CustomDict(data)

        self.assertEqual(custom_dict.__getitem__(key), expected)

    def test_CustomDict_iter(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        iterator = custom_dict.__iter__()

        self.assertEqual(next(iterator), 'foo')
        self.assertEqual(next(iterator), 'bar')
        self.assertRaises(StopIteration, next, iterator)

    def test_CustomDict_keys(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(list(custom_dict.keys()), ['foo', 'bar'])

    def test_CustomDict_len(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.__len__(), 2)

    def test_CustomDict_get(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.get('foo'), 'foo1')

    def test_CustomDict_caller(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'},
                                        method=lambda *x, **y: x)
        custom_dict.on = 0
        custom_dict.methods = [lambda dic, *x, **y: dic[x[0]]]

        self.assertEqual(custom_dict._CustomDict__caller('foo', bar='bar1'), 'foo1')

    def test_CustomDict_getattr(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.__getattr__('__class__'), dict)
        self.assertEqual(custom_dict.__getattr__('foo'), 'foo1')

    def test_CustomDict_repr(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.__repr__(), "{'foo': 'foo1', 'bar': 'bar1'}")

    def test_CustomDict_contains(self):
        custom_dict = common.CustomDict({'foo': 'foo1', 'bar': 'bar1'})

        self.assertEqual(custom_dict.__contains__('foo'), True)

    def test_CustomList_getattr(self):
        custom_list = common.CustomList(['foo', 'bar'])

        self.assertEqual(custom_list.__getattr__('__len__')(), 2)

    def test_CustomList_getitem(self):
        custom_list = common.CustomList(['foo', 'bar'])

        self.assertEqual(custom_list.__getitem__(1), 'bar')

    def test_CustomList_iter(self):
        custom_list = common.CustomList(['foo', 'bar'])

        iterator = custom_list.__iter__()

        self.assertEqual(next(iterator), 'foo')
        self.assertEqual(next(iterator), 'bar')
        self.assertRaises(StopIteration, iterator.next)

    def test_Struct_repr(self):
        struct = common.Struct(foo='foo1', bar='bar1')

        self._assertEqual(struct.__repr__(),
                          "<foo : 'foo1'\n bar : 'bar1'\n entries : {'foo': 'foo1', 'bar': 'bar1'}>",
                          "<entries : {'foo': 'foo1', 'bar': 'bar1'}\n foo : 'foo1'\n bar : 'bar1'>")
        self.assertEqual(struct.foo, 'foo1')

    @parameterized.expand([
        ('str', 'foobar', ['foobar']),
        ('int_str', '1', [1]),
        ('bool_str', 'True', [True]),
        ('str_with_backslash', 'foo\\bar', ['foobar']),
        ('nested_str', 'foo(bar(baz))', ['foo', ['bar', ['baz']]]),
    ])
    def test_tokenize(self, _description, string, expected):
        self.assertEqual(common.tokenize(string), expected)

    @parameterized.expand([
        ('str', '62791a11c2c506c5afdfce76', ObjectId('62791a11c2c506c5afdfce76')),
        ('str_not_oid', 'not_oid', 'not_oid'),
        ('list', ['62791a11c2c506c5afdfce76'], [ObjectId('62791a11c2c506c5afdfce76')]),
        ('dict', {'_id': '62791a11c2c506c5afdfce76'}, {'_id': ObjectId('62791a11c2c506c5afdfce76')})
    ])
    def test_convertToOID(self, _description, obj, expected):
        self.assertEqual(common.convertToOID(obj), expected)

    @parameterized.expand([
        ('str', 'foo', 'foo'),
        ('list', ['foo', 'bar'], 'foo bar'),
        ('dict', {'foo': 'bar'}, "{'foo': 'bar'}"),
    ])
    def test_stringify_tokens(self, _description, tokens, expected):
        self.assertEqual(common.stringify_tokens(tokens), expected)

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
        self.assertEqual(common.toMongoObj(tokens), expected)

    @parameterized.expand([
        ('and_wrong_placement', 'and foo'),
        ('and_unexpected_end_of_query', 'foo and'),
        ('unexpected_end_of_query', 'foo bar'),
        ('invalid_comparison_operator', 'foo bar baz'),
        ('invalid_usage_is', 'foo is bar'),
    ])
    def test_toMongoObj_failures(self, _description, tokens):
        self.assertRaises(Exception, common.toMongoObj, tokens)

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
    ])
    def test_checkKey(self, _description, value, condition, expected):
        self.assertEqual(common.checkKey(value, condition), expected)

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

        self.assertEqual(common.checkMongoQuery(data, query),
                         expected)

    def test_getValue(self):
        self.assertEqual(common.getValue({'foo': {'bar': 'baz'}}, 'foo.bar'),
                         'baz')

    def test_evalString(self):
        self.assertEqual(common.evalString('foo:{foo}', {'foo': 'bar'}),
                         'foo:bar')

    def test_format_int(self):
        self.assertEqual(common.format_int(12345), '12,345')

    @parameterized.expand([
        ('with_decimals', 12345.6789, '12,345.678'),
        ('no_decimals', 12345, '12,345.000'),
    ])
    def test_format_float(self, _description, value, expected):
        self.assertEqual(common.format_float(value, 3), expected)

    @parameterized.expand([
        ('with_percent_fraction', 0.5, True, True, '50.000 %'),
        ('without_percent_not_fraction', 50, False, False, '50.000'),
    ])
    def test_format_percent(self, _description, value, is_fraction, include_symbol, expected):
        self.assertEqual(common.format_percent(value, is_fraction, 3, include_symbol),
                         expected)


if __name__ == '__main__':
    unittest.main()
