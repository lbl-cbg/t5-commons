import unittest
from lapinpy import mysqlrestful
from lapinpy.mysqlrestful import MySQLRestful
from parameterized import parameterized
from datetime import datetime
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestMysqlRestful(unittest.TestCase):

    def setUp(self):
        self.mysqlrestful = MySQLRestful('host', 'user', 'password', 'db')
        self.connection = Mock()
        self.mysqlrestful.connections = [self.connection]
        self.connect = Mock()
        self.connection.return_value = self.connect
        self.cursor = Mock()
        self.connection.cursor.return_value = self.cursor

    @parameterized.expand([
        ('null', '0000-', None),
        ('not_null', '2011-02-28 09:45:22', datetime(2011, 2, 28, 9, 45, 22)),
    ])
    def test_datetime_or_None(self, _description, value, expected):
        self.assertEqual(mysqlrestful.datetime_or_None(value), expected)

    def test_int64(self):
        self.assertEqual(mysqlrestful.int64('100'), 100)

    @patch.object(mysqlrestful, 'pymysql')
    def test_MySQLRestful_connect(self, pymysql):
        self.mysqlrestful.connections = []
        autocommit = Mock()
        pymysql.connect.return_value = autocommit

        self.mysqlrestful.connect()

        pymysql.connect.assert_called()
        autocommit.autocommit.assert_called_with(True)

    def test_MySQLRestful_getCvss(self):
        self.cursor.fetchall.return_value = [{'foo_table_id': 0, 'status': 'Foo', 'foo_table_desc': 'Foobar'},
                                             {'foo_table_id': 1, 'status': 'Bar', 'bar_table_desc': 'Barfoo'}]

        self.assertEqual(self.mysqlrestful.getCvs('foo_table', prepend=True, ints=True),
                         {'foo_table': {0: 'Foo', '0': 'Foo', 'Foo': 0, 1: 'Bar', '1': 'Bar', 'Bar': 1}})

    @parameterized.expand([
        ('alive', True),
        ('not_alive', False),
    ])
    def test_MySQLRestful_get_howami(self, _description, alive):
        self.connection.open = 1 if alive else 0

        self.assertEqual(self.mysqlrestful.get_howami(None, None), {'mysql_connection_alive': alive})

    def test_MySQLRestful_query(self):
        self.cursor.fetchall.return_value = [{'foo_table_id': 0, 'status': 'Foo', 'foo_table_desc': 'Foobar', '.to_be_removed': 1},
                                             {'foo_table_id': 1, 'status': 'Bar', 'bar_table_desc': 'Barfoo', '.to_be_removed': 2}]

        self.assertEqual(self.mysqlrestful.query('select * from cv where status in("%s", "%s")',
                         ['Foo', 'Bar'], extras={'tq': 'desc', 'sort': 'status'}),
                         [{'status': 'Foo', 'foo_table_id': 0, 'foo_table_desc': 'Foobar'},
                          {'status': 'Bar', 'bar_table_desc': 'Barfoo', 'foo_table_id': 1}])
        self.cursor.execute.assert_called_with(
            'select * from cv where status in("%s", "%s") order by status  desc limit 500', ['Foo', 'Bar'])

    def test_MySQLRestful_delete(self):
        self.cursor.execute.return_value = 1

        self.assertEqual(self.mysqlrestful.delete('delete from cv where status in("%s", "%s")', 'Foo', 'Bar'), 1)
        self.cursor.execute.assert_called_with('delete from cv where status in("%s", "%s")', ('Foo', 'Bar'))

    def test_MySQLRestful_smart_insert(self):
        self.cursor.lastrowid = 10

        self.assertEqual(self.mysqlrestful.smart_insert('foobar', {'foo': 1, 'bar': 'baz'}), 10)
        self.cursor.execute.assert_called_with('insert into foobar ( foo, bar) values (%s,%s)', [1, 'baz'])

    def test_MySQLRestful_smart_modify(self):
        self.cursor.execute.return_value = 1

        self.assertEqual(self.mysqlrestful.smart_modify('foobar', 'status<10', {'foo': 1, 'bar': 'baz'}), 1)
        self.cursor.execute.assert_called_with('update foobar set  foo=%s, bar=%s where status<10', [1, 'baz'])

    def test_MySQLRestful_modify(self):
        self.cursor.execute.return_value = 1

        self.assertEqual(self.mysqlrestful.modify('update foobar set foo=%s, bar=%s', 1, 'baz'), 1)
        self.cursor.execute.assert_called_with('update foobar set foo=%s, bar=%s', (1, 'baz'))

    def test_MySQLRestful_parse_default_query(self):
        query, select_count = self.mysqlrestful.parse_default_query('select * from foobar',
                                                                    'select count(*) from foobar',
                                                                    {'query': 'status>10 and foo like "foo" and status nin (1, 3)'})

        self.assertEqual(query, 'select * from foobar where status>10 and foo like "%foo%" and status not in (1, 3)')
        self.assertEqual(select_count, 'select count(*) from foobar where status>10 and foo like "%foo%" and status not in (1, 3)')

    def test_MySQLRestful_construct_query(self):
        query, select_count = self.mysqlrestful.construct_query('foobar',
                                                                {'query': 'status>10', 'fields': ['foo', 'bar'],
                                                                 'id_field': 'id', 'sort': 'status'},
                                                                10)

        self.assertEqual(query, 'select foo,bar,id from foobar  where status>10 order by status limit 0,10')
        self.assertEqual(select_count, 'select count(*) as record_count from foobar  where status>10')

    def test_MySQLRestful_queryResults_dataChange_restful(self):
        self.cursor.fetchall.return_value = [{'foo_table_id': 0, 'status': 11, 'foo_table_desc': 'Foobar'},
                                             {'foo_table_id': 1, 'status': 15, 'bar_table_desc': 'Barfoo'}]

        self.assertEqual(self.mysqlrestful.queryResults_dataChange({'query': 'status>10', 'fields': ['foo', 'bar'],
                                                                    'id_field': 'id', 'sort': 'status'}, 'foobar'),
                         [{'status': 11, 'foo_table_id': 0, 'foo_table_desc': 'Foobar'},
                          {'status': 15, 'bar_table_desc': 'Barfoo', 'foo_table_id': 1}])
        self.cursor.execute.assert_called_with('select foo,bar,id from foobar  where status>10 order by status limit 0,100')

    def test_MySQLRestful_queryResults_dataChange_ui(self):
        self.cursor.fetchall.side_effect = [[{'record_count': 5}], [{'foo_table_id': 0, 'status': 11, 'foo_table_desc': 'Foobar'},
                                                                    {'foo_table_id': 1, 'status': 15, 'bar_table_desc': 'Barfoo'}]]
        self.cursor.execute.return_value = [{'record_count': 5}]

        self.assertEqual(self.mysqlrestful.queryResults_dataChange({'query': 'status>10', 'fields': ['foo', 'bar'],
                                                                    'id_field': 'id', 'sort': 'status', '__ui': 'ui',
                                                                    'queryResults': ['result']}, 'foobar'),
                         {'data': [{'foo_table_desc': 'Foobar', 'foo_table_id': 0, 'status': 11},
                                   {'bar_table_desc': 'Barfoo', 'foo_table_id': 1, 'status': 15}],
                          'record_count': 5,
                          'return_count': 100})
        self.cursor.execute.assert_called_with('select foo,bar,id from foobar  where status>10 order by status limit 0,100')


if __name__ == '__main__':
    unittest.main()
