import unittest
from io import StringIO
from datetime import datetime
import os
from bson.objectid import ObjectId
from parameterized import parameterized
from lapinpy import restful
from lapinpy import common
## PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
## PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, call, PropertyMock
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, call, PropertyMock
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


def set_attribute(key, value):
    def outer(func):
        setattr(func, key, value)
        return func

    return outer


class TestRestful(unittest.TestCase):

    class Application(object):
        def get_bar(self, args, kwargs):
            return {'args': args, 'kwargs': kwargs}

    def setUp(self):
        self.restful = restful.Restful(host='some_host')
        self.app = self.Application()
        self.restful.map_submod('foo', self.app)
        self.core = Mock()
        self.restful.core = self.core
        self.restful.appname = 'foo'
        self.connection = Mock()
        self.restful.connect = lambda: self.connection

    # Helper function for validating dict equality since the orderings are handled differently between PY2 and PY3
    def _assertEqual(self, actual, py2_data, py3_data):
        try:
            self.assertEqual(actual, py2_data)
        except Exception:
            self.assertEqual(actual, py3_data)

    def test_Restful_map_submod(self):
        self.restful.map_submod('bar', self.app)

        self.assertEqual(self.app.address, '/lapinpy.restful/bar')
        self.assertEqual(self.restful.mod_mappings.get('bar'), self.app)
        for method in 'post', 'put', 'get', 'delete':
            self.assertTrue(hasattr(self.restful, '{}_bar'.format(method)))

    def test_Restful_call_submodule(self):
        func = self.restful.call_submodule('get', 'foo')
        self.assertEqual(func(['bar', 'foobar'], {'foo': 'bar'}), {'args': ['foobar'], 'kwargs': {'foo': 'bar'}})

    @parameterized.expand([
        ('with_mod_mappings', 'bar', {'args': ['bar', 'foobar'], 'kwargs': {'foo': 'bar'}}),
        ('without_mod_mappings', '', {'args': ['foobar'], 'kwargs': {'foo': 'bar'}}),
    ])
    def test_Restful_getrestmethod(self, _description, method_name, expected):
        func, code = self.restful.getrestmethod('get', ['foo', method_name])

        self.assertEqual(func(['bar', 'foobar'], {'foo': 'bar'}), expected)

    def test_Restful_stop(self):
        # Currently a no-op method
        self.restful.stop()

    @patch.object(restful, 'RestServer')
    def test_Restful_getCore(self, server):
        self.restful.core = None
        core = Mock()
        server.Instance.return_value = core

        self.restful.getCore().foo()

        self.assertIn(call.core.foo(), core.mock_calls)

    def test_Restful_getSetting(self):
        self.restful.getSetting('bar', 'baz')

        self.core.getSetting.assert_called_with('foo', 'bar', 'baz')

    def test_Restful_saveSetting(self):
        self.restful.saveSetting('bar', 'baz')

        self.core.saveSetting.assert_called_with('foo', 'bar', 'baz')

    def test_Restful_get_connection_none_cached(self):
        self.assertEqual(self.restful.get_connection(), self.connection)

    def test_Restful_get_connection_cached(self):
        connection = Mock()
        self.restful.connections.append(connection)

        self.assertEqual(self.restful.get_connection(), connection)
        self.assertEqual(len(self.restful.connections), 0)

    def test_Restful_put_connection(self):
        connection = Mock()

        self.restful.put_connection(connection)

        self.assertIn(connection, self.restful.connections)

    @patch.object(restful, 'RestServer')
    def test_Restful_error(self, server):
        self.restful.error(500, 'foobar')

        server.Instance().error.assert_called_with(500, 'foobar')

    def test_Restful_getCvs(self):
        query = Mock()
        query.side_effect = [[{'foo_table_id': 0, 'status': 'Foo', 'foo_table_desc': 'Foobar'},
                             {'foo_table_id': 1, 'status': 'Bar', 'bar_table_desc': 'Barfoo'}]]
        self.restful.query = query

        self.assertEqual(self.restful.getCvs('foo_table', prepend=True, ints=True),
                         {'foo_table': {0: 'Foo', '0': 'Foo', 'Foo': 0, 1: 'Bar', '1': 'Bar', 'Bar': 1}})

    def test_ExternalButton_produce(self):
        button = restful.ExternalButton('foobar', 'http://foobar.com', {'foo': 'foo1'}, False, ['bar'], {'baz': 'baz1'})

        self._assertEqual(button.produce({'foo1': 'foo2', 'bar': 'bar1'}),
                          '<form action="http://foobar.com" method="get"><input type="hidden" name="baz" value="baz1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="bar" value="bar1"><input type="submit" value="foobar"></form>',
                          '<form action="http://foobar.com" method="get"><input type="hidden" name="bar" value="bar1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="baz" value="baz1"><input type="submit" value="foobar"></form>')

    def test_Button_produce_url(self):
        button = restful.Button('foobar', 'http://foobar.com', 'bar', baz='baz1')

        self._assertEqual(button.produce({'foo1': 'foo2', 'bar': 'bar1'}, None, None, {'foo': 'foo1'}, False),
                          '<form action="http://foobar.com" method="get"><input type="hidden" name="baz" value="baz1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="bar" value="bar1"><input type="submit" value="foobar"></form>',
                          '<form action="http://foobar.com" method="get"><input type="hidden" name="bar" value="bar1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="baz" value="baz1"><input type="submit" value="foobar"></form>')

    @parameterized.expand([
        ['get'],
        ['post'],
    ])
    @patch.object(restful, 'Curl')
    def test_Button_produce_callback(self, method, curl):
        def foo():
            pass

        curl_mock = Mock()
        curl.return_value = curl_mock
        foo.address = '/foo'
        foo.__name__ = '{}_foo'.format(method)

        button = restful.Button('foobar', foo, 'bar', baz='baz1')

        self._assertEqual(button.produce({'foo1': 'foo2', 'bar': 'bar1'}, '/home', None, {'foo': 'foo1'}, True),
                          '<form action="/api/foo" method="{}"><input type="hidden" name="XXredirect_internalXX" value="/home"><input type="hidden" name="baz" value="baz1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="bar" value="bar1"><input type="submit" value="foobar"></form>'.format(
                              method),
                          '<form action="/api/foo" method="{}"><input type="hidden" name="XXredirect_internalXX" value="/home"><input type="hidden" name="bar" value="bar1"><input type="hidden" name="foo" value="foo2"><input type="hidden" name="baz" value="baz1"><input type="submit" value="foobar"></form>'.format(
                              method))
        curl.assert_called_with('/api/foo')
        if method == 'get':
            curl_mock.get.assert_called_with('', data={'baz': 'baz1', 'foo': 'foo2', 'bar': 'bar1'})
        else:
            curl_mock.post.assert_called_with('', data={'baz': 'baz1', 'foo': 'foo2', 'bar': 'bar1'})

    def test_Selection_produce(self):
        def get_foo():
            pass

        get_foo.address = '/foo'

        selection = restful.Selection('foobar', 'foo', {'option_1': 'value_1'}, get_foo, 'bar')

        self.assertEqual(selection.produce({'bar': 'bar_value'}, '/home', None),
                         '<form action="/api/foo" method="get"><input type="hidden" name="XXredirect_internalXX" value="/home"><select class="selection" name="foo"><option value="option_1">value_1</option></select><input type="hidden" name="bar" value="bar_value"><input type="submit" value="foobar"></form>')

    @parameterized.expand([
        ('generated_data', [['value_1_1', 'value_1_2'], ['value_2_1', 'value_2_2']], None),
        ('result_desc', None, {'last_page': 1, 'total': 2, 'total_formatted': '2',
                               'tbody': '<tr data-id="id_1" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_1_1</td><td>value_1_2</td></tr><tr data-id="id_2" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_2_1</td><td>value_2_2</td></tr>'}),
    ])
    def test_QueryResults_create_html(self, _description, generated_data, result_desc):
        with patch('uuid.UUID.hex', new_callable=PropertyMock) as uuid:
            uuid.side_effect = ['id_1', 'id_2', 'id_3', 'id_4']

            query_results = restful.QueryResults()

            self.assertEqual(
                query_results.create_html([['column_1', 'col1_value'], ['column_2', 'col2_value']], 2, 'foobar', '_id',
                                          generated_data, result_desc,
                                          '<div>some_filter</div>', '<div><button>Action</button></div>',
                                          True, True, True, actions='<form>action</form>'),
                {'headers': ['column 1', 'column 2'],
                 'html': '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">foobar</h3>\n\t<div>some_filter</div>\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t<div class="download_results" title="Download All Results">\n\t<i class="download material-icons md-18" ng-class="(result_desc.total == 0)?\'disabled\':\'\'" ng-click="download_query()"/>&#xE2C4;</i>\n</div>   \t\n       \t<div class="result_desc"><span>Total: 2</span></div>\n       \t<div action="multiSelect">\n     <div class="pipe_seperate"></div>\n     <div class="selected"><span>Selected: <span class="selected_count" ng-class="(select.count == 0)?\'disabled\':\'\'" class="selected" ng-click="view_selected()" title="View Selected Results">{{select.info}}</span></span></div>\n</div>  \n    </div>\n    <div class="result_pager">\n\t<div class="first_func page_func disabled" title="First Page"><div class="left_arrow"></div><div class="left_arrow"></div></div>\n    <div class="prev_func page_func disabled" title="Prev Page"><div class="left_arrow"></div></div>\n    <div class="page"><input value="1"/></div>\n    <div class="next_func page_func disabled" title="Next Page"><div class="right_arrow"></div></div>\n    <div class="last_func page_func disabled" title="Last Page"><div class="right_arrow"></div><div class="right_arrow"></div></div>\n</div>\n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="column_1" >column 1</th><th name="column_2" >column 2</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="2"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="id_1" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_1_1</td><td>value_1_2</td></tr><tr data-id="id_2" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_2_1</td><td>value_2_2</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t<div><button>Action</button></div>\n    <form>action</form>\n</div>\n',
                 'result_desc': {'last_page': 1, 'total': 2, 'total_formatted': '2'},
                 'tbody': '<tr data-id="id_1" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_1_1</td><td>value_1_2</td></tr><tr data-id="id_2" data-name="_id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>value_2_1</td><td>value_2_2</td></tr>'})

    @parameterized.expand([
        ('list_options', ['opt1', 'opt2']),
        ('dict_options', {'opt1': 'foo', 'opt2': {'value': 'opt2'}}),
    ])
    def test_QueryResults_build_select_options(self, _description, options):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__build_select_options(options, 'opt2', True),
                         '<option value=""></option><option value="opt1">Opt1</option><option value="opt2" selected>Opt2</option>')

    def test_QueryResults_build_select(self):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__build_select('foo', ['opt1', 'opt2'], 'bar', True),
                         '<select name="foo" onBlur="save_edit()"><option value=""></option><option value="opt1">Opt1</option><option value="opt2">Opt2</option></select>')

    @parameterized.expand([
        ('filter', {'options': ['opt1', 'opt2']}, '<div class="search_filter" ng-keydown="keypressed($event)">\n\t<form ng-submit="dataChange(1, true)">\n\t\t<div class="saved_query" ng-keydown="keypressed($event)">\n\t<h4>Saved Queries</h4>\n\t<ul>\n\t\t<li><div class="delete" title="Delete">X</div><a href="#" title="saved query 1">query1</a></li>\n\t</ul>\t\n</div>\n\t\t<div class="query">\n\t\t\t<h4>Query</h4>\n\t\t\t<ul>\n\t\t\t\t<li ng-repeat="condition in conditions">\n\t\t\t\t\t<div class="delete" ng-click="remove_filter($index)" title="Remove condition from query">X</div>\n\t\t\t\t\t<a class="condition" value="{{condition.value}}" href="#" ng-click="editFilter($index, condition.key, condition.operator, condition.value, condition.type)" title="Edit Filter">{{get_condition_key_display(condition.key)}} <span class="uppercase">{{condition.operator}}</span> {{condition.value}}</a>\n\t\t\t\t</li>\n\t\t\t</ul>\n\t\t\t<div class="add_query_value">\n                   <select class="filterkey minput" name="key" ng-model="current_filter.key" ng-change="check_type()"><option value="opt1">Opt1</option><option value="opt2">Opt2</option></select>\n                   <select class="filterop minput" name="op" ng-model="current_filter.operator"><option value="=">=</option><option value="&ne;">&ne;</option><option value="<"><</option><option value="<="><=</option><option value=">">></option><option value=">=">>=</option><option value="like">like</option><option value="in">in</option><option value="not in">not in</option></select>\n                   <input class="filterval minput" type="text" name="value" ng-click="value_click()" ng-model="current_filter.value" placeholder="filter" spellcheck="false" />\n                   <input type="hidden" class="minput" disabled/>\n                   <button type="submit" class="button" ng-disabled="!(current_filter.key)" ng-click="add_filter()">Add</button>\n\t\t\t</div>\t\n\t\t</div>\t\t\t\t\t\t\t\n\t\t<div class="actions">\n\t\t\t<button type="submit" class="button" ng-disabled="!filter_enabled || (last_query == query)" value="Filter">Filter</button>\n\t\t\t<button type="button" class="save_query button" disabled="disabled" ng-click="save_query_details()" title="Save Query">Save Query</button>\n\t\t</div>\n\t</form>\n</div>\n'),
        ('no_filter', None, '')
    ])
    def test_QueryResults_create_filter(self, _description, filter, expected):
        query_results = restful.QueryResults()

        self.assertEqual(
            query_results.create_filter(filter, [{'description': 'saved query 1', 'query': '', 'name': 'query1'}]),
            expected)

    def test_QueryResults_create_results_info(self):
        query_results = restful.QueryResults()

        self.assertEqual(query_results.create_results_info(True, True, True, 100),
                         '<div class="result_info">\n  \t<div class="result_total">\n       \t<div class="download_results" title="Download All Results">\n\t<i class="download material-icons md-18" ng-class="(result_desc.total == 0)?\'disabled\':\'\'" ng-click="download_query()"/>&#xE2C4;</i>\n</div>   \t\n       \t<div class="result_desc"><span>Total: 100</span></div>\n       \t<div action="multiSelect">\n     <div class="pipe_seperate"></div>\n     <div class="selected"><span>Selected: <span class="selected_count" ng-class="(select.count == 0)?\'disabled\':\'\'" class="selected" ng-click="view_selected()" title="View Selected Results">{{select.info}}</span></span></div>\n</div>  \n    </div>\n    <div class="result_pager">\n\t<div class="first_func page_func disabled" title="First Page"><div class="left_arrow"></div><div class="left_arrow"></div></div>\n    <div class="prev_func page_func disabled" title="Prev Page"><div class="left_arrow"></div></div>\n    <div class="page"><input value="1"/></div>\n    <div class="next_func page_func disabled" title="Next Page"><div class="right_arrow"></div></div>\n    <div class="last_func page_func disabled" title="Last Page"><div class="right_arrow"></div><div class="right_arrow"></div></div>\n</div>\n</div>\t   \t')

    @parameterized.expand([
        ('desc_direction', {'column': 'header_name', 'direction': 'desc'},
         ('<table><th name="header_name" class="sort">header<span class="sort_indicator arrow down"></span></th>',
          ['header1', 'header2', 'header'])),
        ('asc_direction', {'column': 'header_name', 'direction': 'asc'},
         ('<table><th name="header_name" class="sort">header<span class="sort_indicator arrow up"></span></th>',
          ['header1', 'header2', 'header'])),
        ('no_default', None,
         ('<table><th name="header_name" class="sort">header<span class="sort_indicator "></span></th>',
          ['header1', 'header2', 'header'])),
    ])
    def test_QueryResults_process_common_th(self, _description, default, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._process_common_th('<table>', 'header_name', 'header', ['header1', 'header2'],
                                                          True, default), expected)

    @parameterized.expand([
        ('no_grouped_columns', [['col1', {'type': 'percent', 'title': 'Column 1'}], ['col2', {}]],
         {'enabled': True, 'default': {'column': 'col2', 'direction': 'asc'}},
         None,
         ('<tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="col1" class="sort">% Column 1<span class="sort_indicator "></span></th><th name="col2" class="sort">col2<span class="sort_indicator arrow up"></span></th></tr>',
          ['% Column 1', 'col2'])
         ),
        ('grouped_columns', [['col1', {'type': 'percent', 'title': 'Column 1', 'header_group': 'col1'}], ['col2', {}]],
         {'enabled': True, 'default': {'column': 'col2', 'direction': 'asc'}},
         None,
         ('<tr class="group"><th colspan="1">col1</th><th></th></tr><tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="col1" class="sort">% Column 1<span class="sort_indicator "></span></th><th name="col2" class="sort">col2<span class="sort_indicator arrow up"></span></th></tr>',
          ['% Column 1', 'col2'])
         ),
        ('no_columns', [],
         {'enabled': True, 'default': {'column': 'col2', 'direction': 'asc'}},
         {'col_1': 'foo', 'col2': 'bar'},
         ('<tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="col_1" class="sort">col 1<span class="sort_indicator "></span></th><th name="col2" class="sort">col2<span class="sort_indicator arrow up"></span></th></tr>',
          ['col 1', 'col2'])
         ),
    ])
    def test_QueryResults_create_thead(self, _description, columns, sort, first_row, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results.create_thead(columns, True, sort, first_row),
                         expected)

    @parameterized.expand([
        ('not_generated_data', [{'id': 'id_1', 'col1': 'foo', 'col2': 3}],
         [['col1', {'type': 'string'}], ['col2', {'type': 'int'}]], False,
         '<tr data-id="id_1" data-name="id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>foo</td><td><div type="int">3</div></td></tr>'),
        ('generated_data', [['foo', '3']],
         [['col1', {'type': 'string'}], ['col2', {'type': 'int'}]], True,
         '<tr data-id="id_1" data-name="id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>foo</td><td>3</td></tr>'),
        ('no_columns', [{'col1': 'foo', 'col2': 3}],
         [], True,
         '<tr data-id="id_1" data-name="id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>3</td><td>foo</td></tr>',
         '<tr data-id="id_1" data-name="id"><td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td><td>foo</td><td>3</td></tr>'),
    ])
    def test_QueryResults_create_tbody(self, _description, dataset, columns, generated_data, expected_py2,
                                       expected_py3=None):
        query_results = restful.QueryResults()

        with patch('uuid.UUID.hex', new_callable=PropertyMock) as uuid:
            uuid.return_value = 'id_1'

            self._assertEqual(query_results.create_tbody('id', dataset,
                                                         columns, True, generated_data),
                              expected_py2, expected_py3)

    def test_QueryResults_create_select_actions(self):
        query_results = restful.QueryResults()

        self.assertEqual(query_results.create_select_actions({'actions': [{'name': 'action_1'}]}),
                         '<div class="select_actions"><div class="action_buttons"><button disabled="disabled" type="button" class="button" title="action_1" ng-click="confirm_select_action({\'name\': \'action_1\'})">action_1</button></div></div>')

    def test_QueryResults_create_actions(self):
        query_results = restful.QueryResults()

        self.assertEqual(query_results.create_actions('<form action="/api/foo" method="get"></form>'),
                         '<div class="actions"><div class="action_buttons"><form action="/api/foo" method="get"></form></div></div>')

    @parameterized.expand([
        ('input_in_row', 'foo'),
        ('dynamic', '{{foo}}'),
        ('constant', 'bar')
    ])
    def test_QueryResults_get_input_value(self, _description, input):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__get_input_value({'foo': 'bar'}, input),
                         'bar')

    @parameterized.expand([
        ('bool', True,
         '<div class="bool button" value="My button" title="My button"  data="foo" method="post" url="/button_action">value_1</div>'),
        ('not_bool', False,
         '<input class="button" type="submit" value="value_1" title="My button" method="post" url="/button_action" data="foo"/>'),
    ])
    def test_QueryResults_create_button(self, _description, is_bool, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__create_button('value_1', 'My button', 'post', '/button_action',
                                                                    ' data="foo"', is_bool),
                         expected)

    @parameterized.expand([
        (True, (True, '&#10003;')),
        (False, (False, 'x')),
    ])
    def test_QueryResults_get_bool_text(self, value, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__get_bool_text(value), expected)

    @parameterized.expand([
        ('check', 1),
        ('no_check', 0),
    ])
    def test_QueryResults_generate_bool(self, _description, checked):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__generate_bool(checked, 'some text', 'bool_class', 'data="foo"'),
                         '<div class="bool bool_class" value="{}" title="{}" data="foo">some text</div>'
                         .format(bool(checked), bool(checked)))

    @parameterized.expand([
        ('toggle', True, {'method': 'POST', 'data': {'baz': 'foobar'}},
         '<div class="bool button" value="True" title="True"  data-baz="foobar" method="POST" url="js">&#10003;</div>'),
        ('link', 'foobar1',
         {'text': 'foo', 'title': 'My Title', 'url': '/api'},
         '<a title="My Title" href="/api">bar</a>'),
        ('text', 'foobar1',
         {'text': 'foo', 'title': 'My Title', 'url': '/api'},
         'foobar1'),
        ('condition', 'foobar1', {'condition': 'False'}, 'foobar1'),
    ])
    def test_QueryResults_get_value_cell_action(self, type, value, inputs, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__get_value_cell_action(value, type, {'foo': 'bar'},
                                                                            inputs), expected)

    @parameterized.expand([
        ('textbox', 'key', 'value', {}, ''),
        ('textarea', 'key', 'value', {}, ''),
        ('select', 'key', 'value', {'options': ['opt1', 'opt2']},
         '<select name="key" onBlur="save_edit()"><option value=""></option><option value="opt1">Opt1</option><option value="opt2">Opt2</option></select>'),
    ])
    def test_QueryResults_get_value_cell_edit(self, type, key, value, inputs, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__get_value_cell_edit(key, value, type, {}, inputs),
                         expected)

    @parameterized.expand([
        ('button', 'key1', 'value1', {},
         '<input class="button" type="submit" value="value1" title="value1" method="GET" url="js"/>'),
        ('link', 'key1', 'value1', {}, '<a title="value1" href="js">value1</a>'),
        ('toggle', 'key1', 'value1', {},
         '<div class="bool button" value="value1" title="value1"  method="GET" url="js">&#10003;</div>'),
        ('textbox', 'key1', 'value1', {}, ''),
        ('select', 'key1', 'value1', {'options': ['opt1', 'opt2']},
         '<select name="key1" onBlur="save_edit()"><option value=""></option><option value="opt1">Opt1</option><option value="opt2">Opt2</option></select>'),
        ('textarea', 'key1', 'value1', {}, ''),
        ('none_value', 'key1', None, {}, ''),
        ('none_value_string', 'key1', 'None', {}, ''),
        ('none_value', 'key1', None, {}, ''),
        ('number', 'key1', 1, {}, '<div type="int">1</div>'),
        ('int', 'key1', 1, {}, '<div type="int">1</div>'),
        ('float', 'key1', 1.5, {}, '<div type="int">1.50</div>'),
        ('float', 'key1', 1.5, {'decimal_pnts': 5}, '<div type="int">1.50000</div>'),
        ('percent', 'key1', 25, {}, '<div type="int">25.00</div>'),
        ('percent_frac', 'key1', 0.25, {}, '<div type="int">25.00</div>'),
        ('bool', 'key1', False, {}, '<div class="bool " value="False" title="False" >x</div>'),
        ('boolean', 'key1', True, {}, '<div class="bool " value="True" title="True" >&#10003;</div>'),
        ('date', 'key1', datetime(2000, 1, 2), {}, '01/02/2000'),
    ])
    def test_QueryResults_convert_for_type(self, type, key, value, inputs, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__convert_for_type(key, value, type, {}, inputs, None),
                         expected)

    @patch.object(restful, 'RestServer')
    def test_QueryResults_convert_for_type_func(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__convert_for_type('', '', 'func', {}, {},
                                                                       {'page': 'api', 'function': 'my_func'}),
                         {'foo': 'bar'})
        server.run_method.assert_called_with('api', 'my_func')

    @parameterized.expand([
        ('title', 'Delete Row', '<div class="delete" ng-click="delete_row" title="Delete Row">X</div>'),
        ('no_title', None, '<div class="delete" ng-click="delete_row" title="Delete">X</div>'),
    ])
    def test_QueryResults_create_delete(self, _description, title, expected):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__create_delete('delete_row', title),
                         expected)

    def test_QueryResults_create_saved_queries(self):
        query_results = restful.QueryResults()

        self.assertEqual(query_results._QueryResults__create_saved_queries([{'description': 'Some description',
                                                                             'query': 'unused',
                                                                             'name': 'Some query'}]),
                         '<li><div class="delete" title="Delete">X</div><a href="#" title="Some description">Some query</a></li>')

    @patch.object(restful, 'open')
    def test_QueryResults_get_template(self, open):
        read = StringIO(u'<html><body>Foo</body></html')
        open.return_value = read
        query_results = restful.QueryResults()

        self.assertEqual(query_results.get_template('some_page.html'),
                         '<html><body>Foo</body></html')

    def test_copyattrs(self):
        class Data():
            pass

        src = Data()
        src.one = 1
        src.two = '2'
        src.__three = 3
        src.func_four = 4
        dest = Data()

        restful.copyattrs(src, dest)

        self.assertTrue(hasattr(dest, 'one'))
        self.assertTrue(hasattr(dest, 'two'))
        self.assertFalse(hasattr(dest, '__three'))
        self.assertFalse(hasattr(dest, 'func_four'))

    def test_menu(self):
        @restful.menu('menu_item', 3)
        def func():
            pass

        self.assertEqual(func.menuname, 'menu_item')
        self.assertEqual(func.order, 3)

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_chart(self, _description, func_url):
        def url_path(arg):
            return arg

        @restful.chart(url_path if func_url else '/charts', 'Some chart', {'foo': 'bar'}, False, 'chart')
        def func():
            pass

        f = url_path if func_url else func
        self.assertEqual(f.display_type, 'chart')
        self.assertEqual(f.is_ui, True)
        self.assertEqual(f.title, 'Some chart')
        self.assertEqual(f.onlyshowmap, False)
        self.assertEqual(f.map, {'foo': 'bar'})
        self.assertEqual(f.restful, True)
        self.assertEqual(f.url_path, '/charts' if not func_url else None)

    def test_pagevalidator(self):
        @restful.pagevalidator
        def func(self, args, kwargs):
            pass

        func(self, [], {'query': 'some query', 'return_count': 3})

    def test_pagevalidator_fails_validation(self):
        @restful.pagevalidator
        def func(self, args, kwargs):
            pass

        self.assertRaises(common.ValidationError, func, self, [], {})

    @parameterized.expand([
        ('found', 'acute.select', True),
        ('not_found', 'non_existing_file', False),
    ])
    def test_get_page_script(self, _description, file_path, found):
        self.assertEqual(bool(restful.get_page_script(os.path.dirname(os.path.abspath(__file__)) + '/../../scripts', file_path)),
                         found)

    @parameterized.expand([
        ('new_query_results', False),
        ('existing_query_results', True),
    ])
    def test_queryResults(self, _description, has_query_results):
        if has_query_results:
            @restful.queryResults({'title': 'Some title'})
            @set_attribute('queryResults', [])
            def func():
                pass
        else:
            @restful.queryResults({'title': 'Some title'})
            def func():
                pass

        self.assertEqual(func.display_type, 'queryResults')
        self.assertEqual(func.is_ui, True)
        self.assertEqual(func.restful, True)
        self.assertEqual(func.title, 'Some title')
        self.assertEqual(func.queryResults[0].get('data'), {})
        self.assertEqual(func.queryResults[0].get('filter'), None)

    @parameterized.expand([
        ('no_ui', {'query': 'foo'},
         call([], return_count=100, select=['f1', 'f2'], what='foo'),
         {'end': 1, 'record_count': 1, 'records': [{'foo': 'bar'}], 'start': 0}),
        ('ui', {'query': 'foo', '__ui': True},
         call([], select=['key1'], sort=None, return_count=100, modifiers={'key1': 'foobar'}, what='foo',
              key_map={'key1': {'order': 0, 'value': 'foobar', 'title': 'Some title'}}),
         {'end': 1, 'record_count': 1, 'records': [{'foo': 'bar'}], 'start': 0}),
        ('no_query', {},
         None, {'start': 0, 'record_count': 0, 'end': 0, 'records': []}),
        ('order_gte_headers', {'query': 'foo'},
         call([], return_count=100, select=['f1', 'f2'], what='foo'),
         {'end': 1, 'record_count': 1, 'records': [{'foo': 'bar'}], 'start': 0},
         1),
    ])
    def test_pagetable(self, _description, extra_kwargs, expected_pagequery_call, expected, order=0):
        @restful.pagetable('Some title', [],
                           {'key1': {'order': order, 'value': 'foobar', 'title': 'Some title'}},
                           None)
        def func():
            pass

        pagequery = Mock()
        self.pagequery = pagequery
        pagequery.return_value = {'end': 1, 'record_count': 1, 'records': [{'foo': 'bar'}], 'start': 0}
        kwargs = {'return_count': 100, 'fields': ['f1', 'f2']}
        kwargs.update(extra_kwargs)

        self.assertEqual(func(self, None, kwargs), expected)
        self.assertEqual(func.display_type, 'template')
        self.assertEqual(func.is_ui, True)
        self.assertEqual(func.template, 'page_table.html')
        self.assertEqual(func.title, 'Some title')
        self.assertEqual(func.sort, None)
        self.assertEqual(func.actions, None)
        self.assertEqual(func.restful, True)
        self.assertEqual(func.map, {'key1': {'order': order, 'title': 'Some title', 'value': 'foobar'}})
        self.assertEqual(func.headers[order], ['key1', 'Some title', 'string'])
        if expected_pagequery_call:
            self.assertIn(expected_pagequery_call, pagequery.mock_calls)
        else:
            pagequery.assert_not_called()

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_table(self, _description, func_url):
        def url_path(arg):
            return arg

        @restful.table('Some title', url_path if func_url else '/api', {}, True, {'foo': 'bar'}, ['foo', 'bar'])
        def func():
            pass

        f = url_path if func_url else func
        self.assertEqual(f.display_type, 'table')
        self.assertEqual(f.is_ui, True)
        self.assertEqual(f.title, 'Some title')
        self.assertEqual(f.sort, {'foo': 'bar'})
        self.assertEqual(f.onlyshowmap, True)
        self.assertEqual(f.restful, True)
        self.assertEqual(f.url_path, '/api' if not func_url else None)
        self.assertEqual(f.map, {'bar': {'order': 1}, 'foo': {'order': 0}})

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_form(self, _description, func_url):
        def url_path(arg):
            return arg

        @restful.form('Some title', url_path if func_url else '/api', {'foo': 'bar'}, True, 'foobar')
        def func():
            pass

        f = url_path if func_url else func
        self.assertEqual(f.display_type, 'form')
        self.assertEqual(f.is_ui, True)
        self.assertEqual(f.title, 'Some title')
        self.assertEqual(f.method, 'post')
        self.assertEqual(f.onlyshowmap, True)
        self.assertEqual(f.restful, True)
        self.assertEqual(f.submitto, 'foobar')
        self.assertEqual(f.map, {'foo': 'bar'})
        self.assertEqual(f.url_path, '/api' if not func_url else None)

    def test_prepend(self):
        @restful.prepend(1, 2, 3, foo='foo1', bar='bar2')
        def func(*args, **kwargs):
            self.assertEqual(args, (1, 2, 3))
            self.assertEqual(kwargs, {'foo': 'foo1', 'bar': 'bar2'})

        func()

    @parameterized.expand([
        ('list', True),
        ('dict', False)
    ])
    def test_link(self, _description, is_list):
        def function(self, *args, **kwargs):
            return 'bar_value'

        @restful.link(function, 'foo_header', 'foo_key')
        def func(self, *args, **kwargs):
            if is_list:
                return [row]
            return row

        row = {'foo': 'bar', 'foo_header': 'foo_value'}

        func(self)

        self.assertEqual(func.links[0].get('function'), function)
        self.assertEqual(func.links[0].get('key'), 'foo_key')
        self.assertEqual(row, {'foo': 'bar', 'foo_header': 'foo_value', 'foo_key': 'bar_value'})

    @parameterized.expand([
        ('string', 'foo', ['foo']),
        ('string', ['foo', 'bar'], ['foo', 'bar']),
    ])
    def test_permissions(self, _description, params, expected):
        @restful.permissions(params)
        def func():
            pass

        self.assertEqual(func.permissions, expected)

    def test_raw(self):
        @restful.raw
        def func():
            pass

        self.assertEqual(func.raw, True)

    def test_ui_link(self):
        @restful.ui_link('POST')
        def func():
            pass

        self.assertEqual(func.ui_links, ['POST'])

    def test_table_link(self):
        def function(self, *args, **kwargs):
            return 'foobar_response'

        query_results = {'foo': 'bar', 'key': 'foobar'}

        @restful.table_link(function, 'key', query_results, 'api_key')
        def func(self, *args, **kwargs):
            return [query_results]

        self.assertEqual(func(self, [], query_results), {'api_key': 'foobar_response', 'foo': 'bar', 'key': 'foobar'})
        self.assertEqual({key: func.table_links[0].get(key) for key in ('queryResults', 'api_key', 'key')},
                         {'queryResults': query_results, 'api_key': 'api_key', 'key': 'key'})

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_generatedhtml(self, _description, func_url):
        def url_path(arg):
            return arg

        @restful.generatedhtml(url_path if func_url else '/api', 'Some title', {'foo': 'bar'}, True)
        def func():
            pass

        self.assertEqual(func.display_type, 'generated')
        self.assertEqual(func.is_ui, True)
        self.assertEqual(func.title, 'Some title')
        self.assertEqual(func.map, {'foo': 'bar'})
        self.assertEqual(func.onlyshowmap, True)
        self.assertEqual(func.restful, True)
        self.assertEqual(func.url_path, '/api' if not func_url else None)

    def test_ui(self):
        @restful.ui
        def func():
            pass

        self.assertEqual(func.is_ui, True)

    @patch.object(restful, 'RestServer')
    def test_run_internal(self, restserver):
        server = Mock()
        server.run_method.return_value = {'foo': 'bar'}
        restserver.Instance.return_value = server

        restful.run_internal('module', 'function', 'foo', 'bar', foo='bar')

        server.run_method.assert_called_with('module', 'function', 'foo', 'bar', foo='bar')

    def test_async_func(self):
        @restful.async_func('Some function')
        @set_attribute('queue', Mock())
        def func():
            pass

        func(self, 'foo', 'bar', foo='bar')

        self.assertEqual(func.is_async, True)
        self.assertEqual(func.async_description, 'Some function')
        self.assertEqual(func.resources, ['internal'])
        self.assertTrue(func.old_function is not None)
        func.queue.add.assert_called_with({'args': ('foo', 'bar'), 'kwargs': {'foo': 'bar'}})

    @parameterized.expand([
        ('list', [{'foo': 'foo1'}, {'bar': 'bar1'}]),
        ('dict', {'foo': 'foo1'}),
    ])
    def test_single(self, _description, response):
        @restful.single
        def func(self, *args, **kwargs):
            return response

        self.assertEqual(func(self), {'foo': 'foo1'})
        self.assertEqual(func.is_single, True)

    @parameterized.expand([
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        ('single_key', {'foo': {'type': str}}, {'foo': {'type': basestring, 'required': True, 'allow_extra': False}}),
        ('multiple_types', {'foo': {'type': (str, int)}}, {'foo': {'type': (basestring, int), 'required': True, 'allow_extra': False}}),
        ('validator', {'foo': {'type': list, 'validator': {'*': {'type': str}}}},
         {'foo': {'allow_extra': False, 'required': True, 'type': list,
                  'validator': {'*': {'allow_extra': False,
                                      'required': True,
                                      'type': basestring}}}}),
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup
        # ('single_key', {'foo': {'type': str}}, {'foo': {'type': str, 'required': True, 'allow_extra': False}}),
        # ('multiple_types', {'foo': {'type': (str, int)}}, {'foo': {'type': (str, int), 'required': True, 'allow_extra': False}}),
        # ('validator', {'foo': {'type': list, 'validator': {'*': {'type': str}}}},
        #  {'foo': {'allow_extra': False, 'required': True, 'type': list,
        #           'validator': {'*': {'allow_extra': False,
        #                               'required': True,
        #                               'type': str}}}}),
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
        ('multiple_key', {'foo|bar': {'type': int}}, {'foo': {'type': int, 'required': False, 'allow_extra': False},
                                                      'bar': {'type': int, 'required': False, 'allow_extra': False}}),
        ('list_values', {'foo': {'type': int, 'values': ['a', 'b', 'c']}},
         {'foo': {'allow_extra': False, 'required': True, 'type': int,
                  'values': {'a': 'a', 'b': 'b', 'c': 'c'}}}),
        ('dict_values', {'foo': {'type': int, 'values': {'A': 'a', 'B': 'b', 'C': 'c'}}},
         {'foo': {'allow_extra': False, 'required': True, 'type': int,
                  'values': {'a': 'a', 'b': 'b', 'c': 'c', 'A': 'a', 'B': 'b', 'C': 'c'}}}),
    ])
    def test_validateValidator(self, _description, validator, expected):
        restful.validateValidator(validator)

        self.assertEqual(validator, expected)

    @parameterized.expand([
        ('invalid_wildcard', {'*:a': {'type': int}}),
        ('mixed', {'*:1': {'type': int}, 'foo': {'type': int}}),
        ('invalid_value_key', {'foo': {'invalid': 3}}),
        ('required_not_bool', {'foo': {'type': int, 'required': 1}}),
        ('allow_extra_not_bool', {'foo': {'type': int, 'allow_extra': 1}}),
        ('invalid_type_in_tuple', {'foo': {'type': (int, 'invalid')}}),
        ('doc_not_string', {'foo': {'type': int, 'doc': 1}}),
        ('invalid_values_type', {'foo': {'type': int, 'values': 'bar'}}),
    ])
    def test_validateValidator_failures(self, _description, validator):
        self.assertRaises(Exception, restful.validateValidator, validator)

    @parameterized.expand([
        ('no_errors', {'foo': 1}, {'foo': {'type': int, 'required': True}}, []),
        ('missing_required_key_default', {'bar': 1}, {'foo': {'type': int, 'required': True, 'default': 2},
                                                      'bar': {'type': int, 'required': False}}, []),
        ('data_not_dict', ['foo', 1], {'foo': {'type': int, 'required': True}}, ['Must pass in a dictionary']),
        ('wildcard', {'foo': 1}, {'*:2': {'type': int, 'required': True}}, ['Must pass in at least 2 keys to dictionary ']),
        ('missing_required_key_no_default', {'bar': 1}, {'foo': {'type': int, 'required': True},
                                                         'bar': {'type': int, 'required': False}},
         ["Required key 'foo' not provided"]),
        ('wrong_type', {'foo': 1}, {'foo': {'type': bool, 'required': True}},
         ["Key 'foo' has the incorrect type, '<type 'int'>' passed, but expected '<type 'bool'>' "],
         ["Key 'foo' has the incorrect type, '<class 'int'>' passed, but expected '<class 'bool'>' "]),
        ('wrong_cv_type', {'foo': 1}, {'foo': {'type': 'cv', 'values': {'bar': 'bar1'}, 'required': True}},
         ["Key 'foo' has an incorrect value, please look at the docs. "],),
        ('incorrect_list_value', {'foo': ['bar']}, {'foo': {'type': list, 'validator': {'*': {'type': int}}, 'required': True}},
         ["List 'foo' contains an invalid entry at index 0. value passed 'bar' expected 'int' "]),
        ('incorrect_dict_value', {'foo': {'bar': 'baz'}},
         {'foo': {'type': dict, 'validator': {'bar': {'type': int, 'required': True, 'allow_extra': False}}, 'required': True, 'allow_extra': False}},
         ["Key 'foo.bar' has the incorrect type, '<type 'str'>' passed, but expected '<type 'int'>' "],
         ["Key 'foo.bar' has the incorrect type, '<class 'str'>' passed, but expected '<class 'int'>' "]),
        ('extra_not_allowed', {'foo': 1, 'bar': 2}, {'foo': {'type': int, 'required': True}}, ["Invalid parameter 'bar' passed"]),
        ('extra_not_allowed_prekey', {'foo': 1, 'bar': 2}, {'foo': {'type': int, 'required': True}},
         ["Invalid key 'bar' passed to dictionary 'prekey'"], None, 'prekey'),
    ])
    def test_checkdata(self, _description, data, validator, errors_py2, errors_py3=None, prekey=''):
        self._assertEqual(restful.checkdata(data, validator, prekey, False), errors_py2, errors_py3)

    def test_validateArgsValidator(self):
        validator = {'type': str}

        restful.validateArgsValidator([validator])

        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        self.assertEqual(validator, {'type': basestring})
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup
        # self.assertEqual(validator, {'type': str})
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

    @parameterized.expand([
        ('invalid_key', {'invalid_key': int}),
        ('invalid_type', {'type': 'invalid_type'}),
    ])
    def test_validateArgsValidator_failures(self, _description, validator):
        self.assertRaises(Exception, restful.validateArgsValidator, [validator])

    @parameterized.expand([
        ('cv', 'foo', 'cv', (True, 'foo')),
        ('oid', ObjectId('62791a11c2c506c5afdfce76'), 'oid', (True, ObjectId('62791a11c2c506c5afdfce76'))),
        ('oid_string', '62791a11c2c506c5afdfce76', 'oid', (True, ObjectId('62791a11c2c506c5afdfce76'))),
        ('oid_failure', 'foo', 'oid', False),
        ('date', datetime(2000, 1, 2), 'date', (True, datetime(2000, 1, 2))),
        ('date_string', '2000-01-02', 'date', (True, datetime(2000, 1, 2))),
        ('date_failure', 'foo', 'date', False),
        ('*', 'foo', '*', (True, 'foo')),
        ('list', '2000-01-02', ['oid', 'date'], (True, datetime(2000, 1, 2))),
        ('list_failure', 'foo', ['oid', 'date'], False),
        ('tuple', '2000-01-02', ('oid', 'date'), (True, datetime(2000, 1, 2))),
        ('tuple_failure', 'foo', ('oid', 'date'), False),
        ('value_match_type', 'foo', str, (True, 'foo')),
        ('int', 1, int, (True, 1)),
        ('int_failure', 'foo', int, False),
        ('float', 1.0, float, (True, 1.0)),
        ('float_failure', 'foo', float, False),
        ('bool', False, bool, (True, False)),
        ('bool_failure', 'foo', bool, False)
    ])
    def test_checkType(self, _description, value, type, expected):
        self.assertEqual(restful.checkType(value, type), expected)

    @parameterized.expand([
        ('success', [1], [{'type': int, 'required': True}], []),
        ('missing_required_argument', [], [{'type': int, 'required': True, 'name': 'foo_validator'}],
         ['missing a required argument "foo_validator" at position 1']),
        ('incorrect_type', ['foo'], [{'type': int, 'required': True}],
         ["Incorrect value has been passed at position 1, 'foo' is not of type int"]),
    ])
    def test_checkArgs(self, _description, args, validators, expected):
        self.assertEqual(restful.checkArgs(args, validators), expected)

    def test_validate(self):
        @restful.validate({'foo': {'type': int, 'required': True}}, [{'type': int}], False)
        def func(self, args, kwargs):
            pass

        func(self, [1], {'foo': 1})

    @parameterized.expand([
        ('invalid_arg', {'foo': {'type': int, 'required': True}}, [{'type': int}], ['foo'], {'foo': 1}),
        ('invalid_kwarg', {'foo': {'type': int, 'required': True}}, [{'type': int}], [1], {'foo': 'foo'}),
        ('extra_values', {'foo': {'type': int, 'required': True}}, [{'type': int}], [1], {'foo': 1, 'bar': 2}),
    ])
    def test_validate_errors(self, _description, validator, args_validator, args, kwargs, allow_extra=False):
        @restful.validate(validator, args_validator, allow_extra)
        def func(self, args, kwargs):
            pass

        self.assertRaises(common.ValidationError, func, self, args, kwargs)

    def test_search(self):
        @restful.search('foo')
        def func():
            pass

        self.assertEqual(func.search, 'foo')

    def test_doc(self):
        @restful.doc('Doc description', {'foo': 'bar'}, False)
        def func():
            pass

        self.assertEqual(func.description, 'Doc description')
        self.assertEqual(func.returns, {'foo': 'bar'})
        self.assertEqual(func.restful, True)
        self.assertEqual(func.public, False)

    def test_onFinishLoad(self):
        @restful.onFinishLoad
        def func():
            pass

        self.assertEqual(func.call_on_finish, True)

    def test_onload(self):
        @restful.onload
        def func():
            pass

        self.assertEqual(func.call_on_finish_single, True)

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_template(self, _description, func_url):
        @set_attribute('__func__', lambda x: x)
        def url_path(arg):
            return arg

        @restful.template('some_template', url_path if func_url else '/templates', 'Some title')
        def func():
            pass

        f = url_path if func_url else func
        self.assertEqual(f.display_type, 'template')
        self.assertEqual(f.is_ui, True)
        self.assertEqual(f.template, 'some_template')
        self.assertEqual(f.title, 'Some title')
        self.assertEqual(f.url_path, '/templates' if not func_url else url_path)

    def test_customTemplate(self):
        @restful.customTemplate('some_template')
        def func():
            pass

        self.assertEqual(func.custom_template, 'some_template')

    def test_cron(self):
        @restful.cron('1', '2', '3', '4', '5')
        def func():
            pass

        self.assertEqual(func.cron, ('1', '2', '3', '4', '5'))
        self.assertEqual(func.lastRan, None)
        self.assertEqual(func.lastJobName, None)
        self.assertEqual(func.nextEvent, None)

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_rawHTML(self, _description, func_url):
        def url_path(arg):
            return arg

        @restful.rawHTML(url_path if func_url else '/rawHTML', 'Some html')
        def func():
            pass

        f = url_path if func_url else func
        self.assertEqual(f.display_type, 'raw')
        self.assertEqual(f.is_ui, True)
        self.assertEqual(f.title, 'Some html')
        self.assertEqual(f.url_path, '/rawHTML' if not func_url else None)

    @parameterized.expand([
        ('string_url', False),
        ('func_url', True),
    ])
    def test_passreq(self, _description, func_join):
        def join(arg):
            return arg

        @restful.passreq(join if func_join else True, True)
        def func():
            pass

        f = join if func_join else func
        self.assertEqual(f.joinauth, not func_join)
        self.assertEqual(f.passreq, True)
        self.assertEqual(f.include_perms, True)

    def test_passuser(self):
        @restful.passuser
        def func():
            pass

        self.assertEqual(func.passuser, True)

    def test_sm(self):
        self.assertEqual(restful.sm('int_key', int, foo='bar'), {'key': 'int_key', 'type': int, 'foo': 'bar'})

    def test_usewhen(self):
        @restful.usewhen('foo', 'bar')
        def func():
            pass

        self.assertEqual(getattr(func, '__usewhen'), ('bar', 'foo'))


if __name__ == '__main__':
    unittest.main()
