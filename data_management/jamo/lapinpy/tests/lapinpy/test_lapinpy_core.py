import os
import types
import unittest
from collections import OrderedDict
import cherrypy
from lapinpy import lapinpy_core
from parameterized import parameterized
from Crypto.Cipher import AES
from Crypto.Hash import MD5
import datetime
from decimal import Decimal
from lapinpy.lapinpy_core import RestServer
from lapinpy import restful
from cherrypy import HTTPRedirect
import copy
from lapinpy import job
from pkg_resources import resource_filename
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, call
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, call
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestLapinpyCore(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        self.temp_folder = '{}/folder'.format(self.temp_dir.name)
        self.temp_file = '{}/file.py'.format(self.temp_dir.name)
        os.mkdir(self.temp_folder)
        self.filehandle = open(self.temp_file, 'w')
        self.app_import_finder = lapinpy_core.AppImportFinder(self.temp_dir.name, 'my_app')
        self.flow = Mock()
        self.core = Mock()
        self.server = RestServer.Instance()
        self.original = copy.copy(self.server)
        self.server.flow = self.flow
        self.server.core = self.core
        cherrypy.request.headers = {}
        cherrypy.request.cookie = {}

    def tearDown(self):
        self.filehandle.close()
        self.temp_dir.cleanup()
        RestServer._instance = self.original

    # Helper function for validating dict equality since the orderings are handled differently between PY2 and PY3
    def _assertEqual(self, actual, py2_data, py3_data):
        try:
            self.assertEqual(actual, py2_data)
        except Exception:
            self.assertEqual(actual, py3_data)

    def test_sslwrap(self):
        @lapinpy_core.sslwrap
        def func(*args, **kwargs):
            self.assertEqual(args, ('foo',))
            self.assertEqual(kwargs, {'bar': 'bar1', 'ssl_version': 2})

        func('foo', bar='bar1')

    def test_AppImportFinder_find_module(self):
        self.assertEqual(self.app_import_finder.find_module('file'), self.app_import_finder)

    @parameterized.expand([
        ('file'),
        ('folder'),
    ])
    def test_AppImportFinder_load_module(self, module):
        self.assertTrue(isinstance(self.app_import_finder.load_module(module), types.ModuleType))

    def test_encrypt(self):
        m = MD5.new()
        lapinpy_core.cipher = AES.new(m.digest(), AES.MODE_ECB)

        self.assertEqual(lapinpy_core.encrypt(b'abcd' * 4), b'WUQX7KgRFIBjN0kkPcheTg==')

    def test_decrypt(self):
        m = MD5.new()
        lapinpy_core.cipher = AES.new(m.digest(), AES.MODE_ECB)

        self.assertEqual(lapinpy_core.decrypt('WUQX7KgRFIBjN0kkPcheTg=='), 'abcd' * 4)

    def test_link(self):
        self.assertEqual(lapinpy_core.link('/api/core/foo', 'Foo'), '<a href="/api/core/foo">Foo</a>')

    @parameterized.expand([
        ('type', type(str), type(str).__name__),
        ('isoformat', datetime.datetime.fromordinal(1), datetime.datetime.fromordinal(1).isoformat()),
        ('decimal', Decimal(1), "{0:.2f}".format(Decimal(1))),
        ('str', 'foo', 'foo'),
    ])
    def test_jsonify(self, _description, obj, expected):
        self.assertEqual(lapinpy_core.jsonify(obj), expected)

    def test_to_json(self):
        expected_py2 = '{"bar": "1.00", "foo": "foo1", "type": "str", "baz": "0001-01-01T00:00:00"}'
        expected_py3 = '{"type": "str", "foo": "foo1", "bar": "1.00", "baz": "0001-01-01T00:00:00"}'

        self._assertEqual(lapinpy_core.to_json(
            {'type': str, 'foo': 'foo1', 'bar': Decimal(1), 'baz': datetime.datetime.fromordinal(1)}),
            expected_py2, expected_py3)

    def test_PageResponse_len(self):
        page_response = lapinpy_core.PageResponse({'record_count': 1}, 'file')

        self.assertEqual(len(page_response), 1)

    def test_PageResponse_iter(self):
        self.server.run_method = lambda a, b, *c, **d: {'record_count': 2,
                                                        'records': [{'record_id': 'bar'}]}

        page_response = lapinpy_core.PageResponse({'record_count': 2,
                                                   'records': [{'record_id': 'foo'}],
                                                   'cursor_id': 'my_cursor'}, 'file')
        i = iter(page_response)

        self.assertEqual(next(i), {'record_id': 'foo'})
        self.assertEqual(next(i), {'record_id': 'bar'})
        self.assertRaises(StopIteration, next, i)

    @parameterized.expand([
        ('alpha', 'foo', True),
        ('alpha_numeric', 'Foo123', True),
        ('underscore', '_foo123', True),
        ('space', 'foo 123', False),
        ('symbol', 'foo@', False),
    ])
    def test_RestServer_isidentifier(self, _description, text, expected):
        server = RestServer.Instance()

        self.assertEqual(bool(server.isidentifier(text)), expected)

    @parameterized.expand([
        ('str', None, 'bar', {'id': 'foo', 'label': 'Foo', 'pattern': '', 'type': 'string'}),
        ('numeric', None, '123', {'id': 'foo', 'label': 'Foo', 'pattern': '', 'type': 'number'}),
        ('map', {'title': 'My title', 'type': 'html'}, '<html>', {'id': 'foo', 'label': 'My title', 'pattern': '', 'type': 'string'})
    ])
    def test_RestServer_getcol(self, _description, map_data, value, expected):
        server = RestServer.Instance()

        self.assertEqual(server.getcol('foo', map_data, value), expected)

    @parameterized.expand([
        ('list', ['Internal server error', 'Server not accessible'], {'error_code': 500, 'errors': ['Internal server error', 'Server not accessible']}),
        ('str', 'Internal server error', {'error_code': 500, 'errors': ['Internal server error']})
    ])
    def test_RestServer_error(self, _description, messages, expected):
        server = RestServer.Instance()

        self.assertEqual(server.error(500, messages), expected)

    @parameterized.expand([
        ('permission_match', ['admin'], ['admin'], None, True),
        ('permission_mismatch', ['admin'], ['user'], ['user'], False),
    ])
    def test_RestServer_check_permissions(self, _description, permissions, method_permissions, method_perms, expected):
        def func():
            pass

        method_ref = func
        func.permissions = method_permissions
        server = RestServer.Instance()

        self.assertEqual(server.check_permissions(method_ref, permissions, method_perms), expected)

    @parameterized.expand([
        ('raw', 'Bearer TOKEN', True, True, True, 'foobar', False, True, {}, 'foobar', None),
        ('json', 'Application TOKEN', False, True, False, [{'record_id': 'id_1'}], True, True, {}, '[{"record_id": "id_1"}]', None),
        ('tq', 'Application TOKEN', False, True, False, [{'record_id': 'id_1'}], False, False,
         {'tq': True, 'tqx': 'reqId:bar'},
         'google.visualization.Query.setResponse({"table": {"rows": [{"c": [{"v": "id_1"}]}], "cols": [{"pattern": "", "type": "string", "id": "record_id", "label": "Record Id"}]}, "reqId": "bar"});',
         'google.visualization.Query.setResponse({"table": {"cols": [{"id": "record_id", "label": "Record Id", "type": "string", "pattern": ""}], "rows": [{"c": [{"v": "id_1"}]}]}, "reqId": "bar"});'),
    ])
    def test_RestServer_default_api_auth(self, _description, authorization, joinauth, include_perms, raw, func_return_value,
                                         use_read, use_authorization, kwargs, expected_py2, expected_py3):
        def func(args, kwargs):
            return func_return_value

        if use_authorization:
            cherrypy.request.headers = {
                'Authorization': authorization,
                'Content-Length': 1000,
            }
        else:
            sessionid = Mock()
            sessionid.value.return_value = 'SESSION_ID'
            cherrypy.request.cookie = {
                'sessionid': sessionid
            }
        args = ['api', 'my_module', 'my_method', 'foo', 'bar']
        self.core.get_permissions_from_user_token.return_value = ['admin']
        self.core.get_userinfo_from_user_token.return_value = {'user': 'foo', 'group': 'sdm'}
        self.core.get_apppermissions.return_value = ['admin']
        self.core.get_appinfo_from_token.return_value = {'user': 'foo', 'group': 'sdm'}
        body = Mock()
        del body.rawbody
        if use_read:
            body.read.return_value = b'{"foo": "bar"}'
        else:
            body.read.side_effect = [Exception('error')]
            body.readline.return_value = '{"foo": "bar"}'
        cherrypy.request.body = body
        app = Mock()
        app.getrestmethod.return_value = func, 1
        self.server.apps = {'my_module': app}
        func.sort = lambda: True
        func.permissions = ['admin']
        func.passreq = True
        func.joinauth = joinauth
        func.include_perms = include_perms
        if raw:
            func.raw = raw

        actual = self.server.default(*args, **kwargs)
        self._assertEqual(actual, expected_py2, expected_py3)

    def test_RestServer_default_api_internal_redirect(self):
        args = ['api', 'my_module', 'my_method']
        kwargs = {'XXredirect_internalXX': '/api/my_module/my_method'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server.default(*args, **kwargs)

        self.assertEqual(cm.exception.args, ((['http://127.0.0.1:8080/my_module/my_method'], 303)))

    def test_RestServer_default_script(self):
        args = ['my_module', 'scripts', 'script.js']
        kwargs = {}
        expected = b'var foo = 1;'
        app = Mock()
        app.location = self.temp_folder
        self.server.apps = {'my_module': app}
        os.mkdir('{}/scripts'.format(self.temp_folder))

        with open('{}/scripts/script.js'.format(self.temp_folder), 'w') as f:
            f.writelines('var foo = 1;')
            f.flush()

            actual = self.server.default(*args, **kwargs)
            self.assertEqual(next(actual), expected)

    def test_RestServer_default_page_template(self):
        def func(*args, **kwargs):
            return [{'record_id': 'id_1'}]

        args = ['views']
        kwargs = {}
        expected = u'<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="en_US" xml:lang="en-US">\n<head>\n    <title>JGI Data Management</title>\n    <script type="text/javascript" src="https://www.google.com/jsapi"></script>\n    <script type="text/javascript" src="/scripts/common.js"></script>\n    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>\n    <script src="/scripts/chosen.jquery.js" type="text/javascript"></script>\n    <link rel=\'stylesheet\' href=\'/scripts/style.css\' type=\'text/css\'>\n    <link rel="stylesheet" href="/scripts/chosen.css">\n    <link rel="icon" href="/images/favicon.png">\n    <link rel="stylesheet" href="/scripts/table.css">\n\n    \n        <script type="text/javascript">\n            var dataSourceUrl = \'/api/views\';\n        </script>\n\n        \n    \n\n    \n   </head>\n<body>\n    <div id="top">\n        <img id="logo" src="/images/JGI_logo.jpg"/> \n\t<div id="site-name">My site</div>\n        <div id="top-right">\n          \n          <div id=\'username\'>Welcome Foo Bar</div>\n          \n          <form class="searchform" method="get" action="/globalsearch/">\n            <div>\n                <input class="s" name="query" type="text" value="" size="16" tabindex="1" placeholder="Search this website ...">\n\t\t\t\t<input type="hidden" value="global" name="what">\n                <input type="submit" class="button" value="SEARCH" tabindex="2">\n            </div>\n         </form>\n          \n          \n       </div>\n    </div>\n    <div id="menubar2">\n    <nav class="main">\n        <ul class="primary">\n            \n            <li > \n                \n                <a href="/">home</a>\n                \n                \n            </li>\n            \n        </ul>\n    </nav>\n    </div>\n\n    <div id="content">\n        \n        \n            <h2> My title </h2>\n        \n\n        <div id=\'edit_chart\'> </div>\n        <div id="table"> </div>\n\n        \n    </div>\n    <div id="modalmask"></div>\n\n    <script type="text/javascript" src="/scripts/template_footer.js" ></script>\n</body>\n</html>'
        self.flow.authorization_url.return_value = (u'http://localhost?state=foo&token=bar', None)
        self.core.get_user_from_id.return_value = [
            {'id': 1, 'user_id': 1, 'token': 'some_token', 'email': 'foo@lbl.gov', 'name': 'Foo Bar',
             'group': 'sdm'}]
        self.server.rest_ui_components = {'/views': {'method': func, 'rest_url': '/views'}}
        func.display_type = 'My func'
        func.title = 'My title'
        config = Mock()
        config.site_name = 'My site'
        self.server.config = config

        actual = self.server.default(*args, **kwargs)

        self.assertEqual(actual, expected)

    def test_RestServer_default_page_globalsearch(self):
        what = 'global'
        expected = u'<html xmlns="http://www.w3.org/1999/xhtml" dir="ltr" lang="en_US" xml:lang="en-US">\n<head>\n    <title>JGI Data Management</title>\n    <script type="text/javascript" src="https://www.google.com/jsapi"></script>\n    <script type="text/javascript" src="/scripts/common.js"></script>\n    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>\n    <script src="/scripts/chosen.jquery.js" type="text/javascript"></script>\n    <link rel=\'stylesheet\' href=\'/scripts/style.css\' type=\'text/css\'>\n    <link rel="stylesheet" href="/scripts/chosen.css">\n    <link rel="icon" href="/images/favicon.png">\n    <link rel="stylesheet" href="/scripts/table.css">\n\n    \n\n    \n        <script src="/scripts/template_additionalcontent_nontemplate.js" type="text/javascript"></script>\n    \n   </head>\n<body>\n    <div id="top">\n        <img id="logo" src="/images/JGI_logo.jpg"/> \n\t<div id="site-name">My site</div>\n        <div id="top-right">\n          \n          <a href=\'http://localhost?state=globalsearch&token=bar\'>Login</a>\n          \n          \n       </div>\n    </div>\n    <div id="menubar2">\n    <nav class="main">\n        <ul class="primary">\n            \n            <li > \n                \n                <a href="/">home</a>\n                \n                \n            </li>\n            \n        </ul>\n    </nav>\n    </div>\n\n    <div id="content">\n        \n        \n            <h2> Search results for: _id </h2>\n        \n\n        <div id=\'edit_chart\'> </div>\n        <div id="table"> </div>\n\n        \n            \n                \n                    \n                        <h3>my_module</h3>\n                    \n                    \n                        <div class="sub-content" style=\'padding-left:60px\'>foo<br>bar</div>\n                    \n                \n            \n        \n    </div>\n    <div id="modalmask"></div>\n\n    <script type="text/javascript" src="/scripts/template_footer.js" ></script>\n</body>\n</html>'

        self.flow.authorization_url.return_value = (u'http://localhost?state=foo&token=bar', None)
        self.core.get_user_from_id.return_value = []
        args = ['globalsearch']
        kwargs = {'what': what, 'query': '_id'}
        app = Mock()
        app.search.return_value = ['foo', 'bar']
        self.server.apps = {'my_module': app}
        config = Mock()
        config.site_name = 'My site'
        self.server.config = config

        self.assertEqual(self.server.default(*args, **kwargs), expected)

    def test_RestServer_default_page_globalsearch_redirect(self):
        self.flow.authorization_url.return_value = (u'http://localhost?state=foo&token=bar', None)
        self.core.get_user_from_id.return_value = []
        args = ['globalsearch']
        kwargs = {'what': 'local', 'query': '_id'}
        self.server.search_components = {'local/_id': '/module/local'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server.default(*args, **kwargs)

        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/module/local'], 303))

    @patch.object(lapinpy_core, 'Curl')
    def test_RestServer_default_page_oauth2callback(self, curl):
        self.flow.authorization_url.return_value = (u'http://localhost?state=foo&token=bar', None)
        self.core.get_user_from_id.return_value = []
        args = ['oauth2callback']
        kwargs = {'what': 'local', 'query': '_id'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server.default(*args, **kwargs)

        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/'], 303))

    @parameterized.expand([
        ('root page', ()),
        ('child page', ('path', 'to', 'child')),
    ])
    def test_restserver_get_authorization_url(self, _description, args):
        self.flow.authorization_url.return_value = (u'http://localhost?state=foo&token=bar', None)
        expected_authorization_url = 'http://localhost?state={}&token=bar'.format('%2F'.join(args))

        authorization_url = self.server.get_authorization_url(args)

        self.assertEqual(authorization_url, expected_authorization_url)

    @patch.object(lapinpy_core, 'Curl')
    # Unfortunately `patch` and `parameterized` don't seem to play well together when testing exceptions,
    # so we'll need separate test cases for different variations
    def test_restserver_oauth2_callback_no_state_redirects_to_root(self, curl):
        curl_get = Mock()
        curl_get.get.return_value = {'email': 'foo@bar.com'}
        curl.return_value = curl_get
        self.core.get_user.return_value = {'email': 'foo@bar.com', 'name': 'John Doe'}
        kwargs = {'code': 'some_code'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server._RestServer__oauth2_callback('userid', kwargs)

        # Validate the redirect exception
        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/'], 303))
        self.flow.fetch_token.assert_called_with(code='some_code')
        # User found, validate no call to `_post_user`
        self.core._post_user.assert_not_called()

    @patch.object(lapinpy_core, 'Curl')
    # Unfortunately `patch` and `parameterized` don't seem to play well together when testing exceptions,
    # so we'll need separate test cases for different variations
    def test_restserver_oauth2_callback_has_state_redirects_to_state_path(self, curl):
        curl.get.return_value = {'email': 'foo@bar.com'}
        self.core.get_user.return_value = {'email': 'foo@bar.com', 'name': 'John Doe'}
        kwargs = {'code': 'some_code', 'state': 'foo/bar'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server._RestServer__oauth2_callback('userid', kwargs)

        # Validate the redirect exception
        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/foo/bar'], 303))
        self.flow.fetch_token.assert_called_with(code='some_code')
        # User found, validate no call to `_post_user`
        self.core._post_user.assert_not_called()

    @patch.object(lapinpy_core, 'Curl')
    def test_restserver_oauth2_callback_no_user_found_calls_post_user(self, curl):
        curl_get = Mock()
        curl_get.get.return_value = {'email': 'foo@bar.com'}
        curl.return_value = curl_get
        self.core.get_user.return_value = None
        kwargs = {'code': 'some_code', 'state': 'foo/bar'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server._RestServer__oauth2_callback('userid', kwargs)

        # Validate the redirect exception
        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/foo/bar'], 303))
        self.flow.fetch_token.assert_called_with(code='some_code')
        # No user found, validate call to `_post_user`
        self.core._post_user.assert_called_with(None, {'email': 'foo@bar.com'})

    @patch.object(lapinpy_core, 'Curl')
    def test_restserver_oauth2_callback_flow_raises_value_error_redirects_to_root(self, curl):
        curl_get = Mock()
        curl_get.get.return_value = {'email': 'foo@bar.com'}
        curl.return_value = curl_get
        self.core.get_user.return_value = None
        self.flow.fetch_token.side_effect = ValueError('Error')
        kwargs = {'code': 'some_code', 'state': 'foo/bar'}

        with self.assertRaises(HTTPRedirect) as cm:
            self.server._RestServer__oauth2_callback('userid', kwargs)

        # Validate the redirect exception (should be root path, since there was a `ValueError` raised
        self.assertEqual(cm.exception.args, (['http://127.0.0.1:8080/'], 303))
        self.flow.fetch_token.assert_called_with(code='some_code')

    @parameterized.expand([
        ('no_user_info', [], None, [{'href': '/', 'name': 'home', 'order': 0}], {}),
        ('cached_menus', ['admin'], {'email': 'foo@bar.com'}, [{'href': '/foo', 'name': 'foo', 'order': 1}],
         {'admin': [{'href': '/foo', 'name': 'foo', 'order': 1}]}),
        ('non_cached_menus', ['admin'], {'email': 'foo@bar.com'},
         [{'pages': OrderedDict([('Edit ', {'pages': OrderedDict([(' Info', {'href': '/edit/info'})])})]), 'name': 'My menu', 'order': 0}, {'href': '/', 'order': 0, 'name': 'home'}],
         {}),
    ])
    def test_RestServer_getMenus(self, _description, permissions, user_info, expected, cached_menus):
        self.server.cachedMenus = cached_menus
        app = Mock()
        app.menus = [{
            'permissions': ['admin'],
            'title': 'Edit > Info',
            'href': '/edit/info'
        }]
        app.menuname = 'My menu'
        app.order = 0
        self.server.apps = {'my_module': app}

        self.assertEqual(self.server.getMenus(permissions, user_info), expected)

    @parameterized.expand([
        ('list_int', {'type': [int]}, 1),
        ('list_list', {'type': [list], 'validator': {'type': int}}, [1]),
        ('list_dict', {'type': [dict], 'validator': {'foo': {'type': int}}}, {'foo': 1}),
        ('list_str', {'type': [str]}, 'a string'),
        ('list_float', {'type': [float]}, 3.14159265359),
        ('list_bool', {'type': [bool]}, False),
    ])
    @patch.object(lapinpy_core, 'random')
    def test_RestServer_createJsonTestObj(self, _description, validator, expected, mock_random):
        mock_random.randrange.return_value = 1

        self.assertEqual(self.server.createJsonTestObj(validator), expected)

    def test_RestServer_getJSONHtml(self):
        data = {'type': [dict], 'validator': {'foo': {'type': str}}}
        kwargs = {'bar': 'baz'}

        self.assertEqual(self.server.getJSONHtml(data, **kwargs),
                         '<div class="json">{<br>&nbsp&nbsp&nbsp&nbsp"bar":"baz",<br>&nbsp&nbsp&nbsp&nbsp"foo":"a&nbspstring"<br>}</div>')

    def test_RestServer_createArgsPath(self):
        validator = [{'name': 'foo', 'type': str}, {'name': 'bar', 'type': int}]

        self.assertEqual(self.server.createArgsPath(validator), '/{foo}/{bar}')

    @parameterized.expand([
        ('str', {'foo.bar': 'baz'}, {'foobarfoo.bar': 'baz'}, []),
        ('list', {'foo.bar': ['baz']}, {}, ['<h3>foobarfoo.bar</h3><ul class="colapse" id="foobarfoo_bar"><li>baz</li></ul>']),
        ('list_map', {'foo.bar': [{'foobar': 'foobar1'}]}, {}, ['<h3>foobarfoo.bar<h3>']),
    ])
    def test_RestServer_colapseData(self, _description, results, expected, expected_additional_content):
        def func():
            pass

        additional_content = []
        func.map = {'foobarfoo.bar': lambda x: x}

        self.assertEqual(self.server.colapseData(results, func, additional_content, 'foobar'),
                         expected)
        self.assertEqual(additional_content, expected_additional_content)

    def test_RestServer_getPageDetails_page_does_not_exist(self):
        kwargs = {}
        permissions = []
        user_info = {}
        expected = {'errors': ['The page you have requested: /my_module/my_method does not exist'], 'error_code': 404}

        args = ['my_module', 'my_method', 'foo']

        self.assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected)

    @patch.object(restful, 'RestServer')
    def test_RestServer_getPageDetails_queryResults(self, restserver):
        def func(args, kwargs):
            return {'id': 'foobar'}

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'queryResults'
        expected_py2 = {'additional_content': [
            u'<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"sort": "", "id_field": "_id", "data": {"record_per_page": 100, "foo": "foo1", "db_address": "127.0.0.1/db", "default_query": "_id = foo"}, "functions": [], "fields": ["f"], "tbody": "", "multi_select": null, "record_per_page": 100, "headers": ["f"], "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t\\n\\t\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t\\n    </div>\\n    \\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th name=\\"f\\" class=\\"sort\\">f<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"1\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t\\n    \\n</div>\\n", "result_desc": {"total_formatted": 1, "total": 1, "foo": "bar"}, "columns": ["foo"]},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="f" class="sort">f<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n\n</div>',
            'Extra data'],
            'rest_url': '/api/my_module/my_method/foo',
            'title': 'View foo for foobar',
            'type': 'queryResults'}
        expected_py3 = {'title': 'View foo for foobar', 'rest_url': '/api/my_module/my_method/foo', 'type': 'queryResults',
                        'additional_content': [
                            '<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"fields": ["f"], "columns": ["foo"], "data": {"foo": "foo1", "db_address": "127.0.0.1/db", "record_per_page": 100, "default_query": "_id = foo"}, "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t\\n\\t\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t\\n    </div>\\n    \\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th name=\\"f\\" class=\\"sort\\">f<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"1\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t\\n    \\n</div>\\n", "headers": ["f"], "result_desc": {"foo": "bar", "total_formatted": 1, "total": 1}, "tbody": "", "multi_select": null, "functions": [], "record_per_page": 100, "id_field": "_id", "sort": ""},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="f" class="sort">f<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n\n</div>',
                            'Extra data']}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View {{value}} for {{id}}'
        render = Mock()
        render.render.return_value = 'Extra data'
        env = Mock()
        env.get_template.return_value = render
        func.env = env
        self.core.chart_mappings = {'/api/my_module/my_method': '/api/my_module/my_method_2/tq=di'}

        self.server.rest_ui_components = {'/my_module/my_method': {'method': func, 'rest_url': '/my_module/my_method'}}
        func.queryResults = [{'foo': 'bar', 'table': {'title': 'My data', 'columns': ['foo']}, 'title': 'My table',
                              'data': {'foo': 'foo1'}, 'filter': None}]
        func.address = '127.0.0.1/db'
        func.custom_template = True

        server = Mock()
        server.run_method.return_value = {'foo': 'bar', 'total_formatted': 1, 'total': 1}
        restserver.Instance.return_value = server

        self._assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected_py2, expected_py3)

    @patch('lapinpy.restful.uuid')
    def test_RestServer_getPageDetails_generated_dict(self, mock_uuid):
        def func(args, kwargs):
            return {'id': [{'id': 'foobar'}],
                    'API_KEY': {'foo': 'bar', 'table': {'title': 'My data', 'columns': ['foo']}, 'title': 'My table',
                                'data': {'foo': 'foo1'}, 'filter': None},
                    '_id': 'foobar'}

        def link(args, kwargs):
            return 'Link data'

        def ui_link(args, kwargs):
            return 'UI Link data'

        def ui_link_2(args, kwargs):
            return 'More UI Link data'

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'generated'
        expected_py2 = {'additional_content': [
            u'<table>\n    \n\n    <tr><td class="key"> Id:</td> <td class="value">foobar</td></tr>\n\n    \n</table>',
            {'class': 'ui_link', 'content': 'More UI Link data'},
            {'class': 'ui_link', 'content': 'UI Link data'},
            '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">link</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Id" >Id</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>foobar</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n',
            u'<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"sort": "", "id_field": "_id", "data": {"record_per_page": 100, "foo": "foo1", "db_address": "127.0.0.1/db", "default_query": "_id = foo"}, "functions": [], "fields": ["f"], "tbody": "", "multi_select": null, "record_per_page": 100, "headers": ["f"], "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h3 class=\\"sub-table\\">My data</h3>\\n\\t\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t\\n    </div>\\n    \\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th name=\\"f\\" class=\\"sort\\">f<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"1\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t\\n    \\n</div>\\n", "result_desc": {"total_formatted": 1, "total": 1, "foo": "bar"}, "columns": ["foo"]},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My data</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="f" class="sort">f<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n\n</div>'],
            'rest_url': None,
            'title': "View foo for [{'id': 'foobar'}]",
            'type': 'generated'}
        expected_py3 = {'title': "View foo for [{'id': 'foobar'}]", 'rest_url': None, 'type': 'generated',
                        'additional_content': [
                            '<table>\n    \n\n    <tr><td class="key"> Id:</td> <td class="value">foobar</td></tr>\n\n    \n</table>',
                            {'class': 'ui_link', 'content': 'More UI Link data'},
                            {'class': 'ui_link', 'content': 'UI Link data'},
                            '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">link</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Id" >Id</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>foobar</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n',
                            '<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"fields": ["f"], "columns": ["foo"], "data": {"foo": "foo1", "db_address": "127.0.0.1/db", "record_per_page": 100, "default_query": "_id = foo"}, "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h3 class=\\"sub-table\\">My data</h3>\\n\\t\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t\\n    </div>\\n    \\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th name=\\"f\\" class=\\"sort\\">f<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"1\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t\\n    \\n</div>\\n", "headers": ["f"], "result_desc": {"foo": "bar", "total_formatted": 1, "total": 1}, "tbody": "", "multi_select": null, "functions": [], "record_per_page": 100, "id_field": "_id", "sort": ""},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My data</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="f" class="sort">f<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n\n</div>']}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View {{value}} for {{id}}'
        func.__self__ = self
        self.core.chart_mappings = {'/api/my_module/my_method': '/api/my_module/my_method_2/tq=di'}

        self.server.run_method = lambda a, b, *c, **d: {'foo': 'bar', 'total_formatted': 1, 'total': 1}
        app = Mock()
        app.getrestmethod.return_value = func, 0
        self.server.apps = {'my_module': app}
        func.is_ui = True
        func.address = '127.0.0.1/db'
        func.links = [{'function': link, 'key': 'id'}]
        ui_link_mock_1 = Mock()
        ui_link_mock_1.produce = ui_link
        ui_link_mock_1.method = ui_link
        func.ui_links = [ui_link_mock_1, ui_link_2]
        func.table_links = [{'api_key': 'API_KEY',
                             'queryResults':
                                 {'foo': 'bar', 'table': {'title': 'My data', 'columns': ['foo']}, 'title': 'My table',
                                  'data': {'foo': 'foo1'}, 'filter': None}}
                            ]
        uuid4 = Mock()
        uuid4.hex = 'ABC'
        mock_uuid.uuid4.return_value = uuid4

        self._assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected_py2, expected_py3)

    @patch('lapinpy.restful.uuid')
    def test_RestServer_getPageDetails_generated_list(self, mock_uuid):
        def func(args, kwargs):
            return ['foo', 'bar']

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'generated'
        expected_py2 = {'additional_content': [
            '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">View foo</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 2</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Value" >Value</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>foo</td></tr><tr data-id="ABC" data-name="_id"><td>bar</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n'],
            'rest_url': '/api/my_module/my_method/foo',
            'title': 'View foo',
            'type': 'generated'}
        expected_py3 = {'title': 'View foo', 'rest_url': '/api/my_module/my_method/foo', 'type': 'generated',
                        'additional_content': [
                            '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">View foo</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 2</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Value" >Value</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="1"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>b\'foo\'</td></tr><tr data-id="ABC" data-name="_id"><td>b\'bar\'</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n']}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View foo'
        func.__self__ = self

        self.server.run_method = lambda a, b, *c, **d: {'foo': 'bar', 'total_formatted': 1, 'total': 1}
        app = Mock()
        app.getrestmethod.return_value = func, 0
        self.server.apps = {'my_module': app}
        func.is_ui = True
        uuid4 = Mock()
        uuid4.hex = 'ABC'
        mock_uuid.uuid4.return_value = uuid4

        self._assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected_py2, expected_py3)

    def test_RestServer_getPageDetails_generated_template(self):
        def func(args, kwargs):
            return ['foo', 'bar']

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'template'
        expected = {'additional_content': [
            u'<style type="text/css">\n.views-table{\n    border-top: 1px solid #9E9E9E;\n    margin: 0 0 3em 0;\n    width: 100%;\n    border-collapse: collapse;\n    border-spacing: 0;\n}\ncaption{\n    text-align:left;\n    line-height:1.1;\n}\ncaption strong{\n    font-size:20px;\n    text-transform: capitalize;\n}\ncaption p{\n    margin-top: 0.5em;\n    font-size: 14px;\n}\n.views-field-title {\n    width: 35%;\n}\n.views-field-title{\n    font-size: 16px;\n    vertical-align: baseline;\n}\n.views-field-body{\n    font-size: 14px;\n}\ntd{\n    border-bottom: 1px solid #9E9E9E;\n    padding: 8px 0 8px 0;\n    vertical-align: top;\n}\ntr:hover{\n    background-color:#EEE;\n}\nth, thead th {\n    padding: 8px 0;\n    width: 140px;\n    border-bottom: 1px solid #9E9E9E;\n    font-weight: bold;\n    text-align: left;\n}\n.method{\n    text-transform:uppercase;\n    float:left;\n    padding-right:10px;\n}\n#center-content{\n    min-height: 400px;\n    margin-bottom: 6em;\n    overflow: hidden;\n    max-width: 960px;\n    margin: 0 auto;\n}\n#center-content h2{\n    margin:0;\n}\n</style>\n<div id="center-content">\n<h2>Documentation</h2>\n\n<table class="views-table cols-2">\n    <caption><strong><a href="/doc/app/" ></a></strong><p></p></caption>\n</table>\n\n<table class="views-table cols-2">\n    <caption><strong><a href="/doc/app/" ></a></strong><p></p></caption>\n</table>\n\n</div>'],
            'title': 'View foo',
            'type': 'template'}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View foo'
        func.__self__ = self

        self.server.run_method = lambda a, b, *c, **d: {'foo': 'bar', 'total_formatted': 1, 'total': 1}
        app = Mock()
        app.getrestmethod.return_value = func, 0
        self.server.apps = {'my_module': app}
        func.is_ui = True
        func.template = 'applications.html'

        self.assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected)

    def test_RestServer_getPageDetails_generated_form(self):
        def func(args, kwargs):
            return ['foo', 'bar']

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'form'
        expected = {'additional_content': [
            '<form class="form" method="POST" action="/forms"><table><tr><input type="hidden" name="XXredirect_internalXX" value="/my_module/my_method/foo"><tr><td><button name="save">Save</button></td></tr></form>'],
            'rest_url': None,
            'title': 'View foo',
            'type': 'form'}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View foo'
        func.map = {}
        func.submitto = '/forms'
        func.method = 'POST'

        app = Mock()
        app.getrestmethod.return_value = func, 0
        self.server.apps = {'my_module': app}
        func.is_ui = True

        self.assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected)

    def test_RestServer_getPageDetails_generated_raw(self):
        def func(args, kwargs):
            return ['foo', 'bar']

        kwargs = {}
        permissions = []
        user_info = {'email': 'foo@bar.com'}
        type = 'raw'
        expected = {'additional_content': ["['foo', 'bar']"], 'title': 'View foo', 'type': 'raw'}

        args = ['my_module', 'my_method', 'foo']
        func.display_type = type
        func.passreq = True
        func.joinauth = True
        func.passuser = True
        func.title = 'View foo'
        func.map = {}
        func.submitto = '/forms'
        func.method = 'POST'

        app = Mock()
        app.getrestmethod.return_value = func, 0
        self.server.apps = {'my_module': app}
        func.is_ui = True

        self.assertEqual(self.server.getPageDetails(args, kwargs, permissions, user_info), expected)

    def test_RestServer_createForm(self):
        def func(*args, **kwargs):
            pass

        results = {'_id1': 'my_id1', '_id2': 'my_id2', '_id3': 'my_id3'}
        expected = '<form class="form" method="POST" action="/forms/{{_id}}"><table><tr><td class="form_title"> Id1:</td><td><input name="_id1" value="my_id1"></td></tr><td class="form_title"> Id2:</td><td><textarea name="_id2">my_id2</textarea></td></tr><td class="form_title"> Id3:</td><td><select name="_id3"><option  value="my_id1">my_id1</option><option  value="my_id2">my_id2</option><option selected  value="my_id3">my_id3</option></select></td></tr><input type="hidden" name="XXredirect_internalXX" value="/foo"><tr><td><button name="save">Save</button></td></tr></form>'
        current_page = '/foo'

        func.map = [{'key': '_id1', 'type': 'string'},
                    {'key': '_id2', 'type': 'bigstring'},
                    {'key': '_id3', 'type': 'selection', 'options': ['my_id1', 'my_id2', 'my_id3']}]
        func.submitto = '/forms/{{_id}}'
        func.method = 'POST'

        self.assertEqual(self.server.createForm(func, results, current_page), expected)

    def test_RestServer_getMappedData_dict(self):
        def func(*args, **kwargs):
            pass

        response = [{'foo': 'foo1', 'bar': 'bar1', 'baz': 1, 'boo': True}]
        args = []
        query_results = True
        expected_py2 = {'cols': [{'id': 'foo', 'label': 'Foo', 'pattern': '', 'type': 'string'},
                                 {'id': 'baz', 'label': 'Baz', 'pattern': '', 'type': 'number'},
                                 {'id': 'boo', 'label': 'Boo', 'pattern': '', 'type': 'boolean'}],
                        'rows': [['foo1', 1, True]]}
        expected_py3 = {'cols': [{'id': 'foo', 'label': 'Foo', 'pattern': '', 'type': 'string'},
                                 {'id': 'baz', 'label': 'Baz', 'pattern': '', 'type': 'number'},
                                 {'id': 'boo', 'label': 'Boo', 'pattern': '', 'type': 'boolean'}],
                        'rows': [[b'foo1', 1, True]]}

        func.map = {'foo': {'order': 0}, 'bar': {'show': False}}

        self._assertEqual(self.server.getMappedData(func, response, args, query_results), expected_py2, expected_py3)

    def test_RestServer_getMappedData_list(self):
        def func(*args, **kwargs):
            pass

        def call_func(*args, **kwargs):
            return '{}_call_func'.format(args[0].get('value'))

        def produce_func(*args, **kwargs):
            return '{}_produce_func'.format(args[0].get('value'))

        response = ['foo', 'bar', 'baz']
        args = []
        query_results = True
        expected_py2 = {'cols': [{'id': 'value', 'label': 'Value', 'pattern': '', 'type': 'string'},
                                 {'id': 'baz', 'label': 'Baz', 'pattern': '', 'type': 'string'},
                                 {'id': 'bar', 'label': 'Bar', 'pattern': '', 'type': 'string'}],
                        'rows': [['foo', 'foo_call_func', 'foo_produce_func'],
                                 ['bar', 'bar_call_func', 'bar_produce_func'],
                                 ['baz', 'baz_call_func', 'baz_produce_func']]}
        expected_py3 = {'cols': [{'id': 'value', 'label': 'Value', 'pattern': '', 'type': 'string'},
                                 {'id': 'bar', 'label': 'Bar', 'pattern': '', 'type': 'string'},
                                 {'id': 'baz', 'label': 'Baz', 'pattern': '', 'type': 'string'}],
                        'rows': [['foo', 'foo_produce_func', 'foo_call_func'],
                                 ['bar', 'bar_produce_func', 'bar_call_func'],
                                 ['baz', 'baz_produce_func', 'baz_call_func']]}

        produce_func_mock = type('', (), {})()
        produce_func_mock.produce = produce_func
        func.address = '127.0.0.1/db'
        func.map = {'foo': {}, 'bar': {'value': produce_func_mock}, 'baz': {'value': call_func}}

        self._assertEqual(self.server.getMappedData(func, response, args, query_results), expected_py2, expected_py3)

    def test_RestServer_renderTable_queryResults(self):
        def func(*args, **kwargs):
            pass

        address = '{}/../lapinpy/scripts'.format(os.path.dirname(os.path.abspath(__file__)))

        data = [{'id': 'foo'}]
        args = []
        query_results = {'title': 'My title',
                         'table': {'columns': [['id', 'name']],
                                   'sort': {'enabled': True, 'default': {'column': '_id', 'direction': 'asc'}},
                                   'multi_select': {'actions': [{'name': 'action_1'}]}, 'edit': True,
                                   'actions': ['action_2']},
                         'filter': {'options': ['id'],
                                    'saved_queries': ['metadata-search2']},
                         'data': {'url': 'scripts', 'id_field': 'id', 'download': True}, 'file': address}
        kwargs = {'what': 'id = foo'}
        expected_py2 = u'<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"sort": "_id asc", "record_per_page": 100, "functions": ["multi_select", "filter", "filter_savedqueries", "download", "pager", "edit"], "id_field": "id", "fields": ["id"], "tbody": "", "multi_select": {"actions": [{"name": "action_1"}]}, "filter": {"search_keys": ["id"], "always_use_default_query": false, "allow_empty_query": false}, "headers": ["id"], "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h3 class=\\"sub-table\\">My title</h3>\\n\\t<div class=\\"search_filter\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<form ng-submit=\\"dataChange(1, true)\\">\\n\\t\\t<div class=\\"saved_query\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h4>Saved Queries</h4>\\n\\t<ul>\\n\\t\\t<li><div class=\\"delete\\" title=\\"Delete\\">X</div><a href=\\"#\\" title=\\"id is foo\\">query_1</a></li><li><div class=\\"delete\\" title=\\"Delete\\">X</div><a href=\\"#\\" title=\\"id is bar\\">query_2</a></li>\\n\\t</ul>\\t\\n</div>\\n\\t\\t<div class=\\"query\\">\\n\\t\\t\\t<h4>Query</h4>\\n\\t\\t\\t<ul>\\n\\t\\t\\t\\t<li ng-repeat=\\"condition in conditions\\">\\n\\t\\t\\t\\t\\t<div class=\\"delete\\" ng-click=\\"remove_filter($index)\\" title=\\"Remove condition from query\\">X</div>\\n\\t\\t\\t\\t\\t<a class=\\"condition\\" value=\\"{{condition.value}}\\" href=\\"#\\" ng-click=\\"editFilter($index, condition.key, condition.operator, condition.value, condition.type)\\" title=\\"Edit Filter\\">{{get_condition_key_display(condition.key)}} <span class=\\"uppercase\\">{{condition.operator}}</span> {{condition.value}}</a>\\n\\t\\t\\t\\t</li>\\n\\t\\t\\t</ul>\\n\\t\\t\\t<div class=\\"add_query_value\\">\\n                   <select class=\\"filterkey minput\\" name=\\"key\\" ng-model=\\"current_filter.key\\" ng-change=\\"check_type()\\"><option value=\\"id\\">Id</option></select>\\n                   <select class=\\"filterop minput\\" name=\\"op\\" ng-model=\\"current_filter.operator\\"><option value=\\"=\\">=</option><option value=\\"&ne;\\">&ne;</option><option value=\\"<\\"><</option><option value=\\"<=\\"><=</option><option value=\\">\\">></option><option value=\\">=\\">>=</option><option value=\\"like\\">like</option><option value=\\"in\\">in</option><option value=\\"not in\\">not in</option></select>\\n                   <input class=\\"filterval minput\\" type=\\"text\\" name=\\"value\\" ng-click=\\"value_click()\\" ng-model=\\"current_filter.value\\" placeholder=\\"filter\\" spellcheck=\\"false\\" />\\n                   <input type=\\"hidden\\" class=\\"minput\\" disabled/>\\n                   <button type=\\"submit\\" class=\\"button\\" ng-disabled=\\"!(current_filter.key)\\" ng-click=\\"add_filter()\\">Add</button>\\n\\t\\t\\t</div>\\t\\n\\t\\t</div>\\t\\t\\t\\t\\t\\t\\t\\n\\t\\t<div class=\\"actions\\">\\n\\t\\t\\t<button type=\\"submit\\" class=\\"button\\" ng-disabled=\\"!filter_enabled || (last_query == query)\\" value=\\"Filter\\">Filter</button>\\n\\t\\t\\t<button type=\\"button\\" class=\\"save_query button\\" disabled=\\"disabled\\" ng-click=\\"save_query_details()\\" title=\\"Save Query\\">Save Query</button>\\n\\t\\t</div>\\n\\t</form>\\n</div>\\n\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t<div class=\\"download_results\\" title=\\"Download All Results\\">\\n\\t<i class=\\"download material-icons md-18\\" ng-class=\\"(result_desc.total == 0)?\'disabled\':\'\'\\" ng-click=\\"download_query()\\"/>&#xE2C4;</i>\\n</div>   \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t<div action=\\"multiSelect\\">\\n     <div class=\\"pipe_seperate\\"></div>\\n     <div class=\\"selected\\"><span>Selected: <span class=\\"selected_count\\" ng-class=\\"(select.count == 0)?\'disabled\':\'\'\\" class=\\"selected\\" ng-click=\\"view_selected()\\" title=\\"View Selected Results\\">{{select.info}}</span></span></div>\\n</div>  \\n    </div>\\n    <div class=\\"result_pager\\">\\n\\t<div class=\\"first_func page_func disabled\\" title=\\"First Page\\"><div class=\\"left_arrow\\"></div><div class=\\"left_arrow\\"></div></div>\\n    <div class=\\"prev_func page_func disabled\\" title=\\"Prev Page\\"><div class=\\"left_arrow\\"></div></div>\\n    <div class=\\"page\\"><input value=\\"1\\"/></div>\\n    <div class=\\"next_func page_func disabled\\" title=\\"Next Page\\"><div class=\\"right_arrow\\"></div></div>\\n    <div class=\\"last_func page_func disabled\\" title=\\"Last Page\\"><div class=\\"right_arrow\\"></div><div class=\\"right_arrow\\"></div></div>\\n</div>\\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th align=\\"left\\" class=\\"q_select\\"><label><input type=\\"checkbox\\" ng-model=\\"select_all\\" ng-change=\\"select_all_tr()\\"><div class=\\"custom-checkbox\\"></div></label></th><th name=\\"id\\" class=\\"sort\\">id<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"2\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t<div class=\\"select_actions\\"><div class=\\"action_buttons\\"><button disabled=\\"disabled\\" type=\\"button\\" class=\\"button\\" title=\\"action_1\\" ng-click=\\"confirm_select_action({\'name\': \'action_1\'})\\">action_1</button></div></div>\\n    <div class=\\"actions\\"><div class=\\"action_buttons\\">action_2</div></div>\\n</div>\\n", "columns": [["id", "name"]], "saved_queries": {"page": ["metadata-search2"], "queries": [{"description": "id is foo", "id": "foo", "query": "", "_id": "62791a11c2c506c5afdfce76", "page": "metadata-search2", "name": "query_1"}, {"description": "id is bar", "id": "bar", "query": "", "_id": "62791a11c2c506c5afdfce77", "page": "metadata-search2", "name": "query_2"}]}, "result_desc": {"total_formatted": 1, "total": 1}, "data": {"default_query": "id = foo", "url": "scripts", "id_field": "id", "record_per_page": 100, "db_address": "acute.selec/scripts", "download": true}, "pageScript": "", "user": "foobar"},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n        <script type="text/javascript" src="/scripts/query_results_multi_select.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_filter.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_filter_savedqueries.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_download.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_pager.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_edit.js"></script>\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My title</h3>\n\t<div class="search_filter" ng-keydown="keypressed($event)">\n\t<form ng-submit="dataChange(1, true)">\n\t\t<div class="saved_query" ng-keydown="keypressed($event)">\n\t<h4>Saved Queries</h4>\n\t<ul>\n\t\t<li><div class="delete" title="Delete">X</div><a href="#" title="id is foo">query_1</a></li><li><div class="delete" title="Delete">X</div><a href="#" title="id is bar">query_2</a></li>\n\t</ul>\t\n</div>\n\t\t<div class="query">\n\t\t\t<h4>Query</h4>\n\t\t\t<ul>\n\t\t\t\t<li ng-repeat="condition in conditions">\n\t\t\t\t\t<div class="delete" ng-click="remove_filter($index)" title="Remove condition from query">X</div>\n\t\t\t\t\t<a class="condition" value="{{condition.value}}" href="#" ng-click="editFilter($index, condition.key, condition.operator, condition.value, condition.type)" title="Edit Filter">{{get_condition_key_display(condition.key)}} <span class="uppercase">{{condition.operator}}</span> {{condition.value}}</a>\n\t\t\t\t</li>\n\t\t\t</ul>\n\t\t\t<div class="add_query_value">\n                   <select class="filterkey minput" name="key" ng-model="current_filter.key" ng-change="check_type()"><option value="id">Id</option></select>\n                   <select class="filterop minput" name="op" ng-model="current_filter.operator"><option value="=">=</option><option value="&ne;">&ne;</option><option value="<"><</option><option value="<="><=</option><option value=">">></option><option value=">=">>=</option><option value="like">like</option><option value="in">in</option><option value="not in">not in</option></select>\n                   <input class="filterval minput" type="text" name="value" ng-click="value_click()" ng-model="current_filter.value" placeholder="filter" spellcheck="false" />\n                   <input type="hidden" class="minput" disabled/>\n                   <button type="submit" class="button" ng-disabled="!(current_filter.key)" ng-click="add_filter()">Add</button>\n\t\t\t</div>\t\n\t\t</div>\t\t\t\t\t\t\t\n\t\t<div class="actions">\n\t\t\t<button type="submit" class="button" ng-disabled="!filter_enabled || (last_query == query)" value="Filter">Filter</button>\n\t\t\t<button type="button" class="save_query button" disabled="disabled" ng-click="save_query_details()" title="Save Query">Save Query</button>\n\t\t</div>\n\t</form>\n</div>\n\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t<div class="download_results" title="Download All Results">\n\t<i class="download material-icons md-18" ng-class="(result_desc.total == 0)?\'disabled\':\'\'" ng-click="download_query()"/>&#xE2C4;</i>\n</div>   \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t<div action="multiSelect">\n     <div class="pipe_seperate"></div>\n     <div class="selected"><span>Selected: <span class="selected_count" ng-class="(select.count == 0)?\'disabled\':\'\'" class="selected" ng-click="view_selected()" title="View Selected Results">{{select.info}}</span></span></div>\n</div>  \n    </div>\n    <div class="result_pager">\n\t<div class="first_func page_func disabled" title="First Page"><div class="left_arrow"></div><div class="left_arrow"></div></div>\n    <div class="prev_func page_func disabled" title="Prev Page"><div class="left_arrow"></div></div>\n    <div class="page"><input value="1"/></div>\n    <div class="next_func page_func disabled" title="Next Page"><div class="right_arrow"></div></div>\n    <div class="last_func page_func disabled" title="Last Page"><div class="right_arrow"></div><div class="right_arrow"></div></div>\n</div>\n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="id" class="sort">id<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="2"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t<div class="select_actions"><div class="action_buttons"><button disabled="disabled" type="button" class="button" title="action_1" ng-click="confirm_select_action({\'name\': \'action_1\'})">action_1</button></div></div>\n    <div class="actions"><div class="action_buttons">action_2</div></div>\n</div>\n\n</div>'
        expected_py3 = '<script src="/scripts/angular.js"></script>\n<script src="/scripts/angular-sanitize.js"></script>\n<script src="/scripts/angular-strap.min.js" ></script>\n<script src="/scripts/angular-strap.tpl.min.js"></script>\n<script src="/scripts/angular-animate.js"></script>\n<script src="/scripts/text_formatting.js"></script>\n<script src="/scripts/query_results.js"></script>\n<link rel="stylesheet" href="/scripts/bootstrap.min.css">\n<link rel="stylesheet" href="/scripts/angular-motion.min.css">\n<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">\n<!-- <link rel="stylesheet" href="/scripts/table.css"> -->\n\n<div class="queryresults">\n    <script type=\'text/javascript\'>\n        var query_results = angular.module(\'query_results\', [\'ngAnimate\', \'ngSanitize\', \'mgcrea.ngStrap\']),\n            data = {"fields": ["id"], "columns": [["id", "name"]], "data": {"url": "scripts", "id_field": "id", "download": true, "db_address": "acute.selec/scripts", "record_per_page": 100, "default_query": "id = foo"}, "pageScript": "", "filter": {"search_keys": ["id"], "always_use_default_query": false, "allow_empty_query": false}, "user": "foobar", "saved_queries": {"queries": [{"name": "query_1", "_id": "62791a11c2c506c5afdfce76", "id": "foo", "page": "metadata-search2", "description": "id is foo", "query": ""}, {"name": "query_2", "_id": "62791a11c2c506c5afdfce77", "id": "bar", "page": "metadata-search2", "description": "id is bar", "query": ""}], "page": ["metadata-search2"]}, "html": "<div class=\\"search_window\\" ng-app=\'query_results\' ng-controller=\\"QueryResults\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h3 class=\\"sub-table\\">My title</h3>\\n\\t<div class=\\"search_filter\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<form ng-submit=\\"dataChange(1, true)\\">\\n\\t\\t<div class=\\"saved_query\\" ng-keydown=\\"keypressed($event)\\">\\n\\t<h4>Saved Queries</h4>\\n\\t<ul>\\n\\t\\t<li><div class=\\"delete\\" title=\\"Delete\\">X</div><a href=\\"#\\" title=\\"id is foo\\">query_1</a></li><li><div class=\\"delete\\" title=\\"Delete\\">X</div><a href=\\"#\\" title=\\"id is bar\\">query_2</a></li>\\n\\t</ul>\\t\\n</div>\\n\\t\\t<div class=\\"query\\">\\n\\t\\t\\t<h4>Query</h4>\\n\\t\\t\\t<ul>\\n\\t\\t\\t\\t<li ng-repeat=\\"condition in conditions\\">\\n\\t\\t\\t\\t\\t<div class=\\"delete\\" ng-click=\\"remove_filter($index)\\" title=\\"Remove condition from query\\">X</div>\\n\\t\\t\\t\\t\\t<a class=\\"condition\\" value=\\"{{condition.value}}\\" href=\\"#\\" ng-click=\\"editFilter($index, condition.key, condition.operator, condition.value, condition.type)\\" title=\\"Edit Filter\\">{{get_condition_key_display(condition.key)}} <span class=\\"uppercase\\">{{condition.operator}}</span> {{condition.value}}</a>\\n\\t\\t\\t\\t</li>\\n\\t\\t\\t</ul>\\n\\t\\t\\t<div class=\\"add_query_value\\">\\n                   <select class=\\"filterkey minput\\" name=\\"key\\" ng-model=\\"current_filter.key\\" ng-change=\\"check_type()\\"><option value=\\"id\\">Id</option></select>\\n                   <select class=\\"filterop minput\\" name=\\"op\\" ng-model=\\"current_filter.operator\\"><option value=\\"=\\">=</option><option value=\\"&ne;\\">&ne;</option><option value=\\"<\\"><</option><option value=\\"<=\\"><=</option><option value=\\">\\">></option><option value=\\">=\\">>=</option><option value=\\"like\\">like</option><option value=\\"in\\">in</option><option value=\\"not in\\">not in</option></select>\\n                   <input class=\\"filterval minput\\" type=\\"text\\" name=\\"value\\" ng-click=\\"value_click()\\" ng-model=\\"current_filter.value\\" placeholder=\\"filter\\" spellcheck=\\"false\\" />\\n                   <input type=\\"hidden\\" class=\\"minput\\" disabled/>\\n                   <button type=\\"submit\\" class=\\"button\\" ng-disabled=\\"!(current_filter.key)\\" ng-click=\\"add_filter()\\">Add</button>\\n\\t\\t\\t</div>\\t\\n\\t\\t</div>\\t\\t\\t\\t\\t\\t\\t\\n\\t\\t<div class=\\"actions\\">\\n\\t\\t\\t<button type=\\"submit\\" class=\\"button\\" ng-disabled=\\"!filter_enabled || (last_query == query)\\" value=\\"Filter\\">Filter</button>\\n\\t\\t\\t<button type=\\"button\\" class=\\"save_query button\\" disabled=\\"disabled\\" ng-click=\\"save_query_details()\\" title=\\"Save Query\\">Save Query</button>\\n\\t\\t</div>\\n\\t</form>\\n</div>\\n\\n\\t<div class=\\"results\\">\\n\\t\\t<div class=\\"result_info\\">\\n  \\t<div class=\\"result_total\\">\\n       \\t<div class=\\"download_results\\" title=\\"Download All Results\\">\\n\\t<i class=\\"download material-icons md-18\\" ng-class=\\"(result_desc.total == 0)?\'disabled\':\'\'\\" ng-click=\\"download_query()\\"/>&#xE2C4;</i>\\n</div>   \\t\\n       \\t<div class=\\"result_desc\\"><span>Total: 1</span></div>\\n       \\t<div action=\\"multiSelect\\">\\n     <div class=\\"pipe_seperate\\"></div>\\n     <div class=\\"selected\\"><span>Selected: <span class=\\"selected_count\\" ng-class=\\"(select.count == 0)?\'disabled\':\'\'\\" class=\\"selected\\" ng-click=\\"view_selected()\\" title=\\"View Selected Results\\">{{select.info}}</span></span></div>\\n</div>  \\n    </div>\\n    <div class=\\"result_pager\\">\\n\\t<div class=\\"first_func page_func disabled\\" title=\\"First Page\\"><div class=\\"left_arrow\\"></div><div class=\\"left_arrow\\"></div></div>\\n    <div class=\\"prev_func page_func disabled\\" title=\\"Prev Page\\"><div class=\\"left_arrow\\"></div></div>\\n    <div class=\\"page\\"><input value=\\"1\\"/></div>\\n    <div class=\\"next_func page_func disabled\\" title=\\"Next Page\\"><div class=\\"right_arrow\\"></div></div>\\n    <div class=\\"last_func page_func disabled\\" title=\\"Last Page\\"><div class=\\"right_arrow\\"></div><div class=\\"right_arrow\\"></div></div>\\n</div>\\n</div>\\t   \\t\\t\\n\\t\\t<div class=\\"results_table\\" cust-scroller>\\n\\t\\t    <table class=\\"qresults\\" cellspacing=\\"0\\">\\n                <thead>\\n                    <tr class=\\"header\\"><th align=\\"left\\" class=\\"q_select\\"><label><input type=\\"checkbox\\" ng-model=\\"select_all\\" ng-change=\\"select_all_tr()\\"><div class=\\"custom-checkbox\\"></div></label></th><th name=\\"id\\" class=\\"sort\\">id<span class=\\"sort_indicator \\"></span></th></tr>\\n\\t\\t        </thead>\\n\\t\\t        <tbody>\\n                    <tr class=\\"noResults hideElement\\"><td colspan=\\"2\\"><p>&nbsp;</p></td></tr>\\n\\t\\t\\t       \\t\\n\\t\\t       </tbody>\\n\\t\\t    </table>\\n\\t    </div>\\n\\t</div>\\n\\t<div class=\\"select_actions\\"><div class=\\"action_buttons\\"><button disabled=\\"disabled\\" type=\\"button\\" class=\\"button\\" title=\\"action_1\\" ng-click=\\"confirm_select_action({\'name\': \'action_1\'})\\">action_1</button></div></div>\\n    <div class=\\"actions\\"><div class=\\"action_buttons\\">action_2</div></div>\\n</div>\\n", "headers": ["id"], "result_desc": {"total_formatted": 1, "total": 1}, "tbody": "", "multi_select": {"actions": [{"name": "action_1"}]}, "functions": ["multi_select", "filter", "filter_savedqueries", "download", "pager", "edit"], "record_per_page": 100, "id_field": "id", "sort": "_id asc"},\n            functions = data[\'functions\'],\n            $queryresults = $(\'.queryresults\');\n\n        query_results.config(function ($modalProvider) {\n            angular.extend($modalProvider.defaults, {html: true});\n        }).controller(\'QueryResults\', [\'$scope\', \'$http\', \'$location\', \'$modal\', function ($scope, $http, $location, $modal) {\n            query_results_initialize($scope, $http, $location, $modal);\n        }]);\n    </script>\n\n    \n        <script type="text/javascript" src="/scripts/query_results_multi_select.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_filter.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_filter_savedqueries.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_download.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_pager.js"></script>\n    \n        <script type="text/javascript" src="/scripts/query_results_edit.js"></script>\n    \n<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My title</h3>\n\t<div class="search_filter" ng-keydown="keypressed($event)">\n\t<form ng-submit="dataChange(1, true)">\n\t\t<div class="saved_query" ng-keydown="keypressed($event)">\n\t<h4>Saved Queries</h4>\n\t<ul>\n\t\t<li><div class="delete" title="Delete">X</div><a href="#" title="id is foo">query_1</a></li><li><div class="delete" title="Delete">X</div><a href="#" title="id is bar">query_2</a></li>\n\t</ul>\t\n</div>\n\t\t<div class="query">\n\t\t\t<h4>Query</h4>\n\t\t\t<ul>\n\t\t\t\t<li ng-repeat="condition in conditions">\n\t\t\t\t\t<div class="delete" ng-click="remove_filter($index)" title="Remove condition from query">X</div>\n\t\t\t\t\t<a class="condition" value="{{condition.value}}" href="#" ng-click="editFilter($index, condition.key, condition.operator, condition.value, condition.type)" title="Edit Filter">{{get_condition_key_display(condition.key)}} <span class="uppercase">{{condition.operator}}</span> {{condition.value}}</a>\n\t\t\t\t</li>\n\t\t\t</ul>\n\t\t\t<div class="add_query_value">\n                   <select class="filterkey minput" name="key" ng-model="current_filter.key" ng-change="check_type()"><option value="id">Id</option></select>\n                   <select class="filterop minput" name="op" ng-model="current_filter.operator"><option value="=">=</option><option value="&ne;">&ne;</option><option value="<"><</option><option value="<="><=</option><option value=">">></option><option value=">=">>=</option><option value="like">like</option><option value="in">in</option><option value="not in">not in</option></select>\n                   <input class="filterval minput" type="text" name="value" ng-click="value_click()" ng-model="current_filter.value" placeholder="filter" spellcheck="false" />\n                   <input type="hidden" class="minput" disabled/>\n                   <button type="submit" class="button" ng-disabled="!(current_filter.key)" ng-click="add_filter()">Add</button>\n\t\t\t</div>\t\n\t\t</div>\t\t\t\t\t\t\t\n\t\t<div class="actions">\n\t\t\t<button type="submit" class="button" ng-disabled="!filter_enabled || (last_query == query)" value="Filter">Filter</button>\n\t\t\t<button type="button" class="save_query button" disabled="disabled" ng-click="save_query_details()" title="Save Query">Save Query</button>\n\t\t</div>\n\t</form>\n</div>\n\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t<div class="download_results" title="Download All Results">\n\t<i class="download material-icons md-18" ng-class="(result_desc.total == 0)?\'disabled\':\'\'" ng-click="download_query()"/>&#xE2C4;</i>\n</div>   \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t<div action="multiSelect">\n     <div class="pipe_seperate"></div>\n     <div class="selected"><span>Selected: <span class="selected_count" ng-class="(select.count == 0)?\'disabled\':\'\'" class="selected" ng-click="view_selected()" title="View Selected Results">{{select.info}}</span></span></div>\n</div>  \n    </div>\n    <div class="result_pager">\n\t<div class="first_func page_func disabled" title="First Page"><div class="left_arrow"></div><div class="left_arrow"></div></div>\n    <div class="prev_func page_func disabled" title="Prev Page"><div class="left_arrow"></div></div>\n    <div class="page"><input value="1"/></div>\n    <div class="next_func page_func disabled" title="Next Page"><div class="right_arrow"></div></div>\n    <div class="last_func page_func disabled" title="Last Page"><div class="right_arrow"></div><div class="right_arrow"></div></div>\n</div>\n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th><th name="id" class="sort">id<span class="sort_indicator "></span></th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="2"><p>&nbsp;</p></td></tr>\n\t\t\t       \t\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t<div class="select_actions"><div class="action_buttons"><button disabled="disabled" type="button" class="button" title="action_1" ng-click="confirm_select_action({\'name\': \'action_1\'})">action_1</button></div></div>\n    <div class="actions"><div class="action_buttons">action_2</div></div>\n</div>\n\n</div>'

        func.address = 'acute.select'
        self.server.run_method = lambda a, b, *c, **d: {'total_formatted': 1, 'total': 1}
        mongo = Mock()
        mongo.getUserQueries.return_value = [{'name': 'query_2', '_id': '62791a11c2c506c5afdfce77', 'id': 'bar', 'page': 'metadata-search2', 'description': 'id is bar', 'query': ''},
                                             {'name': 'query_1', '_id': '62791a11c2c506c5afdfce76', 'id': 'foo', 'page': 'metadata-search2', 'description': 'id is foo', 'query': ''}]
        self.server.temp_mongo = mongo
        self.server.userdata = {'foobar': {'name': 'foobar'}}

        self._assertEqual(self.server.renderTable(func, data, args, query_results, kwargs), expected_py2, expected_py3)

    def test_RestServer_renderTable_dict(self):
        def func(*args, **kwargs):
            pass

        data = {'No_0_id_number_1_perc_20percent': 'foo'}
        args = []
        query_results = None
        kwargs = {}
        expected = u'<table>\n    \n\n    <tr><td class="key"># 0 Id # 1 Perc 20%:</td> <td class="value">foo</td></tr>\n\n    \n</table>'

        self.assertEqual(self.server.renderTable(func, data, args, query_results, kwargs), expected)

    @parameterized.expand([
        ('data', [{'foo': 'foo1', 'bar': 'bar1', 'baz': 1, 'boo': True}],
         '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My title</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Foo" >Foo</th><th name="Baz" >Baz</th><th name="Boo" >Boo</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="3"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>foo1</td><td>1</td><td>True</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n',
         '<div class="search_window" ng-app=\'query_results\' ng-controller="QueryResults" ng-keydown="keypressed($event)">\n\t<h3 class="sub-table">My title</h3>\n\t\n\t<div class="results">\n\t\t<div class="result_info">\n  \t<div class="result_total">\n       \t\n       \t<div class="result_desc"><span>Total: 1</span></div>\n       \t\n    </div>\n    \n</div>\t   \t\t\n\t\t<div class="results_table" cust-scroller>\n\t\t    <table class="qresults" cellspacing="0">\n                <thead>\n                    <tr class="header"><th name="Foo" >Foo</th><th name="Baz" >Baz</th><th name="Boo" >Boo</th></tr>\n\t\t        </thead>\n\t\t        <tbody>\n                    <tr class="noResults hideElement"><td colspan="3"><p>&nbsp;</p></td></tr>\n\t\t\t       \t<tr data-id="ABC" data-name="_id"><td>b\'foo1\'</td><td>1</td><td>True</td></tr>\n\t\t       </tbody>\n\t\t    </table>\n\t    </div>\n\t</div>\n\t\n    \n</div>\n'),
        ('no_data', [], '<h3>My title<h3>', None),
    ])
    @patch('lapinpy.restful.uuid')
    def test_RestServer_renderTable_other(self, _description, data, expected_py2, expected_py3, mock_uuid):
        def func(*args, **kwargs):
            pass

        args = []
        query_results = None
        kwargs = {}

        func.title = 'My title'
        func.map = {'foo': {}, 'bar': {'show': False}}
        uuid4 = Mock()
        uuid4.hex = 'ABC'
        mock_uuid.uuid4.return_value = uuid4

        self._assertEqual(self.server.renderTable(func, data, args, query_results, kwargs), expected_py2, expected_py3)

    def test_RestServer_googleChartify(self):
        def func(*args, **kwargs):
            pass

        response = [{'foo': 'foo1', 'bar': 'bar1', 'baz': 1, 'boo': True}]
        args = []
        kwargs = {'tqx': 'foo'}
        expected_py2 = 'google.visualization.Query.setResponse({"table": {"rows": [{"c": [{"v": "bar1"}, {"v": "foo1"}, {"v": 1}, {"v": true}]}], "cols": [{"pattern": "", "type": "string", "id": "bar", "label": "Bar"}, {"pattern": "", "type": "string", "id": "foo", "label": "Foo"}, {"pattern": "", "type": "number", "id": "baz", "label": "Baz"}, {"pattern": "", "type": "boolean", "id": "boo", "label": "Boo"}]}, "reqId": "foo"});'
        expected_py3 = 'google.visualization.Query.setResponse({"table": {"cols": [{"id": "foo", "label": "Foo", "type": "string", "pattern": ""}, {"id": "bar", "label": "Bar", "type": "string", "pattern": ""}, {"id": "baz", "label": "Baz", "type": "number", "pattern": ""}, {"id": "boo", "label": "Boo", "type": "boolean", "pattern": ""}], "rows": [{"c": [{"v": "foo1"}, {"v": "bar1"}, {"v": 1}, {"v": true}]}]}, "reqId": "foo"});'

        self._assertEqual(self.server.googleChartify(response, func, args, kwargs), expected_py2, expected_py3)

    @parameterized.expand([
        ('no_page', {'message': 'my_func called'}, {'message': 'my_func called'}, False),
        ('page', {'records': [{'message': 'my_func called'}], 'record_count': 1}, {'message': 'my_func called'}, True),
    ])
    def test_RestServer_run_method_no_page(self, _description, func_response, expected, is_paged):
        def func(*args, **kwargs):
            return func_response

        app = Mock()
        app.my_method = func
        self.server.apps = {'my_module': app}
        if is_paged:
            func.paged = True
            self.assertEqual(next(iter(self.server.run_method('my_module', 'my_method'))), expected)
        else:
            self.assertEqual(self.server.run_method('my_module', 'my_method'), expected)

    def test_RestServer_reloadApp(self):
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'admin'}]
        app_path = resource_filename('lapinpy', 'apps/file')
        app = self.server.loadApp('{}.py'.format(app_path))
        self.server.apps[app.appname] = app

        self.server.reloadApp('{}.py'.format(app_path))

        # Verify new instance loaded
        self.assertNotEqual(self.server.apps.get(app.appname), app)

    def test_RestServer_loadApp(self):
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'admin'}]
        app_path = resource_filename('lapinpy', 'apps/file.py')

        app = self.server.loadApp(app_path)

        self.assertEqual(app.appname, 'file')

    def test_RestServer_loadApp_2(self):
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'admin'}]
        app_path = resource_filename('lapinpy', 'apps/file.py')

        app = self.server.loadApp(app_path)

        self.assertEqual(app.appname, 'file')

    def test_RestServer_reloadUrls(self):
        app = Mock()
        self.server.apps = {'my_module': app}
        app.ui_mappings = {'foo': 'foo1'}
        app.searchMethods = {'bar': 'bar1'}

        self.server.reloadUrls()

        self.assertEqual(self.server.rest_ui_components, {'foo': 'foo1'})
        self.assertEqual(self.server.search_components, {'bar': 'bar1'})

    def test_RestServer_getAppFiles(self):
        app_path = resource_filename('lapinpy', 'apps')

        found = False
        for f in self.server.getAppFiles([app_path]):
            if 'file.py' in f:
                found = True
                break

        self.assertTrue(found)

    @patch('cherrypy.process.plugins.BackgroundTask')
    def test_RestServer_loadApps(self, task):
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'admin'}]
        app_path = resource_filename('lapinpy', 'apps/file.py')

        self.server.loadApps([app_path])

        self.assertIsNotNone(self.server.apps.get('file'))

    def test_RestServer_task_thread(self):
        def func(app, *args, **kwargs):
            self.assertEqual(args, ('foo',))
            self.assertEqual(kwargs, {'bar': 'bar1'})

        queue_manager = Mock()
        self.server.queueManager = queue_manager
        queue_manager.next.side_effect = [{'queue': 'my_module/my_method',
                                           'tid': 'TID',
                                           'data': {'args': ['foo'], 'kwargs': {'bar': 'bar1'}}}, None]
        app = Mock()
        self.server.apps = {'my_module': app}
        app.my_method = func
        func.old_function = func

        self.server.task_thread()

        self.assertIn(call.finished('TID'), queue_manager.mock_calls)

    def test_RestServer_auto_reload_thread(self):
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'admin'}]
        app = Mock()
        app.location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'apps')
        app.auto_reload = True
        app.loaded_time = datetime.datetime(2000, 1, 2)
        config_manager = Mock()
        self.server.apps = {'file': app}
        self.server.configManager = config_manager

        self.server.auto_reload_thread()

        self.assertIn(call.check_for_changes(), config_manager.mock_calls)

    def test_RestServer_fillJob(self):
        ret = Mock()
        ret.job_path = '/path/to/job'
        ret.permissions = ['admin']
        ret.environment = ''
        config = Mock()
        config.url = '127.0.0.1'
        config.jamo_token = 'JAMO_TOKEN'
        config.jamo_url = '127.0.0.1/jamo'
        config.pipeline_token = 'PIPELINE_TOKEN'
        config.pipeline_url = '127.0.0.1/sdm'
        self.server.config = config

        self.server.fillJob(ret, 'file', 'get_files')

        self.assertEqual(ret.environment,
                         'export JAMO_URL="127.0.0.1/jamo" JAMO_TOKEN="JAMO_TOKEN"\nexport PIPELINE_URL="127.0.0.1/sdm" PIPELINE_TOKEN="PIPELINE_TOKEN"\n')
        self.assertEqual(ret.job_path, '/path/to/job/file/get_files/current')
        self.assertEqual(ret.rest_address, '127.0.0.1/api/core/job')

    @parameterized.expand([
        ('waitFor', True),
        ('no_waitFor', False),
    ])
    @patch.object(lapinpy_core, 'datetime')
    def test_RestServer_run_cron_method(self, _description, wait_for, mock_datetime):
        def func(*args, **kwargs):
            return mock_job

        expected = datetime.datetime(2000, 1, 2, 0, 0)
        mock_job = Mock(spec=job.Job)
        mock_job.jobPath = '/path/to/job'
        mock_job.permissions = ['admin']
        mock_job.environment = ''
        mock_job.jobName = 'my_job'
        mock_datetime.datetime.now.return_value = datetime.datetime(2000, 1, 2)
        mock_datetime.datetime.return_value = datetime.datetime(2000, 1, 2)
        config = Mock()
        config.url = '127.0.0.1'
        self.server.config = config
        func_mock = Mock()
        func.nextEvent = datetime.datetime(2000, 1, 2)
        func.enabled = True
        func.cron = ('0', '*', '*', '*', '*')
        func.__func__ = func_mock
        if wait_for:
            func.waitFor = 'foo'
        self.core.get_job.return_value = {'ended_date': None}

        self.server.run_cron_method(func, True)

        self.assertEqual(func.__func__.nextEvent, expected)

    @parameterized.expand([
        ('*', '*', [0, 1, 2, 3, 4, 5, 6]),
        ('-', '1-3', [1, 2, 3]),
        ('/', '*/2', [0, 2, 4, 6]),
        ('int', '2,3,4', [2, 3, 4]),
    ])
    def test_RestServer_getAllowedValues(self, _description, allowed, expected):
        self.assertEqual(self.server._RestServer__getAllowedValues(allowed, 0, 6), expected)

    @patch.object(lapinpy_core, 'cherrypy')
    def test_RestServer_exit(self, cherrypy):
        app = Mock()
        self.server.apps = {'file': app}

        self.server._exit(0, 0)

        self.assertIn(call.stop(), app.mock_calls)

    @patch.object(lapinpy_core, 'Flow')
    @patch.object(lapinpy_core, 'start_http_server')
    @patch.object(lapinpy_core, 'cherrypy')
    def test_RestServer_start(self, cherrypy, start_http_server, flow):
        def load_app(path):
            if 'core' in path:
                return self.core
            app = Mock()
            app.ui_mappings = {'foo': 'foo1'}
            app.searchMethods = {'bar': 'bar1'}
            app.location = path
            app.appname = os.path.basename(path).rstrip('.py')
            return app

        def load_apps(paths):
            return [load_app(path) for path in paths]

        config = {
            'lapinpy': {
                'hostname': '127.0.0.1',
                'port': 8080,
                'admins': ['foo'],
                'oauthsecretfile': '/path/to/secrets.json',
                'sslport': 8081,
                'ssl_cert': 'SSL_CERT',
                'ssl_pkey': 'SSL_PKEY',
                'ssl_chain': 'SSL_CHAIN',
                'thread_pool': 'THREAD_POOL',
                'socket_queue_size': 5,
                'cypher_key': b'cypher_key',
            }
        }
        self.core.get_permissions.return_value = [{'id': 1, 'name': 'editor'}]
        self.core.ui_mappings = {'foo': 'foo1'}
        self.core.searchMethods = {'bar': 'bar1'}
        self.core.get_user.return_value = None
        self.core._post_user.return_value = {'user_id': 'foo'}
        self.core.get_modules.return_value = [
            {'path': '/path/to', 'name': 'my_module'}
        ]
        apps = ['file']
        self.server.loadApp = load_app
        self.server.loadApps = load_apps
        my_module = Mock()
        self.server.loadCallbacks = {'my_module': my_module}

        self.server.start(config, apps)

        self.assertIn(call.post_permission(None, {'name': 'admin'}), self.core.mock_calls)
        self.assertIn(call.get_user(['foo'], None), self.core.mock_calls)
        self.assertIn(call._post_user(None, {'email': 'foo', 'name': 'foo'}), self.core.mock_calls)
        self.assertIn(call.post_userpermission(None, {'user_id': 'foo', 'permission': 'admin'}), self.core.mock_calls)
        self.assertIn(
            call.from_client_secrets_file('/path/to/secrets.json', redirect_uri='http://127.0.0.1:8080/oauth2callback',
                                          scopes=['https://www.googleapis.com/auth/userinfo.profile',
                                                  'https://www.googleapis.com/auth/userinfo.email', 'openid']),
            flow.mock_calls)
        self.assertIn(call._cpserver.Server(), cherrypy.mock_calls)
        self.assertIn(call._cpserver.Server().subscribe(), cherrypy.mock_calls)
        self.assertIn(call.engine.start(), cherrypy.mock_calls)


if __name__ == '__main__':
    unittest.main()
