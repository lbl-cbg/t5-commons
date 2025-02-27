### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import unittest
from lapinpy.apps.doc import Doc
from lapinpy.apps import doc
import os
import sys
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, call, mock_open
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, call, mock_open
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestDoc(unittest.TestCase):

    def setUp(self):
        self.config = Mock()
        self.doc = Doc(self.config)

    @patch.object(doc, 'restful')
    def test_Doc_finishLoading(self, restserver):
        app = Mock()
        app.moduleName = 'my_module'
        server = Mock()
        server.apps = {'my_app': app}
        server.env = ['python']
        restserver.RestServer.Instance.return_value = server
        self.doc.appname = 'my_app'
        self.doc.menus = []

        self.doc.finishLoading()

        self.assertIn(call.RestServer.Instance().reloadUrls(), restserver.mock_calls)
        self.assertIn({'permissions': '', 'href': '/doc/my_module', 'title': 'my_module'}, self.doc.menus)
        self.assertEqual(self.doc.modules, {'my_module': ['my_app']})

    def test_Doc_createNav(self):
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            os.mkdir('{}/bar'.format(temp_dir))
            with open('{}/bar/foo.html'.format(temp_dir), 'w'):
                self.assertEqual(self.doc.createNav(temp_dir, 'foo'),
                                 [{'children': [{'link': 'foo/bar/foo.html', 'title': 'foo'}],
                                   'link': 'foo/bar',
                                   'title': 'bar'}])

    @parameterized.expand([
        ('api_reference',
         ['api_reference', 'my_app', 'func', 'get'],
         True,
         {'content': 'Hello there',
          'moduleName': 'my_module',
          'navigation': [{'link': '/my_app/my_module/foo.html', 'title': 'foo'},
                         {'children': [{'children': [{'children': [{'highlight': True,
                                                                    'link': '/doc/my_module/api_reference/my_app/func/get',
                                                                    'selected': True,
                                                                    'title': 'get'}],
                                                      'link': '/doc/my_module/api_reference/my_app/func',
                                                      'selected': True,
                                                      'title': 'func'}],
                                        'link': '/doc/my_module/api_reference/my_app',
                                        'selected': True,
                                        'title': 'my_app'}],
                          'link': '/doc/my_module/api_reference',
                          'selected': True,
                          'title': 'Public API Reference'}],
          'path': ['api_reference', 'my_app', 'func', 'get']}
         ),
        ('index.html',
         [],
         False,
         {'content': 'Foo <aside class="note"><b>Note:</b>',
          'moduleName': 'my_module',
          'navigation': [{'link': '/my_app/my_module/foo.html', 'title': 'foo'},
                         {'children': [{'children': [
                             {'children': [{'link': '/doc/my_module/priv_api_reference/my_app/func/get',
                                            'title': 'get'}],
                              'link': '/doc/my_module/priv_api_reference/my_app/func',
                              'title': 'func'}],
                             'link': '/doc/my_module/priv_api_reference/my_app',
                             'title': 'my_app'}],
                             'link': '/doc/my_module/priv_api_reference',
                             'title': 'Private API Reference'}],
          'path': []}
         )
    ])
    def test_Doc_getApp(self, _description, args, is_public, expected):
        def func():
            pass

        with TemporaryDirectory(suffix='tmp') as temp_dir:
            os.mkdir('{}/doc'.format(temp_dir))
            with open('{}/doc/foo.html'.format(temp_dir), 'w'):
                with open('{}/doc/index.html'.format(temp_dir), 'w') as temp_html:
                    temp_html.write('Foo <aside>')
                    temp_html.flush()
                    app = Mock()
                    app.moduleName = 'my_module'
                    app.get_func = func
                    app.location = temp_dir
                    func.restful = True
                    func.description = 'Custom function'
                    func.public = is_public
                    tmpl = Mock()
                    tmpl.render.return_value = 'Hello there'
                    env = Mock()
                    env.get_template.return_value = tmpl
                    self.doc.modules = {'my_module': ['my_app']}
                    self.doc.apps = {'my_app': app}
                    self.doc.appname = 'my_app'
                    self.doc.env = env

                    self.assertEqual(self.doc.getApp('my_module', args), expected)

    @parameterized.expand([
        ('unknown', None, 'unknown :('),
        ('str', str, 'str'),
        ('list', [int, bool], ['int', 'bool'])
    ])
    def test_Doc_convertType(self, _description, ptype, expected):
        self.assertEqual(self.doc.convertType(ptype), expected)

    @parameterized.expand([
        ('example', {'type': int, 'example': 1}, [1]),
        ('bool', {'type': bool}, [False, True]),
        ('int', {'type': int}, [1, -5]),
        ('str', {'type': str}, ['a string']),
        ('list', {'type': list, 'validator': {'*': {'type': int}}}, [[1, -5]]),
        ('dict_no_validator', {'type': dict}, [{'any_key': 'any_value'}]),
        ('dict_validator_str', {'type': dict, 'validator': '*'}, [{'any_key': 'any_value'}]),
        ('dict_validator', {'type': dict, 'validator': {'foo': {'type': str}}}, [{'foo': 'a string'}]),
        ('unknown', {'type': float}, ["unknown"])
    ])
    def test_Doc_getExampleData(self, _description, parameter, expected):
        self.assertEqual(self.doc.getExampleData(parameter), expected)

    @parameterized.expand([
        ('str', '*:1', {'*': {'doc': 'Can pass in any number of keys that have any type',
                              'examples': ['string', 4, {'blah': 'hello'}],
                              'required': True,
                              'type': 'any type'}}),
        ('simple_validator', {'type': int}, {'type': 'int'}),
        ('complex_validator',
         {'type': dict,
          'validator': {'foo': {'type': list,
                                'validator': {'*': {'type': list, 'validator': {'*': {'type': int}}}, 'type': list}}}},
         {'foo': {'examples': [[[1, -5]]],
                  'required': True,
                  'type': 'list',
                  'validator': {'*': {'type': list,
                                      'validator': {'*': {'type': int}}},
                                'type': list}}, 'foo.type': 'list'}
         ),
    ])
    def test_Doc_generateValidatorCode(self, _description, validator, expected):
        self.assertEqual(self.doc.generateValidatorCode(validator), expected)

    @parameterized.expand([
        ('smaller_length', 'foo bar', 5, 'foo...'),
        ('greater_length', 'foo bar', 10, 'foo bar'),
    ])
    def test_Doc_shorten(self, _description, string, length, expected):
        self.assertEqual(self.doc.shorten(string, length), expected)

    def test_Doc_getappC(self):
        def func():
            pass

        app = Mock()
        app.moduleName = 'my_module'
        app.description = 'Some app description'
        app.return_value = lambda x: x
        app.appname = 'my_app'
        func.description = 'Some method description'
        func.restful = True
        app.get_func = func
        self.doc.apps = {'my_app': app}

        self.assertEqual(self.doc.getappC('my_app'), {'description': 'Some app description',
                                                      'methods': [{'description': 'Some method description',
                                                                   'method': 'get',
                                                                   'name': 'func'}],
                                                      'name': 'my_app'})

    def test_Doc_createArgsPath(self):
        validator = [{'name': 'foo', 'type': str}, {'name': 'bar', 'type': int}]

        self.assertEqual(self.doc.createArgsPath(validator), '/{foo}/{bar}')

    @patch('builtins.open' if sys.version_info[0] >= 3 else '__builtin__.open', new_callable=mock_open,
           read_data='@foo foo1\n@example\n@bar bar1\nbar')
    @patch.object(doc, 'os')
    def test_Doc_get_method(self, exists, mock_open):
        def func():
            pass

        app = Mock()
        app.moduleName = 'my_module'
        app.description = 'Some app description'
        app.return_value = lambda x: x
        app.appname = 'my_app'
        app.location = '/path/to/app'
        func.description = 'Some method description'
        func.restful = True
        func.returns = 'int'
        func.permissions = ['admin']
        func.validator = {'foo': {'type': str}, 'bar': {'type': int, 'required': False}}
        func.argsvalidator = [{'name': 'name', 'type': str}]
        app.get_func = func
        self.doc.apps = {'my_app': app}
        exists.os.path.exists.return_value = True

        self.assertEqual(self.doc.get_method(['my_app', 'func', 'get'], None),
                         {'application': 'my_app',
                          'args': '/{name}',
                          'description': 'Some method description',
                          'examples': [{'foo': 'foo1'}, {'bar': 'bar1bar', 'foo': 'foo1'}],
                          'html_method': 'get',
                          'name': 'func',
                          'parameters': [{'examples': ['a string'],
                                          'name': 'foo',
                                          'required': True,
                                          'type': 'str'},
                                         {'examples': [1, -5],
                                          'name': 'bar',
                                          'required': False,
                                          'type': 'int'}],
                          'permissions': ['admin'],
                          'returns': 'int',
                          'url': '/api/my_app/func'}
                         )

    def test_Doc_get_app(self):
        def func():
            pass

        app = Mock()
        app.moduleName = 'my_module'
        app.description = 'Some app description'
        app.return_value = lambda x: x
        app.appname = 'my_app'
        func.description = 'Some method description'
        func.restful = True
        app.get_func = func
        self.doc.apps = {'my_app': app}

        self.assertEqual(self.doc.get_app(['my_app'], None),
                         [{'description': 'Some app description',
                           'methods': [{'description': 'Some method description',
                                        'method': 'get',
                                        'name': 'func'}],
                           'name': 'my_app'}]
                         )

    def test_Doc_get_apps(self):
        def func():
            pass

        app = Mock()
        app.moduleName = 'my_module'
        app.description = 'Some app description'
        app.return_value = lambda x: x
        app.appname = 'my_app'
        func.description = 'Some method description'
        func.restful = True
        app.get_func = func
        self.doc.apps = {'my_app': app}

        self.assertEqual(self.doc.get_apps(None, None),
                         [{'description': 'Some app description',
                           'methods': [{'description': 'Some method description',
                                        'method': 'get',
                                        'name': 'func'}],
                           'name': 'my_app'}]
                         )


if __name__ == '__main__':
    unittest.main()
