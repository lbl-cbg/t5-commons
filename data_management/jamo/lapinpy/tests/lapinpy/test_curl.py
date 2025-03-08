import unittest
from parameterized import parameterized
from datetime import datetime
from decimal import Decimal
from lapinpy import curl
from lapinpy.curl import Curl, CurlHttpException, MultiPartForm
import urllib.error
import urllib.request
from io import StringIO
import sys
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock
    from future import standard_library
    standard_library.install_aliases()
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestCurl(unittest.TestCase):

    @parameterized.expand([
        ('type', type(str), type(str).__name__),
        ('isoformat', datetime.fromordinal(1), datetime.fromordinal(1).isoformat()),
        ('decimal', Decimal(1), "{0:.2f}".format(Decimal(1))),
        ('str', 'foo', 'foo'),
    ])
    def test_handler(self, _description, obj, expected):
        self.assertEqual(curl.handler(obj), expected)

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_get(self, urlopen, request):
        response = Mock()
        response.read.return_value = '{"foo": "bar"}'
        urlopen.return_value = response

        self.assertEqual(curl.get('http://someurl/get', foo='bar'), {'foo': 'bar'})
        request.assert_called_with('http://someurl/get?foo=bar&')

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_post(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        response.read.return_value = '{"foo": "bar"}'
        urlopen.return_value = response

        self.assertEqual(curl.post('http://someurl/post', foo='bar'), {'foo': 'bar'})
        request.assert_called_with('http://someurl/post')

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_Curl_post(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')
        method = 'post'

        resp = curl.post('api/service/endpoint', data={'foo': 'bar'}, output='https')
        self.assertEqual(resp, '{"bar":"foo"}')
        self.assertEqual(req.get_method(), method.upper())

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_Curl_put(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')
        method = 'put'

        resp = curl.put('api/service/endpoint', data={'foo': 'bar'}, output='https')
        self.assertEqual(resp, '{"bar":"foo"}')
        self.assertEqual(req.get_method(), method.upper())

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_Curl_delete(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')
        method = 'delete'

        resp = curl.delete('api/service/endpoint', data={'foo': 'bar'}, output='https')
        self.assertEqual(resp, '{"bar":"foo"}')
        self.assertEqual(req.get_method(), method.upper())

    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    def test_Curl_get_no_cache(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')

        resp = curl.get('api/service/endpoint', data={'foo': 'bar'}, output='https')
        self.assertEqual(resp, '{"bar":"foo"}')
        self.assertEqual(req.get_method(), 'GET')
        self.assertEqual(len(curl.cache), 0)

    def test_Curl_get_cache(self):
        curl = Curl('http://127.0.0.1')
        curl.cache = {'api/service/endpoint': '{"bar":"foo"}'}

        resp = curl.get('api/service/endpoint', data={'foo': 'bar'}, output='https', cache=True)
        response = resp
        self.assertEqual(response, '{"bar":"foo"}')

    @parameterized.expand([
        (500,),
        (524,),
    ])
    @patch.object(urllib.request, 'Request')
    @patch.object(urllib.request, 'urlopen')
    @patch.object(curl, 'sleep')
    def test_Curl_retry_with_retriable_error(self, http_code, sleep, urlopen, request):
        error = urllib.error.HTTPError('http://127.0.0.1', http_code, 'Error', [], None)
        error.readlines = lambda: (b'Error', )
        error.geturl = lambda: 'http://127.0.0.1'
        urlopen.side_effect = error
        curl = Curl('http://127.0.0.1', retry=3)

        self.assertRaises(CurlHttpException, curl.get, 'api/service/endpoint', data={'foo': 'bar'}, output='https')
        # Verify 4 calls (original call and 3 retries)
        self.assertEqual(len(urlopen.mock_calls), 4)

    @patch.object(urllib.request, 'Request')
    def test_Curl_to_struct(self, request):
        curl = Curl('http://127.0.0.1')

        struct = curl.toStruct({'queue_status': {'PREP_FAILED': 6}})

        self.assertTrue(callable(struct))
        self.assertEqual(struct.queue_status.PREP_FAILED, 6)

    @parameterized.expand([
        ('get_json', 'GET', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('get_text', 'GET', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('delete_json', 'DELETE', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('delete_text', 'DELETE', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('put_json_data_dict', 'PUT', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json'},
         '{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('put_text_data_dict', 'PUT', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json'},
         '{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('put_json_data_str', 'PUT', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, 'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('put_text_data_str', 'PUT', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, 'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('post_json_data_dict', 'POST', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json'},
         '{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('post_text_data_dict', 'POST', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json'},
         '{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('post_json_data_str', 'POST', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, 'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('post_text_data_str', 'POST', 'foo=foo+bar', 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, 'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
    ])
    @patch('lapinpy.curl.urllib.request.urlopen')
    def test_Curl_call(self, _description, method, data, output, expected, expected_request_headers,
                       expected_request_data, expected_full_url, urlopen_mock):
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if sys.version_info[0] >= 3:
            if expected_request_data is not None:
                expected_request_data = expected_request_data.encode('utf-8')
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

        urlopen_response = Mock()
        urlopen_response.read = lambda: '{"foo": "bar"}'
        urlopen_mock.return_value = urlopen_response
        curl = Curl('http://127.0.0.1', appToken='SOME_TOKEN')

        self.assertEqual(curl._Curl__call('some/api', method, data, output), expected)
        request = urlopen_mock.call_args[0][0]
        self.assertEqual(request.headers, expected_request_headers)
        self.assertEqual(request.data, expected_request_data)
        self.assertEqual(request.full_url, expected_full_url)

    @patch.object(curl, 'make_boundary')
    @patch('lapinpy.curl.urllib.request.urlopen')
    def test_Curl_call_multipartform(self, urlopen_mock, make_boundary_mock):
        make_boundary_mock.return_value = '===============1234567891234567891=='
        method = 'POST'
        data = MultiPartForm()
        output = 'json'
        expected = {'foo': 'bar'}
        expected_request_headers = {'Content-type': 'multipart/form-data; boundary================1234567891234567891==', 'Authorization': 'Application SOME_TOKEN',
                                    'Content-length': 42}
        expected_request_data = str(MultiPartForm())
        expected_full_url = 'http://127.0.0.1/some/api'
        urlopen_response = Mock()
        urlopen_response.read = lambda: '{"foo": "bar"}'
        urlopen_mock.return_value = urlopen_response
        curl = Curl('http://127.0.0.1', appToken='SOME_TOKEN')
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if sys.version_info[0] >= 3:
            expected_request_data = expected_request_data.encode('utf-8')
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

        self.assertEqual(curl._Curl__call('some/api', method, data, output), expected)
        request = urlopen_mock.call_args[0][0]
        self.assertEqual(request.headers, expected_request_headers)
        self.assertEqual(request.data, expected_request_data)
        self.assertEqual(request.full_url, expected_full_url)

    @patch.object(curl, 'make_boundary')
    def test_Multipartform_get_content_type(self, boundary):
        boundary.return_value = 'A' * 40
        multipartform = MultiPartForm()

        self.assertEqual(multipartform.get_content_type(),
                         'multipart/form-data; boundary={}'.format('A' * 40))

    def test_Multipartform_add_field(self):
        multipartform = MultiPartForm()

        multipartform.add_field('foo', 'bar')

        self.assertEqual(multipartform.form_fields, [('foo', 'bar')])

    def test_Multipartform_add_file_guess_mime_type(self):
        multipartform = MultiPartForm()
        filehandle = StringIO(u'{"foo": "bar"}')

        multipartform.add_file('field', '/tmp/foo.json', filehandle)

        self.assertEqual(multipartform.files, [('field', '/tmp/foo.json', 'application/json', u'{"foo": "bar"}')])

    def test_Multipartform_add_file_explicit_mime_type(self):
        multipartform = MultiPartForm()
        filehandle = StringIO(u'foo=bar')

        multipartform.add_file('field', '/tmp/foo', filehandle, 'text/plain')

        self.assertEqual(multipartform.files, [('field', '/tmp/foo', 'text/plain', u'foo=bar')])

    @patch.object(curl, 'make_boundary')
    def test_Multipartform_str(self, boundary):
        boundary.return_value = 'A' * 40
        filehandle = StringIO(u'{"foo": "bar"}')
        multipartform = MultiPartForm()
        multipartform.add_field('foo', 'bar')
        multipartform.add_file('field', '/tmp/foo.json', filehandle)
        expected = ('--AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n'
                    'Content-Disposition: form-data; name="foo"\r\n\r\n'
                    'bar\r\n'
                    '--AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n'
                    'Content-Disposition: file; name="field"; filename="/tmp/foo.json"\r\n'
                    'Content-Type: application/json\r\n\r\n'
                    '{"foo": "bar"}\r\n'
                    '--AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA--\r\n')

        self.assertEqual(str(multipartform), expected)


if __name__ == '__main__':
    unittest.main()
