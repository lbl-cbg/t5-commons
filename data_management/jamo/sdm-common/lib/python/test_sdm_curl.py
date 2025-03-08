import unittest
try:
    ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock
    from multiprocessing.reduction import ForkingPickler
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import sys
from sdm_curl import Curl, CurlHttpException, MultiPartForm
import sdm_curl
from parameterized import parameterized
from datetime import datetime
from decimal import Decimal
import urllib.error
from io import StringIO


class TestSdmCurl(unittest.TestCase):

    @parameterized.expand([
        ('type', type(str), type(str).__name__),
        ('isoformat', datetime.fromordinal(1), datetime.fromordinal(1).isoformat()),
        ('decimal', Decimal(1), "{0:.2f}".format(Decimal(1))),
        ('str', 'foo', 'foo'),
    ])
    def test_handler(self, _description, obj, expected):
        self.assertEqual(sdm_curl.handler(obj), expected)

    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_get(self, urlopen, request):
        response = Mock()
        response.read.return_value = '{"foo": "bar"}'
        urlopen.return_value = response

        self.assertEqual(sdm_curl.get('http://someurl/get', foo='bar'), {'foo': 'bar'})
        request.assert_called_with('http://someurl/get?foo=bar&')

    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_post(self, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        response.read.return_value = '{"foo": "bar"}'
        urlopen.return_value = response

        self.assertEqual(sdm_curl.post('http://someurl/post', foo='bar'), {'foo': 'bar'})
        request.assert_called_with('http://someurl/post')
        self.assertEqual(req.data, b'{"foo": "bar"}')

    @parameterized.expand([
        ('return_http_status_code_true', True),
        ('return_http_status_code_false', False),
    ])
    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_post_return_http_status_code(self, _description, return_http_status_code, urlopen, request):
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')

        resp = curl.post('api/service/endpoint', data={'foo': 'bar'}, output='https',
                         return_http_status_code=return_http_status_code)
        if return_http_status_code:
            response, status = resp
        else:
            response, status = resp, None
        self.assertEqual(response, '{"bar":"foo"}')
        self.assertEqual(status, 200 if return_http_status_code else None)

    @parameterized.expand(['put', 'delete'])
    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_curl_methods(self, method, urlopen, request):
        req = Mock()
        request.return_value = req
        response = Mock()
        urlopen.return_value = response
        response.read.return_value = '{"bar":"foo"}'
        response.getcode.return_value = 200
        curl = Curl('http://127.0.0.1')

        resp = curl.__getattribute__(method)('api/service/endpoint', data={'foo': 'bar'}, output='https')
        self.assertEqual(resp, '{"bar":"foo"}')
        self.assertEqual(req.get_method(), method.upper())

    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_curl_get_no_cache(self, urlopen, request):
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

    def test_curl_get_cache(self):
        curl = Curl('http://127.0.0.1')
        curl.cache = {'api/service/endpoint': '{"bar":"foo"}'}

        resp = curl.get('api/service/endpoint', data={'foo': 'bar'}, output='https', cache=True)
        response = resp
        self.assertEqual(response, '{"bar":"foo"}')

    @patch('sdm_curl.urllib.request.Request')
    @patch('sdm_curl.urllib.request.urlopen')
    @patch.object(sdm_curl, 'sleep')
    def test_curl_retry_with_retriable_error(self, sleep, urlopen, request):
        error = urllib.error.HTTPError('http://127.0.0.1', 502, 'Error', [], None)
        error.readlines = lambda: (b'Error', )
        error.geturl = lambda: 'http://127.0.0.1'
        urlopen.side_effect = error
        curl = Curl('http://127.0.0.1', errorsToRetry=(502,), retry=3)

        self.assertRaises(CurlHttpException, curl.get, 'api/service/endpoint', data={'foo': 'bar'}, output='https')
        # Verify 4 calls (original call and 3 retries)
        self.assertEqual(len(urlopen.mock_calls), 4)

    @patch('sdm_curl.urllib.request.Request')
    def test_curl_to_struct(self, request):
        curl = Curl('http://127.0.0.1')

        struct = curl.toStruct({'queue_status': {'PREP_FAILED': 6}})

        self.assertEqual(struct.queue_status.PREP_FAILED, 6)
        ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if sys.version_info[0] >= 3:
            # Verify serialization/deserialization
            self.assertEqual(ForkingPickler.loads(ForkingPickler.dumps(struct)), struct)
        ## PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup

    @parameterized.expand([
        ('get_json', 'GET', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json; charset=utf-8'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('get_text', 'GET', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json; charset=utf-8'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('delete_json', 'DELETE', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json; charset=utf-8'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('delete_text', 'DELETE', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-type': 'application/json; charset=utf-8'}, None,
         'http://127.0.0.1/some/api?foo=foo1&'),
        ('put_json_data_dict', 'PUT', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json; charset=utf-8'},
         b'{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('put_text_data_dict', 'PUT', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json; charset=utf-8'},
         b'{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('put_json_data_str', 'PUT', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, b'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('put_text_data_str', 'PUT', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, b'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('post_json_data_dict', 'POST', {'foo': 'foo1'}, 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json; charset=utf-8'},
         b'{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('post_text_data_dict', 'POST', {'foo': 'foo1'}, 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 15, 'Content-type': 'application/json; charset=utf-8'},
         b'{"foo": "foo1"}', 'http://127.0.0.1/some/api'),
        ('post_json_data_str', 'POST', 'foo=foo+bar', 'json', {'foo': 'bar'},
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, b'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
        ('post_text_data_str', 'POST', 'foo=foo+bar', 'text', '{"foo": "bar"}',
         {'Authorization': 'Application SOME_TOKEN', 'Content-length': 13}, b'foo=foo%2Bbar',
         'http://127.0.0.1/some/api'),
    ])
    @patch('sdm_curl.urllib.request.urlopen')
    def test_Curl_call(self, _description, method, data, output, expected, expected_request_headers,
                       expected_request_data, expected_full_url, urlopen_mock):
        urlopen_response = Mock()
        urlopen_response.read = lambda: '{"foo": "bar"}'
        urlopen_mock.return_value = urlopen_response
        curl = Curl('http://127.0.0.1', appToken='SOME_TOKEN')

        self.assertEqual(curl._Curl__call('some/api', method, data, output), expected)
        request = urlopen_mock.call_args[0][0]
        self.assertEqual(request.headers, expected_request_headers)
        self.assertEqual(request.data, expected_request_data)
        self.assertEqual(request.full_url, expected_full_url)

    @patch('sdm_curl.random.choice')
    @patch('sdm_curl.urllib.request.urlopen')
    def test_Curl_call_multipartform(self, urlopen_mock, random_mock):
        random_mock.return_value = 'A'
        method = 'POST'
        data = MultiPartForm()
        output = 'json'
        expected = {'foo': 'bar'}
        expected_request_headers = {'Content-type': 'multipart/form-data; boundary=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'Authorization': 'Application SOME_TOKEN',
                                    'Content-length': 46}
        expected_request_data = MultiPartForm()
        expected_full_url = 'http://127.0.0.1/some/api'
        urlopen_response = Mock()
        urlopen_response.read = lambda: '{"foo": "bar"}'
        urlopen_mock.return_value = urlopen_response
        curl = Curl('http://127.0.0.1', appToken='SOME_TOKEN')

        self.assertEqual(curl._Curl__call('some/api', method, data, output), expected)
        request = urlopen_mock.call_args[0][0]
        self.assertEqual(request.headers, expected_request_headers)
        self.assertEqual(str(request.data), str(expected_request_data))
        self.assertEqual(request.full_url, expected_full_url)

    @patch('sdm_curl.random.choice')
    def test_multipartform_get_content_type(self, random):
        random.return_value = 'A'
        multipartform = MultiPartForm()

        self.assertEqual(multipartform.get_content_type(),
                         'multipart/form-data; boundary={}'.format('A' * 40))

    def test_multipartform_add_field(self):
        multipartform = MultiPartForm()

        multipartform.add_field('foo', 'bar')

        self.assertEqual(multipartform.form_fields, [('foo', 'bar')])

    def test_multipartform_add_file_guess_mime_type(self):
        multipartform = MultiPartForm()
        filehandle = StringIO(u'{"foo": "bar"}')

        multipartform.add_file('field', '/tmp/foo.json', filehandle)

        self.assertEqual(multipartform.files, [('field', '/tmp/foo.json', 'application/json', u'{"foo": "bar"}')])

    def test_multipartform_add_file_explicit_mime_type(self):
        multipartform = MultiPartForm()
        filehandle = StringIO(u'foo=bar')

        multipartform.add_file('field', '/tmp/foo', filehandle, 'text/plain')

        self.assertEqual(multipartform.files, [('field', '/tmp/foo', 'text/plain', u'foo=bar')])

    @patch('sdm_curl.random.choice')
    def test_multipartform_str(self, random):
        random.return_value = 'A'
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
