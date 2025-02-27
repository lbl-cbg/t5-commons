import unittest
from jadmin import JAdmin
from unittest.mock import patch, call
from parameterized import parameterized


class TestJadmin(unittest.TestCase):

    @parameterized.expand([
        ('source', ['/path/to/my_file', 'jamo_file', 'my_source'],
         call.post('api/tape/replacefile', src='/path/to/my_file', dest='my_metadata_id', source='my_source')
         ),
        ('no_source', ['/path/to/my_file', 'jamo_file'],
         call.post('api/tape/replacefile', src='/path/to/my_file', dest='my_metadata_id', source=None)
         ),
    ])
    @patch('jadmin.sdm_curl.Curl')
    def test_JAdmin_replace(self, _description, args, expected_curl_call, curl_mock):
        curl_mock.return_value = curl_mock
        curl_mock.get.return_value = {'metadata_id': 'my_metadata_id'}
        jadmin = JAdmin({}, skipAuth=True)

        jadmin.replace(args)

        self.assertIn(expected_curl_call, curl_mock.mock_calls)

    @parameterized.expand([
        ('less_than_2_args', ['foo']),
        ('greater_than_3_args', ['foo1', 'foo2', 'foo3', 'foo4']),
    ])
    def test_JTT_replace_invalid_number_of_arguments_exits(self, _description, args):
        jadmin = JAdmin({}, skipAuth=True)

        self.assertRaises(SystemExit, jadmin.replace, args)


if __name__ == '__main__':
    unittest.main()
