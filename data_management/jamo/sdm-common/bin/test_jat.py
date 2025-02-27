import unittest
from parameterized import parameterized
from tempfile import NamedTemporaryFile
import os
from jat import validate_token_file_permissions, JTT
import yaml
from types import SimpleNamespace
from io import StringIO
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, mock_open, call
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestJat(unittest.TestCase):

    @parameterized.expand([
        ('valid_file_and_parent_directory_permissions', 0o0700, 0o0600, '0o700'),
        ('invalid_file_permissions', 0o0755, 0o0644, '0o755'),
        ('invalid_dir_permissions', 0o0777, 0o0600, '0o755'),
        ('invalid_file_and_dir_permissions', 0o0770, 0o0644, '0o750'),
    ])
    @patch('jat.os.path.expanduser')
    def test_validate_token_file_permissions(self, _description, dir_mode, file_mode, expected_dir_mode, path_expanduser):
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            os.chmod(temp_dir, dir_mode)
            path_expanduser.side_effect = lambda x: x.replace('~', temp_dir)
            with NamedTemporaryFile(dir=temp_dir) as temp_file:
                os.chmod(temp_file.name, file_mode)
                validate_token_file_permissions(temp_file.name)
                # Validate directory permissions
                self.assertEqual(oct(os.stat(temp_dir).st_mode & 0o777), expected_dir_mode)
                # Validate token file permissions
                self.assertEqual(oct(os.stat(temp_file.name).st_mode & 0o777), '0o600')

    @patch('jat.os.path.expanduser')
    @patch('jat.sdm_curl.Curl.post')
    @patch('jat.random.choice')
    @patch('jat.pwd.getpwuid')
    def test_JTT_auth_no_jamo_directory_creates_directory(self, pwd, random, curl, path_expanduser):
        pwd.return_value = ('foo',)
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            # Set home directory to fully open
            os.chmod(temp_dir, 0o0777)
            path_expanduser.side_effect = lambda x: x.replace('~', temp_dir)
            # Make random.choice always return 'A'
            random.return_value = 'A'
            curl.return_value = 'user_token'
            jtt = JTT({}, skipAuth=True)

            jtt.auth()

            # Validate directory created
            self.assertTrue(os.path.exists('{}/.jamo'.format(temp_dir)))
            # Validate directory permissions (755), which comes from the home directory permissions without group/other
            # write bits
            self.assertEqual(oct(os.stat('{}/.jamo'.format(temp_dir)).st_mode & 0o777), '0o755')
            # Validate token file permissions (600)
            self.assertEqual(oct(os.stat('{}/.jamo/token'.format(temp_dir)).st_mode & 0o777), '0o600')
            with open('{}/.jamo/token'.format(temp_dir), 'r') as f:
                # Validate token file content
                token_map = yaml.load(f.read(), Loader=yaml.SafeLoader)
                self.assertDictEqual(token_map, {'https://jamo.jgi.doe.gov': 'user_token'})
            # Verify authorization flow call
            curl.assert_called_with('api/core/associate', token='A' * 50, user='foo')

    @patch('jat.os.path.expanduser')
    @patch('jat.sdm_curl.Curl.post')
    @patch('jat.random.choice')
    @patch('jat.pwd.getpwuid')
    def test_JTT_auth_exiting_token_file_replaces_token(self, pwd, random, curl, path_expanduser):
        pwd.return_value = ('foo',)
        curl.return_value = 'user_token'
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            path_expanduser.side_effect = lambda x: x.replace('~', temp_dir)
            # Make random.choice always return 'A'
            random.return_value = 'A'
            jtt = JTT({}, skipAuth=True)
            os.makedirs('{}/.jamo'.format(temp_dir), 0o0700)
            with open('{}/.jamo/token'.format(temp_dir), 'w') as f:
                token_map = {'https://jamo.jgi.doe.gov': 'some_token'}
                f.write(yaml.safe_dump(token_map, default_flow_style=False))

            jtt.auth()

            # Validate token file permissions (600)
            self.assertEqual(oct(os.stat('{}/.jamo/token'.format(temp_dir)).st_mode & 0o777), '0o600')
            with open('{}/.jamo/token'.format(temp_dir), 'r') as f:
                # Validate token file content
                token_map = yaml.load(f.read(), Loader=yaml.SafeLoader)
                self.assertDictEqual(token_map, {'https://jamo.jgi.doe.gov': 'user_token'})
            # Verify authorization flow call
            curl.assert_called_with('api/core/associate', token='A' * 50, user='foo')

    @parameterized.expand([
        ('source', ['my_analysis_key', 'my_label', '/path/to/my_file', 'my_metadata_file.json', 'my_source'],
         call.post('api/analysis/addfile/my_analysis_key/my_label',
                   file='/path/to/my_file', metadata={},
                   source='my_source')
         ),
        ('no_source', ['my_analysis_key', 'my_label', '/path/to/my_file', 'my_metadata_file.json'],
         call.post('api/analysis/addfile/my_analysis_key/my_label',
                   file='/path/to/my_file', metadata={},
                   source=None)
         ),
    ])
    @patch('builtins.open', new_callable=mock_open, read_data='{}')
    @patch('sys.stderr', new_callable=StringIO)
    @patch('jat.sdm_curl.Curl')
    def test_JTT_addfile(self, _description, args, expected_curl_call, curl_mock, stderr_mock, file_mock):
        curl_mock.return_value = curl_mock
        curl_mock.post.return_value = {'warnings': ['foo', 'bar']}
        jtt = JTT({}, skipAuth=True)

        jtt.addfile(args)

        self.assertIn(expected_curl_call, curl_mock.mock_calls)
        self.assertEqual('foo\nbar\n', stderr_mock.getvalue())

    @parameterized.expand([
        ('less_than_4_args', ['foo']),
        ('greater_than_5_args', ['foo1', 'foo2', 'foo3', 'foo4', 'foo5', 'foo6']),
    ])
    def test_JTT_addfile_invalid_number_of_arguments_exits(self, _description, args):
        jtt = JTT({}, skipAuth=True)

        self.assertRaises(SystemExit, jtt.addfile, args)

    @parameterized.expand([
        ('source', ['my_analysis_key', 'my_metadata_file.json', 'my_source'],
         call.post('api/analysis/addfile/my_analysis_key/my_label',
                   file='/path/to/my_file', metadata={'foo': 'bar'},
                   source='my_source')
         ),
        ('no_source', ['my_analysis_key', 'my_metadata_file.json'],
         call.post('api/analysis/addfile/my_analysis_key/my_label',
                   file='/path/to/my_file', metadata={'foo': 'bar'},
                   source=None)
         ),
    ])
    @patch('builtins.open', new_callable=mock_open,
           read_data='[{"metadata": {"foo": "bar"}, "file": "/path/to/my_file", "label": "my_label"}]')
    @patch('sys.stderr', new_callable=StringIO)
    @patch('jat.sdm_curl.Curl')
    def test_JTT_addfiles(self, _description, args, expected_curl_call, curl_mock, stderr_mock, file_mock):
        curl_mock.return_value = curl_mock
        curl_mock.post.return_value = {'warnings': ['foo', 'bar']}
        jtt = JTT({}, skipAuth=True)

        jtt.addfiles(args)

        self.assertIn(expected_curl_call, curl_mock.mock_calls)
        self.assertIn('foo\nbar\n', stderr_mock.getvalue())

    @parameterized.expand([
        ('less_than_2_args', ['foo']),
        ('greater_than_3_args', ['foo1', 'foo2', 'foo3', 'foo4']),
    ])
    def test_JTT_addfiles_invalid_number_of_arguments_exits(self, _description, args):
        jtt = JTT({}, skipAuth=True)

        self.assertRaises(SystemExit, jtt.addfiles, args)

    @parameterized.expand([
        ('source', ['my_analysis_key', '/path/to/my_metadata_file.json', 'my_source', 'foo=foo1', 'bar=bar1'],
         call.post('api/analysis/importfile', tags=['my_analysis_key'], metadata={'foo': 'foo1', 'bar': 'bar1'},
                   file='/path/to/my_metadata_file.json', tape_options={},
                   source='my_source')
         ),
        ('no_source', ['my_analysis_key', '/path/to/my_metadata_file.json', 'foo=foo1', 'bar=bar1'],
         call.post('api/analysis/importfile', tags=['my_analysis_key'], metadata={'foo': 'foo1', 'bar': 'bar1'},
                   file='/path/to/my_metadata_file.json', tape_options={},
                   source=None)
         ),
    ])
    @patch('jat.os.stat')
    @patch('sys.stderr', new_callable=StringIO)
    @patch('jat.sdm_curl.Curl')
    @patch('builtins.open', new_callable=mock_open,
           read_data='[{"metadata": {"foo": "bar"}, "file": "my_file", "label": "my_label"}]')
    def test_JTT_importfile(self, _description, args, expected_curl_call, file_mock, curl_mock, stderr_mock,
                            os_stat_mock):
        curl_mock.return_value = curl_mock
        curl_mock.post.return_value = {'warnings': ['foo', 'bar'], 'metadata_id': 'my_metadata_id'}
        os_stat_mock.return_value = SimpleNamespace(**{'st_mode': 0o04, 'st_size': 0, 'st_mtime': 0})
        jtt = JTT({}, skipAuth=True)

        jtt.importfile(args)

        self.assertIn(expected_curl_call, curl_mock.mock_calls)
        self.assertEqual('foo\nbar\n', stderr_mock.getvalue())

    @parameterized.expand([
        ('less_than_2_args', ['foo']),
    ])
    @patch('jat.sdm_curl.Curl')
    def test_JTT_importfile_invalid_number_of_arguments_exits(self, _description, args, curl_mock):
        curl_mock.return_value = curl_mock
        jtt = JTT({}, skipAuth=True)

        self.assertRaises(SystemExit, jtt.importfile, args)

    @parameterized.expand([
        ('source', ['my_analysis_template', '/path/to/my_folder', 'my_source'],
         call.post('api/analysis/analysisimport', template_name='my_analysis_template',
                   template_data={'metadata': {'foo': 'bar'}, 'file': 'my_file', 'label': 'my_label', 'outputs': []},
                   location='/path/to/my_folder', skip_folder=False, source='my_source')
         ),
        ('no_source', ['my_analysis_template', '/path/to/my_folder'],
         call.post('api/analysis/analysisimport', template_name='my_analysis_template',
                   template_data={'metadata': {'foo': 'bar'}, 'file': 'my_file', 'label': 'my_label', 'outputs': []},
                   location='/path/to/my_folder', skip_folder=False, source=None)
         ),
    ])
    @patch('jat.os.stat')
    @patch('jat.os.path.isdir')
    @patch('jat.os.path.isfile')
    @patch('jat.sdm_curl.Curl')
    @patch('builtins.open', new_callable=mock_open,
           read_data='{"metadata": {"foo": "bar"}, "file": "my_file", "label": "my_label", "outputs": []}')
    def test_JTT_importa(self, _description, args, expected_curl_call, file_mock, curl_mock, os_file_mock, os_dir_mock, os_stat_mock):
        curl_mock.return_value = curl_mock
        os_stat_mock.return_value = SimpleNamespace(**{'st_mode': 0o05, 'st_size': 0, 'st_mtime': 0})
        jtt = JTT({}, skipAuth=True)

        jtt.importa(args)

        self.assertIn(expected_curl_call, curl_mock.mock_calls)

    @parameterized.expand([
        ('less_than_2_args', ['foo']),
        ('greater_than_3_args', ['foo1', 'foo2', 'foo3', 'foo4']),
    ])
    def test_JTT_importa_invalid_number_of_arguments_exits(self, _description, args):
        jtt = JTT({}, skipAuth=True)

        self.assertRaises(SystemExit, jtt.importa, args)


if __name__ == '__main__':
    unittest.main()
