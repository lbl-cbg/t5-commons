### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import absolute_import
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import unittest
from lapinpy.apps import file
from lapinpy.apps.file import File
from parameterized import parameterized
import os
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestFile(unittest.TestCase):

    # Helper function for validating dict equality since the orderings are handled differently between PY2 and PY3
    def _assertEqual(self, actual, py2_data, py3_data):
        try:
            self.assertEqual(actual, py2_data)
        except Exception:
            self.assertEqual(actual, py3_data)

    @parameterized.expand([
        ('unicode', u'foo', b'foo'),
        ('int', 1, '1')
    ])
    def test_str(self, _description, value, expected):
        self.assertEqual(file.str(value), expected)

    def test_File_getlink(self):
        row = {'is_dir': True, 'file_path': '/path/to/file', 'file_name': 'my_file'}
        expected = '<a href="/file/files/path/to/file/my_file">my_file</a>'

        self.assertEqual(File().getlink.__func__(row, None), expected)

    @patch.object(file, 'os', wraps=os)
    def test_File_get_files(self, os_mock):
        def get_path_size(path):
            if path == temp_dir:
                return 128
            if path == '{}/bar'.format(temp_dir):
                return 64
            if path == '{}/foo'.format(temp_dir):
                return 0

        os_mock.path.getsize.side_effect = get_path_size
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            os.mkdir('{}/bar'.format(temp_dir))
            with open('{}/foo'.format(temp_dir), 'w'):
                self.assertEqual(File().get_files(temp_dir.split('/')[1:], {'tq': 'offset 0'}),
                                 [{'file_name': '..',
                                   'file_path': temp_dir,
                                   'file_size': 128,
                                   'is_dir': True},
                                  {'file_name': 'bar',
                                   'file_path': temp_dir,
                                   'file_size': 64,
                                   'is_dir': True},
                                  {'file_name': 'foo',
                                   'file_path': temp_dir,
                                   'file_size': 0,
                                   'is_dir': False}]
                                 )

    @patch.object(file, 'restful')
    def test_File_get_download(self, restserver):
        restserver.run_internal.side_effect = [
            {
                'cursor_id': 'my_cursor',
                'record_count': 2,
                'records': [{'foo': 'foo1'}]
            },
            {
                'cursor_id': 'my_cursor',
                'record_count': 2,
                'records': [{'foo': 'foo2'}]
            }
        ]

        actual = File().get_download(['json', 'my_module', 'my_method', 'foo', 'bar'], {})

        self._assertEqual(actual, 'foo\n"foo1",\n"foo2",', b'foo\n"b\'foo1\'",\n"b\'foo2\'",')

    def test_File_flatitterate(self):
        self._assertEqual([i for i in File().flatitterate({'foo': 'foo1', 'bar': ['bar1', 'bar2'], 'baz': {'baz1': 'baz2'}})],
                          [('baz.baz1', 'baz2'), ('foo', 'foo1'), ('bar', '"bar1,bar2"')],
                          [('foo', b'foo1'), ('bar', 'b\'"b\\\'bar1,bar2\\\'"\''), ('baz.baz1', b'baz2')])


if __name__ == '__main__':
    unittest.main()
