import unittest
from sdm_common import Struct, ValidationError, HttpException
import sdm_common
from io import StringIO
from parameterized import parameterized
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


class TestSdmCommon(unittest.TestCase):

    @unittest.skip('Method appears to be broken -- should probably be called `__repr__` and needs to access `__dict__`')
    def test_Struct_repr(self):
        struct = Struct(key3='baz', key2='bar', key1='foo')
        expected = {u" key3 : 'baz'",
                    u" key2 : 'bar'",
                    u" key1 : 'foo'",
                    u" entries : {'key3': 'baz', 'key2': 'bar', 'key1': 'foo'}"}

        self.assertEqual(set(struct.repr__().strip('>').replace('<', ' ').split('\n')), expected)
        self.assertEqual(struct.key1, 'foo')
        self.assertEqual(struct.key2, 'bar')
        self.assertEqual(struct.key3, 'baz')

    @patch('sdm_common.os.path')
    @patch('sdm_common.open')
    def test_getToken(self, open, path):
        path.expanduser.return_value = '/some/path'
        path.is_file.return_value = True
        token_data = StringIO(u'[http://localhost]\n'
                              u'LOCALHOST_TOKEN\n'
                              '\n'
                              u'[http://anotherhost]\n'
                              u'ANOTHERHOST_TOKEN\n')
        open.return_value = token_data

        self.assertEqual(sdm_common.getToken(u'http://localhost'), u'LOCALHOST_TOKEN')

    def test_ValidationError(self):
        error = ValidationError('foo')

        self.assertEqual(error.error, 'foo')

    def test_HttpException(self):
        exception = HttpException(200, 'All good')

        self.assertEqual(exception.code, 200)
        self.assertEqual(exception.message, 'All good')

    @parameterized.expand([
        ('int', 5, int, True),
        ('float', 5.1, float, True),
        ('str', 'foo', str, True),
        ('str_or_int', 'foo', (int, str), True),
    ])
    def test_checkType(self, _description, value, type, expected):
        self.assertEqual(sdm_common.checkType(value, type), expected)

    @parameterized.expand([
        ('str_validator_not_enough_arguments', '*:2', {'foo': 'bar'},
         'You must pass in at least 2 number of keys to this call'),
        ('invalid_type', {'foo': {'type': int}}, {'foo': 'bar'},
         'Attribute: "foo" has the wrong data type. Exected int, got str'),
        ('missing_required_value', {'foo': {'type': int, 'required': True}}, {'bar': 1},
         'Missing required field: foo'),
        ('expecting_list', {'foo': {'type': list}}, {'foo': 1},
         'Attribute: "foo" has the wrong data type. Exected list, got int'),
        ('invalid_type_in_list_type', {'foo': {'type': list, 'validator': {'type': int}}}, {'foo': ['bar']},
         'Attribute: "foo[0]" has the wrong data type. Exected int, got str'),
        ('extra_not_allowed', {'foo': {'type': int}}, {'foo': 1, 'bar': 2},
         'Invalid attribute:"bar"', False),
        ('invalid_type_in_list', {'foo': {'type': int}}, [{'foo': 'bar'}],
         'Attribute: "[0].foo" has the wrong data type. Exected int, got str'),
    ])
    def test_checkdata_validator_has_errors(self, _description, validator, data, error, allowExtra=True):

        errors = sdm_common.checkdata(validator, data, allowExtra=allowExtra)

        self.assertIn(error, errors)

    @parameterized.expand([
        ('str_validator', '*:2', {'foo': 'foo1', 'bar': 'bar1'}),
        ('valid_type', {'foo': {'type': int}}, {'foo': 1}),
        ('has_required_value', {'foo': {'type': int, 'required': True}}, {'foo': 1}),
        ('list', {'foo': {'type': list, 'validator': {'type': int}}}, {'foo': [1]}),
        ('extra_allowed', {'foo': {'type': int}}, {'foo': 1, 'bar': 2}, True),
        ('valid_type_in_list', {'foo': {'type': int}}, [{'foo': 1}]),
        ('default_value', {'foo': {'type': int, 'default': 1}, 'bar': {'type': int}}, [{'bar': 2}]),
    ])
    def test_checkdata_validator_succeeds(self, _description, validator, data, allowExtra=False):

        errors = sdm_common.checkdata(validator, data, allowExtra=allowExtra)

        self.assertEqual(len(errors), 0)

    def test_getValidators(self):
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            validator_file = '{}/validators.py'.format(temp_dir)
            with open(validator_file, 'w') as file:
                file.write("my_validator = {'foo': {'type': int}}")
            self.assertEqual(sdm_common.getValidators(validator_file), {'my_validator': {'foo': {'type': int}}})


if __name__ == '__main__':
    unittest.main()
