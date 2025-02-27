import unittest
from lapinpy.config_util import ConfigManager
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestConfigUtil(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory(suffix='tmp')
        self.config_file = '{}/lapinpy.config'.format(self.temp_dir.name)
        with open(self.config_file, 'w') as f:
            f.write('foo_db: database\nfoo_user: foo\nfoo_pass: pass\nshared:\n app_name: lapinpy')
        self.config_manager = ConfigManager(self.temp_dir.name, None)

    def tearDown(self):
        self.temp_dir.cleanup()

    @parameterized.expand([
        ('folder_loc', True),
        ('file_loc', False),
    ])
    def test_ConfigManager_check_for_changes(self, _description, is_folder):
        with open(self.config_file, 'w') as f:
            f.write('foo_db: database\nfoo_user: foo\nfoo_pass: new_pass')

        self.config_manager.loc = self.temp_dir.name if is_folder else self.config_file

        self.assertEqual(self.config_manager.check_for_changes(), ['lapinpy'])
        if is_folder:
            self.assertEqual(self.config_manager.settings.get('lapinpy').get('foo_pass'), 'new_pass')
        else:
            self.assertEqual(self.config_manager.settings.get('foo_pass'), 'new_pass')

    def test_ConfigManager_load_folder(self):
        with open(self.config_file, 'w') as f:
            f.write('foo_db: database\nfoo_user: foo\nfoo_pass: new_pass')

        self.config_manager._load_folder(self.temp_dir.name)
        self.assertEqual(self.config_manager.settings.get('lapinpy').get('foo_pass'), 'new_pass')

    def test_ConfigManager_load_file(self):
        with open(self.config_file, 'w') as f:
            f.write('foo_db: database\nfoo_user: foo\nfoo_pass: new_pass')

        self.config_manager._load_file(self.config_file)
        self.assertEqual(self.config_manager.settings.get('foo_pass'), 'new_pass')

    def test_ConfigManager_get_settings(self):
        settings = self.config_manager.get_settings('lapinpy')

        self.assertEqual(settings.foo_db, 'database')
        self.assertEqual(settings.foo_user, 'foo')
        self.assertEqual(settings.foo_pass, 'pass')
        self.assertEqual(settings.app_name, 'lapinpy')
        self.assertEqual(settings.shared, {'app_name': 'lapinpy'})


if __name__ == '__main__':
    unittest.main()
