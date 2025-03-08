import hsi
import os
import unittest
import subprocess
import datetime

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


scriptFile = __file__


class TestHSI(unittest.TestCase):
    def setUp(self):
        self.connection = hsi.HSI('archive.nersc.gov')

    def tearDown(self):
        self.connection.exit()

    def test__put(self):
        self.assertRegexpMatches(self.connection.put_file(scriptFile, 'testscript.py'), r'^\*')

    def test_failput(self):
        self.assertRaises(subprocess.CalledProcessError, self.connection.put_file, scriptFile + '1234', 'testscript.py')

    def test_failputperms(self):
        self.assertRaises(subprocess.CalledProcessError, self.connection.put_file, scriptFile, '/testscript.py')

    def test_fileinfo(self):
        ret = self.connection.getAllFileInfo('testscript.py')
        self.assertIsInstance(ret[0], basestring)
        self.assertIsInstance(ret[1], basestring)
        self.assertIsInstance(ret[2], basestring)
        self.assertIsInstance(ret[3], int)
        self.assertIsInstance(ret[4], datetime.datetime)
        self.assertEquals(ret[3], os.path.getsize(scriptFile))

    def test_fileinfoFail(self):
        self.assertRaises(hsi.HSIError, self.connection.getAllFileInfo, 'testscript.py1234')

    def test_tapeinfo(self):
        ret = self.connection.getTapeInfo('/home/a/aeboyd/testlength.tar')
        self.assertRegexpMatches(ret[0], '^[A-Z]{2}[0-9]+$')
        self.assertRegexpMatches(ret[1], r'^[0-9]+\+[0-9]+$')

    def test_ontape(self):
        self.assertEquals(self.connection.isontape('/home/a/aeboyd/testlength.tar'), True)
        self.assertEquals(self.connection.isontape('testscript.py'), False)
        self.assertRaises(hsi.HSIError, self.connection.isontape, 'testscript.py1234')


# tester = TestHSI()
# tester.test_fileinfo()
# tester.tearDown()
unittest.main()
