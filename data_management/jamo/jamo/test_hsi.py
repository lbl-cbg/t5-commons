import subprocess
import unittest
from hsi import HSI, HSI_status
import datetime
from time import sleep
from parameterized import parameterized
from unittest.mock import patch, MagicMock


_SCRIPT = r'''
while :
 last_in=$in
 do read in
 if [[ $in == ls\ -D* ]] || [[ $in == ls\ -P* ]]
 then echo '600 ignored user group 1000 file_info Jan 01 01:02:03 2000'
 elif [[ $in == ls\ -U* ]]
 then echo 'ignored ignored ignored ignored ignored ignored ignored TAPE'
 elif [[ $in == last_in ]]
 then echo "$last_in"
 else echo $in
 fi
done
'''


@patch('hsi.time.sleep', new=MagicMock())
class TestHSI(unittest.TestCase):

    def setUp(self):
        self.__initialize()

    @patch('hsi.time.sleep')
    @patch('hsi.Curl')
    @patch('hsi.subprocess')
    def __initialize(self, process_mock, curl_mock, sleep_mock):
        if not self._testMethodName.startswith('test_HSI_status'):
            self.process = subprocess.Popen(_SCRIPT, shell=True, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                            universal_newlines=True, bufsize=0)
            process_mock.Popen.return_value = self.process

            self.hsi = HSI('some_server')

    def tearDown(self):
        if not self._testMethodName.startswith('test_HSI_status'):
            self.process.terminate()
            self.hsi.__del__()

    # IO occurs asynchronously, add some sleeps to let writing to the pipe to get processed
    def _wait_retry(self, callback):
        output = None
        try_num = 0

        while not output and try_num < 10:
            output = callback()
            try_num += 1
            sleep(0.1)
        if not output:
            raise Exception('Unable to get data from pipe')
        return output

    def test_HSI_write_and_get_output(self):
        self.hsi.write('foobar')

        self.assertEqual(self._wait_retry(self.hsi.get_output).strip(), 'foobar')

    def test_HSI_check_output(self):
        self.assertEqual(self.hsi.check_output('foobar').strip(), 'foobar')

    def test_HSI_exit(self):
        self.hsi.exit()

        self.assertTrue(self._wait_retry(lambda: self.process.stdin.closed))

    @patch('hsi.subprocess.check_output')
    def test_HSI_put_file(self, check_output):
        self.hsi.put_file('/from/path', 'to/path')

        check_output.assert_called_with(['hsi', '-h', 'some_server', 'put -p -P /from/path : to/path'], stderr=-2)

    def test_HSI_getAllFileInfo(self):
        self.assertEqual(self.hsi.getAllFileInfo('tape_file'),
                         ('600', 'user', 'group', 1000, datetime.datetime(2000, 1, 1, 1, 2, 3)))

    def test_HSI_getFileInfo(self):
        self.assertEqual(self.hsi.getFileInfo('tape_file'), ('file_info', datetime.datetime(2000, 1, 1, 1, 2, 3)))

    def test_HSI_getTapeInfo(self):
        self.assertEqual(self.hsi.getTapeInfo('tape_file'), ('file_info', '1000'))

    def test_HSI_isontape(self):
        self.assertTrue(self.hsi.isontape('tape_file'))

    def test_HSI_removefile(self):
        self.assertTrue(self.hsi.removefile('tape_file'))

    def test_HSI_purge(self):
        self.assertEqual(self.hsi.purge('tape_file').strip(), 'purge tape_file')

    def test_HSI_movefile(self):
        self.assertTrue(self.hsi.movefile('/from/path', '/to/path'))

    @patch('hsi.subprocess.check_output')
    def test_HSI_runHtarCommand(self, check_output):
        self.assertTrue(self.hsi.runHtarCommand('foobar'))

        check_output.assert_called_with('htar -H server=some_server foobar', shell=True)

    @patch('hsi.subprocess.check_output')
    def test_HSI_removeHtarEntity(self, check_output):
        self.assertTrue(
            self.hsi.removeHtarEntity({'root_path': '/path/to', 'remote_path': '/remote/path'}, '/path/to/tape_file'))

        check_output.assert_called_with('htar -H server=some_server -Df /remote/path tape_file', shell=True)

    @parameterized.expand([
        ('status_Up', 'Up', True),
        ('status_active', 'active', True),
        ('status_up', 'up', True),
        ('status_degraded', 'degraded', True),
        ('status_other', 'other', False),
    ])
    @patch('hsi.Curl.get')
    def test_HSI_status_isup(self, _description, status, expected, curl):
        curl.return_value = {'status': status}
        hsi_status = HSI_status()

        self.assertEqual(hsi_status.isup('hpss.nersc.gov'), expected)


if __name__ == '__main__':
    unittest.main()
