import unittest
import os.path
from lapinpy.job import Job, AvailableMemoryExceedError, AvailableCoresExceedError
from lapinpy import job
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock
    from tempfile import TemporaryDirectory
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock
    from backports.tempfile import TemporaryDirectory
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestJob(unittest.TestCase):

    @parameterized.expand([(False, None, None, 1, None, [], 'local run', '', 0),
                           (True, 1, None, 1, None, ['#SBATCH -c 1', '#SBATCH --mem=4608MB'], 'slurm, cores only, py2 response', 'Submitted batch job 12345\n', 12345),
                           (True, 1, None, 1, None, ['#SBATCH -c 1', '#SBATCH --mem=4608MB'], 'slurm, cores only, py3 response', b'Submitted batch job 12345\n', 12345),
                           (True, 10, 60, 1, None, ['#SBATCH -c 10', '#SBATCH --mem=46080MB'], 'slurm, cores and minutes, py2 response', 'Submitted batch job 1234\n', 1234),
                           (True, 20, None, 1, None, ['#SBATCH -c 20', '#SBATCH --mem=92160MB'], 'slurm, cores and hours, py2 response', 'Submitted batch job 1234\n', 1234),
                           (True, 40, None, 1, None, ['#SBATCH -c 40', '#SBATCH --mem=184320MB'], 'slurm, cores and hours, py2 response', 'Submitted batch job 123\n', 123),
                           (True, 20, None, 1, 1, ['#SBATCH -c 20', '#SBATCH --mem=1024MB'], 'slurm, cores and memory, py2 response', 'Submitted batch job 12\n', 12),
                           (True, None, None, None, 8, ['#SBATCH -c 2', '#SBATCH --mem=8192MB'], 'slurm memory only, py2 response', 'Submitted batch job 12\n', 12),
                           (True, None, None, None, None, ['#SBATCH -c 1', '#SBATCH --mem=1024MB'], 'slurm no options, py2 response', 'Submitted batch job 1\n', 1),
                           (True, None, None, None, None, ['#SBATCH -c 1', '#SBATCH --mem=1024MB'], 'slurm no options, py2 response', 'Submitted batch job 0\n', 0),
                           ])
    @patch.object(job, 'requests')
    @patch('subprocess.check_output')
    @patch('os.system')
    @patch.object(job, 'random')
    def test_local_job(self, use_slurm, slots, minutes, hours, memory, checks, description, result, sge_id, random, os_system, subprocess_check_output, requests):
        response = Mock()
        response.json.return_value = {'api_url': 'https://api.url/', 'use_slurm': use_slurm}
        requests.get.return_value = response
        random.choice.return_value = 'A'
        subprocess_check_output.return_value = result

        with TemporaryDirectory(suffix='tmp') as temp_dir:
            job_test = Job(command='ls -l', jobPath=temp_dir, restAddress='127.0.0.1',
                           restToken='some_token', slots=slots, minutes=minutes, hours=hours, memory=memory)
            expected_script_path = '{}/j{}/script.bash'.format(temp_dir, 'A' * 10)

            job_test.run()

            # Verify script exists and has the requested command
            with open(expected_script_path) as f:
                lines = [line.rstrip('\n') for line in f.readlines()]
                self.assertIn('ls -l', lines, msg=description)
                for line in checks:
                    self.assertIn(line, lines, msg=description)
            # Verify call
            if use_slurm:
                cwd = os.path.dirname(expected_script_path)
                subprocess_check_output.assert_called_with('sbatch {}'.format(expected_script_path), shell=True, cwd=cwd)
                self.assertEquals(job_test.sge_id, sge_id, msg=description)
            else:
                os_system.assert_called_with('/bin/bash -c {} &'.format(expected_script_path))

    @parameterized.expand([(41, 1, AvailableCoresExceedError),
                           (1, 200, AvailableMemoryExceedError)
                           ])
    @patch.object(job, 'requests')
    @patch.object(job, 'random')
    def test_local_job_exceptions(self, slots, memory, exception, random, requests):
        response = Mock()
        response.json.return_value = {'api_url': 'https://api.url/', 'use_slurm': True}
        requests.get.return_value = response
        random.choice.return_value = 'A'

        with TemporaryDirectory(suffix='tmp') as temp_dir:
            with self.assertRaises(exception):
                Job(command='ls -l', jobPath=temp_dir, restAddress='127.0.0.1', restToken='some_token', slots=slots, memory=memory)


if __name__ == '__main__':
    unittest.main()
