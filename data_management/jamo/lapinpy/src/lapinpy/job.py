### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import os
import random
import string
import requests
import subprocess
from . import sdmlogger
from math import ceil


class AvailableMemoryExceedError(Exception):
    pass


class AvailableCoresExceedError(Exception):
    pass


class Job(object):
    """
    This class is used to submit jobs to the job server.  It will create a folder in the jobPath
    and create a script.bash file in that folder.  The script.bash file will contain the command
    to be run.  The job server will then run the script.bash file.
    """

    def __init__(self, command, restAddress=None, restToken=None, slots=None, extra=[], memory=None, hours=1, minutes=None,
                 project=None, jobName=None, jobPath=None, modules=[], waitFor=None, permissions=None, as_app=None,
                 pythonpath=None, pipeline=None, process=None, record_id=None, record_id_type=None, keep_python_path=True,
                 exclusive=None, environment=None, python_environment=None):
        """
        Init the job object

        :param command: The command to run
        :param restAddress: The address of the job server
        :param restToken: The token to use to authenticate with the job server
        :param slots: The number of slots to use
        :param extra: not used
        :param memory: The amount of memory to use in GB
        :param hours: The number of hours to run the job, used if minutes is not provided
        :param minutes: The number of minutes to run the job, recorded in DB, but not used in slurm scheduling
        :param project: The project to run the job under, not used
        :param jobName: The name of the job
        :param jobPath: The path to the folder to create the job in
        :param modules: not used
        :param waitFor: not used
        :param permissions: not used
        :param as_app: not used
        :param pythonpath: The python path to use
        :param pipeline: The pipeline to run the job under
        :param process: The process to run the job under
        :param record_id: The record id to run the job under
        :param record_id_type: The record id type to run the job under
        :param keep_python_path: Whether to keep the existing python path
        :param exclusive: not used
        :param environment: The environment variables to set
        :param python_environment: The python environment to set

        :raises AvailableMemoryExceedError: If the requested memory exceeds the available memory
        :raises AvailableCoresExceedError: If the requested cores exceeds the available cores
        """
        self.logger = sdmlogger.getLogger('job', level=None)

        self.command = command
        self.environment = environment
        self.python_path = pythonpath
        self.python_environment = python_environment
        self.rest_address = restAddress
        if minutes:
            self.minutes = minutes
        elif hours:
            self.minutes = hours * 60.0
        else:
            self.minutes = 60
        self.job_name = jobName
        self.pipeline = pipeline
        self.process = process
        self.record_id = record_id
        self.record_id_type = record_id_type
        self.keep_python_path = keep_python_path
        self.rest_token = restToken
        self.job_path = jobPath
        self.process_id = None
        self.job_platform = None
        self.slots = slots
        self.memory = memory  # in GB, we'll convert to MB below
        self.sge_id = None

        # These are no longer used, kept for compatibility with calling code and may be used in the future when we move to Dori
        self.app = as_app
        self.project = project
        self.permissions = permissions
        self.extra = extra
        self.wait_for = waitFor
        self.modules = modules
        self.exclusive = exclusive
        # end

        if restToken:
            response = requests.get(self.rest_address + '/api/core/settings/shared', headers={"Authorization": "Application %s" % self.rest_token})
            self.config = response.json()
        else:
            self.config = {}

        if self.job_path is None:
            if 'scratch' in self.config:
                base_path = self.config['scratch']
            else:
                base_path = os.path.expanduser("~")
            self.job_path = os.path.join(base_path, "jobs")

        self.use_slurm = self.config.get('use_slurm', False)

        if self.use_slurm:
            machine_cores = self.config.get('machine_cores', 40)
            machine_memory = self.config.get('machine_memory', 180) * 1024
            memory_per_core = machine_memory / machine_cores

            if self.memory:
                self.memory = int(round(self.memory * 1024))
                if not self.slots:
                    # slots not set, so size according to the memory requested
                    self.slots = ceil(self.memory / memory_per_core)
            elif self.slots:
                # no memory requested, so set memory based on the number of slots requested
                self.memory = int(round(self.slots * memory_per_core))
            else:
                # set minimums
                self.slots = 1
                self.memory = 1024

            if self.memory > machine_memory:
                raise AvailableMemoryExceedError("Requested Memory %s exceeds available machines" % str(self.memory))
            if self.slots > machine_cores:
                raise AvailableCoresExceedError("Requested Cores %s exceeds available machines" % str(self.slots))

        if not os.path.exists(self.job_path):
            os.makedirs(self.job_path)

        if 'job_platform' in self.config:
            self.job_platform = self.config['job_platform']

        if python_environment is None and 'python_environment' in self.config:
            self.python_environment = self.config['python_environment']

    def __get_curl_cmd(self, data, method='PUT'):
        """
        Generate a curl command to send the data to the job server
        :param data: The data to send
        :param method: The method to use, defaults to PUT
        :return: The curl command
        """
        json_str = "'{"
        for key, value in data.items():
            json_str += '"%s":' % key
            if isinstance(value, int):
                json_str += '%d' % value
            elif value.startswith('$'):
                json_str += '"\'%s\'"' % value
            else:
                json_str += '"%s"' % value
            json_str += ","
        json_str = json_str[:-1] + "}'"

        return 'curl -k -s -o /dev/null -X %s -d %s %s' % (method, json_str, self.rest_address + '/api/core/job')

    def __create_job_folder(self):
        """
        Create a folder for the job
        :return: The path to the folder
        """
        if self.job_name is None:
            ### PYTHON2_BEGIN ###   # noqa: E266 - to be removed after migration cleanup
            self.job_name = 'j' + (''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10)))
            ### PYTHON2_END ###   # noqa: E266 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###   # noqa: E266 - to be removed after migration cleanup
            # self.job_name = 'j' + (''.join(random.choices(string.ascii_lowercase + string.digits, k=10)))
            ### PYTHON3_END ###   # noqa: E266 - to be removed after migration cleanup
        full_job_path = os.path.join(self.job_path, self.job_name)
        while os.path.exists(full_job_path):
            full_job_path += random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)

        os.makedirs(full_job_path)

        return full_job_path

    def run(self):
        """
        Run the job
        :return: The process id of the job
        """
        # logging.info("In jobs.run")
        if self.process_id is not None:
            return self.process_id

        # we need to create a folder for this

        job_data = {}
        job_data['job_path'] = job_path = self.__create_job_folder()
        job_data['job_name'] = self.job_name
        job_data['memory'] = self.memory
        job_data['cores'] = self.slots
        job_data['minutes'] = self.minutes
        job_data['pipeline'] = self.pipeline
        job_data['process'] = self.process
        job_data['record_id'] = self.record_id
        job_data['record_id_type'] = self.record_id_type
        if self.job_platform:
            job_data['platform'] = self.job_platform

        script = os.path.join(job_path, 'script.bash')

        response = requests.post(self.rest_address + '/api/core/job', data=job_data)
        job_data['job_id'] = int(response.text)

        with open(script, 'w') as scriptFile:
            scriptFile.write('#!/bin/bash\n')

            if self.use_slurm:
                if self.slots:
                    scriptFile.write('#SBATCH -c {slots}\n'.format(slots=self.slots))
                if self.memory:
                    scriptFile.write('#SBATCH --mem={memory}MB\n'.format(memory=self.memory))
                scriptFile.write('#SBATCH -J {job_name}\n'.format(job_name=self.job_name))
                scriptFile.write('#SBATCH -e slurm-%j.err\n')
                scriptFile.write('#SBATCH -o slurm-%j.out\n\n')
            else:
                scriptFile.write('\n/bin/rm %s/stderr %s/stdout > /dev/null 2>&1\n\n' % (job_path, job_path))
                # redirect stdout and stderr
                scriptFile.write('# Close file descriptors 1 and 2\n')
                scriptFile.write('exec 1<&-\n')
                scriptFile.write('exec 2<&-\n')

                scriptFile.write('\n# Reopen 1 and 2 to stdout and stderr\n')
                scriptFile.write('exec 1<>%s/stdout\n' % job_path)
                scriptFile.write('exec 2<>%s/stderr\n\n' % job_path)

            scriptFile.write('echo Process Id: $$, Machine: ${HOSTNAME}\n')
            scriptFile.write('echo Start `date`\n')

            # environment variables
            if self.environment is not None:
                scriptFile.write('\n%s' % self.environment)

            # set python environment
            if self.python_environment is not None:
                scriptFile.write('\n%s' % self.python_environment)

            # set python path
            if self.python_path is not None:
                scriptFile.write('\nexport PYTHONPATH=%s%s' % (self.python_path, ':$PYTHONPATH\n' if self.keep_python_path else '\n'))

            if self.rest_address is not None:
                job_obj = {'process_id': '$$', 'machine': '${HOSTNAME}', 'status': 'Started', 'job_id': job_data['job_id']}
                scriptFile.write('\n' + self.__get_curl_cmd(job_obj))
                scriptFile.write('\necho Curl status: $?, Job Status ' + job_obj['status'] + ', Date `date`\n')

                job_obj['exit_code'] = '$?'
                job_obj['status'] = 'Killed'

                scriptFile.write('\ntrap_exit()\n{'
                                 + '\n  ' + self.__get_curl_cmd(job_obj)
                                 + '\n  echo Curl status: $?, Job Status ' + job_obj['status'] + ', Date `date`'
                                 + '\n  echo trap_exit `date`'
                                 + '\n  sleep 1'
                                 + '\n  pkill -P $$'
                                 + '\n  sleep 5'
                                 + '\n  exit'
                                 + '\n}\n')
                scriptFile.write('\ntrap trap_exit 1 2 3 4 10 19 24\n')

            scriptFile.write('\ncd %s \n\n' % job_path)
            scriptFile.write(self.command + '\n')

            if self.rest_address is not None:
                job_obj['status'] = '$state'
                scriptFile.write('\nif [ $? -eq 0 ]; then state="Finished"; else state="Failed"; fi\n')
                # Generate a new call back with the above job_obj['status']
                scriptFile.write('\n' + self.__get_curl_cmd(job_obj))
                scriptFile.write('\necho Curl status: $?, Job Status $state, Date `date`\n')

            scriptFile.write('\necho End `date`\n')

        # Run the job locally
        os.chmod(script, 0o751)
        if self.use_slurm:
            result = subprocess.check_output('sbatch {script}'.format(script=script), shell=True, cwd=job_path)
            # convert to a string if we are in python 3 (which subprocess returns bytes)
            if isinstance(result, bytes):
                result = result.decode('utf-8')
            self.sge_id = job_data['sge_id'] = int(result.split('Submitted batch job ')[1].split(' ')[0])
            requests.put(self.rest_address + '/api/core/job', data=job_data)
        else:
            os.system("/bin/bash -c {} &".format(script))
        return job_data['job_id']
