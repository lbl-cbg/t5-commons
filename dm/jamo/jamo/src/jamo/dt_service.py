from tempfile import TemporaryDirectory
import re
import argparse
import copy
import datetime
import multiprocessing
import os
import platform
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import traceback
import itertools

from . import hsi
import sdm_logger
from .hsi import HSI
from sdm_curl import Curl
from grp import getgrgid
from pwd import getpwuid
from typing import Any, Optional


class ResourceLostException(Exception):
    def __init__(self, resource, service, globally):
        self.resource = resource
        self.globally = globally
        self.service = service
        Exception.__init__(self, f'The resource {resource} has become unavailable')


class HSIVerificationFailedException(Exception):
    """Exception to be raised if validation after writing to HSI fails.
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class BackupServiceConfigurationException(Exception):
    """Exception to be raised if there is an issue with backup service configuration.
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class HSIQueue(multiprocessing.Process):

    def __init__(self, hsi_server, logger):
        self.commands = []
        self.logger = logger
        self.lock = multiprocessing.Lock()
        self.running = False
        self.finished = []
        self.fetch_lock = multiprocessing.Lock()
        multiprocessing.Process.__init__(self)
        self.hsi = HSI(hsi_server)

    def queue(self, command, data):
        with self.lock:
            self.commands.append({'command': command, 'info': data})
        if not self.running:
            self.running = True
            self.start()

    def run(self):
        self.running = True
        while True:
            fileinfo = None
            with self.lock:
                if len(self.commands) > 0:
                    fileinfo = self.commands.pop(0)
            if fileinfo is None:
                self.running = False
                self.hsi.exit()
                return
            command = fileinfo.get('command')
            with self.fetch_lock:
                self.logger.info(f'running hsi command {command}')
                output = self.hsi.check_output(command)
                self.logger.info(f'command returned {output}')
                self.finished.append(fileinfo.get('info'))


class DTService(object):
    def __init__(self, curl, features, tasks, threads, debug, service_id, division_name, log_ext=None, email=None):
        name = '-'.join(x for x in ['dt_service', platform.node(), log_ext] if x)
        sdm_logger.config(f'{name}.log', emailTo=email, curl=curl)  # , backupCount=20)
        self.logger = sdm_logger.getLogger('dt_service')
        self.logger.info('starting dt service')
        self.sdm_curl = curl
        self.debug = debug
        self.features = features
        self.threads = threads
        self.tasks = tasks
        self.service_id = service_id
        self.division_name = division_name
        self.hsi_gone = {}
        self.hsi_list = [feature for feature in features if 'hsi' in feature]
        config = curl.get('api/core/settings/tape')
        self.temp_dir = os.getcwd()
        if 'division' in config:
            for div_conf in config.get('division'):
                if div_conf['name'] ==  self.division_name:
                    self.temp_dir = div_conf.get('tape_temp_dir', self.temp_dir)
        self.remote_services = {}
        self.cv = curl.toStruct(curl.get('api/tape/cvs'))
        # file file has been touched in N time, extend reservation by N
        self.SECONDS_IN_DAY = 60 * 60 * 24
        if 'purge_file_accessed' in config:
            self.PURGE_ACCESSED = int(config.get('purge_file_accessed')) * self.SECONDS_IN_DAY
        else:
            self.PURGE_ACCESSED = 10 * self.SECONDS_IN_DAY
        if 'purge_file_extend' in config:
            self.PURGE_EXTEND = int(config.get('purge_file_extend'))
        else:
            self.PURGE_EXTEND = 10
        self.remote_sources = config.get('remote_sources')
        self.backup_services = config.get('backup_services', {})
        self.task_runners = {'ingest': self.run_ingest_info, 'copy': self.run_copy, 'md5': self.run_md5,
                             'put': self.run_put, 'pull': self.run_pull, 'tar': self.run_tar,
                             'general': self.remove_file, 'purge': self.run_purge, 'delete': self.run_delete,
                             'prep': self.run_prep_batch}
        self.current_thread_count = multiprocessing.Value('i', 0)
        self.stop = multiprocessing.Value('i', 0)
        self.hsi_state = hsi.HSI_status()
        for i in range(threads):
            multiprocessing.Process(target=self.runner, args=(self.stop, self.current_thread_count)).start()

    def check_services(self):
        # Run through the list of services and turn them on/off if they were off/on
        for hsi_service in self.hsi_list:
            if hsi_service in self.hsi_gone:
                resource_id = self.hsi_gone.get(hsi_service)
                service = self.get_service(resource_id)
                if self.hsi_state.isup(service.get('server')):
                    del self.hsi_gone[hsi_service]
                    if hsi_service not in self.features:
                        # add the resource back to our feature list
                        self.features.append(hsi_service)
                    self.sdm_curl.post('api/tape/resourceonline', resource=hsi_service, service_id=self.service_id)
            else:
                # this should be refactored
                resource_id = int(hsi_service.split('_')[1])
                service = self.get_service(resource_id)
                if not self.hsi_state.isup(service.get('server')):
                    self.hsi_gone[hsi_service] = resource_id
                    self.features.remove(hsi_service)
                    self.sdm_curl.post('api/tape/resourceoffline', resource=hsi_service, globally=True,
                                       service_id=self.service_id)

    def to_folder_str(self, number, total_length, width_of_each_folder):
        folder_name = str(number)
        folder_name = '0' * (total_length - len(folder_name)) + folder_name
        return '/'.join([folder_name[i:i + width_of_each_folder] for i in range(0, total_length, width_of_each_folder)])

    def set_threads(self, thread_count):
        for i in range(thread_count - self.current_thread_count.value):
            multiprocessing.Process(target=self.runner, args=(self.stop, self.current_thread_count)).start()

    def runner(self, stop, thread_count):
        prev_task_id = None
        prev_ret = None
        pid = os.getpid()
        with thread_count.get_lock():
            thread_count.value += 1
        while stop.value == 0:
            task = self.sdm_curl.post('api/tape/nexttask', features=self.features, tasks=self.tasks,
                                      previous_task_id=prev_task_id, service=self.service_id, returned=prev_ret,
                                      division=self.division_name)
            if task is None:
                break
            prev_task_id = task.get('task_id')
            self.logger.info(f'task {task.get("task")} was received, pid = {pid}')
            task_name = task.get('task')
            try:
                prev_ret = self.task_runners.get(task_name)(task.get('data'))
            except ResourceLostException as e:  # noqa: F841
                self.sdm_curl.put('api/tape/task', task=task)
                break
            except Exception as e:  # noqa: F841
                # This probably should be a critical
                self.logger.error(f'failed to run task: {str(e)}, pid = {pid}')
                break

        # set our last task to complete
        self.sdm_curl.put('api/tape/taskcomplete', task_id=prev_task_id, returned=prev_ret, division=self.division_name)
        with thread_count.get_lock():
            thread_count.value -= 1

    def stop_threads(self):
        self.stop.value = 1

    def get_service(self, service):
        if service not in self.remote_services:
            service_o = self.sdm_curl.get(f'api/tape/backupservice/{service}')
            if service_o is not None:
                self.remote_services[service] = service_o
        return self.remote_services.get(service)

    def run_put(self, in_file):
        service = self.get_service(in_file.get('service'))
        if service.get('type') == 'HPSS':
            if not self.hsi_state.isup(service.get('server')):
                raise ResourceLostException(f'hsi_{in_file.get("service")}', in_file.get('service'), True)
            # We are writing a single file to tape
            if len(in_file.get('records')) == 1:
                '''run this as a single file transfer'''
                in_file = in_file.get('records')[0]
                local_file = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
                file_stat = None
                try:
                    file_stat = os.stat(local_file)
                except FileNotFoundError as e:  # noqa: F841
                    self.logger.warning(f"file: {local_file} doesn't exist anymore...")
                    self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                                      data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                    return False
                remote_file = os.path.join(self._get_sharded_path(service.get('default_path')),
                                           f'{local_file}.{in_file.get("backup_record_id")}'[1:])
                remote_file_path, remote_file_name = os.path.split(remote_file)
                put_cmd = ['hsi', '-h', service.get('server'), f'put -p -P {local_file} : {remote_file}']
                try:
                    if not self.debug:
                        subprocess.run(put_cmd, check=True)
                        # Verify file is in hsi
                        self._verify_path_in_hsi(service.get('server'), remote_file, file_stat.st_size)
                    # overwriting the remote file name and path, make sure the tar record id is also removed
                    self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                                      data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_COMPLETE,
                                            'remote_file_name': remote_file_name, 'remote_file_path': remote_file_path,
                                            'tar_record_id': None})
                except Exception as e:
                    self.logger.warning(f'Failed putting file via hsi: {e}')
                    self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                                      data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                    return False
                if local_file.startswith(self.temp_dir):
                    file_info = self.sdm_curl.get(f'api/tape/file/{in_file.get("file_id")}')
                    if file_info.get('file_status_id') == self.cv.file_status.BACKUP_COMPLETE:
                        # Is this is unlinking the file before md5 can complete?  Switching to adding a keep date of tomorrow and letting tape purge it
                        # os.unlink(localFile)
                        self.sdm_curl.put('api/tape/savefile', file=local_file, days=1)
                return True
            # Create a tarball of files
            else:
                root_dir = in_file.get('root_dir')
                tar_id = self.sdm_curl.post('api/tape/tar', root_path=root_dir).get('tar_record_id')
                backup_records = []
                file_list_loc = f'{tar_id}_tar'
                file_list_loc = os.path.join(self.temp_dir, file_list_loc)
                if not os.path.exists(file_list_loc):
                    os.makedirs(file_list_loc)
                total_file_sizes = 0
                for file_item in in_file.get('records'):
                    local_file = os.path.join(file_item.get('file_path'), file_item.get('file_name'))
                    file_stat = None
                    try:
                        file_stat = os.stat(local_file)
                    except FileNotFoundError:
                        self.logger.warning(f"file: {local_file} doesn't exist anymore...")
                        self.sdm_curl.put(f'api/tape/backuprecord/{file_item.get("backup_record_id")}',
                                          data={
                                              'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                    else:
                        total_file_sizes += file_stat.st_size
                        rfile_name = file_item.get('file_name')
                        exten = f'.{file_item.get("backup_record_id")}'
                        if len(rfile_name + exten) > 90:
                            rfile_name = rfile_name[:90 - len(exten)]
                        rfile_name += exten
                        backup_records.append({'backup_record_id': file_item.get('backup_record_id'),
                                               'backup_record_status_id': self.cv.backup_record_status.TRANSFER_COMPLETE,
                                               'tar_record_id': tar_id, 'remote_file_name': rfile_name,
                                               'remote_file_path': '.'})
                        if os.path.exists(os.path.join(file_list_loc, rfile_name)):
                            os.unlink(os.path.join(file_list_loc, rfile_name))
                        os.symlink(local_file, os.path.join(file_list_loc, rfile_name))

                tar_location = os.path.join(self._get_sharded_path(service.get('default_path')),
                                            f'{self.to_folder_str(tar_id, 9, 3)[:-3]}{tar_id}.tar')

                self.sdm_curl.put(f'api/tape/tar/{tar_id}', remote_path=tar_location)
                put_cmd = ['htar', '-P', '-h', '-H', 'server=' + service.get('server'), '-cf', tar_location, '-T', '10',
                           '.']
                put_cmd = f'cd {file_list_loc}; {" ".join(put_cmd)}'
                try:
                    if not self.debug:
                        subprocess.run(put_cmd, shell=True, check=True)
                        # Verify tarball is in hsi. Since we use `htar` to create the tarball in HSI, we can't do an
                        # exact file size match, since `htar` does not provide a `dry-run` option, and also it appends
                        # data to the tarball for consistency purposes. Instead, we verify that the tarball size is at
                        # least the sum of sizes of the tarball contents.
                        self._verify_path_in_hsi(service.get('server'), tar_location, total_file_sizes, False)
                    self.sdm_curl.put('api/tape/backuprecords', records=backup_records)
                except Exception as e:
                    self.logger.warning(f'Failed putting tarball via htar: {e}')
                    # Set status for backup records as failed
                    failed_backup_records = [{'backup_record_id': backup_record.get('backup_record_id'),
                                              'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED}
                                             for backup_record in backup_records]
                    self.sdm_curl.put('api/tape/backuprecords', records=failed_backup_records)
                    return False
                else:
                    shutil.rmtree(file_list_loc)
                    return True
        elif service.get('type') == 'globus':
            return self._put_globus(service, in_file)
        else:
            # service not supported yet.... how did one get here??
            self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                              data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
            return True

    def run_ingest_info(self, in_file):
        from_file = os.path.realpath(in_file.get('_file'))
        callback = in_file.get('_callback')
        if not os.path.exists(from_file):
            self.logger.info(f'File missing or perm issues: {from_file}')
            self.sdm_curl.post(f'api/tape/{callback}/{in_file.get("file_ingest_id")}',
                               data={'file_ingest_status_id': self.cv.file_status.INGEST_FILE_MISSING})
            return False
        data = dict()
        try:
            stat = os.stat(from_file)
            data['file_path'], data['file_name'] = os.path.split(from_file)
            try:
                data['file_owner'] = getpwuid(stat.st_uid).pw_name
            except Exception:
                data['file_owner'] = stat.st_uid
            try:
                data['file_group'] = getgrgid(stat.st_gid)[0]
            except Exception:
                data['file_group'] = stat.st_gid
            data['file_size'] = stat.st_size
            data['file_permissions'] = oct(stat.st_mode)
            data['file_date'] = datetime.datetime.fromtimestamp(stat.st_mtime)
            data['_is_folder'] = os.path.isdir(from_file)
            data['_is_file'] = os.path.isfile(from_file)
            data['file_ingest_status_id'] = self.cv.file_status.INGEST_STATS_COMPLETE

            self.sdm_curl.post(f'api/tape/{callback}/{in_file.get("file_ingest_id")}', data=data)
        except Exception as e:  # noqa: F841
            self.logger.warning(f'failed to lookup info for file {from_file}')
            self.sdm_curl.post(f'api/tape/{callback}/{in_file.get("file_ingest_id")}',
                               data={'file_ingest_status_id': self.cv.file_status.INGEST_STATS_FAILED})
            return False
        return True

    def run_copy(self, in_file: dict[str, Any]) -> bool:
        """Runs the copy operation. It looks at the `tape`'s configuration to find any file path prefix mappings
        that are configured as remote sources and copies the data via `rsync`, otherwise does a "local" copy.

        :param dict in_file: Dictionary for the file record and should contain values for the following keys:
            ['origin_file_path']: (str) Original file path for file (required)
            ['origin_file_name']: (str) Original file name for file (required)
            ['file_path']: (str) File path for where file will be written to (required)
            ['file_name']: (str) File name for where file will be written to (required)
            ['file_size']" (int) Size of file (in bytes) (required)
            ['file_id']: (int) ID of file (required)
        """
        from_file = os.path.join(in_file.get('origin_file_path'), in_file.get('origin_file_name'))
        to_file = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
        try:
            remote_config = self._get_remote_config(from_file, in_file)
            if remote_config is not None:
                # Found a matching remote source
                rsync_results = self.rsync(from_file, to_file, remote_config.get('rsync_uri'),
                                           remote_config.get('rsync_password'),
                                           remote_config.get('path_prefix_destination'))
                copied_file_size = rsync_results.get('total_transferred_file_size')
                if copied_file_size == 0:
                    # No errors running rsync but rsync determined the file did not need updating, so nothing was
                    # transferred. Setting it as -1 so that we know nothing was transferred.
                    copied_file_size = -1
            else:
                # Source is "local"
                if not os.path.exists(in_file.get('file_path')):
                    try:
                        os.makedirs(in_file.get('file_path'), 0o751)
                    except Exception:
                        pass
                temp_file = os.path.join(in_file.get('file_path'), '.' + in_file.get('file_name'))
                shutil.copyfile(from_file, temp_file)
                os.rename(temp_file, to_file)
                shutil.copystat(from_file, to_file)
                os.chmod(to_file, 0o640)
                copied_file_size = os.path.getsize(to_file)
            if copied_file_size > -1 and in_file.get('file_size') != copied_file_size:
                self.logger.warning(f'copy size is different for file {to_file}')
                self.sdm_curl.put(f'api/tape/file/{in_file.get("file_id")}', data={'file_size': copied_file_size})
            self.sdm_curl.put(f'api/tape/file/{in_file.get("file_id")}',
                              data={'file_status_id': self.cv.file_status.COPY_COMPLETE})
            # We delete the file only after a successful update to the database to `COPY_COMPLETE`, at which point the
            # file will no longer be needed
            temp_dir = self.temp_dir if remote_config is None else remote_config.get('path_temp')
            if from_file.startswith(temp_dir):
                # Source file is in SDM owned configured temporary directory (which means it was created by us), so we
                # want to delete it
                os.remove(from_file)
        except Exception as e:  # noqa: F841
            self.logger.warning(f'file {from_file} failed to copy to {to_file}')
            self.sdm_curl.put(f'api/tape/file/{in_file.get("file_id")}',
                              data={'file_status_id': self.cv.file_status.COPY_FAILED})
            return False
        return True

    '''
        We are assuming that the file that is going
        be checked already because it should be.
        return the md5 for the file using linux md5
    '''

    def run_md5(self, in_file):
        file_path = in_file.get('file_path')
        try:
            result = subprocess.run(['md5sum', file_path], stdout=subprocess.PIPE, check=True)
            md5 = result.stdout.decode('utf-8').split(' ')[0]
            data = {'md5sum': md5, 'queue_status_id': self.cv.queue_status.COMPLETE}
            self.sdm_curl.put(f'api/tape/md5/{in_file.get("md5_queue_id")}', data)
        except subprocess.CalledProcessError:
            self.sdm_curl.put(f'api/tape/md5/{in_file.get("md5_queue_id")}',
                              data={'queue_status_id': self.cv.queue_status.FAILED})
            traceback.print_exc()
            return False
        except Exception:
            traceback.print_exc()
            return False
        return True

    def run_prep_batch(self, records: list[dict[str, Any]]) -> bool:
        """Executes batch prep (lookup volume, position_a, position_b for a given file on tape and updates the
        appropriate `pull_queue` records).

        :param records: Prep requests to process
        :return: True if prep is successful for all records, False otherwise
        """

        def put_pull_queue_status(recs: list[dict[str, Any]], queue_status_id: int) -> None:
            for rec in recs:
                self.sdm_curl.put(f'api/tape/pull/{rec.get("pull_queue_id")}', queue_status_id=queue_status_id)

        records_by_service_id = {}
        for record in records:
            if not records_by_service_id.get(record.get('service'), None):
                records_by_service_id[record.get('service')] = []
            records_by_service_id.get(record.get('service')).append(record)
        success = True
        for service_id, recs in records_by_service_id.items():
            service = self.get_service(service_id)
            if not self.hsi_state.isup(service.get('server')):
                # HSI is down, requeue records.
                put_pull_queue_status(recs, self.cv.queue_status.REGISTERED)
                success = False
                continue
            tape_files_to_records = {}
            for rec in recs:
                tar_record_id = rec.get('tar_record_id')
                if tar_record_id is None:
                    tape_file = os.path.join(rec.get('remote_file_path'), rec.get('remote_file_name'))
                else:
                    tape_file = rec.get('remote_path')
                if not tape_files_to_records.get(tape_file, None):
                    tape_files_to_records[tape_file] = []
                tape_files_to_records.get(tape_file).append(rec)
            hsi_cmd = ['hsi', '-P', '-q', '-h', service.get('server')]
            hsi_data = bytes(''.join([f'ls -P -N {f}\n' for f in tape_files_to_records]), 'utf-8')
            hsi_output = ''
            tries = 1
            # Attempt a few times as this occasionally fails (according to existing code). Since we're doing a batch
            # `ls`, we can't rely on the process exit code, since if some files are not found, it will return a `64`
            # exit code (which happens to be overloaded). We make the assumption that if `stdout` has no lines
            # containing `FILE` that something may have gone wrong on `hsi` side, so we retry.
            while 'FILE' not in hsi_output and tries <= 3:
                tries += 1
                result = subprocess.run(hsi_cmd, stdout=subprocess.PIPE, input=hsi_data)
                hsi_output = result.stdout.decode('utf-8')
            for line in hsi_output.strip('\n').split('\n'):
                if 'FILE\t' in line:
                    file_info = line.split('\t')
                    tape_file = file_info[1]
                    # Tape sorting code only uses the first 6 characters for volume.
                    volume = file_info[5][:6]
                    (position_a, position_b) = file_info[4].split('+')
                    for rec in tape_files_to_records.get(tape_file):
                        self.sdm_curl.put(f'api/tape/pull/{rec.get("pull_queue_id")}', volume=volume,
                                          position_a=position_a, position_b=position_b,
                                          queue_status_id=self.cv.queue_status.REGISTERED)
                    del tape_files_to_records[tape_file]
            if len(tape_files_to_records) > 0:
                # Not all files prep info were retrieved, set anything not processed to `PREP_FAILED`.
                put_pull_queue_status(list(itertools.chain(*tape_files_to_records.values())),
                                      self.cv.queue_status.PREP_FAILED)
                success = False
        return success

    def run_pull(self, files):
        if len(files) == 0:
            return 0
        service = self.get_service(files[0].get('service'))
        volume = files[0].get('volume')
        if not self.hsi_state.isup(service.get('server')):
            self.sdm_curl.put(f'api/tape/releaselockedvolume/{self.division_name}/{volume}')
            # HSI is down, requeue records.
            for rec in files:
                self.sdm_curl.put(f'api/tape/pull/{rec.get("pull_queue_id")}',
                                  queue_status_id=self.cv.queue_status.REGISTERED)
            return False
        orig_dir = os.getcwd()
        self.logger.info(f'pull volume {volume}, {len(files)} files')
        temp_dir = os.path.join(self.temp_dir, "tape_" + volume + datetime.datetime.now().strftime("_%Y%m%d_%H%M%S"))
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except OSError as e:
                if e.errno != 17:
                    raise
        os.chdir(temp_dir)
        tar_files = {}
        tape_list = {}
        # loop through list, create missing dirs and save off list of files we are going to restore
        for in_file in files:
            # where we are restoring to
            in_file['restore_path'] = restore_path = os.path.join(in_file.get('file_path'),
                                                                  '.' + in_file.get('file_name'))
            # where the file will eventually end up
            in_file['destination_path'] = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
            if not os.path.exists(in_file.get('file_path')):
                try:
                    os.makedirs(in_file.get('file_path'), 0o751)
                except OSError as e:
                    if e.errno != 17:
                        raise
            if in_file.get('tar_record_id') is None:
                # what the file is called on tape
                tape_file = os.path.join(in_file.get('remote_file_path'), in_file.get('remote_file_name'))
                key_file = in_file.get('remote_file_name')
                cmd = f'get {restore_path} : {tape_file}'
            else:
                # what the file is called on tape
                key_file = tape_file = in_file.get('remote_path')
                # where the tar file will be restored to
                restore_path = os.path.join(temp_dir, in_file.get('remote_path').split('/')[-1])
                cmd = f'get {restore_path} : {tape_file}'
                # file within the tar ball restore path
                tar_file = in_file.get('remote_file_path') if in_file.get('remote_file_path').endswith(
                    in_file.get('remote_file_name')) else os.path.join(in_file.get('remote_file_path'),
                                                                       in_file.get('remote_file_name'))
                in_file['tar_restore_path'] = os.path.join(temp_dir, tar_file.lstrip("/"))
                if key_file in tar_files:
                    tar_files.get(key_file).append(tar_file)
                else:
                    tar_files[key_file] = [tar_file]
            key = f'{in_file.get("position_a"):015}{in_file.get("position_b"):015}{key_file}'
            tape_list[key] = cmd
        # write sorted list to task file
        with open(volume + '.cmd', 'w') as f:
            for key, value in sorted(tape_list.items()):
                f.write(value + '\n')
        # execute task file
        pull_cmd = ['hsi', '-h', service.get('server'), f'in {volume}.cmd']
        ret_value = 1
        if not self.debug:
            try:
                subprocess.run(pull_cmd, timeout=60 * 60 * 3, check=True)
            # except subprocess.TimeoutExpired as e:
            except Exception as e:
                self.logger.warning(f'failed to run hsi command: {" ".join(pull_cmd)}, ({repr(e)})')
                for in_file in files:
                    self.sdm_curl.put(f'api/tape/pull/{in_file.get("pull_queue_id")}',
                                      queue_status_id=self.cv.queue_status.FAILED)
                ret_value = 0
        # loop through tar balls, extract files and put them in the correct location
        if ret_value:
            for tar in tar_files:
                tar_cmd = ['tar', 'xvf', os.path.basename(tar)] + tar_files.get(tar)
                try:
                    if not self.debug:
                        subprocess.run(tar_cmd, check=True)
                except Exception as e:
                    self.logger.error(f'failed to run tar command: {" ".join(tar_cmd)}, ({repr(e)})')
                    for in_file in tar_files.get(tar):
                        try:
                            os.remove(in_file)
                        except Exception:
                            pass
            # loop through files, renaming them to the final name, setting jamo to success
            for in_file in files:
                try:
                    if in_file.get('tar_record_id') is not None:
                        # doing the tar in two steps (move, then rename) as we might be on a different file system
                        shutil.copy2(str(in_file.get('tar_restore_path')), str(in_file.get('restore_path')))
                    os.rename(str(in_file.get('restore_path')), str(in_file.get('destination_path')))
                    self.sdm_curl.put(f'api/tape/pull/{in_file.get("pull_queue_id")}',
                                      queue_status_id=self.cv.queue_status.COMPLETE)
                except Exception:
                    self.sdm_curl.put(f'api/tape/pull/{in_file.get("pull_queue_id")}',
                                      queue_status_id=self.cv.queue_status.FAILED)
        # remove restore directory
        os.chdir(orig_dir)
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass
        # release volume lock
        self.sdm_curl.put(f'api/tape/releaselockedvolume/{self.division_name}/{volume}')
        self.logger.info(f'release volume {volume}')
        return ret_value

    def add_extracted_file(self, in_file, info, metadata_record):
        data = {'file': in_file, 'file_type': info.get('file_type')}
        for key in ['validate_mode', 'local_purge_days', 'metadata', 'user']:
            if metadata_record.get(key) is not None:
                data[key] = metadata_record.get(key)

        data.get('metadata').update(info.get('metadata'))
        if 'origin_file_name' in metadata_record:
            # TODO: This call won't work as this destination will collide with the tarball being generated, so whichever
            #  gets copied second (tarball or extracted file) will fail since the path in one case is a tarball and the
            #  other is a directory...
            data['destination'] = os.path.join(metadata_record.get('file_path'), metadata_record.get('file_name'),
                                               info.get('path'))

        return self.sdm_curl.post('api/metadata/file', data=data).get('metadata_id')

    def get_relative_link(self, link_loc, file_loc):
        on = 0
        while len(link_loc) > on and len(file_loc) > on and link_loc[on] == file_loc[on]:
            on += 1
        return '../' * link_loc[on:].count('/') + file_loc[on:]

    def remove_file(self, record):
        try:
            file = record.get('file')
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.unlink(file)
        except Exception:
            # TODO: This call won't work as the API signature for `Tape.put_task` does not match this call...
            self.sdm_curl.put(f'api/tape/task/{record.get("task_id")}', task_status_is=self.cv.queue_status.FAILED)
        else:
            # TODO: This call won't work as the API signature for `Tape.put_task` does not match this call...
            self.sdm_curl.put(f'api/tape/task/{record.get("task_id")}', task_status_is=self.cv.queue_status.COMPLETE)

    def run_tar(self, metadata_record):
        if metadata_record.get('origin_file_name') is not None and metadata_record.get('local_purge_days') != 0:
            root_folder = os.path.join(metadata_record.get('origin_file_path'), metadata_record.get('origin_file_name'))
            copy_tar = True
            file_path = metadata_record.get('origin_file_path')
        else:
            root_folder = os.path.join(metadata_record.get('file_path'), metadata_record.get('file_name'))
            copy_tar = False
            file_path = metadata_record.get('file_path')
        remote_config = self._get_remote_config(file_path, metadata_record)
        temp_dir = self.temp_dir if remote_config is None else remote_config.get('path_temp')
        tar_file = os.path.join(temp_dir, f'{metadata_record.get("file_name")}.{metadata_record.get("file_id")}.tar')
        root_folder = root_folder.replace('.tar', '')
        ignore = metadata_record.get('ignore') if 'ignore' in metadata_record else []
        extract = metadata_record.get('extract') if 'extract' in metadata_record else []
        extract_keys = {}
        for rec in extract:
            extract_keys[rec.get('path')] = rec
        del extract
        tar = tarfile.open(tar_file, 'w')
        index = metadata_record.get('index')
        file_idx = []
        metadata_records = []
        for root, dirs, files in os.walk(root_folder):
            new_dirs = copy.copy(dirs)
            for folder in new_dirs:
                if os.path.join(root, folder)[len(root_folder) + 1:] in ignore:
                    dirs.remove(folder)

            for file_name in files:
                file = os.path.join(root, file_name)
                tar_dest = file[len(root_folder) + 1:]
                if ignore is not None and tar_dest in ignore:
                    continue

                if os.path.islink(file):
                    realpath = os.path.realpath(file)
                    # if realpath.startswith('/home/assembly_QC/ESTs'):
                    #     realpath = realpath.replace('/home/assembly_QC/ESTs','/house/groupdirs/assembly_QC/ESTs_Sync/ESTs')
                    if realpath.startswith(root_folder + '/'):
                        info = tarfile.TarInfo(tar_dest)
                        info.size = 0
                        info.mode = 493
                        info.type = tarfile.SYMTYPE
                        if file == realpath:
                            continue
                        info.linkname = self.get_relative_link(file, realpath)
                        tar.addfile(info)
                        continue
                    else:
                        rec = self.sdm_curl.get('api/tape/latestfile', file=realpath)
                        if rec is None:
                            if os.path.exists(realpath):
                                file = realpath
                            else:
                                self.logger.warning(
                                    f'tar file {root_folder} has a broken link from {tar_dest} to {realpath}')
                                continue
                        else:
                            tar_folder, tar_file_name = os.path.split(tar_dest)
                            metadata_records.append(
                                {'file_name': tar_file_name, 'file_path': tar_folder, 'id': rec.get('metadata_id')})
                            continue

                if tar_dest in extract_keys:
                    tar_folder, tar_file_name = os.path.split(tar_dest)
                    metadata_records.append({'file_name': tar_file_name, 'file_path': tar_folder,
                                             'id': self.add_extracted_file(file, extract_keys.get(tar_dest),
                                                                           metadata_record)})
                    continue

                if index and len(files) < 100:
                    tar_folder, tar_file_name = os.path.split(tar_dest)
                    file_idx.append({'file_name': tar_file_name, 'file_path': tar_folder})
                if os.access(file, os.R_OK):
                    tar.add(file, arcname=tar_dest)
                else:
                    self.logger.warning(f'tar file {file} can not be read.. skipping')
        tar.close()
        if len(file_idx) > 100:
            file_idx = []
        file_size = os.path.getsize(tar_file)
        self.sdm_curl.put('api/metadata/file', data={"id": metadata_record.get('_id'),
                                                     "data": {'folder_index': file_idx + metadata_records,
                                                              'current_location': tar_file, 'file_size': file_size}})
        current_path, current_file = os.path.split(tar_file)
        if copy_tar:
            self.sdm_curl.put(f'api/tape/file/{metadata_record.get("file_id")}',
                              file_status_id=self.cv.file_status.TAR_COMPLETE,
                              origin_file_path=current_path, origin_file_name=current_file,
                              file_size=file_size, next_status=self.cv.file_status.COPY_READY)
        else:
            self.sdm_curl.put(f'api/tape/file/{metadata_record.get("file_id")}',
                              file_status_id=self.cv.file_status.TAR_COMPLETE,
                              file_path=current_path, file_name=current_file,
                              origin_file_path=metadata_record.get('file_path'),
                              origin_file_name=metadata_record.get('file_name'),
                              file_size=file_size, next_status=self.cv.file_status.BACKUP_READY)
        return True

    def run_purge(self, in_file):
        local_file = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
        if not os.path.exists(local_file):
            self.sdm_curl.put(f'api/tape/file/{in_file.get("file_id")}', file_status_id=self.cv.file_status.PURGED)
            return True

        # If the file has been accessed in PURGE_ACCESSED days (or one day after the last update), then extend the reservation
        if not local_file.startswith(self.temp_dir):
            file_changed_plus_day = (datetime.datetime.now() - datetime.datetime.strptime(in_file.get('modified_dt'),
                                                                                          "%Y-%m-%dT%H:%M:%S")).total_seconds() - self.SECONDS_IN_DAY
            if time.time() - os.stat(local_file).st_atime < min(self.PURGE_ACCESSED, file_changed_plus_day):
                self.sdm_curl.put('api/tape/savefile', file=local_file, days=self.PURGE_EXTEND)
                self.logger.info(f'purge extended file {in_file.get("file_id")} reservation {self.PURGE_EXTEND} days')
                return True
        try:
            os.remove(local_file)
        except Exception:
            self.logger.info(f'file {in_file.get("file_id")} failed to purge')
        if not os.path.exists(local_file):
            self.sdm_curl.put(f'api/tape/file/{in_file.get("file_id")}', file_status_id=self.cv.file_status.PURGED)
        else:
            return False
        return True

    def run_delete(self, in_file):
        local_file = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
        if os.path.exists(local_file):
            if os.path.getsize(local_file) == in_file.get('file_size'):
                os.remove(local_file)
                self.remove_dir(in_file.get('file_path'))
        return True

    def remove_dir(self, directory):
        if len(os.listdir(directory)) == 0:
            os.rmdir(directory)
            return 1 + self.remove_dir(directory[0:directory.rfind("/")])
        else:
            return 0

    def rsync(self, source: str, destination: str, rsync_uri: str, rsync_password: str,
              path_prefix: str) -> dict[str, int]:
        """Rsync data from source to destination.

        :param source: Path to source
        :param destination: Path to destination
        :param rsync_uri: URI to rsync
        :param rsync_password: Password to rsync
        :param path_prefix: Prefix to rsync path
        :return: Returns stats from rsync transfer
        """
        # rsync doesn't seem to support creating parent path if it's more than a level deep, so we symlink the right
        # path in a temporary directory and rsync the directory
        with TemporaryDirectory(suffix='tmp') as temp_dir:
            # Remove the path_prefix since the remote rsync is set up to write to that directory
            relative_temp_path = os.path.dirname(destination).replace(path_prefix, '')
            if relative_temp_path.startswith('/'):
                relative_temp_path = relative_temp_path[1:]
            temp_remote_path = os.path.join(temp_dir, relative_temp_path)
            os.makedirs(temp_remote_path, exist_ok=True)
            os.symlink(source, os.path.join(temp_remote_path, os.path.basename(destination)))
            results = subprocess.run(['rsync', '-aL', '--no-h', '--stats', f'{temp_dir}/', f'{rsync_uri}/'],
                                     env={'RSYNC_PASSWORD': rsync_password}, stdout=subprocess.PIPE, check=True)
            ret = {}
            if results.stdout:
                for line in results.stdout.decode('utf-8').split('\n'):
                    match = re.search(r'([\w\s]+): (\d+)', line)
                    if match:
                        ret[match.group(1).lower().replace(' ', '_')] = int(match.group(2))
        return ret

    def _get_sharded_path(self, path: str) -> str:
        """Get sharded path. It appends the current year to `path`, e.g., `/path/to/temp` becomes
        `/path/to/temp_YYYY`, where `YYYY` is the current year. Removes trailing `/` from path if applicable.

        :param path: Path to shard
        :return: sharded path
        """
        return f'{path if not path.endswith("/") else path[:-1]}_{datetime.datetime.today().year}'

    def _get_remote_config(self, file_path: str, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Get remote configuration. If record has `source` as a key, it will use it to return the appropriate
        configuration. Otherwise, try to match remote configurations by file_path (by matching against configurations'
        `path_prefix_source` if defined). Otherwise, return None.

        :param str file_path: File path to try to match against configurations' `path_prefix_source`
        :param dict record: Record to check
        :return: dict for the matching configuration, None if no matches
        """
        source = record.get('source', None)
        if source is not None:
            # `source` is defined in the record, return the corresponding config
            return self.remote_sources.get(source, None)
        for feature, remote_config in self.remote_sources.items():
            if 'path_prefix_source' in remote_config:
                # Check if it's a remote source by path
                if file_path.startswith(remote_config.get('path_prefix_source')):
                    return remote_config
        return None

    def _verify_path_in_hsi(self, server: str, path: str, expected_size: int, exact_size: bool = True) -> None:
        """Verify that the path exists in HSI and its size. Note that this may not necessarily mean it's on tape, since
        HSI may return results from its cache and may not actually write to tape until a later time.

        :param server: Server to connect to when using HSI
        :param path: Path to check in HSI
        :param expected_size: Expected size in bytes
        :param exact_size: If `True`, check that the size matches exactly, otherwise check that the path size is at least `expected_size`
        """
        cmd = ['hsi', '-P', '-q', '-h', server, f'ls -1s {path}']
        results = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        size = int(results.stdout.decode('utf-8').split()[0])
        if exact_size:
            if size != expected_size:
                raise HSIVerificationFailedException(
                    f'HSI size {size} does not match expected size {expected_size} for {path}')
        else:
            if size < expected_size:
                raise HSIVerificationFailedException(
                    f'HSI size {size} is not at least expected size {expected_size} for {path}')

    def _put_globus(self, service: dict[str, Any], in_file: dict[str, Any]) -> bool:
        """Put file (if single file) or tarball (if multiple files) in directory to be used for Globus transfer via
        Globus timer. Operation behaves as "atomic" by creating a temporary (dot) file and atomically renaming it if
        successful.

        :param service: Globus service backing up to
        :param in_file: File record(s) to back up
        """
        # We are writing a single file to Globus
        if len(in_file.get('records')) == 1:
            in_file = in_file.get('records')[0]
            local_file = os.path.join(in_file.get('file_path'), in_file.get('file_name'))
            try:
                os.stat(local_file)
            except FileNotFoundError:
                self.logger.warning(f'file: {local_file} does not exist anymore...')
                self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                                  data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                return False
            remote_file_path = f'{datetime.datetime.today().year}/{in_file.get("file_path")[1:]}'
            remote_file_name = f'{in_file.get("file_name")}.{in_file.get("backup_record_id")}'
            globus_source_path, globus_temp_path = self._get_backup_service_paths(service.get('name'))
            globus_file_path = os.path.join(globus_source_path, remote_file_path)
            globus_file_name_temp = os.path.join(globus_temp_path, remote_file_name)
            globus_file_name = os.path.join(globus_file_path, remote_file_name)
            try:
                os.makedirs(globus_file_path, exist_ok=True)
                # Copy into a hidden dot file to make the operation "atomic"
                shutil.copy2(local_file, globus_file_name_temp)
                # Rename to non hidden dot file
                os.rename(globus_file_name_temp, globus_file_name)
            except Exception as e:
                self.logger.warning(f'Failed writing file to Globus directory: {e}')
                self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                                  data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                if os.path.exists(globus_file_name_temp):
                    os.remove(globus_file_name_temp)
                return False
            # Overwriting the remote file name and path, make sure the tar record id is also removed
            self.sdm_curl.put(f'api/tape/backuprecord/{in_file.get("backup_record_id")}',
                              data={'backup_record_status_id': self.cv.backup_record_status.TRANSFER_COMPLETE,
                                    'remote_file_name': remote_file_name,
                                    'remote_file_path': os.path.join(service.get('default_path'), remote_file_path),
                                    'tar_record_id': None})
            if local_file.startswith(self.temp_dir):
                file_info = self.sdm_curl.get(f'api/tape/file/{in_file.get("file_id")}')
                if file_info.get('file_status_id') == self.cv.file_status.BACKUP_COMPLETE:
                    # Is this is unlinking the file before md5 can complete?  Switching to adding a keep date of
                    # tomorrow and letting tape purge it
                    self.sdm_curl.put('api/tape/savefile', file=local_file, days=1)
            return True
        # Create a tarball of files
        else:
            root_dir = in_file.get('root_dir')
            tar_id = self.sdm_curl.post('api/tape/tar', root_path=root_dir).get('tar_record_id')
            backup_records = []
            tar_location = f'{datetime.datetime.today().year}/{self.to_folder_str(tar_id, 9, 3)[:-3]}{tar_id}.tar'
            globus_source_path, globus_temp_path = self._get_backup_service_paths(service.get('name'))
            globus_tar_file_path, globus_tar_file_name = os.path.split(os.path.join(globus_source_path, tar_location))
            os.makedirs(globus_tar_file_path, exist_ok=True)
            # Opening tarball in temporary directory, to later be moved to final directory to allow "atomic" operation
            tar_file_name_temp = os.path.join(globus_temp_path, globus_tar_file_name)
            tar_file_name = os.path.join(globus_tar_file_path, globus_tar_file_name)
            not_found_records = set()
            try:
                with tarfile.open(tar_file_name_temp, 'w') as tar:
                    for file_item in in_file.get('records'):
                        local_file = os.path.join(file_item.get('file_path'), file_item.get('file_name'))
                        try:
                            os.stat(local_file)
                        except FileNotFoundError:
                            self.logger.warning(f'file: {local_file} does not exist anymore...')
                            not_found_records.add(file_item.get('backup_record_id'))
                            self.sdm_curl.put(f'api/tape/backuprecord/{file_item.get("backup_record_id")}',
                                              data={
                                                  'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED})
                        else:
                            rfile_name = file_item.get('file_name')
                            exten = f'.{file_item.get("backup_record_id")}'
                            # Limiting file name to 90 characters to be under the 99 file name character limit for
                            # tar contents as specified by POSIX 1003.1
                            if len(rfile_name + exten) > 90:
                                rfile_name = rfile_name[:90 - len(exten)]
                            rfile_name = f'{rfile_name}{exten}'
                            tar.add(local_file, arcname=rfile_name)
                            backup_records.append({'backup_record_id': file_item.get('backup_record_id'),
                                                   'backup_record_status_id':
                                                       self.cv.backup_record_status.TRANSFER_COMPLETE,
                                                   'tar_record_id': tar_id, 'remote_file_name': rfile_name,
                                                   'remote_file_path': '.'})
                # Rename temp file to final file to allow it to be transferred via Globus timer
                os.rename(tar_file_name_temp, tar_file_name)
            except Exception as e:
                self.logger.warning(f'Failed creating tarball: {e}')
                # Set status for backup records as failed
                failed_backup_records = [{'backup_record_id': file_item.get('backup_record_id'),
                                          'backup_record_status_id': self.cv.backup_record_status.TRANSFER_FAILED}
                                         for file_item in in_file.get('records') if
                                         file_item.get('backup_record_id') not in not_found_records]
                self.sdm_curl.put('api/tape/backuprecords', records=failed_backup_records)
                if os.path.exists(tar_file_name_temp):
                    os.remove(tar_file_name_temp)
                return False
            self.sdm_curl.put(f'api/tape/tar/{tar_id}',
                              remote_path=os.path.join(service.get('default_path'), tar_location))
            self.sdm_curl.put('api/tape/backuprecords', records=backup_records)
            return True

    def _get_backup_service_paths(self, name: str) -> tuple[str, str]:
        """Get the configured source path and temp path for a backup service.

        :param str name: Backup service name (as defined in the `backup_services` configuration
        """
        if name not in self.backup_services:
            raise BackupServiceConfigurationException(f'Backup service configuration not found for {name}')
        config = self.backup_services.get(name)
        if 'source_path' not in config:
            raise BackupServiceConfigurationException(
                f'Backup service configuration missing `source_path` for {name}')
        if 'temp_path' not in config:
            raise BackupServiceConfigurationException(
                f'Backup service configuration missing `temp_path` for {name}')
        return config.get('source_path'), config.get('temp_path')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str, help="The server address of the restful service")
    parser.add_argument('-d', '--debug', help='Run in debug mode', action='store_true')
    parser.add_argument('-t', '--threads', type=int, help='Number of threads to run', default=1)
    parser.add_argument('-f', '--features', type=str, help='The features that can run with this manager')
    parser.add_argument('-k', '--tasks', type=str, help='The tasks that can run with this manager')
    parser.add_argument('-r', '--time', type=int, help='How long this service will run for in seconds. 0 runs forever',
                        default=36000)
    parser.add_argument('-l', '--log', type=str, help='Log name extension', default=None)
    parser.add_argument('-j', '--jamo_token_env_var', type=str,
                        help='The environemnt variable to get the JAMO token from, defaults to JAMO_TOKEN',
                        default='JAMO_TOKEN')
    parser.add_argument('-D', '--division', type=str, help='Division for tasks that can run with this manager')
    parser.add_argument('-e', '--email', type=str, help='Email to send notificaitons to when critical errors occur', default=None)
    parser.add_argument('-R', '--n_retry', type=int, help='The number of retries when getting HTTP errors', default=1000)
    args = parser.parse_args()

    # We are going to look in the environment variable specified by the -j flag for the JAMO token
    # in case we are wrapping this in tmux as all sessions on the same machine share the same environment
    jamo_token = os.environ.get(args.jamo_token_env_var)
    if jamo_token is None:
        print(f'{args.jamo_token_env_var} environment variable not set')
        sys.exit(1)

    os.umask(0o027)

    curl = Curl(args.host, appToken=jamo_token, retry=args.n_retry, errorsToRetry=[524])

    service_id = curl.post('api/tape/service',
                           data={'tasks': args.tasks, 'started_dt': 'now()', 'hostname': platform.node(),
                                 'available_threads': args.threads, 'seconds_to_run': args.time,
                                 'division': args.division}).get('service_id')
    run_tasks = args.tasks.split(',')
    dtn_service = DTService(curl, args.features.split(','), run_tasks, args.threads, args.debug, service_id,
                            args.division, log_ext=args.log, email=args.email)
    start_time = datetime.datetime.now()

    def finish_service(stop_gracefully=True):
        if stop_gracefully:
            dtn_service.stop_threads()
            dtn_service.logger.info('Waiting for threads to finish before terminating')
        curl.put(f'api/tape/service/{service_id}', data={'ended_dt': 'now()'})

    def sigterm_handler(signum, frame):
        dtn_service.logger.info(f'Caught signal {signum}. Finishing up')
        finish_service(True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)
    stop_threads = True

    while True:
        try:
            tasks = curl.get(f'api/tape/heartbeat/{args.division}/{service_id}')
            dtn_service.check_services()
            if args.time > 0 and (datetime.datetime.now() - start_time).total_seconds() >= args.time:
                dtn_service.stop_threads()
                break
            else:
                total_count = 0
                for name, task in tasks.items():
                    if name in run_tasks:
                        total_count += task.get('record_count')
                dtn_service.logger.info(f'task count is {total_count}')
                dtn_service.logger.info(f'threads running is {dtn_service.current_thread_count.value}')
                if total_count > 0:
                    dtn_service.set_threads(args.threads)
            time.sleep(240)
        except KeyboardInterrupt:
            # Someone hit Ctrl-C. Exit gracefully.
            break
        except Exception as e:
            # No time to exit gracefully, but make sure we record that this service has completed
            dtn_service.logger.info(f'Exiting with exception:\n{traceback.format_exc()}')
            stop_threads = False
            break
    finish_service(stop_threads)


if __name__ == '__main__':
    main()
