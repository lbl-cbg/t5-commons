import re
import datetime
import os
import threading
import time
import json
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.dates as mdates
import math
from io import BytesIO
import base64
from lapinpy import sdmlogger, restful, common
from lapinpy.mysqlrestful import MySQLRestful
from typing import Any, Optional, Callable

from . import task
from .hsi import HSI, HSI_status

MIN_SINGLE_SIZE = 1024 * 1024 * 1024
MAX_TAR_SIZE = 1024 * 1024 * 1024 * 1024
MAX_TAR_FILE_SIZE = 64 * 1024 * 1024 * 1024

PUT_MODES = {
    'Default': 0,
    'Replace_If_Newer': 2,
    'Replace_Force': 3
}

VALIDATION_MODES = {
    'Generate_MD5': 0,
    'Validate_Tape': 1,
    'No_MD5': 2
}

TRANSFER_MODES = {
    'Copy': 0,
    'Move': 1,
    'Purge': 2
}


class ConfigurationException(Exception):
    """Exception to be raised if there is an issue with configuration.
    """
    def __init__(self, message):
        Exception.__init__(self, message)


@restful.menu('tape')
class Tape(MySQLRestful):

    def __init__(self, config=None):
        if config is not None:
            self.config = config
            if self.config.instance_type == 'dev':
                self.auto_reload = True

        self.logger = sdmlogger.getLogger('tape')
        self.logger.info('Starting tape with the following settings:')
        self.logger.info('mysql host: %s' % self.config.mysql_host)
        self.logger.info('mysql user: %s' % self.config.mysql_user)
        self.logger.info('mysql db: %s' % self.config.tape_db)
        self.logger.info('repository root: %s' % self.config.dm_archive_root)

        MySQLRestful.__init__(self, self.config.mysql_host, self.config.mysql_user, self.config.mysql_pass, self.config.tape_db, host_port=getattr(self.config, 'mysql_port', None))
        self.module_name = 'JAMO'
        self.backup_services = self._get_backup_services()
        self.file_trees = {}
        self.cv = self.getCvs('file_status', 'backup_record_status', 'queue_status')   # This call modifies the object, sets attributes
        self.transfer_lock = threading.Lock()
        self.add_to_queue_lock = threading.Lock()
        self.cron_enabled = True
        self.hsi = None
        self.hsi_state = HSI_status()
        self.resources_gone = {}
        self.hpss_lock = threading.Lock()
        self.requestor_counts = {}
        self.held_volumes = {}
        self._lock = threading.Lock()
        self.tar_record_info = {}
        self.disk_usage = {}
        self.quota_used = 0
        self.remote_server_client = None
        self.repository_root_pattern = self.config.dm_archive_root + '%'

        # Construct a map to map service to the backup_service_id
        bs_map = dict()
        for i, bs in enumerate(self.config.backup_services):
            bs_map[bs['name']] = i + 1   # add one to calculate the backup_service_id that's based on an autoincrement

        self.default_backup_services = {division_config['name']: bs_map[division_config.get('default_backup_service')] for
                                        division_config in self.config.division}
        self.dm_archive_roots = self.config.dm_archive_root_by_division
        self.ingest_lock = threading.Lock()

        # get the last saved quota
        quota = self.query('select quota, used from quota order by quota_id desc limit 1')
        if len(quota) > 0:
            self.quota_used = int(quota[0].get('used', 0))
            self.config.disk_size = int(quota[0].get('quota', self.config.disk_size))

        # Initialize our repository stats
        self.repository_footprint()
        self.requested_restores = {}
        self.save_requested_restores()
        self.get_diskusage(None, None)

        self.refresh_tar_info()
        self.refresh_priority_counts()

        # Distribute any tarball location information we might have
        for rec in self.query('select distinct tar_record_id from pull_queue where queue_status_id = 1 and tar_record_id is not null and volume is null'):
            tar_record_id = rec['tar_record_id']
            if tar_record_id in self.tar_record_info:
                tar_record = self.tar_record_info[tar_record_id]
                self.modify('update pull_queue set volume=%s, position_a=%s, position_b=%s where tar_record_id=%s and queue_status_id=1 and volume is null', tar_record['volume'], tar_record['position_a'], tar_record['position_b'], tar_record_id)

        # Initialize remote path configuration
        self.remote_path_prefixes = {}
        for feature, remote_config in self.config.remote_sources.items():
            if 'path_prefix_source' in remote_config:
                # We only want to handle remote sources by path if the path source is defined
                self.remote_path_prefixes[remote_config.get('path_prefix_source')] = feature
        self.remote_path_filter = '|'.join(f'^{p}' for p in self.remote_path_prefixes)

        self.divisions = {}
        for division_config in self.config.division:
            division = self.divisions[division_config['name']] = Tape.Division(division_config['name'], self, division_config)

            # Initialize put queue
            self.init_put_queue(division_config['name'])

            # Initialize the active locks (volumes)
            division.pull_queue.init_locks(self.query(
                'select distinct volume from pull_queue join file using(file_id) where queue_status_id = %s and division = %s',
                [self.queue_status.IN_PROGRESS, division_config['name']], uselimit=False))

        self.logger.info('started tape')

    def getHpssFileInfo(self, in_file):
        with self.hpss_lock:
            if self.hsi is None:
                self.hsi = HSI(self.config.hpss_server)
            return self.hsi.getAllFileInfo(in_file)

    def shutdown(self):
        if self.hsi is not None:
            with self.hpss_lock:
                self.hsi.exit()

    def add_files(self, backup_records, release=False):
        for backup_record in backup_records:
            if release:
                self.modify(
                    'update backup_record set backup_record_status_id=%s,dt_to_release=null where backup_record_id=%s and backup_record_status_id=%s',
                    self.backup_record_status.TRANSFER_READY, backup_record.get('backup_record_id'),
                    self.backup_record_status.HOLD)

            rec = {'file_id': backup_record.get('file_id'), 'file_name': backup_record.get('file_name'),
                   'file_path': backup_record.get('file_path'),
                   'file_size': backup_record.get('file_size'), 'division': backup_record.get('division'),
                   'backup_records': [
                       {'backup_record_id': backup_record.get('backup_record_id'),
                        'service': backup_record.get('service')}]}
            if backup_record.get('local_purge_days') == 0 and (
                    backup_record.get('origin_file_path') is not None and backup_record.get('origin_file_path') != ''):
                rec['file_path'] = backup_record.get('origin_file_path')
                rec['file_name'] = backup_record.get('origin_file_name')
            self.add_file(rec)

    def init_put_queue(self, division_name):
        backup_records = self.query(
            'select f.local_purge_days, f.origin_file_path, f.origin_file_name, f.file_id, file_name, file_path, service, backup_record_id, file_status_id, backup_record_status_id, file_size, division from backup_record b left join file f on f.file_id = b.file_id where f.file_status_id in (%s,%s) and backup_record_status_id in (%s,%s) and dt_to_release is null and division = %s',
            [
                self.file_status.BACKUP_READY, self.file_status.BACKUP_IN_PROGRESS,
                self.backup_record_status.REGISTERED, self.backup_record_status.TRANSFER_READY, division_name],
            uselimit=False)
        self.add_files(backup_records)

    # Mark a task as completed
    @restful.permissions('tape')
    def put_taskcomplete(self, _args, kwargs):
        self.divisions.get(kwargs.get('division')).task_manager.set_task_complete(kwargs['task_id'], kwargs['returned'])

    # Get the next task to work on (marks the previous task as complete)
    @restful.validate({'features': {'type': list, 'validator': {'*': {'type': str}}},
                       'tasks': {'type': list, 'validator': {'*': {'type': str}}}, 'division': {'type': str}})
    @restful.permissions('tape')
    def post_nexttask(self, _args, kwargs):
        def create_task(next_task: dict[str, Any], queue_name: str) -> dict[str, Any]:
            return {'task': queue_name, 'data': next_task.get('data'), 'task_id': None,
                    'features': next_task.get('uses_resources'), 'service': kwargs.get('service'),
                    'created': datetime.datetime.now(), 'division': kwargs.get('division')}

        division = self.divisions.get(kwargs.get('division'))
        if 'previous_task_id' not in kwargs:
            kwargs['previous_task_id'] = None
        if 'prep' in kwargs.get('tasks'):
            if kwargs.get('previous_task_id') is not None:
                # Since the DB backed prep queue does not use `TaskManager`, we want to make sure to update
                # the task in `TaskManager` if the previous task originated from it
                division.task_manager.set_task_complete(kwargs.get('previous_task_id'), kwargs.get('returned'))
            next_task = division.prep_queue.next(kwargs.get('features'))
            if next_task:
                return create_task(next_task, division.prep_queue.name)
        if 'pull' in kwargs.get('tasks'):
            if kwargs.get('previous_task_id') is not None:
                # Since the DB backed pull queue does not use `TaskManager`, we want to make sure to update
                # the task in `TaskManager` if the previous task originated from it
                division.task_manager.set_task_complete(kwargs.get('previous_task_id'), kwargs.get('returned'))
            next_task = division.pull_queue.next(kwargs.get('features'))
            if next_task:
                return create_task(next_task, division.pull_queue.name) | {'records': len(next_task.get('data'))}
        return division.task_manager.get_task(kwargs.get('features'), kwargs.get('tasks'), kwargs.get('previous_task_id'),
                                              kwargs.get('service'), kwargs.get('returned'))

    # Put a task back into the queue
    @restful.permissions('tape')
    def put_task(self, _args, kwargs):
        data = kwargs.get('task')
        queue = getattr(self.divisions.get(data.get('division')), f'{data.get("task")}_queue')
        add_default_features = not any([feature in data.get('features') for feature in self.config.remote_sources])
        if isinstance(data.get('data'), list):
            # Request payload contains a list of records to add
            queue.add_all(data.get('data'), data.get('features'), add_default_features)
        else:
            # Request payload has a single record to add
            queue.add(data.get('data'), data.get('features'), add_default_features)
#       if 'task_id' in data:
#           queue = getattr(self, data['task'] + 'Queue')
#           queue.add(data['data'], data['features'])
#       else:
#           self.logger.info('put_task called with empty task_id')

    @restful.permissions('tape')
    def put_releaselockedvolume(self, args, _kwargs):
        self.divisions.get(args[0]).pull_queue.clear_lock(args[1])

    # Dump what is contained in the queues and the orphan list
    @restful.permissions('admin')
    def get_tasklist(self, _args, _kwargs):
        ret_value = {}
        for division in self.divisions.values():
            ret_value[division.division_name] = {'orphan_files': division.orphan_files}
            for key, value in division.__dict__.items():
                if isinstance(value, task.Queue):
                    for key2, value2 in value.feature_queues.items():
                        features = ret_value.get(division.division_name).setdefault(key, {})
                        features[key2] = list(value2)
        return ret_value

    # Get a volume for a tar record
    @restful.permissions('tape')
    def get_tarrecordinfo(self, args, _kwargs):
        if args[0] in self.tar_record_info:
            return self.tar_record_info[args[0]]
        return {}

    # Dump the task states
    def get_taskstatus(self, args, _kwargs):
        ret = {}
        for division in self.divisions.values():
            if 'reset' in args:
                division.task_manager.reset()
            ret[division.division_name] = division.task_manager.get_status()
        ret['resources_gone'] = self.resources_gone
        return ret

    # Dump the status for the gateway monitor
    def get_tape_status(self, _args, _kwargs):
        ret = {}
        ret['foot_print'] = self.disk_usage.get('disk_usage_files')
        ret['dna_free_tb'] = self.disk_usage.get('dna_free_tb')
        ret['pull'] = self.query('select count(*) N from pull_queue where queue_status_id in (1,2)')[0].get('N')
        ret['prep'] = self.query('select count(*) N from pull_queue where queue_status_id in (%s, %s) and volume is null',
                                 [self.queue_status.REGISTERED, self.queue_status.PREP_IN_PROGRESS])[0].get('N')
        ret['put'] = self.query('select count(*) N from backup_record where backup_record_status_id = 3')[0].get('N')
        ret['error'] = self.query('select count(*) N from backup_record where backup_record_status_id = 5')[0].get('N')
        ret['error'] += self.query('select count(*) N from pull_queue where queue_status_id in (4, 6)')[0].get('N')
        ret['error'] += self.query('select count(*) N from file where file_status_id in (5, 9, 17)')[0].get('N')
        ret['error'] += self.query('select count(*) N from file_ingest where file_ingest_status_id = 20')[0].get('N')
        ret['other'] = self.query('select count(*) N from file where file_status_id in (1, 2, 3, 15)')[0].get('N')
        ret['ingest'] = len(restful.run_internal('metadata', 'ingest_retry_records', None))
        ret['vol'] = self.query('select count(distinct volume) N from pull_queue where queue_status_id = 1')[0].get('N')
        ret['recent'] = self.query(
            'select count(*) N from pull_queue where queue_status_id = 3 and dt_modified > now() - interval 1 hour')[
            0].get('N')
        ret['hpss'] = (self.hsi_state.isup('archive') ^ 1) + (10 * (self.hsi_state.isup('hpss') ^ 1))
        ret['queue_status'] = self.query(
            'select a.priority, min(a.dt_modified) as min, min(b.dt_begin) as dt_modified, count(distinct a.pull_queue_id) as n from pull_queue a join pull_queue_status_history b on a.pull_queue_id = b.pull_queue_id where a.queue_status_id not in (0, 3) group by 1 order by 1')
        ret['active_states'] = self.query('select * from active')
        ret['active'] = self.query(
            'select a.requestor, count(*) as N, sum(b.file_size) / 1e9 as gb from pull_queue a join file b on a.file_id = b.file_id where a.queue_status_id not in (0, 3) group by 1 order by count(*) desc')
        ret['age'] = self.query('select * from age')
        ret['request'] = self.get_requested_last_five_days_by_user(None, None)
        ret['publish'] = self.get_ingested_last_five_days_by_user(None, None)
        ret['pull_stats'] = self.get_restored_last_five_days_by_hour(None, None)
        ret['put_stats'] = self.get_ingested_last_five_days_by_hour(None, None)
        ret['requested_restores'] = self.requested_restores
        ret['enabled_queues'] = {division_name: division.pull_queue.enabled_queues for division_name, division in
                                 self.divisions.items()}
        return ret

    @restful.permissions('admin')
    def post_reset_failed(self, _args, _kwargs):
        # update records and add them to the task queue
        def requeue(queue: task.Queue, table: str, from_state: int, to_state: int, state_key: str, id_key: str,
                    division_name: str, file_path_extract: Callable[[dict], str], extra: str = '') -> None:
            """Requeue records from `table`.

            :param queue: Queue to add records to
            :param table: Table to query records from
            :param from_state: State to process records for
            :param to_state: State to set records to
            :param state_key: Column name for the state
            :param id_key: Column name for the ID
            :param division_name: Division name to filter records on
            :param file_path_extract: Function to extract file path from record
            :param extra: Additional condition to use when querying for records to reset, optional
            :return: None
            """
            records = self.query(f'select * from {table} where {state_key} = %s and division = %s {extra}',
                                 [from_state, division_name], uselimit=False)
            for rec in records:
                self.modify(f'update {table} set {state_key} = %s where {id_key} = %s', to_state, rec[id_key])
                rec[state_key] = to_state
                self._add_to_queue_by_feature(queue, file_path_extract(rec), rec)

        def requeue_pull_queue(pull_queue_from_state: int, pull_queue_to_state: int, division_name: str,
                               file_to_state: int = None, extra: str = '') -> None:
            """Requeue records from `pull_queue`. Also updates associated `file` record if `file_to_state` is not
            `None`.

            :param pull_queue_from_state: `pull_queue` state to process records for
            :param pull_queue_to_state: `pull_queue` state to set records to
            :param division_name: Division name
            :param file_to_state: `file` state to set records to
            :param extra: Additional condition to use when querying for `pull_queue` records to reset
            """
            # Get `pull_queue` records to update
            pull_queue_records = self.query(
                f'select pull_queue_id, file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s {extra}',
                [pull_queue_from_state, division_name], uselimit=False)
            for pull_queue_record in pull_queue_records:
                # Update `pull_queue` record status
                self.modify('update pull_queue set queue_status_id = %s where pull_queue_id = %s', pull_queue_to_state,
                            pull_queue_record.get('pull_queue_id'))
                if file_to_state is not None:
                    # Update `file` record status
                    self.modify('update file set file_status_id = %s where file_id = %s', file_to_state,
                                pull_queue_record.get('file_id'))

        for division in self.divisions.values():
            # Update any ingest failures
            requeue(division.ingest_queue, 'file_ingest', self.file_status.INGEST_STATS_FAILED,
                    self.file_status.REGISTERED, 'file_ingest_status_id', 'file_ingest_id', division.division_name,
                    file_path_extract=lambda record: record.get('_file'))

            # Update any ingest failures
            requeue(division.ingest_queue, 'file_ingest', self.file_status.INGEST_FILE_MISSING,
                    self.file_status.REGISTERED, 'file_ingest_status_id', 'file_ingest_id',
                    division.division_name, file_path_extract=lambda record: record.get('_file'))

            # Add back to the queue any potentially lost file ingests.  These happen if the module was reloaded while
            # these were actively being reprocessed
            requeue(division.ingest_queue, 'file_ingest', self.file_status.REGISTERED,
                    self.file_status.REGISTERED, 'file_ingest_status_id', 'file_ingest_id',
                    division.division_name, extra='and _dt_modified < now() - interval 30 minute',
                    file_path_extract=lambda record: record.get('_file'))

            # Reset copy failed
            requeue(division.copy_queue, 'file', self.file_status.COPY_FAILED, self.file_status.COPY_READY,
                    'file_status_id', 'file_id', division.division_name,
                    file_path_extract=lambda record: record.get('origin_file_path'))

            # Reset tar records
            requeue(division.tar_queue, 'file', self.file_status.TAR_FAILED, self.file_status.TAR_READY,
                    'file_status_id', 'file_id', division.division_name, file_path_extract=self._get_tar_record_path)

            # Move any Delete state records to tar ready (part of the tar creation loop)
            requeue(division.tar_queue, 'file', self.file_status.DELETE, self.file_status.TAR_READY, 'file_status_id',
                    'file_id', division.division_name, file_path_extract=self._get_tar_record_path)

            # we need a session that stays open to create and use our temporary tables, we'll bypass the standard
            # methods
            conn = self.connect()
            cursor = conn.cursor()

            # file backups
            cursor.execute('update file set file_status_id = %s where file_status_id = %s and division = %s',
                           [self.file_status.BACKUP_READY, self.file_status.BACKUP_FAILED, division.division_name])

            # restart backups where they are failed, we'll put them in the hold queue and let the current machinery
            # requeue them
            cursor.execute('update backup_record b '
                           'join file f on b.file_id = f.file_id and backup_record_status_id = %s and division = %s '
                           'set backup_record_status_id = %s, dt_to_release = now()',
                           [self.backup_record_status.TRANSFER_FAILED, division.division_name,
                            self.backup_record_status.HOLD])

            # delete any dup pull queue requests, and any requests where the file has since been replaced
            # get a list of queue entries that are less than two days old that match any failed recs (status = 4)
            cursor.execute('create temporary table a as '
                           'select * from pull_queue '
                           ' where file_id in (select file_id from pull_queue join file using(file_id) where queue_status_id = %s and division = %s) '
                           '   and dt_modified > now() - interval 2 day '
                           ' order by file_id, pull_queue_id',
                           [self.queue_status.FAILED, division.division_name])

            # Get a list of these records and find the first one for each file id
            cursor.execute('create temporary table b as '
                           'select count(*), file_id, min(pull_queue_id) as pull_queue_id '
                           '  from a group by 2 '
                           'having count(*) > 1')

            # delete any failed records where there is a corresponding (earlier) record in the system
            cursor.execute('delete x '
                           'from pull_queue x '
                           'join b on x.file_id = b.file_id '
                           ' and x.pull_queue_id != b.pull_queue_id '
                           ' and x.queue_status_id = %s', [self.queue_status.FAILED])

            # delete any pull requests where the file has been replaced since the request came in
            cursor.execute('delete x '
                           'from pull_queue x '
                           'join file on x.file_id = file.file_id '
                           ' and x.queue_status_id = %s and file.file_status_id <= %s and division = %s',
                           [self.queue_status.FAILED, self.file_status.BACKUP_COMPLETE, division.division_name])

            cursor.fetchall()

            # reset failed pull queue states
            requeue_pull_queue(self.queue_status.FAILED, self.queue_status.REGISTERED, division.division_name,
                               self.file_status.RESTORE_REGISTERED)

            # reset failed prep queue states
            requeue_pull_queue(self.queue_status.PREP_FAILED, self.queue_status.REGISTERED, division.division_name)

            # reset lost prep queue states
            # Reset any prep records in `PREP_IN_PROGRESS` to `REGISTERED` if `dt_modified` > 10 minutes old
            requeue_pull_queue(self.queue_status.PREP_IN_PROGRESS, self.queue_status.REGISTERED, division.division_name,
                               extra='and dt_modified < now() - interval 10 minute')

            # Experimental: reset any in progress jobs that have been hanging out for 3+ hours
            requeue_pull_queue(self.queue_status.IN_PROGRESS, self.queue_status.REGISTERED,
                               division.division_name, file_to_state=self.file_status.RESTORE_REGISTERED,
                               extra='and dt_modified < now() - interval 3 hour')

    # Dump what is in the caches
    def get_cachestatus(self, _args, _kwargs):
        return {'requestor_counts': self.requestor_counts, 'tar_record_cache': self.tar_record_info}

    # Returns the number and footprint of the files that have been pulled from tape in the last arg|1 hours
    def get_pullcompletestats(self, args, _kwargs):
        if len(args) == 0:
            hours = 1
        else:
            hours = int(args[0])
        # informational query, so not going to deal with dst issues right now
        recs = self.query('select count(*) as files, sum(b.file_size) / 1000000000 as gbytes from pull_queue a join file b on a.file_id = b.file_id where a.dt_modified >= now() - interval %d hour and a.queue_status_id = 3' % hours)
        return {'files': recs[0]['files'], 'gbytes': recs[0]['gbytes']}

    # Returns true of false if the file is safe (e.g. copied to the repository or backup complete)
    def get_filesafe(self, args, _kwargs):
        if len(args) == 0:
            raise common.HttpException(400, 'Sorry you must provide a file_id')
        else:
            try:
                file_id = int(args[0])
            except Exception:
                raise common.HttpException(400, 'Sorry you must provide a file_id')
        # Consider only history records that have a copy complete or backup complete that were added after the file
        # create date. The join to the file table is to account for replaced files (e.g. we are concerned with the
        # new copy, not whether the orginal copy was successful)
        records = self.query('select count(*) as c from file_status_history a join file b on a.file_id = b.file_id and a.dt_begin >= b.created_dt where a.file_id = %s and a.file_status_id in (4, 8)' % file_id, uselimit=False)
        if len(records) != 1:
            raise common.HttpException(400, 'file_id %s not found' % file_id)
        if records[0]['c'] > 0:
            return True
        return False

    def add_file(self, record):
        with self.transfer_lock:
            division = self.divisions.get(record.get('division'))
            # file is big enough to go off to tape by itself
            if record.get('file_size') >= MIN_SINGLE_SIZE:
                for backup_record in record.get('backup_records'):
                    division.put_queue.add(
                        {'file_size': record.get('file_size'), 'service': backup_record.get('service'), 'records': [
                            {'file_id': record.get('file_id'),
                             'backup_record_id': backup_record.get('backup_record_id'),
                             'file_name': record.get('file_name'), 'file_path': record.get('file_path')}]},
                        [self._get_backup_service_feature_name(backup_record.get('service'))])
            #  else we are going to save it off and put into a tarball
            else:
                for backup_record in record.get('backup_records'):
                    service = backup_record.get('service')
                    #  If we don't have a collection for this service, start a new list
                    if str(service) not in division.orphan_files:
                        division.orphan_files[str(service)] = {
                            'size': record.get('file_size'),
                            'root_dir': record.get('file_path'),
                            'backup_records': [{'file_id': record.get('file_id'),
                                                'backup_record_id': backup_record.get('backup_record_id'),
                                                'file_name': record.get('file_name'),
                                                'file_path': record.get('file_path')}]}
                    # Else add it to the current list of files to send off
                    else:
                        orphan_list = division.orphan_files[str(service)]
                        orphan_list['size'] += record.get('file_size')
                        orphan_list.get('backup_records').append(
                            {'file_id': record.get('file_id'),
                             'backup_record_id': backup_record.get('backup_record_id'),
                             'file_name': record.get('file_name'), 'file_path': record.get('file_path')})
                        orphan_list['root_dir'] = os.path.commonprefix(
                            [orphan_list.get('root_dir'), record.get('file_path')])
                        # If our collection is now big enough, send off the set to tape and remove the list
                        if orphan_list.get('size') > MIN_SINGLE_SIZE:
                            division.put_queue.add(
                                {'file_size': orphan_list.get('size'), 'root_dir': orphan_list.get('root_dir'),
                                 'records': orphan_list.get('backup_records'), 'service': service},
                                [self._get_backup_service_feature_name(backup_record.get('service'))])
                            del division.orphan_files[str(service)]

    # calculate the current repository footprint
    @restful.cron('*/30', '*', '*', '*')
    def repository_footprint(self):
        # disk allocation is in MB, block sizes are 512 bytes for files < 384 and 32k for everything larger
        data = self.query('select count(*) as files, ifnull(sum(case when file_size < 384 then 512 else ceil(file_size/32768.0)*32768 end), 0) as disk_usage from file where file_status_id not in (%s, %s) and file_path like %s',
                          [self.cv.get('file_status').get('PURGED', 0),
                           self.cv.get('file_status').get('DELETE', 0),
                           self.repository_root_pattern])
        if len(data) > 0:
            self.disk_usage['files'] = int(data[0].get('files', 0))
            self.disk_usage['disk_usage_files'] = int(data[0].get('disk_usage', 0))

        # Files being restored
        data = self.query('select count(*) as files, ifnull(sum(case when file_size < 384 then 512 else ceil(file_size/32768.0)*32768 end), 0) as disk_usage from file where file_status_id = %s and file_path like %s',
                          [self.cv.get('file_status').get('RESTORE_IN_PROGRESS', 0),
                           self.repository_root_pattern])
        if len(data) > 0:
            self.disk_usage['files_restoring'] = int(data[0].get('files', 0))
            self.disk_usage['disk_usage_files_restoring'] = int(data[0].get('disk_usage', 0))

        # all the space used that isn't accounted for by on-disk JAMO files we'll assign to other
        used_by_other = self.quota_used - (self.disk_usage.get('disk_usage_files', 0) - self.disk_usage.get('disk_usage_files_restoring', 0))
        self.disk_usage['disk_usage_other'] = used_by_other

        # bytes free: ideally we could just use the disk remaining and subtract what is left to restore, but since
        # the quota numbers will only be updated daily, we need to calculate this ourselves
        # we assume portal_usage is fairly static, so we only need to update this every 30 minutes

        self.disk_usage['date_updated'] = datetime.datetime.today()
        disk_usage = self.disk_usage.get('disk_usage_files', 0) + used_by_other
        bytes_free = self.config.disk_size - disk_usage
        self.disk_usage['disk_reserve'] = self.config.disk_reserve
        self.disk_usage['bytes_used'] = disk_usage
        self.disk_usage['bytes_free'] = bytes_free
        self.disk_usage['dna_free_tb'] = self.disk_usage.get('bytes_free', 0) / 1e12

    # @restful.cron('*/5','*','*','*')
    def monitor(self):
        for division in self.divisions.values():
            division.task_manager.monitor_lost_tasks()

    # @restful.cron('0','10','*','*')
    def fix_stalled_files(self):
        # fix md5 jobs that have somehow gotten kill in between
        self.modify('update file f set file_status_id=%s where file_status_id=%s and validate_mode=0 and f.md5sum is not null and not exists (select backup_record_id from backup_record b where b.file_id=f.file_id and backup_record_status_id<>%s)',
                    self.file_status.BACKUP_COMPLETE, self.file_status.BACKUP_IN_PROGRESS, self.backup_record_status.TRANSFER_COMPLETE)
        self.modify('update backup_record set backup_record_status_id=%s where backup_record_status_id=%s and dt_modified<date_sub(curdate(), interval 1 day)',
                    self.backup_record_status.TRANSFER_READY, self.backup_record_status.TRANSFER_IN_PROGRESS)

    # this should run a few times a day
    # @restful.cron('0','3','*','*')
    def delete_records(self):
        delete_records = self.query(
            'select tar_record_id, file_date, service, backup_record_id, backup_record_status_id, b.file_id, remote_file_name, remote_file_path, file_path, file_name from backup_record b left join file f on f.file_id=b.file_id where file_status_id=%s', [self.file_status.DELETE])
        if len(delete_records) == 0:
            return
        try:
            backup_record_ids = []
            file_ids = []
            for record in delete_records:
                backup_record_ids.append(record['backup_record_id'])
                file_ids.append(record['file_id'])
            file_ids = ', '.join(map(str, file_ids))
            backup_record_ids = ', '.join(map(str, backup_record_ids))
            self.modify('delete from backup_record_status_history where backup_record_id in (%s)' % backup_record_ids)
            self.modify('delete from backup_record where backup_record_id in (%s)' % backup_record_ids)
            self.modify('delete from file_status_history where file_id in (%s)' % file_ids)
            self.modify('delete from pull_queue where file_id in (%s)' % file_ids)
            self.modify('delete from file where file_id in (%s)' % file_ids)
        except Exception:
            pass

    @restful.cron('*/15', '*', '*', '*')
    def release_backup_records(self):
        backup_records = self.query(
            'select f.local_purge_days, f.origin_file_path, f.origin_file_name, f.file_id, file_name, file_path, service, backup_record_id, file_status_id, backup_record_status_id, file_size, file_status_id, division from backup_record b left join file f using(file_id) where f.file_status_id = %s and backup_record_status_id = %s and dt_to_release is not null and dt_to_release <= now()',
            [self.file_status.BACKUP_READY, self.backup_record_status.HOLD], uselimit=False)
        self.add_files(backup_records, True)

    @restful.cron('5', '0', '*', '*')
    def refresh_tar_info(self):
        self.tar_record_info = {}
        # dst safe because it is only run at 00:05
        recs = self.query('select distinct tar_record_id, volume, position_a, position_b from pull_queue where dt_modified > now() - interval 1 day and volume is not null and tar_record_id is not null', uselimit=False)
        for rec in recs:
            self.tar_record_info[rec['tar_record_id']] = {'volume': rec['volume'], 'position_a': rec['position_a'], 'position_b': rec['position_b']}

    @restful.cron('5', '0', '*', '*')
    def reset_priority_counts(self):
        self.requestor_counts = {}

    def refresh_priority_counts(self):
        requestor_counts = {}
        recs = self.query('select requestor, count(*) as n from pull_queue where dt_modified >= curdate() group by requestor', uselimit=False)
        for rec in recs:
            requestor_counts[rec['requestor']] = rec['n']
        self.requestor_counts = requestor_counts

    # Body from post_grouprestore and post_restore
    def add_to_pull_queue(self, in_file, days, requestor, source=None):
        def get_priority_and_limit_for_request():
            if self.config.queue_2_match in requestor:
                limit = self.config.queue_2_limit
                priority = 2
            else:
                limit = self.config.queue_1_limit
                priority = 1

            # increment moved into lock after check of pre-existing request for same file
            if requestor in self.requestor_counts:
                # if this succeeds in a restore request, we'll be above the limit below, so >= rather than >
                for limit_cutoff in limit:
                    if self.requestor_counts[requestor] >= limit_cutoff:
                        priority += 2
                    else:
                        break

            # If the free space is less than our reserve, we'll bump the priority up to 8 (the 'lowest', e.g., don't actively restore)
            if self.disk_usage['bytes_free'] < self.config.disk_reserve:
                priority = 8

            return priority, limit

        # Add the request to a log table
        request_id = self.modify('insert into request (file_id, requestor) values (%s, %s)', in_file['file_id'],
                                 requestor)
        if source:
            # Add to egress table if remote data center source is set
            if self.query(
                    'select count(*) as cnt from egress where file_id=%s and source=%s and egress_status_id in (%s, %s)',
                    [in_file['file_id'], source, self.cv['queue_status']['REGISTERED'],
                     self.cv['queue_status']['IN_PROGRESS']])[0]['cnt'] == 0:
                # Only add to table if there's no egress request where file_id + source + egress_status_id in
                # (REGISTERED, IN_PROGRESS)
                self.modify('insert into egress(file_id, egress_status_id, requestor, source, request_id) values(%s, %s, %s, %s, %s)',
                            in_file['file_id'], self.cv['queue_status']['REGISTERED'], requestor, source, request_id)
        # This file should be one under Tape's control, so we don't need to call our custom exists
        if in_file['file_status_id'] in [self.file_status.PURGED]:  # or (not os.path.exists(os.path.join(in_file['file_path'], in_file['file_name'])) and in_file['file_status_id'] in [self.file_status.BACKUP_COMPLETE, self.file_status.PURGED, self.file_status.RESTORE_IN_PROGRESS, self.file_status.RESTORED]):
            # We should only need to rely on the purge state, since if the file is not on the FS, it could be because it is in several
            # earlier states.  But if there is a failure somewhere, it could be in other states.  If it is still in the queue, the code below
            # should address this.
            file_id = in_file['file_id']
            priority, limit = get_priority_and_limit_for_request()

            # look up the tar record id, this will slow down the insert, but help speed up the prep
            tar_record = self.query('select tar_record_id from backup_record where file_id = %d and service = 1' % file_id)
            tar_record_id = tar_record[0]['tar_record_id'] if len(tar_record) > 0 else None
            # Get the vol/position if we know about it
            if tar_record_id and tar_record_id in self.tar_record_info:
                volume, position_a, position_b = self.tar_record_info[tar_record_id]['volume'], self.tar_record_info[tar_record_id]['position_a'], self.tar_record_info[tar_record_id]['position_b']
                in_file['volume'], in_file['position_a'], in_file['position_b'] = volume, position_a, position_b
            else:
                volume, position_a, position_b = None, None, None
            pqid = 0
            with self.add_to_queue_lock:
                if len(self.query('select * from pull_queue where file_id=%s and queue_status_id not in (%s, %s)',
                                  [file_id, self.queue_status.COMPLETE, self.queue_status.FAILED])) == 0:
                    # Moving the increment here as user may make multiple requests before file is restored, putting it here will prevent inflation of requests
                    self.requestor_counts[requestor] = self.requestor_counts.get(requestor, 0) + 1
                    pqid = self.modify('insert into pull_queue (file_id, requestor, priority, tar_record_id, volume, position_a, position_b) values (%s, %s, %s, %s, %s, %s, %s)',
                                       file_id, requestor, priority, tar_record_id, volume, position_a, position_b)

            # continuation of above if, mainly because we want the lock to live only briefly
            # it is possible that multiple threads have gotten in here with the same file, which will cause an
            # inconsistency in the file_state.  But the winner should set the state correctly and only one request
            # should get into the restore queue
            if pqid:
                self.put_file([file_id], {'file_status_id': self.file_status.RESTORE_REGISTERED})
                # switch to curdate to address DST issues
                self.modify('update file set user_save_till=date_add(curdate(), interval %s day) where file_id=%s', days, file_id)
                in_file['pull_queue_id'] = pqid
                in_file['priority'] = priority
                restful.run_internal('metadata', 'add_update', {'file_id': in_file['file_id']}, {'dt_to_purge': self.get_file([in_file['file_id']], None)['dt_to_purge']})  # , 'file_status': 'RESTORE_IN_PROGRESS'})

            # We want to account for what people/portal are restoring so we don't go over the limit
            #   before the next update of diskusage.
            # Round up to the nearest 32k (block size), we won't worry about the small blocks here,
            #   this will get corrected in the next update.
            # Portal is going to look at bytes_free before sending the next request
            disk_footprint = math.ceil(in_file['file_size'] / 32768.0) * 32768
            self.disk_usage['bytes_free'] -= disk_footprint
            self.disk_usage['bytes_used'] += disk_footprint
            return True
        elif in_file.get('file_status_id') == self.file_status.RESTORE_REGISTERED:
            # Restore has been requested, need to check whether to update priority for the request
            file_id = in_file.get('file_id')
            priority, limit = get_priority_and_limit_for_request()
            with self.add_to_queue_lock:
                existing_pull_queue_records = self.query(
                    'select * from pull_queue where file_id=%s and queue_status_id not in (%s, %s)',
                    [file_id, self.queue_status.COMPLETE, self.queue_status.FAILED])
                if len(existing_pull_queue_records) > 0:
                    # Existing record in pull_queue, update the priority and requestor if higher (lower priority
                    # numeric value)
                    if priority < existing_pull_queue_records[0].get('priority'):
                        self.modify('update pull_queue set priority=%s, requestor=%s where pull_queue_id=%s',
                                    priority, requestor, existing_pull_queue_records[0].get('pull_queue_id'))
                        # Decrement count for previous requestor
                        previous_requestor = existing_pull_queue_records[0].get('requestor')
                        previous_requestor_new_request_count = self.requestor_counts.get(previous_requestor, 0) - 1
                        if previous_requestor_new_request_count > 0:
                            self.requestor_counts[previous_requestor] = previous_requestor_new_request_count
                        elif previous_requestor in self.requestor_counts:
                            del self.requestor_counts[previous_requestor]
                        # Increment count for new requestor
                        self.requestor_counts[requestor] = self.requestor_counts.get(requestor, 0) + 1
            return True
        return False

    @restful.doc('Puts a request in to restore a list of files.  There is a limit of 500 files per request.')
    @restful.validate({'files': {'type': list, 'validator': {'*': {'type': str}},
                                 'doc': 'A list of jamo ids, which are the value of the _id key',
                                 'example': ['51d4c50e067c014cd6eab68c', '51d4a405067c014cd6ea1cb7']},
                       'days': {'required': False, 'type': int,
                                'doc': 'How many days to keep this file around after it has been restored. default is 90 days',
                                'example': 30},
                       'requestor': {'type': str,
                                     'doc': 'the user making the request that will be used for logging purposes.  Value should be the NERSC username for internal and/or pipeline restores, or "portal/<emailaddress>" for requests coming from portal.',
                                     'example': 'rqc', 'required': True},
                       'source': {'type': str, 'required': False, 'default': None,
                                  'doc': 'Source name for data center (e.g., igb, dori)'}
                       },
                      )
    def post_grouprestore(self, _args, kwargs):
        days = 90
        if 'days' in kwargs:
            try:
                # run a max in case the passed days resolves to zero
                days = max(min(int(kwargs['days']), days), 5)
            except Exception:
                pass
        metadata_ids = kwargs['files']
        if len(metadata_ids) == 0:
            return
        # note that this currently has a 500 file limit
        files = self.query('select file_id, file_status_id, file_name, file_path, file_size from file where metadata_id in (%s)' % ('%s,' * len(metadata_ids))[:-1], metadata_ids)

        restored_count = 0
        updated_count = 0
        for in_file in files:
            if self.add_to_pull_queue(in_file, days, kwargs['requestor'], kwargs['source']):
                restored_count += 1
            else:
                # switch to curdate to address DST issues
                self.modify('update file set user_save_till=date_add(curdate(), interval %s day) where file_id=%s and (user_save_till is null or user_save_till< date_add(curdate(), interval %s day))',
                            days, in_file['file_id'], days)
                restful.run_internal('metadata', 'add_update', {'file_id': in_file['file_id']}, {'dt_to_purge': self.get_file([in_file['file_id']], None)['dt_to_purge']})
                updated_count += 1
        return {'url': '/tape/pullqueue', 'restored_count': restored_count, 'updated_count': updated_count}

    @restful.doc('Adds a file to the restore queue. This file will be restored to its original location')
    @restful.validate({'file': {'type': str, 'required': False},
                       'file_id': {'type': int, 'required': False},
                       'requestor': {'type': str,
                                     'doc': 'the user making the request that will be used for logging purposes.  Value should be the NERSC username for internal and/or pipeline restores, or "portal/<emailaddress>" for requests coming from portal.',
                                     'example': 'rqc', 'required': True},
                       'source': {'type': str, 'required': False, 'default': None,
                                  'doc': 'Source name for data center (e.g., igb, dori)'}
                       })
    def post_restore(self, _args, kwargs):
        days = 90
        if 'days' in kwargs:
            days = min(kwargs['days'], days)

        if 'file_id' not in kwargs:
            if 'file' not in kwargs:
                raise common.HttpException(400, 'Sorry you must provide a file_id or a file_path')
            in_file = kwargs['file']
            file_path, file_name = os.path.split(in_file)
            files = self.query('select file_id, file_status_id, file_name, file_path, file_size from file where file_name=%s and file_path=%s order by file_date desc limit 1', [file_name, file_path])
        else:
            in_file = kwargs['file_id']
            files = self.query('select file_id, file_status_id, file_name, file_path, file_size from file where file_id=%s', [kwargs['file_id']])

        if len(files) == 0:
            raise common.HttpException(400, 'Sorry file %s does not have a record in the tape system' % in_file)

        self.add_to_pull_queue(files[0], days, kwargs['requestor'], kwargs['source'])

    # we should make sure that there are pull services running
    @restful.validate({'file_id': {'type': int}, 'callback': {'type': str}})
    def post_pullrequest(self, _args, kwargs):
        return self.smart_insert('pull_queue', kwargs)

    @restful.permissions('tape')
    def post_tar(self, _args, kwargs):
        return {'tar_record_id': self.smart_insert('tar_record', kwargs)}

    def validate_backup_service(self, service_id: int, division_name: str):
        """Checks whether a requested backup service exists and the user's division has access to the service.

        :param service_id: ID of backup service
        :param division_name: Name of division
        :return:
        """
        service = self.backup_services.get(service_id)
        if service is None:
            # Reload backup services from database in case it's been changed (keeping old behavior).
            self.backup_services = self._get_backup_services()
            service = self.backup_services.get(service_id)
        return service is not None and service.get('division') == division_name

    def get_cvs(self, _args, _kwargs):
        return self.cv

    backup_service_validator = {
        'name': {'type': str},
        'server': {'type': str},
        'type': {'type': str, 'required': False},
        'staging_path': {'type': str, 'required': False}
    }

    # TODO: Is this being used? Has field `staging_path` that is different in prod schema as `default_path`
    @restful.doc('Adds a new backup service', {'backup_service_id': {'type': int}}, public=False)
    @restful.validate(backup_service_validator)
    @restful.permissions('tape')
    def post_backupservice(self, _args, kwargs):
        return {'backup_service_id': self.smart_insert('backup_service', kwargs)}

    @restful.doc('Returns the backup service information for request service', public=False)
    @restful.validate(argsValidator=[{'name': 'backup_service_id', 'type': int}])
    @restful.single
    def get_backupservice(self, args, _kwargs):
        return self.query('select * from backup_service where backup_service_id = %s', args)

    @restful.queryResults({'title': 'Backup services',
                           'table': {'columns': [['name', {}],
                                                 ['type', {}],
                                                 ['server', {}],
                                                 ['default_path', {}],
                                                 ['backup_service_id', {'title': 'Id'}]],
                                     'sort': {'enabled': False}},
                           'data': {'default_query': ''}})
    @restful.menu('Backup services')
    @restful.table(title='Backup Services')
    def get_backupservices(self, _args, kwargs):
        if kwargs and kwargs.get('queryResults', None):
            return self.queryResults_dataChange(kwargs, 'backup_service')
        else:
            return self.query('select * from backup_service')

    backup_record_validator = {
        'service': {'type': int},
        'file_id': {'type': int}
    }

    @restful.doc('Saves a backup record for the specified file_id', public=False)
    @restful.validate(backup_record_validator)
    @restful.permissions('tape')
    def post_backuprecord(self, _args, kwargs):
        return self.smart_insert('backup_record', kwargs)

    backup_record_put_validator = {
        'md5sum': {'type': str, 'required': False},
        'backup_status_id': {'type': int, 'required': False},
        'tar_record_id': {'type': int, 'required': False},
        'remote_file_name': {'type': str, 'required': False},
        'remote_file_path': {'type': str, 'required': False},
    }

    @restful.permissions('tape')
    def put_backuprecords(self, _args, kwargs):
        for record in kwargs['records']:
            self.put_backuprecord([record['backup_record_id']], record)

    @restful.permissions('tape')
    @restful.validate(argsValidator=[{'name': 'file_id', 'type': int}])
    def delete_file(self, args, _kwargs):
        record = {}
        deletes = 0
        backup_record_ids = ''
        if len(args) == 1:
            file_id = str(args[0])
            # Set the file record to delete
            if file_id:
                self.modify('update file set file_status_id = 11 where file_id = %s', file_id)
                record['file'] = self.query('select * from file where file_id = %s', file_id, uselimit=False)
                record['file_status_history'] = self.query('select * from file_status_history where file_id = %s',
                                                           file_id, uselimit=False)
                record['pull_queue'] = self.query('select * from pull_queue where file_id = %s', file_id,
                                                  uselimit=False)
                record['request'] = self.query('select * from request where file_id = %s', file_id, uselimit=False)

                record['backup_record'] = self.query('select * from backup_record where file_id = %s', file_id,
                                                     uselimit=False)
                backup_record_ids = ', '.join(
                    [str(backuprecord.get('backup_record_id')) for backuprecord in record.get('backup_record')])
                if backup_record_ids:
                    record['backup_record_status_history'] = self.query(
                        'select * from backup_record_status_history where backup_record_id in (%s)' % backup_record_ids,
                        uselimit=False)
                else:
                    record['backup_record_status_history'] = []

            pullqueueids = ', '.join([str(pull_queue.get('pull_queue_id')) for pull_queue in record.get('pull_queue')])
            if pullqueueids:
                record['pull_queue_status_history'] = self.query(
                    'select * from pull_queue_status_history where pull_queue_id in (%s)' % pullqueueids,
                    uselimit=False)
            else:
                record['pull_queue_status_history'] = []

            if backup_record_ids:
                deletes += self.modify(
                    'delete from backup_record_status_history where backup_record_id in (%s)' % backup_record_ids)
                deletes += self.modify('delete from backup_record where backup_record_id in (%s)' % backup_record_ids)
            if file_id:
                deletes += self.modify('delete from file_status_history where file_id = %s', file_id)
                deletes += self.modify('delete from pull_queue where file_id = %s', file_id)
                deletes += self.modify('delete from request where file_id = %s', file_id)
                deletes += self.modify('delete from file where file_id = %s', file_id)
            if len(record.get('file')) == 1:
                file = record.get('file')[0]
                self.divisions.get(file.get('division')).delete_queue.add(file)
        return {'tape_records': deletes, 'tape_data': record}

    @restful.permissions('tape')
    def post_undelete_file(self, args, _kwargs):
        # This is called by metadata post_undelete and is handed the _tape_data element of
        # the deleted_file record
        inserts = 0
        if len(args) > 0:
            data = args[0]
            if 'file' in data:
                # Set the file record to purge
                data['file'][0]['file_status_id'] = 10
                # New delete method, note that we need to do these in order because of
                # foreign key constraints
                for table in ('file',
                              'file_status_history',
                              'backup_record',
                              'backup_record_status_history',
                              'pull_queue',
                              'pull_queue_status_history',
                              'request'):
                    if table in data:
                        for record in data[table]:
                            inserts += 1
                            self.smart_insert(table, record)
            else:
                # Old delete method
                sh = data['status_history']
                br = data['backup_records']
                del data['status_history']
                del data['backup_records']
                del data['file_status']
                del data['dt_to_purge']
                self.smart_insert('file', data)
                inserts += 1
                for record in sh:
                    inserts += 1
                    self.smart_insert('file_status_history', record)
                for record in br:
                    inserts += 1
                    self.smart_insert('backup_record', record)
        return {'tape_records': inserts}

    @restful.cron('4', '*', '*', '*')
    def purgefiles(self):
        for division in self.divisions.values():
            if division.purge_queue.get_size() == 0:
                for record in self.get_purgeable([division.division_name], None):
                    division.purge_queue.add(record)

    @restful.cron('0,30', '*', '*', '*')
    def enable_portal_short(self):
        for division in self.divisions.values():
            division.pull_queue.enable_short()

    @restful.cron('0', '20', '*', '*')
    def enable_portal_long(self):
        for division in self.divisions.values():
            division.pull_queue.enable_long()

    @restful.menu('Purgable')
    @restful.table(title='Purgable files')
    def get_purgeable(self, args, kwargs):
        return self.query('select file_id, file_path, file_name, file_permissions, modified_dt, division from file where ((file_status_id = %s and local_purge_days is not null) or file_status_id = %s) and GREATEST(ifnull(DATE_ADD(created_dt, INTERVAL local_purge_days DAY),\'0000-00-00 00:00:00\'), ifnull(user_save_till,\'0000-00-00 00:00:00\')) < now() and division = %s', [self.file_status.BACKUP_COMPLETE, self.file_status.RESTORED, args[0]], extras=kwargs, uselimit=False)

    @restful.doc('Modifies a backup_record', public=False)
    @restful.permissions('tape')
    @restful.validate(backup_record_put_validator, [{'name': 'backup_record_id', 'type': int}])
    def put_backuprecord(self, args, kwargs):
        self.smart_modify('backup_record', 'backup_record_id=%s' % args[0], kwargs)
        # we could probably cache this instead of doing it this way
        if kwargs.get('backup_record_status_id', None) == self.backup_record_status.TRANSFER_COMPLETE:
            record = self.query('select * from backup_record b left join file f on f.file_id=b.file_id where backup_record_id=%s', args)[0]
            if record['validate_mode'] == VALIDATION_MODES['Validate_Tape']:
                kwargs['backup_record_status_id'] = self.backup_record_status.WAIT_FOR_TAPE
                self.put_backuprecord(args, kwargs)
            else:
                records = self.query('select backup_record_status_id from backup_record where file_id=%s and backup_record_status_id<>%s',
                                     [record['file_id'], self.backup_record_status.TRANSFER_COMPLETE])
                if len(records) == 0:
                    self.put_file([record['file_id']], {'file_status_id': self.file_status.BACKUP_COMPLETE})
        elif kwargs.get('backup_record_status_id', None) == self.backup_record_status.VALIDATION_COMPLETE:
            records = self.query('select file_status_id,f.file_id, backup_record_id,backup_record_status_id, f.md5sum as file_md5sum, b.md5sum as backup_md5sum from backup_record b left join file f on f.file_id=b.file_id where b.file_id in (select file_id from backup_record where backup_record_id=%s)', args)
            file_id = None
            validation_failed = False
            for record in records:
                file_id = record['file_id']
                if record['backup_record_status_id'] != self.backup_record_status.VALIDATION_COMPLETE:
                    return
                if record['file_md5sum'] != record['backup_md5sum']:
                    # hmm validation failed... this could be because the file was updated and needing replacing
                    validation_failed = True
            if validation_failed and len(self.query('select * from hook where file_id=%s and on_status=8', [record['file_id']])) == 0:
                self.logger.error('validation failed for file %d' % record['file_id'])
            else:
                self.put_file([file_id], {'file_status_id': self.file_status.BACKUP_COMPLETE})

    backup_records_table = {'title': 'Backup records',
                            'table': {'columns': [['backup_record_id', {'title': 'id'}],
                                                  ['service', {}],
                                                  ['remote_file_name', {'title': 'remote name'}],
                                                  ['remote_file_path', {'title': 'remote path'}],
                                                  ['tar_record_id', {}],
                                                  ['backup_record_status_id', {'title': 'status id'}],
                                                  ['md5sum', {'title': 'md5sum'}],
                                                  ['dt_modified', {'title': 'modified'}]],
                                      'sort': {'enabled': False}},
                            'data': {'url': 'backuprecords',
                                     'default_query': 'file_id = {{value}}'}}

    @restful.queryResults(backup_records_table)
    @restful.doc('Returns all the backup records for specifed file_id')
    @restful.validate(argsValidator=[{'name': 'file_id', 'type': int}])
    def get_backuprecords(self, args, kwargs):
        if kwargs and kwargs.get('queryResults', None):
            return self.queryResults_dataChange(kwargs, 'backup_record')
        else:
            return self.query('select * from backup_record where file_id=%s', args)

    @restful.menu('Status>HPSS State')
    @restful.template('hpss_state.html')
    def get_hpss_state(self, _args, _kwargs):
        return [{'id': 1, 'system': 'archive', 'status': self.hsi_state.isup('archive')},
                {'id': 2, 'system': 'hpss', 'status': self.hsi_state.isup('hpss')}]

    @restful.menu('Status>DB States')
    @restful.template('db_states.html')
    def get_db_states(self, _args, _kwargs):
        return self.query('select * from status')

    def get_active_db_states(self, _args, _kwargs):
        return self.query('select * from active')

    def get_pullqueuesummary(self, _args, _kwargs):
        return self.query('select a.queue_status_id as status_id, c.status, a.requestor, count(*) as N, a.priority as priority_queue, sum(b.file_size) / 1e9 as gb from pull_queue a join file b on a.file_id = b.file_id join queue_status_cv c on a.queue_status_id = c.queue_status_id where a.queue_status_id not in (3) group by 1, 2, 3, 5 order by 1, 2, 3, 5')

    def get_tape_volumes(self, _args, _kwargs):
        return self.query('select count(distinct volume) as n from pull_queue a where a.queue_status_id <> 3')

    def get_prep_count(self, _args, _kwargs):
        return self.query('select count(*) as n from pull_queue where queue_status_id = 1 and volume is null')

    def get_restored_in_last_hour(self, _args, _kwargs):
        return self.query('select count(*) as N, a.priority as priority_queue, sum(b.file_size) / 1e9 as gb from pull_queue a join file b on a.file_id = b.file_id where a.queue_status_id = 3 and a.dt_modified > now() - interval 1 hour group by priority_queue order by priority_queue')

    def plot_data(self, data, sets, titles, labels, name):
        """
        Generate a plot of the data and output it as html-wrapped base64 byte-stream

        Args:
            data: An array of dictionaries containing the data to plot, each dictionary should have a 'ymdh' key (year-month-day-hour) in the format of '%y-%m-%d %H'.  Array does not have to be sorted nor do all points needs to be present.
            sets: A Tuple of the keys to plot, each key should be present in the data dictionaries
            titles: A Tuple of the titles for each element of sets, these will be used as the graph title
            labels: A Tuple of the labels for each element of sets, these will be used as the graph labels
            name: The title of the graph set

        Returns:
            A blank string if the number of elements is less than 2, otherwise a html string containing '<h3>name</h3><img src="data:image/png;base64,base64encodedimage"/>'
        """

        # if the graph as 0 or 1 points, return an empty string
        if len(data) < 2:
            return ''

        # convert data into a dictionary where the key is the datetime version of ymdh and the value is a dictionary of the elements listed in 'sets'
        # Create an entry for now, so our graph plots out to the current time.  If there is an element in data, it will just be overwritten.
        d = {datetime.datetime.now().replace(second=0, microsecond=0, minute=0): {_: 0 for _ in sets}}
        for i in data:
            d[datetime.datetime.strptime(i['ymdh'], '%y-%m-%d %H')] = {_: i[_] for _ in sets}

        # create a set of plots, one for each element in sets
        plt.rcParams['figure.autolayout'] = True
        fig = plt.figure(figsize=(6, 10.9))
        plots = [fig.add_subplot(len(sets), 1, 1)]
        for i in range(2, len(sets) + 1):
            plots.append(fig.add_subplot(len(sets), 1, i, sharex=plots[0]))
        plt.xticks(rotation=90)

        # Create the plot
        for plot, field, title, label in zip(plots, sets, titles, labels):
            plot.grid(axis='both', linestyle='-', which='major', visible=True, c='lightgray', zorder=0)
            plot.get_yaxis().set_major_formatter(tkr.FuncFormatter(lambda _, p: format(int(_), ',')))
            if field != sets[-1]:
                plot.tick_params('x', labelbottom=False)
            plot.set_title(title)
            plot.set(ylabel=label)
            x = sorted(d)
            y = [d[_][field] for _ in x]
            plot.bar(x, y, width=0.03, edgecolor='none', zorder=3)
        plot.get_xaxis().set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        # convert the plot to a bytestream and into a format that can be embedded in html
        byte_stream = BytesIO()
        fig.savefig(byte_stream, format='png')
        return f'<h3>{name}</h3><img src="data:image/png;base64,{base64.b64encode(byte_stream.getvalue()).decode("utf-8")}">'

    # INGEST

    def get_ingested_last_five_days_by_hour(self, _args, _kwargs):
        return self.query('select date_format(created_dt, "%y-%m-%d %H") as ymdh, count(*) as N, source, sum(file_size) / 1e9 as gb from file where created_dt > now() - interval 5 day group by ymdh, source order by ymdh desc')

    def get_ingested_last_five_days_by_user(self, _args, _kwargs):
        return self.query('select file_owner, count(*) as N, source, sum(file_size) / 1e9 as gb from file where created_dt > now() - interval 5 day group by file_owner, source order by file_owner', uselimit=False)

    @restful.menu('Status>Ingested Last 5 days')
    @restful.template('ingest_stats.html')
    def get_ingest_stats(self, _args, _kwargs):
        data = {}
        data['hour'] = self.get_ingested_last_five_days_by_hour(None, None)
        data['user'] = self.get_ingested_last_five_days_by_user(None, None)
        data['graph'] = self.plot_data(data['hour'],
                                       ('N', 'gb'),
                                       ('Number of Files', 'File Footprint in GB'),
                                       ('Files', 'GB'),
                                       'Files Published by N Files and Footprint over the Last Five Days')
        return data

    # REQUEST

    def get_requested_last_five_days_by_hour(self, _args, _kwargs):
        return self.query('select date_format(dt_modified, "%y-%m-%d %H") as ymdh, count(*) as N, sum(b.file_size) / 1e9 as gb from request a join file b on a.file_id = b.file_id where dt_modified > now() - interval 5 day group by 1 order by ymdh desc')

    def get_requested_last_five_days_by_user(self, _args, _kwargs):
        return self.query('select a.requestor, count(*) as N, sum(b.file_size) / 1e9 as gb from request a join file b on a.file_id = b.file_id where dt_modified > now() - interval 5 day group by 1 order by count(*) desc', uselimit=False)

    @restful.menu('Status>Requested Last 5 days')
    @restful.template('request_stats.html')
    def get_request_stats(self, _args, _kwargs):
        data = {}
        data['hour'] = self.get_requested_last_five_days_by_hour(None, None)
        data['user'] = self.get_requested_last_five_days_by_user(None, None)
        data['graph'] = self.plot_data(data['hour'],
                                       ('N', 'gb'),
                                       ('Number of Files', 'File Footprint in GB'),
                                       ('Files', 'GB'),
                                       'Files Requested by N Files and Footprint over the Last Five Days')
        return data

    # RESTORE

    def get_restored_last_five_days_by_hour(self, _args, _kwargs):
        return self.query('select date_format(dt_modified, "%y-%m-%d %H") as ymdh, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from pull_queue p use index (pull_queue_status_id_dt_modified) join file f on f.file_id = p.file_id where queue_status_id = 3 and dt_modified > now() - interval 5 day group by ymdh order by ymdh desc')

    def get_restored_last_five_days_by_user(self, _args, _kwargs):
        return self.query('select requestor, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from pull_queue p use index (pull_queue_status_id_dt_modified) join file f on f.file_id = p.file_id where queue_status_id = 3 and dt_modified > now() - interval 5 day group by 1 order by N desc', uselimit=False)

    @restful.menu('Status>Restored Last 5 days')
    @restful.template('restore_stats.html')
    def get_restore_stats(self, _args, _kwargs):
        data = {}
        data['hour'] = self.get_restored_last_five_days_by_hour(None, None)
        data['user'] = self.get_restored_last_five_days_by_user(None, None)
        data['graph'] = self.plot_data(data['hour'],
                                       ('N', 'vol', 'gb'),
                                       ('Number of Files', 'Tape Volumes to Load', 'File Footprint in GB'),
                                       ('Files', 'Volumes', 'GB'),
                                       'Files Restored by N Files, Footprint, and Tape Volumes over the Last Five Days')
        return data

    @restful.cron('0', '*', '*', '*')
    def save_requested_restores(self):
        self.requested_restores = self.query(
            'select date_format(dt_begin, "%y-%m-%d %H") as ymdh, count(distinct volume) as vol, count(*) as N, sum(file_size) / 1e9 as gb from'
            ' (select p.pull_queue_id, p.file_id, min(dt_begin) as dt_begin, volume'
            '   from pull_queue p'
            '   join pull_queue_status_history pqsh on p.pull_queue_id = pqsh.pull_queue_id'
            '   join request r on p.file_id = r.file_id'
            '     and p.requestor = r.requestor'
            '   where p.dt_modified > now() - interval 20 day'
            '     and r.dt_modified > now() - interval 5 day'
            '     and dt_begin > now() - interval 5 day'
            '     and pqsh.queue_status_id = 1'
            '   group by 1, 2'
            ' ) as x join file f on f.file_id = x.file_id group by ymdh order by ymdh desc')

    def get_request_age(self, _args, _kwargs):
        return self.query('select a.priority as priority_queue, min(a.dt_modified) as status_date, min(b.dt_begin) as oldest_date, count(distinct a.pull_queue_id) as n, sum(f.file_size) / 1e9 as gb from pull_queue a join file f on a.file_id = f.file_id join pull_queue_status_history b on a.pull_queue_id = b.pull_queue_id where a.queue_status_id not in (0, 3) group by 1 order by 1')

    def get_pull_error_list(self, _args, _kwargs):
        return self.query('select a.queue_status_id, a.file_id, a.requestor, min(b.dt_begin) as dt_begin, a.volume, c.file_size from pull_queue a join pull_queue_status_history b on a.pull_queue_id = b.pull_queue_id join file c on a.file_id = c.file_id where a.queue_status_id in (4, 6) group by 1, 2, 3, 5, 6 order by 1')

    def get_footprint(self, _args, _kwargs):
        # This call is used by portal to determine if it has enough room to send restore requests.
        # they used bytes_free (and subtract 25TB) to determine if they can proceed
        return {'disk_reserve': self.disk_usage.get('disk_reserve', 0) / 1e12,
                'files': self.disk_usage.get('files', 0),
                'disk_usage_files': self.disk_usage.get('disk_usage_files', 0) / 1e12,
                'files_restoring': self.disk_usage.get('files_restoring', 0),
                'disk_usage_files_restoring': self.disk_usage.get('disk_usage_files_restoring', 0) / 1e12,
                'dna_free': self.disk_usage.get('bytes_free', 0) / 1e12,
                'dna_stats_updated': self.disk_usage.get('date_updated', 0),
                'dna_used': self.disk_usage.get('bytes_used', 0) / 1e12,
                'disk_usage_other': self.disk_usage.get('disk_usage_other', 0) / 1e12}

    @restful.menu('status>Tape Restore Queue Detail')
    @restful.template('restore_queue.html')
    def get_pullqueue(self, _args, kwargs):
        return self.query('select pull_queue_id as queue_id, file_name, q.dt_modified as date_modified, q.priority as priority_queue, status, file_size, requestor from pull_queue q left join queue_status_cv c on c.queue_status_id=q.queue_status_id left join file f on f.file_id=q.file_id where q.queue_status_id<>3 order by status, priority_queue, date_modified', extras=kwargs, uselimit=False)

    def get_active_restores(self, _args, _kwargs):
        return self.query('select pull_queue_id, file_name, timediff(now(), q.dt_modified) as time_running, q.priority as priority_queue, file_size, q.volume, requestor from pull_queue q left join queue_status_cv c on c.queue_status_id=q.queue_status_id left join file f on f.file_id=q.file_id where q.queue_status_id=2 order by time_running desc', uselimit=False)

    @restful.menu('status>Tape Restore Queue Summary')
    @restful.template('tape_status.html')
    def get_status(self, args, kwargs):
        return {'active_db_states': self.get_active_db_states(args, kwargs),
                'active_restores': self.get_active_restores(args, kwargs),
                'footprint': self.get_footprint(args, kwargs),
                'in_queue': self.get_pullqueuesummary(args, kwargs),
                'prep_count': self.get_prep_count(args, kwargs),
                'pull_errors': self.get_pull_error_list(args, kwargs),
                'request_age': self.get_request_age(args, kwargs),
                'restored_in_last_hour': self.get_restored_in_last_hour(args, kwargs),
                'tape_volumes': self.get_tape_volumes(args, kwargs)}

    file_validator = {
        'file': {'type': str},
        'backup_services': {'type': list, 'required': False, 'validator': {'*': {'type': int}}, 'example': [1],
                            'doc': 'What archivers to send this file to.<br>1: archive.nersc.gov<br>2: hpss.nersc.gov.<br> The default just [1]'},
        'destination': {'type': str, 'required': False,
                        'doc': 'Used by metadata'},
        'put_mode': {'type': (int, str), 'required': False,
                     'doc': 'Pass in either:<br>Default: will not replace if it is already on tape, <br>Replace_If_Newer: will replace the record on tape if this file is newer<br>Replace_Force: will replace previous backups.<br> Default is Default'},
        'validate_mode': {'type': (str, int), 'required': False,
                          'doc': 'Pass in either:<br>Generate_MD5: will md5 this file and not create md5\'s for the tape records.<br>Validate_Tape: Will generate a md5 for the file and for each file on tape and validate they match.<br>No_MD5: Don\'t generate any md5s and dont\'t validate.<br> default is Validate_Tape'},
        'local_purge_days': {'type': int, 'required': False, 'default': 270,
                             'doc': 'The number of days that you want this file to stay around for.<br> If 0 is passed this file will be purged right after it has been archived.'},
        'transfer_mode': {'type': int, 'required': False,
                          'doc': 'Pass in either:<br>Copy: will copy the file to the destination.<br>Move: Copy the file to the destination and then remove the local version if access is permitted.<br> default is Copy'}
    }

    @restful.permissions('tape')
    def put_md5(self, args, kwargs):
        md5_id = int(args[0])
        self.modify('update md5_queue set queue_status_id=%d where md5_queue_id=%d' % (kwargs['queue_status_id'], md5_id))
        if kwargs['queue_status_id'] == self.queue_status.COMPLETE:
            self.modify('update md5_queue set md5sum="%s", queue_status_id=%d where md5_queue_id=%d' % (kwargs['md5sum'], kwargs['queue_status_id'], md5_id))
            record = self.query('select * from md5_queue where md5_queue_id=%d' % md5_id)[0]
            callback = record['callback']
            if callback.startswith('local://'):
                hook = callback.replace('local://', '')
                method, args = hook.split('/', 1)
                args = args.split('/')
                restful.run_internal('tape', method, *args, md5sum=kwargs['md5sum'])

    @restful.permissions('tape')
    def post_md5(self, _args, kwargs):
        _id = self.smart_insert('md5_queue', kwargs)
        kwargs['md5_queue_id'] = _id
        self.divisions.get(kwargs.get('division')).md5_queue.add(kwargs)

    @restful.permissions('tape_file')
    def post_hpssfile(self, _args, kwargs):
        hpss_file = kwargs['file']
        file_path, file_name = os.path.split(hpss_file)
        dfile_path, dfile_name = os.path.split(kwargs['destination'])
        to_status = self.file_status.PURGED
        try:
            info = self.getHpssFileInfo(hpss_file)
        except Exception:
            raise common.HttpException(400, 'Sorry the file %s does not exist on hpss' % hpss_file)
        file_size = int(info[3])
        data = {'file_owner': info[1], 'file_group': info[2], 'file_size': file_size, 'file_date': str(
            info[4]), 'created_dt': 'now()', 'file_status_id': self.file_status.PURGED, 'file_name': dfile_name, 'file_permissions': 0o100664, 'file_path': dfile_path}
        file_id = self.smart_insert('file', data)
        if file_id is None:
            temp_rows = self.query('select file_id,metadata_id from file where file_name=%s and file_path=%s and file_size=%s and file_date=%s',
                                   [data['file_name'], data['file_path'], data['file_size'], data['file_date']])
            if len(temp_rows) > 0:
                return {'file_id': temp_rows[0]['file_id'], 'status': 'old', 'metadata_id': temp_rows[0]['metadata_id']}
            else:
                self.logger.error('File failed to be created: %s' % str(data))
                raise common.HttpException(500, 'Sorry for some reason we failed to insert your record into the database, please try again')
        else:
            self.post_backuprecord(None, {'service': 1, 'file_id': file_id, 'remote_file_path': file_path, 'remote_file_name': file_name,
                                          'backup_record_status_id': self.backup_record_status.TRANSFER_COMPLETE})
            return {'file_id': file_id, 'status': 'new', 'file_status': self.cv['file_status'][str(to_status)], 'file_status_id': to_status}

    @restful.permissions('tape_file')
    @restful.doc('Registers a file to be backed up to tape.<br> If staging_path is provided, the temp_path will become the real path of the file and then it will be copied to the staging_path which is file_path in the database', {'file_id': {'type': int}, 'status': {'type': str}})
    @restful.validate(file_validator)
    @restful.passreq
    def post_file(self, _args, kwargs):
        data = {}
        data['_file'] = kwargs.get('file')
        data['file_size'] = 0
        data['_callback'] = kwargs.get('callback', 'file_ingest')

        # passwed by metadata
        data['_call_source'] = kwargs.get('call_source', None)

        put_mode = 0
        if 'put_mode' in kwargs:
            mode = kwargs.get('put_mode')
            if isinstance(mode, int):
                put_mode = mode
            elif mode in PUT_MODES:
                put_mode = PUT_MODES[mode]
            else:
                raise common.HttpException(400, f'You have input an incorrect put mode: {mode}')
        data['_put_mode'] = put_mode

        if 'local_purge_days' in kwargs:
            data['local_purge_days'] = max(2, kwargs.get('local_purge_days'))

        # Do some validation first: check if we can access the file and backup services exist
        if 'auto_uncompress' in kwargs:
            data['auto_uncompress'] = kwargs.get('auto_uncompress')

        t_mode = 0
        if 'transfer_mode' in kwargs:
            t_mode = kwargs.get('transfer_mode')
            if not isinstance(t_mode, int):
                if t_mode not in TRANSFER_MODES:
                    raise common.ValidationError('You have passed in an invalid transfer_mode')
                t_mode = TRANSFER_MODES[t_mode]
            if t_mode == 1:
                t_mode = 0
        data['transfer_mode'] = t_mode

        val_mode = 0
        if 'validate_mode' in kwargs:
            val_mode = kwargs.get('validate_mode')
            if isinstance(val_mode, int):
                pass
            elif val_mode in VALIDATION_MODES:
                val_mode = VALIDATION_MODES[val_mode]
            else:
                raise common.HttpException(400, f'The validate mode provided: "{val_mode}" does not exist.')
            if val_mode == 1:
                val_mode = 0
        data['validate_mode'] = val_mode

        division_name = kwargs.get('__auth').get('division')
        # Set the default backup services, overridden below if user provided a value
        services = [self.default_backup_services.get(division_name)]
        if kwargs.get('backup_services', None):
            services = kwargs.get('backup_services')
            for service in services:
                if not self.validate_backup_service(service, division_name):
                    raise common.HttpException(400,
                                               f'You have chosen an invalid backup service {service} for division {division_name}')
        data['_services'] = json.dumps(services)

        # if destination is defined then the file will be in the managed repository (e.g., metadata has called us)
        # At this point we don't know if this is a file or a directory, we are going to trust metadata was called correctly
        # There may be an issue if we are expecting a file or folder and find the other type instead
        # In general a path+filename 'signature' should be unique.  Once we move to more data centers, this may be false
        if kwargs.get('destination', None) and kwargs.get('destination').startswith(
                self.dm_archive_roots.get(division_name)):
            data['_destination'] = kwargs.get('destination')
            file_path, file_name = os.path.split(data.get('_destination'))
        else:
            file_path, file_name = os.path.split(data.get('_file'))
        results = self.query(
            'select * from file where file_name in (%s, %s) and file_path=%s order by file_date desc limit 1',
            [file_name, file_name + '.tar', file_path])
        data['file_path'], data['file_name'] = file_path, file_name
        data['source'] = kwargs.get('source', None)
        data['division'] = division_name

        has_record = False
        data['file_id'] = None
        if len(results) > 0:
            for rec in results:
                if data.get('_call_source') != 'folder' and not rec.get('file_name').endswith('.tar'):
                    has_record = True
                    break
                if data.get('_call_source') == 'folder' and rec.get('file_name').endswith('.tar'):
                    has_record = True
                    break

            if has_record:
                data['file_id'] = rec.get('file_id')
                data['metadata_id'] = rec.get('metadata_id')

            if put_mode in (PUT_MODES.get('Replace_If_Newer'), PUT_MODES.get('Replace_Force')):
                # we actually don't know if this is replaced or not, if the newer file is older,
                # then this should be old.  If replace force is provided and the file doesn't exist
                # then an update shouldn't happen either.  So in both cases we should delay the outcome
                # until we know what is going on.   The main concern is that we don't overwrite info if the
                # outcome is to keep the previous record.
                data['_status'] = 'delayed'
            else:
                data['_status'] = 'old'
        else:
            data['_status'] = 'new'

        # Check to see if we are getting multiple requests from the user
        with self.ingest_lock:
            if data.get('_destination', None):
                results = self.query(
                    'select * from file_ingest where _file=%s and _destination=%s and file_ingest_status_id=1 limit 1',
                    [data.get('_file'), data.get('_destination')])
            else:
                results = self.query('select * from file_ingest where _file=%s and file_ingest_status_id=1 limit 1',
                                     [data.get('_file')])
            ret = {'file_id': data.get('file_id'), 'metadata_id': data.get('metadata_id', None), 'file_path': file_path,
                   'file_name': file_name}
            if len(results) > 0:
                data['file_ingest_id'] = results[0].get('file_ingest_id')
                self.smart_modify('file_ingest', f'file_ingest_id={data.get("file_ingest_id")}', data)
                file_ingest_id = data.get('file_ingest_id')
            else:
                file_ingest_id = self.smart_insert('file_ingest', data)
                file_ingest = self.query('select * from file_ingest where file_ingest_id=%s', [file_ingest_id])[0]
                self._add_to_queue_by_feature(self.divisions.get(division_name).ingest_queue, file_ingest.get('_file'),
                                              file_ingest)
        ret['status'] = data.get('_status')
        ret['file_ingest_id'] = file_ingest_id

        return ret

    # an update function for metadata to call
    @restful.doc('Updates the file_ingest record for the specified file_ingest_id', public=False)
    @restful.permissions('tape')
    def put_file_ingest(self, args, kwargs):
        self.smart_modify('file_ingest', 'file_ingest_id=%s' % args[0], kwargs)

    @restful.doc('Completes the file_ingest work for the specified file_ingest_id', public=False)
    @restful.permissions('tape')
    @restful.validate({'file_ingest_status_id': {'type': int, 'required': False}},
                      [{'name': 'file_ingest_id', 'type': int}])
    def post_file_ingest(self, args, kwargs):
        file_ingest_id = int(args[0])
        # self.logger.info('post_file_ingest - ingest id: %d - start post processing' % file_ingest_id) # XXX
        self.smart_modify('file_ingest', f'file_ingest_id = {file_ingest_id}', kwargs)
        record = self.query('select * from file_ingest where file_ingest_id = %s', file_ingest_id)
        if len(record) > 0:
            record = record[0]
        else:
            self.logger.error(f'post_file_ingest - file_ingest record failed to be found: {file_ingest_id}')
            return
        file_id = record.get('file_id')
        metadata_id = record.get('metadata_id')
        status = self.cv.get('file_status')[str(record.get('file_ingest_status_id'))]
        if record.get('file_ingest_status_id') == self.file_status.INGEST_STATS_COMPLETE:
            # a folder has been selected to write to the repository, add a .tar to the name
            # doing this now as we need to search tape by the new name
            if record.get('_destination') is not None and record.get('_is_folder'):
                FILE_EXT = '.tar'
            else:
                FILE_EXT = ''

            # Find the tape entry if it exists
            if record.get('file_id'):
                # user provided the original file_id
                data = self.query('select * from file where file_id = %s', [record.get('file_id')])
            else:
                # else try to find the existing file
                if record.get('_destination'):
                    # repository name was given, data likely from metadata
                    path, file = os.path.split(record.get('_destination'))
                else:
                    # search by original file name
                    path, file = record.get('file_path'), record.get('file_name')
                data = self.query('select * from file where file_name = %s and file_path = %s', [file + FILE_EXT, path])

            # see if we are replacing an existing file
            if len(data) > 0:
                data = data[0]
                record_date = data.get('file_date')
                #  set db and file resolutions down to the second,  if the existing file is newer and the user has not passed Replaced_Force
                if record_date.replace(microsecond=0) >= record.get('file_date').replace(microsecond=0) and record.get('_put_mode') != 3:
                    # self.logger.info('post_file_ingest - ingest id: %d - not replacing file' % file_ingest_id)  # XXX
                    self.smart_modify('file_ingest', f'file_ingest_id = {file_ingest_id}',
                                      {'file_ingest_status_id': self.file_status.INGEST_COMPLETE, '_status': 'old',
                                       'file_id': data.get('file_id')})
                    return {'file_ingest_id': file_ingest_id, 'file_id': data.get('file_id'), 'status': 'old',
                            'metadata_id': data.get('metadata_id')}
            else:
                data = {}

            # copy over the file ingest info to data
            data.update({x: record[x] for x in record if
                         x.startswith('_') is False and record[x] is not None and 'ingest' not in x})

            if record.get('_destination') is not None:
                # We have a place in the repository, momve path and name to origin
                data['origin_file_path'], data['origin_file_name'] = data.get('file_path'), data.get('file_name')
                temp_path = record.get('_destination')

                # metadata records should have done this already
                if record.get('_is_folder') and temp_path.endswith('/'):
                    temp_path = os.path.join(temp_path, record.get('file_name'))
                data['file_path'], data['file_name'] = os.path.split(temp_path)
                if record.get('_is_folder'):
                    data['file_name'] += '.tar'

            cur_date = datetime.datetime.now()
            if data.get('file_id', None) is None:
                record['_status'] = status = 'new'
                data['created_dt'] = cur_date
                data['modified_dt'] = cur_date
                file_id = self.smart_insert('file', data)
                backup_records = []
                for service in json.loads(record.get('_services')):
                    backup_record_id = self.post_backuprecord(None, {'service': service, 'file_id': file_id})
                    backup_records.append({'backup_record_id': backup_record_id, 'service': service})
            else:
                record['_status'] = status = 'replaced'
                file_id = data.get('file_id')
                data['modified_dt'] = cur_date
                self.smart_modify('file', f'file_id = {file_id}', data)

                # TODO: Remove this once we're requiring `source` to be explicitly passed (and properly defined) --
                #  this is to allow null values
                if record.get('source') is None:
                    self.modify('update file set source = NULL where file_id = %s', file_id)

                # Since the file is being replaced, we want to remove any pending restore requests to prevent the
                # restored file from overwriting the updated file.
                self.divisions.get(data.get('division')).pull_queue.delete_pending_tasks_for_file(file_id)

            # save off the file_id and modification times to pass back to metadata
            record['file_id'] = file_id
            record['created_dt'] = data.get('created_dt')
            record['modified_dt'] = data.get('modified_dt')
            record['file_path'] = data.get('file_path')
            record['file_name'] = data.get('file_name')

            # set the state of the record, put_file will woory about adding work to the queues
            skip_delay = 0
            if record.get('_is_folder'):
                to_status = self.file_status.TAR_READY
            else:
                self.modify(
                    'update backup_record set backup_record_status_id = %s, tar_record_id = NULL where file_id = %s',
                    self.backup_record_status.REGISTERED, file_id)
                if data.get('origin_file_name', None) and data.get('local_purge_days') > 0:
                    # jamo record, copy to the archive
                    to_status = self.file_status.COPY_READY
                else:
                    # tape record, send to tape
                    to_status = self.file_status.BACKUP_READY
                    # we don't want tape records held, just metadata records
                    skip_delay = 1
            record['file_status_id'] = to_status
            record['file_status'] = self.cv.get('file_status')[str(to_status)]

            # prepare to update the file ingest record
            update = {'file_ingest_status_id': self.file_status.INGEST_COMPLETE, 'file_id': file_id, '_status': status}
            # Update metadata
            # self.logger.info('post_file_ingest - ingest id: %d, metadata_id: %s - start update' % (file_ingest_id, str(data.get('metadata_id', None))))  # XXX

            if data.get('metadata_id', None):
                update['metadata_id'] = metadata_id = data.get('metadata_id')

                try:
                    # self.logger.info('tape.post_file_ingest - ingest id: %d, metadata_id: %s - call metadata' % (file_ingest_id, str(data.get('metadata_id', None))))  # XXX
                    restful.run_internal('metadata', 'post_file_ingest', **record)
                except Exception as e:
                    # self.logger.info('post_file_ingest - ingest id: %d, metadata_id: %s - call metadata failed' % (file_ingest_id, str(data.get('metadata_id', None))))  # XXX
                    update['file_ingest_status_id'] = to_status = self.file_status.INGEST_FAILED
                    self.logger.error(
                        f'Update of metadata ingest record {record.get("_metadata_ingest_id")} failed: {str(e)}')

            # update the file state, will push the file to the next task to be worked on
            self.put_file([file_id], {'skip_delay': skip_delay, 'file_status_id': to_status})

            # update the file_ingest record
            self.smart_modify('file_ingest', f'file_ingest_id = {file_ingest_id}', update)

        return {'file_ingest_id': file_ingest_id, 'file_id': file_id, 'status': status, 'metadata_id': metadata_id}

    @restful.doc('Retries to update the metadata record for a file_ingest_id', public=False)
    @restful.permissions('admin')
    @restful.validate(argsValidator=[{'name': 'file_ingest_id', 'type': int}])
    def put_file_ingest_retry(self, args, _kwargs):
        file_ingest_id = int(args[0])
        record = self.query('select * from file_ingest where file_ingest_id=%s', file_ingest_id)
        if len(record):
            record = record[0]

            if record.get('file_ingest_status_id', None) != self.file_status.INGEST_COMPLETE:
                return {'error': 'file_ingest_status_id not in a complete state'}

            if record['metadata_id'] and record['_metadata_ingest_id'] and record['_callback'] == 'file_ingest' and record['file_id']:
                data = self.query('select * from file where file_id=%s', [record['file_id']])
                if len(data):
                    for key in ('created_dt', 'modified_dt', 'file_path', 'file_name', 'file_status_id'):
                        record[key] = data[0][key]
                    record['file_status'] = self.cv['file_status'][str(data[0]['file_status_id'])]
                    try:
                        return restful.run_internal('metadata', 'post_file_ingest', **record)
                    except Exception as e:
                        return {'error': 'Update failed with exception %s' % str(e)}
                else:
                    return {'error': 'file record not found for file_id %d' % record.get('file_id', None)}
            else:
                return {'error': 'Record %d is missing metadata_id, _metadata_ingest_id, file_id or has an incorrect _callback' % file_ingest_id}
        else:
            return {'error': 'No record found'}

    @restful.permissions('admin')
    @restful.validate({'src': {'type': str}, 'dest': {'type': 'oid'}}, allowExtra=True)
    def post_replacefile(self, _args, kwargs):
        src, replaces = kwargs.get('src'), kwargs.get('dest')

        record = self.query('select file_id, file_path, file_name, division from file where metadata_id = %s',
                            [str(replaces)])
        if len(record) == 0:
            raise common.ValidationError(f'The destination provided: "{replaces}" does not exist.')
        data = {'file_id': record[0].get('file_id'),
                'metadata_id': str(replaces),
                'division': record[0].get('division'),
                '_file': src,
                '_destination': os.path.join(record[0].get('file_path'), record[0].get('file_name')),
                '_put_mode': PUT_MODES.get('Replace_Force'),
                '_callback': 'replacefile2',
                '_status': 'new'
                }
        if kwargs.get('source'):
            data['source'] = kwargs.get('source')

        ret = {'file_ingest_id': self.smart_insert('file_ingest', data), 'file_id': data.get('file_id')}
        file_ingest = self.query('select * from file_ingest where file_ingest_id=%s', [ret.get('file_ingest_id')])[0]
        self._add_to_queue_by_feature(self.divisions.get(file_ingest.get('division')).ingest_queue,
                                      file_ingest.get('_file'), file_ingest)

    ingest_remove = {'file': ('origin_file_name', 'origin_file_path', 'auto_uncompress', 'metadata_id'),
                     'folder': ('auto_uncompress', 'metadata_id')}

    # Called by ingest callback, set in post_replace file above
    @restful.permissions('admin')
    @restful.validate({'file_ingest_status_id': {'type': int, 'required': False}},
                      [{'name': 'file_ingest_id', 'type': int}])
    def post_replacefile2(self, args, kwargs):
        file_ingest_id = int(args[0])
        self.smart_modify('file_ingest', f'file_ingest_id = {file_ingest_id}', kwargs)
        record = self.query(f'select * from file_ingest where file_ingest_id = {file_ingest_id}')
        if len(record) > 0:
            record = record[0]
        else:
            self.logger.error(f'post_replace2 - File_ingest record failed to be found: {file_ingest_id}')
            return

        if record.get('file_ingest_status_id') == self.file_status.INGEST_STATS_COMPLETE:
            file_path, file_name = os.path.split(record.get('_destination'))
            file_id = record.get('file_id')
            data = {x: record.get(x) for x in record if
                    x.startswith('_') is False and record.get(x) is not None and 'ingest' not in x}
            # Switch these around, ingest doesn't know what our intent was
            data['origin_file_path'] = data.get('file_path')
            data['origin_file_name'] = data.get('file_name')
            data['file_path'] = file_path
            data['file_name'] = file_name
            data['file_status_id'] = self.file_status.COPY_READY

            # update the tape database
            self.smart_modify('file', f'metadata_id = "{data.get("metadata_id")}"', data)
            self.modify('update backup_record set backup_record_status_id = %s where file_id = %s',
                        self.backup_record_status.REGISTERED, file_id)

            # TODO: Remove this once we're requiring `source` to be explicitly passed (and properly defined) --
            #  this is to allow null values
            if record.get('source') is None:
                self.modify('update file set source = NULL where file_id = %s', file_id)

            # update metadata, first drop the fields as we don't want them in metadata
            if record.get('_call_source'):
                for field in Tape.ingest_remove.get(record.get('_call_source')):
                    data.pop(field, None)

            restful.run_internal('metadata', 'add_update', {'file_id': file_id}, data)
            file = self.get_file([file_id], None)
            self._add_to_queue_by_feature(self.divisions.get(file.get('division')).copy_queue,
                                          file.get('origin_file_path'), file)
            self.post_md5(None, {'file_path': record.get('_destination'), 'file_size': data.get('file_size'),
                                 'callback': f'local://put_file/{file_id}', 'division': record.get('division')})

            # Since the file is being replaced, we want to remove any pending restore requests to prevent the
            # restored file from overwriting the updated file.
            self.divisions.get(file.get('division')).pull_queue.delete_pending_tasks_for_file(file_id)

            # close out our ingest state
            update = {'file_ingest_status_id': self.file_status.INGEST_COMPLETE, '_status': 'replaced'}
            self.smart_modify('file_ingest', 'file_ingest_id=%s' % file_ingest_id, update)

    @restful.permissions('admin')
    @restful.validate(argsValidator=[{'name': 'file_id', 'type': int}])
    def post_generatemd5(self, args, _kwargs):
        file_id = args[0]
        rec = self.query('select * from file where file_id=%s', file_id)
        if len(rec):
            rec = rec[0]
            self.post_md5(None,
                          {'file_path': os.path.join(rec.get('file_path'), rec.get('file_name')),
                           'file_size': rec.get('file_size'), 'callback': f'local://put_file/{file_id}',
                           'division': rec.get('division')})

    def put_file_status(self, args, _kwargs):
        args = list(map(int, args))
        self.modify('update file set file_status_id=%s where file_id=%s', args[0], args[1])
        if args[0] == self.file_status.BACKUP_COMPLETE:
            self.modify('update backup_record set backup_record_status_id=%s where file_id=%s', self.backup_record_status.REGISTERED, args[1])

    ''''@restful.queryResults({'title': 'status history',
                           'table': {'columns':[['file_status_history_id',{'title':'Id'}],
                                            ['file_status_id',{'title':'Status Id'}],
                                            ['dt_begin',{'title':'Begin'}],
                                            ['dt_end',{'title':'End'}]],
                                 'sort':{'enabled':False}},
                           'data': {'id_field':'file_status_history_id',
                                    'default_query': 'file_id = {{value}}'}})
    @restful.validate(argsValidator=[{'name':'file_id','type':int}])'''
    @restful.table(title='status history')
    def get_filehistory(self, args, _kwargs):
        return self.query('select * from file_status_history where file_id=%s', args)

    @restful.queryResults({'title': 'files',
                           'table': {'columns': [['file_id', {'title': 'id', 'type': 'link',
                                                              'inputs': {'text': '{{file_id}}',
                                                                         'title': 'File Id: {{file_id}}',
                                                                         'url': '/tape/file/{{file_id}}'}}],
                                                 ['metadata_id', {'type': 'link',
                                                                  'inputs': {'text': '{{metadata_id}}',
                                                                             'title': 'Metadata Id: {{metadata_id}}',
                                                                             'url': '/metadata/file/{{metadata_id}}'}}],
                                                 ['file_name', {'title': 'name'}],
                                                 ['origin_file_name', {}],
                                                 ['file_path', {'title': 'path'}],
                                                 ['origin_file_path', {}],
                                                 ['validate_mode', {'type': 'bool'}],
                                                 ['user_save_till', {}],
                                                 ['file_permissions', {'title': 'permissions'}],
                                                 ['file_size', {'title': 'size', 'type': 'number'}],
                                                 ['file_group', {'title': 'group'}],
                                                 ['auto_uncompress', {'type': 'bool'}],
                                                 ['file_owner', {'title': 'owner'}],
                                                 ['file_date', {}],
                                                 ['created_dt', {'title': 'created'}],
                                                 ['modified_dt', {'title': 'modified'}],
                                                 ['file_status_id', {'title': 'status id', 'type': 'number'}],
                                                 ['md5sum', {}],
                                                 ['transfer_mode', {'type': 'number'}],
                                                 ['local_purge_days', {'type': 'number'}],
                                                 ['transaction_id', {'type': 'number'}]],
                                     'sort': {'enabled': True, 'default': {'column': 'file_id', 'direction': 'desc'}}},
                           'data': {'id_field': 'file_id',
                                    'default_query': 'file_name like "{{value}}%"'}})
    @restful.menu('files')
    @restful.doc('Returns all file records')
    def get_files(self, args, kwargs):
        if kwargs and kwargs.get('queryResults', None):
            return self.queryResults_dataChange(kwargs, 'file')
        elif len(args) > 0:
            return self.query('select f.*, c.status as file_status, STR_TO_DATE(GREATEST(ifnull(date_add(created_dt, interval local_purge_days day),"0000-00-00 00:00:00"),ifnull(user_save_till,"0000-00-00 00:00:00")),"%%Y-%%m-%%d %%T") as dt_to_purge from file f left join file_status_cv c on c.file_status_id=f.file_status_id  where file_name=%s', args, kwargs)

        return self.query('select * from file', extras=kwargs)

    @restful.validate({'file': {'type': str}})
    @restful.single
    def get_latestfile(self, _args, kwargs):
        file_path, file_name = os.path.split(kwargs['file'])
        return self.query('select * from file where (file_name=%s and file_path=%s) or (origin_file_name=%s and origin_file_path=%s) order by file_id desc limit 1', [file_name, file_path, file_name, file_path])

    @restful.doc('Returns the file information for the requested file_id')
    @restful.generatedhtml(title='File #{{value}}')
    @restful.link(get_filehistory, 'file_id', 'status_history')
    @restful.table_link(get_backuprecords, 'file_id', backup_records_table, 'backup_records')
    @restful.validate(argsValidator=[{'name': 'file_id', 'type': int}])
    @restful.single
    def get_file(self, args, _kwargs):
        return self.query('select f.*, c.status as file_status, STR_TO_DATE(GREATEST(ifnull(date_add(created_dt, interval local_purge_days day),\'0000-00-00 00:00:00\'),ifnull(user_save_till,\'0000-00-00 00:00:00\')),\'%%Y-%%m-%%d %%T\') as dt_to_purge from file f left join file_status_cv c on c.file_status_id=f.file_status_id where file_id = %s', args)

    service_validator = {
        'available_threads': {'type': int, 'required': False},
        'started_dt': {'type': str, 'required': False},
        'hostname': {'type': str, 'required': False},
    }

    @restful.doc('Saves a service record that will be used to modify a service while it is running', public=False)
    @restful.permissions('tape')
    @restful.validate(service_validator | {'division': {'type': str}})
    def post_service(self, _args, kwargs):
        division_name = kwargs.pop('division')
        _id = self.smart_insert('service', kwargs)
        self.divisions.get(division_name).task_manager.add_service(_id, kwargs.get('available_threads'),
                                                                   kwargs.get('hostname'))
        return {'service_id': _id}

    @restful.doc('Updated a service record for the given service_id', public=False)
    @restful.validate(service_validator, [{'name': 'service_id', 'type': int}])
    @restful.permissions('tape')
    def put_service(self, args, kwargs):
        self.smart_modify('service', 'service_id=%d' % int(args[0]), kwargs)

    @restful.single
    @restful.validate(argsValidator=[{'name': 'service_id', 'type': int}])
    def get_service(self, args, _kwargs):
        return self.query('select * from service where service_id=%s', args)

    @restful.permissions('admin')
    @restful.menu('Services')
    @restful.table(title='services', sort='service_id DESC')
    def get_services(self, _args, kwargs):
        return self.query('select * from service', extras=kwargs)

    def get_currentservices(self, _args, _kwargs):
        # not called directly, so we'll not worry about DST issues
        return self.query('select * from service where ended_dt is null and (last_heartbeat is not null and last_heartbeat>date_sub(now(), interval 5 minute) or (last_heartbeat is null and submited_dt> date_sub(now(), interval 5 hour)))')

    @restful.single
    def get_tarrecord(self, args, _kwargs):
        return self.query('select * from tar_record record where tar_record_id=%s', args)

    @restful.permissions('tape')
    def put_tar(self, args, kwargs):
        return self.smart_modify('tar_record', 'tar_record_id=%d' % int(args[0]), kwargs)

    @restful.doc('Modifies the file for the specified file_id', public=False)
    @restful.permissions('tape')
    @restful.validate({'file_status_id': {'type': int, 'required': False},
                       'md5sum': {'type': str, 'required': False},
                       'skip_delay': {'type': int, 'required': False}
                       },
                      [{'name': 'file_id', 'type': int}])
    def put_file(self, args, kwargs):
        file_id = int(args[0])
        next_status = None
        if 'next_status' in kwargs:
            next_status = kwargs.get('next_status')
            del kwargs['next_status']
        if 'skip_delay' in kwargs:
            delay = 0
            del kwargs['skip_delay']
        else:
            delay = 24
        ret = self.smart_modify('file', 'file_id=%s' % args[0], kwargs)
        if 'file_status_id' in kwargs:
            file_status_id = kwargs.get('file_status_id')
            if file_status_id == self.file_status.COPY_READY:
                file = self.get_file(args, None)
                self._add_to_queue_by_feature(self.divisions.get(file.get('division')).copy_queue,
                                              file.get('origin_file_path'), file)
            elif file_status_id == self.file_status.TAR_READY:
                file = self.get_file(args, None)
                self._add_to_queue_by_feature(self.divisions.get(file.get('division')).tar_queue,
                                              self._get_tar_record_path(file), file)
            restful.run_internal('metadata', 'add_update', {'file_id': file_id},
                                 {'file_status_id': kwargs.get('file_status_id'),
                                  'file_status': self.cv.get('file_status')[str(kwargs.get('file_status_id'))]})
        elif 'md5sum' in kwargs:
            restful.run_internal('metadata', 'add_update', {'file_id': file_id}, {'md5sum': kwargs.get('md5sum')})

        if kwargs.get('file_status_id', None) == self.file_status.BACKUP_READY:
            records = self.query(
                'select f.origin_file_path, f.origin_file_name, local_purge_days, f.file_id, file_size, validate_mode, backup_record_id, service, file_name, file_path, f.md5sum, division from backup_record b left join file f on f.file_id = b.file_id where b.file_id = %s and backup_record_status_id in (%s,%s)',
                [int(file_id), self.backup_record_status.REGISTERED, self.backup_record_status.TRANSFER_READY])
            if len(records) > 0:
                rec = records[0]
                if rec.get('local_purge_days') == 0 and rec.get('origin_file_path') is not None:
                    rec['file_path'] = rec.get('origin_file_path')
                    rec['file_name'] = rec.get('origin_file_name')
                # replaced files will have a md5sum from the first go around.  This might be overkill, but ignore any previously
                # calculated md5sum and redo the calculation
                # if rec['md5sum'] is None and rec['validate_mode']!= VALIDATION_MODES['No_MD5'] :
                if rec.get('validate_mode') != VALIDATION_MODES.get('No_MD5'):
                    self.post_md5(None, {'file_path': os.path.join(rec.get('file_path'), rec.get('file_name')),
                                         'file_size': rec.get('file_size'),
                                         'callback': f'local://put_file/{rec.get("file_id")}',
                                         'division': rec.get('division')})
                # Really should sort out the potential DST bug, but if we get a bad date, just set it to the next midnight
                try:
                    self.modify(
                        'update backup_record set backup_record_status_id = %s, dt_to_release=(now() + INTERVAL %s HOUR) where file_id = %s and backup_record_status_id in (%s,%s)',
                        self.backup_record_status.HOLD, delay, int(file_id), self.backup_record_status.REGISTERED,
                        self.backup_record_status.TRANSFER_READY)
                except Exception:
                    self.modify(
                        'update backup_record set backup_record_status_id = %s, dt_to_release = (curdate() + INTERVAL %s HOUR) where file_id = %s and backup_record_status_id in (%s,%s)',
                        self.backup_record_status.HOLD, delay, int(file_id), self.backup_record_status.REGISTERED,
                        self.backup_record_status.TRANSFER_READY)

        file_status_id = kwargs.get('file_status_id', None)
        if file_status_id == self.file_status.COPY_COMPLETE:
            self.put_file([file_id], {'file_status_id': self.file_status.BACKUP_READY})
        elif file_status_id == self.file_status.TAR_COMPLETE:
            self.put_file([file_id],
                          {'file_status_id': self.file_status.COPY_READY if next_status is None else next_status})

        return ret

    def md5_selected(self, record):
        self.modify('update md5_queue set queue_status_id=%s where md5_queue_id=%s', self.queue_status.IN_PROGRESS, record['md5_queue_id'])
        return {}

    def tar_selected(self, record):
        self.modify('update file set file_status_id=%s where file_id=%s', self.file_status.TAR_IN_PROGRESS, record['file_id'])
        # Secondary read/write race condition causes delete if write doesn't get to secondaries, so we are going to retry a
        # few times before giving up.  Ideally this run_internal should point ot the primaries
        try_count = 3
        while try_count > 0:
            metadata_records = restful.run_internal('metadata', 'get_query', file_id=record['file_id'])
            if len(metadata_records):
                break
            time.sleep(10)
            try_count -= 1

        if len(metadata_records) == 0:
            self.modify('update file set file_status_id=%s where file_id=%s', self.file_status.DELETE, record['file_id'])
            return
        record.update(metadata_records[0])
        return record

    def copy_selected(self, record):
        self.modify('update file set file_status_id=%s where file_id=%s', self.file_status.COPY_IN_PROGRESS, record['file_id'])
        return {}

    def transfer_selected(self, record):
        backup_record_ids = ', '.join([str(x['backup_record_id']) for x in record['records']])
        self.modify('update backup_record set backup_record_status_id=%d where backup_record_id in (%s)' % (self.backup_record_status.TRANSFER_IN_PROGRESS, backup_record_ids))
        self.modify('update file f left join backup_record b on b.file_id=f.file_id set file_status_id=%d where backup_record_id in (%s)' % (self.file_status.BACKUP_IN_PROGRESS, backup_record_ids))
        return {}

    def pull_selected(self, volume: str, division_name: str, backup_service: int) -> list[dict[str, Any]]:
        """Get pull task data to be processed by given `volume`. Updates `pull_queue` records to
        `queue_status.IN_PROGRESS` and associated `file` records to `file_status.RESTORE_IN_PROGRESS`.

        :param volume: Volume to get pull task data for
        :param division_name: Division to get pull task data for
        :param backup_service: Backup service to get pull task data from
        :return: List of pull task records
        """
        # Get `pull_queue` records to update
        pull_queue_records = self.query(
            'select pull_queue_id, file_id from pull_queue join file using(file_id) where volume = %s and queue_status_id = %s and division = %s',
            [volume, self.queue_status.REGISTERED, division_name], uselimit=False)
        for pull_queue_record in pull_queue_records:
            # Update `pull_queue` record status
            self.modify('update pull_queue set queue_status_id = %s where pull_queue_id = %s',
                        self.queue_status.IN_PROGRESS, pull_queue_record.get('pull_queue_id'))
            # Update `file` record status
            self.modify('update file set file_status_id = %s where file_id = %s', self.file_status.RESTORE_IN_PROGRESS,
                        pull_queue_record.get('file_id'))
        info = self.query(
            'select q.pull_queue_id, q.volume, q.position_a, q.position_b, q.requestor, q.priority, f.file_permissions, f.file_path, b.service, f.file_name, b.remote_file_path, b.remote_file_name, b.backup_record_id, t.remote_path, b.tar_record_id, f.division from file f join pull_queue q on f.file_id = q.file_id left join backup_record b on f.file_id = b.file_id and b.service = %s left join tar_record t on t.tar_record_id = b.tar_record_id where q.queue_status_id = %s and q.volume = %s',
            [backup_service, self.queue_status.IN_PROGRESS, volume], uselimit=False)
        return info

    @restful.permissions('tape')
    def put_pull(self, args, kwargs):
        self.smart_modify('pull_queue', 'pull_queue_id=%d' % int(args[0]), kwargs)
        file_id = self.query('select file_id from pull_queue where pull_queue_id=%s', [int(args[0])])[0]['file_id']
        queue_status_id = kwargs.get('queue_status_id', None)
        if queue_status_id == self.queue_status.COMPLETE:
            self.put_file([file_id], {'file_status_id': self.file_status.RESTORED})
        elif queue_status_id == self.queue_status.IN_PROGRESS:
            self.put_file([file_id], {'file_status_id': self.file_status.RESTORE_IN_PROGRESS})

    @restful.validate(argsValidator=[{'name': 'division', 'type': str}, {'name': 'service_id', 'type': int}])
    @restful.permissions('tape')
    def get_heartbeat(self, args, _kwargs):
        self.smart_modify('service', 'service_id=%d' % int(args[1]), {'last_heartbeat': 'now()'})
        division = self.divisions.get(args[0])
        tasks = division.task_manager.heartbeat(int(args[1]))
        tasks['prep'] = {'record_count': division.prep_queue.get_pending_tasks_count()}
        tasks['pull'] = {'record_count': division.pull_queue.get_pending_tasks_count()}
        return tasks

    # Mark a resource as gone
    @restful.permissions('tape')
    def post_resourceoffline(self, _args, kwargs):
        if 'resource' in kwargs and 'service_id' in kwargs:
            if kwargs['resource'] not in self.resources_gone:
                self.resources_gone[kwargs['resource']] = {kwargs['service_id']: datetime.datetime.now()}
            else:
                self.resources_gone[kwargs['resource']][kwargs['service_id']] = datetime.datetime.now()

    # Mark a resource as no longer gone
    @restful.permissions('tape')
    def post_resourceonline(self, _args, kwargs):
        if 'resource' in kwargs and 'service_id' in kwargs:
            resource = kwargs['resource']
            if resource in self.resources_gone:
                service_id = kwargs['service_id']
                if service_id in self.resources_gone[resource]:
                    # remove our reference
                    del self.resources_gone[resource][service_id]
                if not self.resources_gone[resource]:
                    # remove the resource from the list if it is now empty
                    del self.resources_gone[resource]

    def get_folders(self, args, _kwargs):
        path = '/' + '/'.join(args)
        folders = self.query('select distinct file_path from file where file_path like %s and file_path not like %s', [path + '/%', path + '/%/%'])
        for folder in folders:
            folder['is_dir'] = True
        files = self.query('select file_name, file_path, file_size from file where file_path=%s', [path])
        return folders + files

    @restful.doc('Ensures that the file passed in will not be deleted until x days from now')
    @restful.validate({'file': {'type': str}, 'days': {'type': int}})
    def put_savefile(self, _args, kwargs):
        days = min(kwargs['days'], 30)
        file_path, file_name = os.path.split(kwargs['file'])
        in_file = self.query('select * from file where file_name=%s and file_path=%s order by file_id desc limit 1', [file_name, file_path])
        if len(in_file) == 0:
            raise common.HttpException(404, 'sorry I could not find a record for file : %s' % kwargs['file'])
        in_file = in_file[0]
        try:
            self.modify('update file set user_save_till=date_add(now(), interval %s day) where file_id=%s and (user_save_till is null or user_save_till< date_add(now(), interval %s day))',
                        days, in_file['file_id'], days)
        except Exception:
            # let us assume the error was an invalid date due to a dst error
            try:
                self.modify('update file set user_save_till=date_add(curdate(), interval %s day) where file_id=%s and (user_save_till is null or user_save_till< date_add(curdate(), interval %s day))',
                            days, in_file['file_id'], days)
            except Exception:
                pass
        restful.run_internal('metadata', 'add_update', {'file_id': in_file['file_id']}, {'dt_to_purge': self.get_file([in_file['file_id']], None)['dt_to_purge']})

    # Record any extra space that is being used that is not accounted for in the file table
    def post_current_quota(self, _args, kwargs):
        """Updates the current quota usage for the repository.
        parses output of command  myquota -N -B -J /global/dna/dm_archive, e.g.:
            {'quotas': [
                    {
                        "fs": "dm_archive",
                        "space_used": "823338673012736.00B",
                        "space_quota": "874111744081920.00B",
                        "space_perc": "94.2%",
                        ...
                    }
                ]
            }
        :param None _args: unused
        :param dict[str, list] kwargs: should be dict with the key of 'quotas' and an array of dicts returned by myquota as values
        """
        try:
            for fs in kwargs.get('quotas', []):
                if fs.get('fs', None) in self.config.dm_archive_root:
                    space_quota = int(fs.get('space_quota', '0.0B').split('.')[0])
                    space_used = int(fs.get('space_used', '0.0B').split('.')[0])
                    space_perc = float(fs.get('space_perc', '0%').split('%')[0])
                    self.quota_used = space_used
                    self.config.disk_size = space_quota

                    self.repository_footprint()
                    self.modify('insert into quota (quota, used, percent) values (%s, %s, %s)', space_quota, space_used, space_perc)
        except Exception as e:
            return {'error': str(e)}

    # Return our predicted repository usage
    @restful.doc('Returns an estimate of the current number of files restored, repository usage, and free space.  Call takes into account disk block allocation size.')
    def get_diskusage(self, _args, _kwargs):
        return self.disk_usage

    @restful.validate(argsValidator=[{'name': 'source', 'type': str}])
    def get_registered_egress_requests(self, args, _kwargs):
        """Gets egress requests that are `REGISTERED` state by `source`.

        :param list[str] args: args[0] should contain the data center source name (e.g., igb, dori)
        :param dict[str, str] _kwargs: unused
        """
        return self.query(
            'select e.egress_id, e.requestor, e.source, e.file_id, f.file_name, f.file_path, f.file_status_id from egress e inner join file f using(file_id) where e.egress_status_id=%s and f.file_status_id in (%s, %s, %s, %s, %s) and e.source=%s',
            [self.cv.get('queue_status').get('REGISTERED'),
             self.cv.get('file_status').get('COPY_COMPLETE'), self.cv.get('file_status').get('BACKUP_READY'),
             self.cv.get('file_status').get('BACKUP_IN_PROGRESS'), self.cv.get('file_status').get('BACKUP_COMPLETE'),
             self.cv.get('file_status').get('RESTORED'),
             args[0]])

    @restful.validate(argsValidator=[{'name': 'source', 'type': str}, {'name': 'file_id', 'type': int}])
    def get_egress_requests(self, args, _kwargs):
        return self.query(
            'select e.egress_id, e.egress_status_id, e.requestor, e.source, e.file_id from egress e where e.source=%s and e.file_id=%s',
            [args[0], [args[1]]])

    @restful.permissions('tape')
    @restful.validate({'egress_status_id': {'type': int, 'required': False},
                       'bytes_transferred': {'type': int, 'required': False}},
                      argsValidator=[{'name': 'egress_id', 'type': int}])
    def put_egress_request(self, args, kwargs):
        """Update egress request by `egress_id`. Primarily used for updating `egress_status_id`.

        :param list[int] args: args[0] should contain the `egress_id`
        :param dict[str, Any] kwargs: Fields to update in egress record
        """
        self.smart_modify('egress', f'egress_id={args[0]}', kwargs)

    def get_dm_archive_roots(self, _args, _kwargs):
        """Get configured `dm_archive_root` for NERSC and other remote data sources.

        :param list _args: Unused
        :param dict _kwargs: Unused
        """
        dm_archive_roots = {'nersc': self.config.dm_archive_root}
        dm_archive_roots.update(
            {source_name: config.get('dm_archive_root_source') for source_name, config in self.config.remote_sources.items() if
             'dm_archive_root_source' in config})
        return dm_archive_roots

    @restful.validate(validator={'services': {'type': list, 'required': True, 'validator': {'*': {'type': int}}}},
                      argsValidator=[{'name': 'file_id', 'type': int}])
    @restful.permissions('admin')
    def post_backup_service_for_file(self, args, kwargs):
        """Creates a new backup_record entry for the given `file_id`. This endpoint won't touch any backups already
        configured (so they'll be a no-op). Only adds new backup record if file state is in BACKUP_READY,
        BACKUP_IN_PROGRESS, BACKUP_COMPLETE, RESTORED. Only if one of the requested backup services isn't already set up
        for the file, then we create a new backup record and reset the file state to BACKUP_READY if file state is
        BACKUP_COMPLETE, RESTORED.

        :param list[int] args: args[0] should contain the file_id for a file record
        :param dict[str, list[int]] kwargs: should be dict with the key of `services` and an array of service ids (`int`)
        """
        valid_file_states = {self.file_status.BACKUP_READY: 'BACKUP_READY',
                             self.file_status.BACKUP_IN_PROGRESS: 'BACKUP_IN_PROGRESS',
                             self.file_status.BACKUP_COMPLETE: 'BACKUP_COMPLETE',
                             self.file_status.RESTORED: 'RESTORED'}
        file_id = args[0]
        files = self.query(
            'select file_name, file_path, file_size, file_status_id, division from file where file_id=%s', file_id)
        if len(files) == 0:
            raise common.HttpException(404, f'File with id {file_id} not found')
        file = files[0]
        if file.get('file_status_id') not in valid_file_states:
            raise common.HttpException(400,
                                       f'You can only add backup services to a file in {", ".join(valid_file_states.values())} states')
        requested_services = set(kwargs.get('services'))
        for service in requested_services:
            division_name = file.get('division')
            if not self.validate_backup_service(service, division_name):
                raise common.HttpException(400,
                                           f'Service id {service} not supported for division {division_name}')
        existing_services = {rec.get('service') for rec in
                             self.query('select service from backup_record where file_id=%s', file_id)}
        new_services = requested_services - existing_services
        new_backup_record_ids = []
        if new_services:
            for service in new_services:
                # Need to add a `backup_record` entry for the new service
                backup_record_id = self.smart_insert('backup_record',
                                                     {'service': service, 'file_id': file_id,
                                                      'backup_record_status_id': self.backup_record_status.TRANSFER_READY})
                new_backup_record_ids.append(backup_record_id)
                rec = {'file_id': file_id, 'file_name': file.get('file_name'), 'file_path': file.get('file_path'),
                       'file_size': file.get('file_size'), 'division': file.get('division'),
                       'backup_records': [{'backup_record_id': backup_record_id, 'service': service}]}
                self.add_file(rec)
            if file.get('file_status_id') in (self.file_status.BACKUP_COMPLETE, self.file_status.RESTORED):
                # Reset file status to BACKUP_READY if it's already been backed up
                self.smart_modify('file', f'file_id={file_id}', {'file_status_id': self.file_status.BACKUP_READY})
        return {'backup_record_ids': new_backup_record_ids}

    def _add_all_to_queue_by_feature(self, queue, records, file_path_extract):
        """Adds file record tasks to the queue. If `record['source']` is set, it will add the record to the source
        feature. Otherwise, if `file_path_extract` matches the pattern for remote file paths, add to the queue with the
        feature defined in the remote sources' configuration, otherwise use the queue's default features.

        :param task.Queue queue: Queue to add file record to
        :param list[dict] records: List of dictionaries for the file records
        :param (dict) -> str file_path_extract: Function reference for extracting file path from the file records
        """
        for record in records:
            self._add_to_queue_by_feature(queue, file_path_extract(record), record)

    def _add_to_queue_by_feature(self, queue, file_path, record):
        """Adds file record task to the queue. If `record['source']` is set, it will add the record to the source
        feature. Otherwise, if `file_path` matches the pattern for remote file paths, add to the
        queue with the feature defined in the remote sources' configuration, otherwise use the queue's default features.

        :param task.Queue queue: Queue to add file record to
        :param str file_path: Path to the file
        :param dict record: Dictionary for the file record
        """
        source = record.get('source', None)
        if source is not None:
            # `source` is defined in the record, add the record for that feature to the queue
            queue.add(record, [source], False)
            return
        match = re.search(self.remote_path_filter, file_path)
        if match:
            queue.add(record, [self.remote_path_prefixes.get(match.group(0))], False)
        else:
            queue.add(record)

    def _get_tar_record_path(self, record):
        """Get the path to the used for tar operations. This reflects the logic used in `dt_service.runTar`

        :param dict record: Dictionary for the file record
        :return: Path to be used for tar operation
        """
        return record.get('origin_file_path') if record.get('origin_file_name') is not None and record.get(
            'local_purge_days') != 0 else record.get('file_path')

    def _get_backup_service_feature_name(self, service_id):
        """Get the feature name (that `dt_service` handles: see `-f` option) for the backup service.

        :param service_id: ID of the service
        """
        if not hasattr(self.config, 'backup_services_to_feature_name'):
            raise ConfigurationException('Configuration missing `backup_services_to_feature_name` mapping')
        mappings = self.config.backup_services_to_feature_name
        feature_name = mappings.get(self.backup_services.get(service_id).get('type'))
        if feature_name is None:
            raise ConfigurationException(
                f'`backup_services_to_feature_name` configuration is missing mapping for backup service type {self.backup_services.get(service_id).get("type")}')
        return f'{feature_name}_{service_id}'

    def _get_backup_services(self):
        """Get backup services configuration.

        :return: dict Mapping of backup service ID to service configuration
        """
        return {service.get('backup_service_id'): service for service in self.get_backupservices(None, None)}

    class PullQueue:
        def __init__(self, name: str, tape: 'Tape', division_name: str, backup_service: int,
                     default_features: list[str] = []):
            self.name = name
            self.tape = tape
            self.division_name = division_name
            self.backup_service = backup_service
            self.volume_locks = {}
            self.enabled_queues = []
            self.default_features = default_features
            self.lock = threading.Lock()

        def next(self, available_features: list[str]) -> Optional[dict[str, Any]]:
            """Get the next pull task. The queue reads directly from the database and returns the highest priority
            pull task to process next. If `available_features` does not contain the default features for this queue,
            it will return `None` (this behavior will be changed when we add support for distributed egress). It will
            return tasks where priority is in 0, 1, and any manually enabled queues (>1).

            :param list[str] available_features: Features supported by the handler making the request
            """
            if not set(self.default_features).intersection(available_features):
                return None
            queues = sorted([0, 1] + self.enabled_queues)
            pull_queue_volume_record = None
            with self.lock:
                query = f'select pull_queue_id, volume, priority from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s and priority in ({", ".join(["%s"] * len(queues))}) '
                if self.volume_locks:
                    query += f'and volume not in ({",".join(["%s"] * len(self.volume_locks))}) '
                query += 'order by priority, pull_queue_id limit 1'
                next_volume = self.tape.query(
                    query,
                    [self.tape.queue_status.REGISTERED, self.division_name] + queues + list(self.volume_locks.keys()))
                if len(next_volume) > 0:
                    pull_queue_volume_record = next_volume[0]
                    self.volume_locks[pull_queue_volume_record.get('volume')] = {
                        'pull_queue_id': pull_queue_volume_record.get('pull_queue_id'),
                        'locked': datetime.datetime.now()}
            if pull_queue_volume_record:
                pull_task_records = self.tape.pull_selected(pull_queue_volume_record.get('volume'), self.division_name,
                                                            self.backup_service)
                with self.lock:
                    if pull_queue_volume_record.get('priority') in self.enabled_queues:
                        remaining_in_pull_queue = self.tape.query(
                            'select count(*) as cnt from pull_queue join file using(file_id) where queue_status_id = %s and priority = %s and division = %s',
                            [self.tape.queue_status.REGISTERED, pull_queue_volume_record.get('priority'),
                             self.division_name])[0]
                        # If the queue is empty turn hold portal back on
                        if remaining_in_pull_queue.get('cnt') == 0:
                            self.enabled_queues.remove(pull_queue_volume_record.get('priority'))
                return {'uses_resources': self.default_features, 'data': pull_task_records}

        def init_locks(self, volumes: list[dict[str, str]]) -> None:
            """Initialize volume locks from list of volumes.

            :param list[dict[str, str]] volumes: List of volume names to lock
            """
            if volumes is not None:
                for volume in volumes:
                    self.volume_locks[volume.get('volume')] = 1

        def clear_lock(self, volume: str) -> None:
            """Clear the cached lock for the given `volume`.
            """
            del self.volume_locks[volume]

        def enable_short(self) -> None:
            """Enable short queues (priority 2, 3).
            """
            for priority in 2, 3:
                if priority not in self.enabled_queues:
                    self.enabled_queues.append(priority)

        def enable_long(self) -> None:
            """Enable long queues (priority 4, 5, 6, 7).
            """
            for priority in 4, 5, 6, 7:
                if priority not in self.enabled_queues:
                    self.enabled_queues.append(priority)

        def get_pending_tasks_count(self) -> int:
            """Get the count of pending tasks.
            """
            with self.lock:
                return self.tape.query(
                    'select count(distinct volume) as cnt from pull_queue join file using(file_id) where queue_status_id = %s and volume is not null and division = %s',
                    [self.tape.queue_status.REGISTERED, self.division_name])[0].get('cnt')

        def delete_pending_tasks_for_file(self, file_id: int) -> None:
            """Deletes any pending tasks (`REGISTERED` state) for given `file_id`.

            :param file_id: File ID to delete pending tasks for
            """
            with self.lock:
                self.tape.modify('delete from pull_queue where file_id = %s and queue_status_id = %s', file_id,
                                 self.tape.queue_status.REGISTERED)

    class PrepQueue:
        def __init__(self, name: str, tape: 'Tape', backup_service: int, default_features: list[str] = []):
            self.name = name
            self.tape = tape
            self.backup_service = backup_service
            self.default_features = default_features
            self.lock = threading.Lock()
            self.db_prep_tasks_max_batch_size = tape.config.db_prep_tasks_max_batch_size if hasattr(tape.config,
                                                                                                    'db_prep_tasks_max_batch_size') else 1000

        def next(self, available_features: list[str]) -> Optional[dict[str, Any]]:
            """Get the next prep batch task. The queue reads directly from the database and returns batch prep tasks to
            process next. If `available_features` does not contain the default features for this queue, it will return
            `None` (this behavior will be changed when we add support for distributed egress).

            :param list[str] available_features: Features supported by the handler making the request
            """
            if not set(self.default_features).intersection(available_features):
                return None
            with self.lock:
                # We limit the prep tasks to those that were backed up to the passed backup service, since we now
                # support multiple backup services and the put call can explicitly set which backup service(s) to use.
                # We don't want to update the `pull_queue.queue_status_id` for any records which were not backed up to
                # the passed service. A given backup service belongs to a particular division.
                prep_recs = self.tape.query(
                    'select pull_queue_id from pull_queue p join backup_record b using(file_id) where volume is null and b.service = %s and queue_status_id = %s limit %s',
                    [self.backup_service, self.tape.queue_status.REGISTERED, self.db_prep_tasks_max_batch_size])
                for rec in prep_recs:
                    self.tape.modify('update pull_queue set queue_status_id = %s where pull_queue_id = %s',
                                     self.tape.queue_status.PREP_IN_PROGRESS, rec.get('pull_queue_id'))
            if prep_recs:
                query = f'select p.pull_queue_id, t.tar_record_id, t.remote_path, b.remote_file_path, b.remote_file_name, b.service from pull_queue p left join tar_record t on p.tar_record_id = t.tar_record_id join backup_record b using(file_id) where b.service = %s and p.pull_queue_id in ({",".join(["%s"] * len(prep_recs))})'
                prep_task_records = self.tape.query(query,
                                                    [self.backup_service] + [rec.get('pull_queue_id') for rec in
                                                                             prep_recs],
                                                    uselimit=False)
                return {'uses_resources': self.default_features, 'data': prep_task_records}

        def get_pending_tasks_count(self) -> int:
            """Get the count of pending tasks.
            """
            with self.lock:
                # We limit the prep tasks to those that were backed up to service 1 (`archive.nersc.gov`), since we now
                # support multiple backup services and the put call can explicitly set which backup service(s) to use,
                # which may not include service 1. We don't want to update the pull_queue.queue_status_id for any
                # records which were not backed up to service 1. We will need to revisit this when we have egress
                # support from other data centers (which may not be using hsi).
                return self.tape.query(
                    'select count(*) as cnt from pull_queue p join backup_record b using(file_id) where volume is null and b.service = %s and queue_status_id = %s',
                    [self.backup_service, self.tape.queue_status.REGISTERED])[0].get('cnt')

    class Division:
        """Wrapper to hold queues and task manager associated with a given division.
        """
        def __init__(self, division_name: str, tape: 'Tape', config: dict[str, Any]):
            self.division_name = division_name
            self.orphan_files = {}
            default_queue_features = config.get('default_queue_features', {})

            # Initialize tasks
            # Initialize the file ingest info queue
            self.ingest_queue = task.Queue('ingest', 0, default_features=default_queue_features.get('ingest', []))
            tape._add_all_to_queue_by_feature(self.ingest_queue, tape.query(
                'select * from file_ingest where file_ingest_status_id = %s and division = %s',
                [tape.file_status.REGISTERED, division_name], uselimit=False), lambda record: record.get('_file'))

            # Prep drives Pull, do Prep first
            self.prep_queue = Tape.PrepQueue('prep', tape, config.get('default_backup_service'),
                                             default_features=default_queue_features.get('prep', []))

            # Pull is a user 'interactive' task, so should do next
            self.pull_queue = Tape.PullQueue('pull', tape, division_name, config.get('default_backup_service'),
                                             default_features=default_queue_features.get('pull', []))

            # Copy and Tar pull data into the managed repository, important for users wanting to delete files, so these
            # are next
            self.copy_queue = task.Queue('copy', 2, default_features=default_queue_features.get('copy', []),
                                         task_selected=tape.copy_selected)
            tape._add_all_to_queue_by_feature(self.copy_queue,
                                              tape.query(
                                                  'select * from file where file_status_id = %s and division = %s',
                                                  [tape.file_status.COPY_READY, division_name], uselimit=False),
                                              lambda record: record.get('origin_file_path'))
            self.tar_queue = task.Queue('tar', 3, default_features=default_queue_features.get('tar', []),
                                        task_selected=tape.tar_selected)
            tape._add_all_to_queue_by_feature(self.tar_queue,
                                              tape.query(
                                                  'select * from file where file_status_id = %s and division = %s',
                                                  [tape.file_status.TAR_READY, division_name], uselimit=False),
                                              tape._get_tar_record_path)

            # End users don't care when these get done, but we don't want anything to backlog up, so put these all in
            # the same priority, so they round-robin
            self.purge_queue = task.Queue('purge', 4, [], default_features=default_queue_features.get('purge', []))
            self.delete_queue = task.Queue('delete', 4, [], default_features=default_queue_features.get('delete', []))
            self.put_queue = task.Queue('put', 5, [], default_features=default_queue_features.get('put', []),
                                        task_selected=tape.transfer_selected)
            self.md5_queue = task.Queue('md5', 5,
                                        tape.query(
                                            'select * from md5_queue where queue_status_id = %s and division = %s',
                                            [tape.file_status.REGISTERED, division_name], uselimit=False),
                                        default_features=default_queue_features.get('md5', []),
                                        task_selected=tape.md5_selected)

            self.task_manager = task.TaskManager(division_name, config.get('max_resources', {}))
            self.task_manager.set_queues(self.ingest_queue, self.put_queue, self.copy_queue, self.md5_queue,
                                         self.tar_queue, self.purge_queue, self.delete_queue)
