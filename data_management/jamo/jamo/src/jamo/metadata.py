import datetime
import lapinpy.decision as decision
import pymongo
import os
import re
import copy
import threading
import urllib
import cherrypy
import collections
from lapinpy import common, restful, curl, sdmlogger
from bson.objectid import ObjectId
from collections import deque
import uuid
import functools
from lapinpy.mongorestful import MongoRestful, convertToOID
from lapinpy.common import toMongoObj
import time
from typing import List

processservices = []


def processservice(name, description, typ, template=None):
    def inner(function):
        processservices.append({'type': typ, 'name': name, 'description': description, 'template': template, 'method': function})
        return function

    return inner


class QueueHash:

    def __init__(self, size):
        self.size = size
        self.array = [None] * size
        self.on = 0
        self.hash = {}

    # would be cool to reorder this item somehow with last accessed

    def __getitem__(self, item):
        return self.hash[item]

    def __contains__(self, key):
        return key in self.hash

    def __setitem__(self, key, value):
        if self.on >= self.size:
            self.on = 0
        if self.array[self.on] is not None and self.array[self.on] in self.hash:
            del self.hash[self.array[self.on]]
        self.hash[key] = value
        self.array[self.on] = key
        self.on += 1

    def clear(self):
        self.hash.clear()


date_fields = {'added_date', 'dt_to_purge', 'file_date', 'modified_date', 'metadata_modified_date'}
mongo_ops = {'$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin', '$and', '$or'}


def convert_dates(query):
    # self.logger.info("converting %s, type=%s" % (str(query),str(type(query))))
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(query, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(query, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        # pagequery can return the whole query as a single string rather than an array of pieces
        try:
            # self.logger.info("attempting to parse %s, type=%s" % (str(query),str(type(query))))
            return parse_date(query)
        except Exception:
            return query
    elif isinstance(query, list):
        return list(map(convert_dates, query))
    ret = dict()
    for key in query.keys():
        if key in date_fields or key in mongo_ops:
            ret[key] = convert_dates(query[key])
        else:
            ret[key] = query[key]
    return ret

@restful.doc('Interface for interacting with JAMO')
@restful.menu('Metadata')
class Metadata(MongoRestful):
    reserved_keywords = ['exists', 'all', 'gt', 'gte', 'in', 'lt', 'lte', 'ne', 'nin', 'or', 'and', 'not', 'nor', 'mod', 'type', 'size', 'elemMatch']
    publishing_flags = ['foo', 'bar', 'baz']
    search_fields = None


    def __init__(self, config=None):
        if config is not None:
            self.config = config
        mongo_options = getattr(self.config, 'mongo_options', None)
        MongoRestful.__init__(self, self.config.mongoserver, self.config.mongo_user, self.config.mongo_pass, self.config.meta_db, mongo_options, host_port=getattr(self.config, 'mongo_port', None))
        # self.backup_location = self.config.update_storage_path
        self.moduleName = 'JAMO'
        self.updateThreadRunning = False
        self.updateLock = threading.Lock()
        self.ingestLock = threading.Lock()
        self.auto_reload = True
        self.cron_enabled = True
        self.stores = {}
        self.process_services = {}
        self.updates = deque()
        self.events = deque()
        self.logger = sdmlogger.getLogger("metadata")
        self.queryLogger = sdmlogger.getQueryLogger('Query')
        self.apiSource = {'source': 'metadata_api'}
        self.clSource = {'source': 'jamo_cltool'}
        self.primary = pymongo.ReadPreference.PRIMARY

        # Load the data stores
        for row in self.get_datastores(None, None):
            root_key = row['key'][0] if isinstance(row['key'], list) else row['key']
            if root_key not in self.stores:
                self.stores[root_key] = []
            self.stores[root_key].append(row)

        self.store_cache = QueueHash(2000)

        for row in self.get_processservices(None, None):
            self.process_services[row['name']] = row
        self.file_validator.update(self.tape_validator)
        self.folder_validator.update(self.tape_validator)
        self.userSettings = {}
        for row in self.get_users(None, None):
            self.userSettings[row['user']] = row
        self.createDecisionTree()

        global publishing_flags
        publishing_flags = getattr(self.config, 'publishing_flags', [])

        self.display_location_cv = getattr(self.config, 'display_location_cv', [])

        global search_fields
        search_fields = getattr(self.config, 'search_fields', {})

        global metadata_search_options_basic
        metadata_search_options_basic  = getattr(self.config, 'metadata_search_options_basic', [])

        global metadata_search_options
        metadata_search_options = getattr(self.config, 'metadata_search_options', {})

        self.dm_archive_roots = self.config.dm_archive_root_by_division
        if hasattr(self.config, 'query_penalty'):
            self.query_penalty = self.config.query_penalty
        else:
            self.query_penalty = 1

    @restful.onload
    def doneloading(self):
        for item in processservices:
            item['address'] = item['method'].address
            del item['method']

    def createDecisionTree(self):
        subscriptions = self.get_subscriptions(None, {'Enabled': True})
        self.subscriptionMap = {}
        for subscription in subscriptions:
            self.subscriptionMap[subscription['name']] = subscription['url']
        self.subscriptionTree = decision.createTree(subscriptions)

    def shutdown(self):
        if self.updateThreadRunning:
            pass

    def addEvent(self, event, file):
        self.events.append((event, file))
        self.startEventThread()

    tape_validator = {
        'backup_services': {'example': [1], 'type': list, 'required': False, 'validator': {'*': {'type': int}},
                            'doc': 'What archivers to send this file to.<br>1: archive.nersc.gov<br>2: hpss.nersc.gov.<br> The default just [1]'},
        'destination': {'type': str, 'required': False,
                        'doc': 'This is the path that you would like your file to end up relative to your configured root path.<br>The final destination will be this value prepended by the configured root path for your user, this should be /global/dna/dm_archive/{user} The tape system will copy this file over to this location and when time comes to purge, this path will get purged.'},
        'put_mode': {'type': (int, str), 'required': False,
                     'doc': 'Pass in either:<br>Default: will not replace if it is already on tape, <br>Replace_If_Newer: will replace the record on tape if this file is newer<br>Replace_Force: will replace previous backups.<br> Default is Default'},
        'validate_mode': {'type': (int, str), 'required': False,
                          'doc': 'Pass in either:<br>Generate_MD5: will md5 this file and not create md5\'s for the tape records.<br>Validate_Tape: Will generate a md5 for the file and for each file on tape and validate they match.<br>No_MD5: Don\'t generate any md5s and dont\'t validate.<br> default is Validate_Tape'},
        'transfer_mode': {'type': str, 'required': False,
                          'doc': 'Pass in either:<br>Copy: will copy the file to the destination.<br>Move: Copy the file to the destination and then remove the local version if access is permitted.<br> default is Copy'},
        'local_purge_days': {'type': int, 'required': False,
                             'doc': 'The number of days that you want this file to stay around for.<br> If 0 is passed this file will be purged right after it has been archived.'}
    }

    @restful.doc('Creates a new record in mongo with the metadata provided, but imports the needed metadata from the mysql file_id', public=False)
    @restful.validate({'file_id': {'type': int, 'doc': 'The file_id that is in the mysql database'},
                       'metadata': {'type': dict, 'doc': 'metadata that will be associated with this file record'},
                       'file_type': {'type': (list, str), 'doc': 'The file type of this file'}}, allowExtra=False)
    @restful.permissions('admin')
    @restful.passreq(True)
    def post_importfromtape(self, args, kwargs):
        metadata = kwargs['metadata']
        file_id = kwargs['file_id']
        file = restful.run_internal('tape', 'get_file', file_id)
        meta, failed = self.processStores(metadata)
        metadata.update(meta)
        cur_time = datetime.datetime.now()
        metadata = {'metadata': metadata, 'user': kwargs['user'], 'group': kwargs['group'], 'added_date': cur_time, 'file_name': file['file_name'], 'file_path': file['file_path'], 'file_type': kwargs['file_type'], 'modified_date': cur_time, 'metadata_modified_date': cur_time, 'file_id': file['file_id']}
        for key in ('file_size', 'file_owner', 'file_permissions', 'file_group', 'file_date', 'dt_to_purge', 'file_status', 'file_status_id'):
            if key in file:
                metadata[key] = file[key]
        id = self.save('file', metadata)
        restful.run_internal('tape', 'put_file', file_id, metadata_id=id)
        return {'metadata_id': id}

    file_validator = {
        'file': {'example': '/full/path/to/file', 'type': str,
                 'doc': 'The complete path to the file that is to be archived. This path must exist and be readable by the account running `dt-service`'},
        'inputs': {'type': list, 'validator': {'*': {'type': 'oid'}}, 'required': False,
                   'doc': 'A list of metadata_ids that were used to generate this file',
                   'example': ['523793daf287494df19c4f86', '523793daf287494df19c4f87']},
        'file_type': {'type': (str, list),
                      'doc': 'The file type of the file, this should be descriptive, but short.'},
        'metadata': {'type': dict,
                     'doc': 'Key/Value pairs that will be associated to the file. Any keys that are passed that have a datastore will trigger the datastore and all the key/values that the data store returns will be associated in the datastore identifier subdocument. These keys can casscade triggering other data stores.'},
        'source': {'type': str, 'required': False, 'default': None,
                   'doc': 'Source name for data center (e.g., igb, dori)'}
    }

    @restful.doc('Adds a single file to the tape system and associates it to the provided metadata.<br>\n'
                 'First a check is made to see if the passed in file has already been added. This condition is met if the file has the same modified date, file_size, file name, and file_path.<br>\n'
                 'If there is already a file that meets this condition and there is no metadata for the record, the provided metadata will be associated and a metadata id will be returned.<br>\n'
                 'If there is already metadata for the record, the previous metadata record id will be returned.<br>\n'
                 'If this file is new it will be added to the tape system and the metadata provided will be stored in the metadata system.\n',
                 {'metadata_id': {'type': str}})
    @restful.permissions('put_metadata')
    @restful.passreq
    @restful.validate(file_validator)
    def post_file(self, args, kwargs):
        kwargs['call_source'] = 'file'
        return self.ingest_data(args, kwargs)

    folder_validator = {
        'index': {'type': bool, 'default': True,
                  'doc': 'If false, don\'t store a list of the contents of this folder. If any folder contains more than 100 files, those files will not be added to the index.'},
        'inputs': {'type': list, 'validator': {'*': {'type': str}}, 'required': False,
                   'doc': 'A list of metadata_ids that were used to generate this file',
                   'example': ['523793daf287494df19c4f86', '523793daf287494df19c4f87']},
        'folder': {'example': '/full/path/to/folder', 'type': str,
                   'doc': 'The full path of the folder to be archived. This must be readable and executable by the account running `dt-service`.'},
        'file_type': {'type': (str, list),
                      'doc': 'The file type of the file, this should be descriptive, but short.'},
        'source': {'type': str, 'required': False, 'default': None,
                   'doc': 'Source name for data center (e.g., igb, dori)'},
        'metadata': {'type': dict,
                     'doc': 'Key/Value pairs that will be associated to the file. Any keys that are passed that have a datastore will trigger the datastore and all the key/values that the data store returns will be associated in the datastore identifier subdocument. These keys can cascade triggering other data stores.'},
        'ignore': {'type': list, 'validator': {'*': {'type': str}}, 'required': False,
                   'doc': 'A list of files or folder relative to the folder to be archived that you don\'t want to archive.'},
        'extract': {'type': list, 'required': False, 'validator': {'*': {'type': dict, 'validator': {
            'path': {'type': str},
            'metadata': {'type': dict, 'required': False},
            'file_type': {'type': (str, list)}}}},
            'doc': 'A list of files that should be archived individually.'}
    }

    @restful.doc('Adds a folder to the metadata system. <br> This api should not be used since many assumptions are made.<br>\n'
                 'Folders are not validated against, so it is possible to easily overwrite a previous structure and have duplicate folders in the metadata system.<br>\n'
                 'This record will first be stored in the metadata system and an id will be returned.<br>\n'
                 'Then a data service will crawl through this folder and check to see if the links are "broken" or the links already point to a file in the metadata system. If the link does\n'
                 'it will not be tared up with the other files. But when this is purged and the folder is requested to be restored, all metadata linked files will also be restored.<br>\n'
                 )
    @restful.passreq
    @restful.permissions('add_folder')
    @restful.validate(folder_validator)
    def post_folder(self, args, kwargs):
        kwargs['call_source'] = 'folder'
        kwargs['file'] = kwargs['folder']
        kwargs['auto_uncompress'] = False
        return self.ingest_data(args, kwargs)

    # Originally the body of post_file, set as a separate function for both post_file and post_folder can use
    # this and both original calls will rely on calling function's validation methods
    def ingest_data(self, _args, kwargs):
        """first we need to make sure this is a new file
           make the call to tape first and see what it returns"""
        metadata = kwargs.get('metadata')
        user = kwargs.get('__auth').get('user')
        group = kwargs.get('__auth').get('group')
        division_name = kwargs.get('__auth').get('division')
        tape_args = kwargs.copy()

        if group is None:
            raise common.ValidationError(
                'You do not have a group set for your account, please set before you add a file to jamo')
        del tape_args['metadata']

        # will remove null keys from the merge
        replace_with_null = True
        if 'replace_with_null' in kwargs:
            del tape_args['replace_with_null']
            rwn = kwargs.get('replace_with_null')
            replace_with_null = rwn is True or rwn == 'True' or rwn == '1' or rwn == 1

        if 'file_id' not in kwargs:
            if 'staging_path' in kwargs:
                tape_args['destination'] = kwargs.get('staging_path')
                del tape_args['staging_path']
            elif 'destination' in kwargs:
                dm_archive_root = self.dm_archive_roots.get(division_name)
                if kwargs.get('destination').startswith('/') and user != 'sdm':
                    raise common.ValidationError(
                        f'The destination path you provided: {kwargs.get("destination")}, is not valid, must not start with /')
                if group not in self.userSettings:
                    self.post_user(None, {'user': group, 'relative_root': f'{dm_archive_root}/{group}'})
                    if group not in self.userSettings:
                        raise common.HttpException(401, 'Sorry there is no user configuration for your account')
                if kwargs.get('destination').startswith('/') and user == 'sdm':
                    tape_args['destination'] = kwargs.get('destination')
                    if 'user' in kwargs:
                        user = kwargs.get('user')
                else:
                    tape_args['destination'] = os.path.join(dm_archive_root, group, kwargs.get('destination'))
                    # if the destination is a folder, then we need to add the file name to the end of the destination
                    # otherwise we'll assume the destination is a path + new file name
                    if tape_args.get('destination').endswith('/'):
                        tape_args['destination'] += os.path.split(kwargs.get('file'))[1]

            if kwargs.get('file').startswith('hpss:'):
                if 'destination' not in kwargs:
                    raise common.ValidationError('When using hpss file as your file you must pass in a destination')
                tape_args['file'] = kwargs.get('file').replace('hpss:', '').replace('//', '/')
                ret = restful.run_internal('tape', 'post_hpssfile', **tape_args)
            else:
                ret = restful.run_internal('tape', 'post_file', **tape_args)
        else:
            # Seems to be a hidden method of creating a metadata record and associating it with an existing file record?
            ret = {'file_id': kwargs.get('file_id'), 'status': 'new'}
        if 'errors' in ret:
            raise common.ValidationError(ret.get('errors'))

        # status is now old, new, and delayed
        #   new - no record found, create jamo record, we'll update the file data later
        #   old - no update should happen, ret
        #   delayed - don't know what to do yet, hold onto the data to process later
        if ret.get('status') == 'old':
            # no change to the file save the record
            meta, failed = self.processStores(metadata)
            self.safeMerge(metadata, meta, replace_with_null)
            if self.smartUpdate('file', {'_id': ObjectId(ret.get('metadata_id'))},
                                {'$set': {'metadata': metadata,
                                          'file_type': kwargs.get('file_type')}}).get('nModified') > 0:
                self.addEvent('update', metadata)
            return {'metadata_id': ret.get('metadata_id')}

        # save off the record to file_ingest
        cur_time = datetime.datetime.now()
        file_rec = {'metadata': metadata, 'user': user, 'group': group, 'division': division_name,
                    'added_date': cur_time, 'modified_date': cur_time, 'metadata_modified_date': cur_time,
                    'file_id': ret.get('file_id'), 'file_ingest_id': ret.get('file_ingest_id'), 'request_count': 1}
        for var in kwargs:
            if var not in file_rec:
                file_rec[var] = kwargs.get(var)

        if 'inputs' in kwargs:  # and len(kwargs['inputs']) > 0:    # we probably shouldn't store empty inputs, but we have been, so let this go for now..
            file_rec['inputs'] = kwargs.get('inputs')
        if ret.get('metadata_id', None):
            # Replace the existing JAMO document. The original 'added_date' field
            # will be overwritten by the field in the metadata dict, which is set to the current time.
            id = file_rec['metadata_id'] = ret.get('metadata_id')
        # save off the record
        file_rec['replace_with_null'] = replace_with_null
        # Look for an old record
        with self.ingestLock:
            if file_rec.get('file_ingest_id', None):
                old_rec = self.query('file_ingest', **{'file_ingest_id': file_rec.get('file_ingest_id'),
                                                       '_read_preference': self.primary})
                if len(old_rec):
                    file_rec['_id'] = old_rec[0].get('_id')
                    file_rec['request_count'] = old_rec[0].get('request_count') + 1
            file_rec['ingest_id'] = self.save('file_ingest', file_rec)

        # if it is a new record, create that, we'll finish up processing later
        if ret.get('status') == 'new' or ret.get('metadata_id', None) is None:
            file_rec.pop('_id', None)
            file_rec.pop('metadata_id', None)
            file_rec.pop('replace_with_null', None)
            # we don't have a file id yet, and this needs to be unique, set it to the negative of file_ingest_id (so it
            # doesn't overlap with existing file_ids, we'll replace it later)
            if file_rec.get('file_id') is None:
                file_rec['file_id'] = - file_rec.get('file_ingest_id')
                file_rec['file_status'] = 'REGISTERED-INGEST'
            try:
                id = self.save('file', file_rec)
            except pymongo.errors.DuplicateKeyError as e:  # noqa: F841
                id = str(self.query('file', **{'file_id': file_rec.get('file_id'),
                                               '_read_preference': self.primary})[0].get('_id'))
            else:
                file_rec['_id'] = id
                self.addEvent('add', file_rec)

        # Add the metadata_id to the ingest record
        self.smartUpdate('file_ingest', {'_id': ObjectId(file_rec.get('ingest_id'))}, {'$set': {'metadata_id': id}})
        # Add the metadata_id to the tape ingest record
        restful.run_internal('tape', 'put_file_ingest', ret.get('file_ingest_id'), metadata_id=id,
                             _metadata_ingest_id=file_rec.get('ingest_id'))
        return {'metadata_id': id}

    @restful.doc('Complete the processing of the jamo record post file ingest', public=False)
    @restful.permissions('tape')
    @restful.passreq
    @restful.validate({'_metadata_ingest_id': {'type': str, 'doc': 'The file_ingest metadata_id'}}, allowExtra=True)
    def post_file_ingest(self, args, kwargs):
        # tape_file_ingest_id = kwargs.get('file_ingest_id', None)
        # meta_file_ingest_id = kwargs.get('_metadata_ingest_id', None)
        # self.logger.info('post_file_ingest - ingest id: %d, metadata_ingest_id: %s - start update' % (tape_file_ingest_id, meta_file_ingest_id))  # XXX

        # get the original request and do the datastore work
        file_ingest_rec = self.query('file_ingest', **{'_id': ObjectId(kwargs.get('_metadata_ingest_id')),
                                                       '_read_preference': self.primary})
        if len(file_ingest_rec) > 0:
            file_ingest_rec = file_ingest_rec[0]
        else:
            raise Exception(f'file_ingest rec not found: {kwargs.get("_metadata_ingest_id")}')

        metadata = file_ingest_rec.get('metadata')

        # Get the rec from the DB
        replace_with_null = file_ingest_rec.get('replace_with_null', True)
        _id = file_ingest_rec.get('metadata_id', None)
        if _id is None:
            _id = kwargs.get('metadata_id', None)
            # self.logger.info('post_file_ingest - ingest id: %d, metadata_ingest_id: %s, metadata_id %s - using the passed _id' % (tape_file_ingest_id, meta_file_ingest_id, _id))  # XXX

        file_rec = self.query('file', **{'_id': _id, '_read_preference': self.primary})
        if len(file_rec) > 0 and 'metadata' in file_rec[0]:
            # self.logger.info('post_file_ingest - ingest id: %d, metadata_ingest_id: %s, metadata_id %s - merge existing metadata' % (tape_file_ingest_id, meta_file_ingest_id, _id))  # XXX
            # merge our new metadata on top of what is in the DB
            self.safeMerge(file_rec[0].get('metadata'), metadata, replace_with_null)
            metadata = file_rec[0].get('metadata')

        meta, failed = self.processStores(metadata)
        self.safeMerge(metadata, meta, replace_with_null)

        if kwargs.get('_status') in ('new', 'replaced'):
            cur_time = file_ingest_rec.get('metadata_modified_date')
            dt_to_purge = cur_time + datetime.timedelta(days=kwargs.get('local_purge_days'))
            metadata_file_rec = {'metadata': metadata,
                                 'user': file_ingest_rec.get('user'),
                                 'group': file_ingest_rec.get('group'),
                                 'division': file_ingest_rec.get('division'),
                                 'added_date': cur_time,
                                 'file_type': file_ingest_rec.get('file_type'),
                                 'modified_date': cur_time,
                                 'dt_to_purge': dt_to_purge,
                                 'metadata_modified_date': cur_time
                                 }

            # we may want to remove this in the future and rely on the checks to find these
            if len(failed) > 0:
                metadata_file_rec['failed_store_calls'] = failed

            for field in ('inputs', 'index', 'ignore', 'extract'):
                if field in file_ingest_rec:
                    metadata_file_rec[field] = file_ingest_rec.get(field)
            if _id:
                # Replace the existing JAMO document. The original 'added_date' field
                # will be overwritten by the field in the metadata dict, which is set to the current time.
                metadata_file_rec['_id'] = ObjectId(_id)

            # file was a folder, add 'folder' to the list of file types
            if kwargs.get('_is_folder'):
                if isinstance(metadata_file_rec.get('file_type'), str):
                    if metadata_file_rec.get('file_type') != 'folder':
                        metadata_file_rec['file_type'] = ['folder', metadata_file_rec.get('file_type')]
                else:
                    if 'folder' not in metadata_file_rec.get('file_type'):
                        metadata_file_rec.get('file_type').append('folder')

            for key in ('file_name', 'file_path', 'file_id', 'file_size', 'file_owner', 'file_permissions',
                        'file_group', 'file_date', 'file_status', 'file_status_id'):
                metadata_file_rec[key] = kwargs.get(key)
            try:
                id = self.save('file', metadata_file_rec)
                # self.logger.info('post_file_ingest - ingest id: %d, metadata_ingest_id: %s, saved as _id: %s - saved file' % (tape_file_ingest_id, meta_file_ingest_id, id))  # XXX
            except pymongo.errors.DuplicateKeyError:
                id = str(self.query('file',
                                    **{'file_id': metadata_file_rec.get('file_id'),
                                       '_read_preference': self.primary})[0].get('_id'))
                # self.logger.info('post_file_ingest - ingest id: %d, metadata_ingest_id: %s, saved as _id: %s - saved file duplicate key' % (tape_file_ingest_id, meta_file_ingest_id, id))  # XXX
            else:
                metadata_file_rec['_id'] = id
                self.addEvent('add', metadata_file_rec)
            restful.run_internal('tape', 'put_file', kwargs.get('file_id'), metadata_id=id)
            return {'metadata_id': id}
        else:
            if self.smartUpdate('file', {'_id': ObjectId(_id)},
                                {'$set': {'metadata': metadata, 'file_type': file_ingest_rec.get('file_type')}}
                                ).get('nModified') > 0:
                self.addEvent('update', metadata)
            return {'metadata_id': file_ingest_rec.get('metadata_id')}

    @restful.passreq(include_perms=True)
    @restful.doc("Updates the specified file's metadata")
    @restful.permissions('metadata_data_update')
    @restful.validate({'id': {'type': 'oid',
                              'doc': 'the id of the metadata record, should look like 522421c72cdd3e4256c74a87.<br> You will only be able to update your own files'},
                       'data': {'type': dict, 'doc': 'Key/value pairs to update'}})
    def put_file(self, args, kwargs):
        # Check to see if have admin or if they own this file/metadata
        admin = 0
        try:
            if 'admin' in kwargs['permissions']:
                admin = 1
        except Exception:
            pass
        try:
            # TODO: This code will not work as is, since the validator implicitly converts the str id into a ObjectId
            #  instance which does not support len() calls, so this will always raise an exception.
            if admin == 0 and kwargs['__auth']['user'] != self.get_file([kwargs['id']], None)['user']:
                # TODO: Why are we raising this exception when we just ignore it? Do we want to allow callers with the
                #  right perms (`metadata_data_update`) to update records?
                raise common.HttpException(403, 'You are attempting to update a file you do not own.')
        except Exception:
            pass
        return self.smartUpdate('file', {'_id': ObjectId(kwargs['id'])}, {'$set': kwargs['data']})

    @restful.passreq(include_perms=True)
    @restful.doc('updates metadata for the specified file')
    @restful.permissions('metadata_update')
    @restful.validate({'id': {'type': str,
                              'doc': 'the id of the metadata record, should look like 522421c72cdd3e4256c74a87.<br> You will only be able to update your own files'},
                       'metadata': {'type': dict, 'validator': {'*:1': {'type': '*'}},
                                    'doc': 'Key/value pairs to update'}})
    def put_filemetadata(self, args, kwargs):
        updatedata = {}
        for key, value in kwargs['metadata'].items():
            updatedata['metadata.' + key] = value
        if len(kwargs['id']) < 20:
            raise common.ValidationError('You have pass an incorrect metadata id of "%s"' % kwargs['id'])
        # Check to see if have admin or if they own this file/metadata
        admin = 0
        if ('permissions' in kwargs and 'admin' in kwargs['permissions']) or \
                ('__auth' in kwargs and 'permissions' in kwargs['__auth'] and 'admin' in kwargs['__auth']['permissions']):
            admin = 1
        if admin == 0 and kwargs['__auth']['user'] != self.get_file([kwargs['id']], None)['user']:
            raise common.HttpException(403, 'You are attempting to update a record you do not own.')
        return self.smartUpdate('file', {'_id': ObjectId(kwargs['id'])}, {'$set': updatedata})

    @restful.permissions('admin')
    def put_filesuper(self, args, kwargs):
        self.smartUpdate('file', kwargs['query'], {'$set': kwargs['data']})

    @restful.passreq
    @restful.doc('deletes any metadata that this user has added')
    @restful.permissions('metadata_update')
    @restful.validate(argsValidator=[{'type': str}])
    def delete_update(self, args, kwargs):
        # TODO: This code will not work as `smartUpdate` expects the value to be updated to be a dict, where here it's a str
        return self.smartUpdate('file', {'_id': ObjectId(args[0])}, {'$unset': 'metadata.%s' % kwargs['__auth']['user']})

    @restful.passreq
    @restful.doc('adds or updates fields in the users metadata subdocument if the user has access to do so')
    @restful.permissions('metadata_update')
    @restful.validate({'*': {'type': dict, 'doc': 'Any metadata that will go into the user subdocument'}}, argsValidator=[{'type': str, 'name': 'metadata_id'}])
    def put_update(self, args, kwargs):
        updatedata = {}
        for key, value in kwargs.items():
            updatedata['metadata.%s.%s' % (kwargs['__auth']['group'], key)] = value
        del updatedata['metadata.%s.__auth' % kwargs['__auth']['group']]
        return self.smartUpdate('file', {'_id': ObjectId(args[0])}, {'$set': updatedata})

    def ingest_retry_records(self, args, kwargs):
        """Get the records from JAMO that may have an incomplete ingest"""
        date = datetime.datetime.now() - datetime.timedelta(minutes=10)
        return self.query('file', **{'file_id': {'$lt': 0}, 'added_date': {'$lt': date}, '_read_preference': self.primary})

    @restful.doc('Retries to update the metadata record for a file_ingest_id', public=False)
    @restful.permissions('admin')
    def post_ingest_retry(self, args, kwargs):
        """Reprocess any outstanding ingests that are over 10 minutes old."""
        n = 0
        for rec in self.ingest_retry_records(None, None):
            n += 1
            restful.run_internal('tape', 'put_file_ingest_retry', -rec['file_id'])
        return {"processed": n}

    def processStores(self, metadata):
        """
            Pull data from any data stores that are indicated by
            the presence of certain keys in metadata.
            The data stores to pull from are stored in Mongo under the
            data_stores collection.
        """
        new_metadata = {}
        extracted_keys = {}
        all_failed = []
        processed = []
        # print self.stores
        # print metadata
        for key in metadata:
            if key in self.stores:
                stores = self.stores[key]
                #  key has been found, run through all the attached data stores
                for store in stores:
                    success = True
                    key_values = collections.OrderedDict()
                    if isinstance(store['key'], list):
                        # dealing with a list of keys
                        for item in store['key']:
                            if item in metadata:
                                # if it is an array of take the first item
                                if isinstance(metadata[item], list):
                                    if len(metadata[item]) == 1:
                                        extracted_keys[item] = key_values[item] = metadata[item][0]
                                    else:
                                        success = False
                                else:
                                    extracted_keys[item] = key_values[item] = metadata[item]
                            else:
                                # not all parts of the composite keys found,
                                # so we need to skip this one
                                success = False
                    else:
                        # dealing with a single key, if it is an array of take the first item
                        if isinstance(metadata[store['key']], list):
                            if len(metadata[store['key']]) == 1:
                                extracted_keys[store['key']] = key_values[store['key']] = metadata[store['key']][0]
                            else:
                                success = False
                        else:
                            extracted_keys[store['key']] = key_values[store['key']] = metadata[store['key']]
                    if success:
                        try:
                            ret = self.processStore(key_values, store, processed, extracted_keys, metadata)
                            if ret:
                                self.safeMerge(new_metadata, ret)
                        except urllib.error.URLError:
                            all_failed.append({'dictionary': key_values, 'processed': processed})
                processed.append(key)
        return new_metadata, all_failed

    def processStore(self, key_values, store, already_processed=[], extracted_keys={}, original_doc={}):
        """
        keys and values now being passed as an ordered dictionary
        """
        # if any of the metadata keys are empty, return
        for key, value in list(key_values.items()):
            if value is None:
                return {}
        try:
            # Check to see if we've found this recently
            cache_key = store['identifier'] + '/' + '/'.join([str(val) for key, val in list(key_values.items())])
            if cache_key in self.store_cache:
                data = copy.deepcopy(self.store_cache[cache_key])
            else:
                url = store['url']
                for key, value in list(key_values.items()):
                    if isinstance(value, list):
                        if len(value) == 1:
                            value = value[0]
                        else:
                            return {}
                    url = url.replace('{{' + key + '}}', str(value))
                # Query the data store with the given URL, swapping the trigger key to the URL
                data = curl.get(url)
                self.store_cache[cache_key] = copy.deepcopy(data)
        except urllib.error.URLError:
            # the host is down.. lets cache this to try again later
            raise
        except Exception:
            data = self.store_cache[cache_key] = None
            self.logger.error('failed to call store: %s' % url)
        store_metadata = {}
        new_metadata = {}
        # We only want to add the first element of a composite key to `already_processed`, since that's used for
        # datastore lookups. We know that the first element will be the lookup key since we're using a `OrderedDict`.
        # Add the key itself if it's a simple key.
        already_processed.append(next(iter(key_values.keys())))
        if data is None:
            return None
        # flatten down any data
        if 'flatten' in store:
            # The case that the caller is returning a an array of dictionaries
            if store['flatten'] is True:
                if isinstance(data, list):
                    if len(data) > 0:
                        data = data[0]
                    else:
                        return None
            # The case that the caller has returned a dictionary, and we want a sub-dictionary
            elif store['flatten'] in data:
                # if it is an array, take the first item
                if isinstance(data[store['flatten']], list):
                    data = data[store['flatten']][0]
                else:
                    data = data[store['flatten']]
        orgin_data = data
        conform_keys = True if 'conform_keys' in store and store['conform_keys'] else False
        # process the map
        if 'map' in store and store['map'] is not None:
            # A mapping for renaming keys
            store_map = store['map']
            extracted_keys.update(self._extract_keys(data, store_map, conform_keys, original_doc))

            for map_name in store_map:
                name = map_name
                old_data = None
                if map_name.count(">") > 0:
                    dir = map_name.split(">")[:-1]
                    old_data = orgin_data
                    for level in dir:
                        if level not in data:
                            break
                        data = data[level]
                    name = map_name.split('>')[-1]

                if name in data:
                    mapped_key = name if 'new_key' not in store_map[map_name] else store_map[map_name]['new_key']
                    mapped_value = data[name]
                    if conform_keys:
                        mapped_key, mapped_value = self.conform(mapped_key, data[name])
                        # mapped_key = mapped_key.replace('-','_').lower()
                    if 'extract' in store_map[map_name] and store_map[map_name]['extract']:
                        # Skip the extract if skip_if_exists key exists in the original metadata doc
                        if 'skip_extract_if_exists' in store_map[map_name] and store_map[map_name]['skip_extract_if_exists'] in original_doc:
                            pass
                        else:
                            extracted_keys[mapped_key] = new_metadata[mapped_key] = mapped_value
                            if mapped_key in self.stores and mapped_key not in already_processed and data[name] is not None:
                                for istore in self.stores[mapped_key]:
                                    # returned_data = self.processStore(mapped_key, mapped_value, istore, already_processed)
                                    # new_metadata.update(returned_data)
                                    success = True
                                    ikey_values = collections.OrderedDict()
                                    if isinstance(istore['key'], list):
                                        # dealing with a list of keys
                                        for item in istore['key']:
                                            if item in extracted_keys:
                                                ikey_values[item] = extracted_keys[item]
                                            else:
                                                # not all parts of the composite keys found,
                                                # so we need to skip this one
                                                success = False
                                    else:
                                        # dealing with a single key
                                        ikey_values[mapped_key] = mapped_value
                                    if success:
                                        returned_data = self.processStore(ikey_values, istore, already_processed, extracted_keys, original_doc)
                                        if returned_data:
                                            self.safeMerge(new_metadata, returned_data)
                    if 'use' not in store_map[map_name] or store_map[map_name]['use']:
                        # store_metadata[mapped_key]=data[name]
                        store_metadata[mapped_key] = mapped_value
                    del data[name]
                    if old_data is not None:
                        data = old_data
        ignore_null = True if 'ignore_null' in store and store['ignore_null'] else False
        if 'only_use_map' not in store or not store['only_use_map']:
            for key in data:
                mapped_key = key
                mapped_value = data[key]
                if conform_keys:
                    # mapped_key = mapped_key.replace('-','_').lower()
                    mapped_key, mapped_value = self.conform(mapped_key, mapped_value)
                if not (ignore_null and (mapped_value is None or mapped_value == '')):
                    store_metadata[mapped_key] = mapped_value
        if 'create_map' not in store or store['create_map']:
            # if there is a '.' in the identifier, create sub-documents
            if '.' in store['identifier']:
                temp = new_metadata
                count = store['identifier'].count('.')
                n = 0
                for element in store['identifier'].split('.'):
                    if n < count:
                        if element not in temp:
                            temp[element] = dict()
                        temp = temp[element]
                    else:
                        temp[element] = store_metadata
                    n += 1
            else:
                new_metadata[store['identifier']] = store_metadata
        return new_metadata

    def conform(self, key, value):
        """
            Recursively remap keys, in case any values are dictionaries themselves
        """
        mapped_key = key.replace('-', '_').lower()
        mapped_value = value
        if isinstance(value, dict):
            mapped_value = dict()
            for sub_key, sub_value in value.items():
                new_sub_key, new_sub_value = self.conform(sub_key, sub_value)
                mapped_value[new_sub_key] = new_sub_value
        elif isinstance(value, list):
            mapped_value = list()
            for item in value:
                mapped_value.append(self.conform("", item)[1])
        return (mapped_key, mapped_value)

    def parseInputs(self, inputs, kwargs):
        variables = {}
        for name in inputs:
            input = inputs[name]
            if 'default' in input and name not in kwargs:
                variables[name] = input['default']
            elif input['required'] and name not in kwargs:
                raise common.ValidationError('key %s was not supplied' % name)
            else:
                variables[name] = self.parseType(kwargs[name], input['type'])
        return variables

    def parseType(self, value, typeS):
        if typeS == 'string':
            return str(value)
        elif typeS == 'number':
            return float(value)
        elif typeS == 'bool':
            return value.lower() == 'true'
        elif typeS.startswith('list'):
            if isinstance(value, list):
                return value
            else:
                value = value.split(',')
                listType = typeS.split(':')[1]
                try:
                    if listType == 'int':
                        return list(map(int, value))
                except Exception:
                    raise common.ValidationError('Failed to parse input, probably misformatted')

        # elif typeS == 'date':
        #    return parser.parse(value)

    # Called by get_query.  May be useful in the future, but currently not used and eval_exp is not defined
    # def evalLogic(self, logic, variables):
    #     for var, string in logic.items():
    #         variables[var] = eval_exp(variables, string)
    #     return variables

    def parseQuery(self, query, variables):
        ret = {}
        for key, value in query.items():
            if key.startswith('#') and key[1:] in variables:
                key = variables[key[1:]]
            key = key.replace('>', '.')
            if isinstance(value, str):
                if value.startswith('#') and value[1:] in variables:
                    value = variables[value[1:]]
            elif isinstance(value, dict):
                value = self.parseQuery(value, variables)
            elif isinstance(value, list):
                newValue = []
                for item in value:
                    if isinstance(item, str) and item.startswith('#') and item[1:] in variables:
                        newValue.append(variables[item[1:]])
                    else:
                        newValue.append(item)
                value = newValue
            ret[key] = value
        return ret

    def getResults(self, row, fields):
        ret = {}
        for field in fields:
            tmpRow = row
            if field.count('>') > 0:
                for col in field.split('>')[:-1]:
                    if col not in tmpRow:
                        break
                    tmpRow = tmpRow[col]
                field = field.split('>')[-1]
            if field in tmpRow:
                ret[field] = tmpRow[field]
            else:
                ret[field] = None
        return ret

    ### restful services ###  # noqa: E266

    @restful.generatedhtml(title='Results',
                           map={'_id': {'type': 'html', 'value': '<a href="/metadata/file/{{value}}">{{value}}</a>'},
                                'metadata': {'show': False}})
    @restful.doc('Runs a query service with the specified arguments and returns a list of documents with the specified fields.  Query returns a maximum of 500 records.  If you have larger queries, you may want to use pagequery (and nextpage) instead.')
    def get_query(self, args, kwargs):
        # function may be obsolete at the point - CJB
        requestor = kwargs.get('requestor', None)
        if len(args) == 0:
            if kwargs is None or len(kwargs) == 0:
                return self.error(400, 'you must provide key-value pairs to search against')
            self.queryLogger.info('%s - %s - %s', str(kwargs), None, requestor, extra=self.apiSource)
            return self.query('file', **kwargs)
        if kwargs is None:
            kwargs = {}
        if args[0] not in self.process_services:
            raise common.HttpException(404, 'Sorry you request a query service that does not exist')
        time.sleep(self.query_penalty)
        service = self.process_services[args[0]]
        query = {}
        variables = {}
        if 'inputs' in service:
            variables.update(self.parseInputs(service['inputs'], kwargs))
        # May be useful in the future, but currently not used
        # if 'logic' in service:
        #     variables.update(self.evalLogic(service['logic'], variables))
        for word in self.reserved_keywords:
            variables[word] = '$' + word
        if 'query' in service:
            query = self.parseQuery(service['query'], variables)
        if '_page' in kwargs:
            query['_page'] = kwargs['_page']

        if len(args) == 2 and args[1] == 'test' and 'test_data' in service:
            test_data = list(map(ObjectId, service['test_data']))
            query = {'_id': {'$in': test_data}}
        self.queryLogger.info('%s', str(query), extra=self.apiSource)

        rows = self.query('file', **query)
        fields = service['return']
        if '_id' not in fields:
            fields.append('_id')
        ret = []
        for row in rows:
            ret.append(self.getResults(row, fields))
        return ret

    def pagequery(self, collection, what=None, select=None, return_count=100, sort=None, modifiers={}, key_map=None, flatten=False):
        """
        Extend this function so we convert specific fields that might be Mongo ObjectIDs
        """
        if isinstance(what, dict):
            if 'inputs' in what:
                # self.logger.info("converting inputs to OIDs in child class extension of pagequery")
                what['inputs'] = convertToOID(what['inputs'])
        return MongoRestful.pagequery(self, collection, what=what, select=select, return_count=return_count, sort=sort, modifiers=modifiers, key_map=key_map, flatten=flatten)

    def query(self, collection, **what):
        """
        Extend this function so we convert specific fields that might be Mongo ObjectIDs
        """
        if 'inputs' in what:
            # self.logger.info("converting inputs to OIDs in child class extension of query")
            what['inputs'] = convertToOID(what['inputs'])
        return MongoRestful.query(self, collection, **what)

    @restful.doc('Performs a query and return the first page of results, to get the next pages call nextpage. PageList can be used for making an iterable over this return result.')
    @restful.validate({'fields': {'required': False, 'type': list, 'validator': {'*': {'type': str}}},
                       'query': {'type': (str, dict), 'validator': {'*:1': {'type': '*'}}},
                       'flatten': {'required': False, 'type': bool, 'default': False}})
    def post_pagequery(self, args, kwargs):
        fields = kwargs.get('fields', None)
        requestor = kwargs.get('requestor', None)
        flatten = kwargs.get('flatten', False)
        source = kwargs.get('source', 'file')
        if fields is None:
            # if there is a select, pull out the select for the fields, and set query to the where part
            if isinstance(kwargs['query'], str) and kwargs['query'].strip().lower().startswith('select'):
                query = kwargs['query'].lower()
                i = 0
                fields = []
                for string in query.split(' '):
                    if string is None:
                        i += 1
                        continue
                    i += len(string) + 1
                    if string == 'where':
                        break
                    if string == ',' or string == 'select':
                        continue
                    fields.extend([f for f in string.split(',') if f is not None and f != ''])
                kwargs['query'] = kwargs['query'][i:]
        else:
            if isinstance(kwargs['query'], str) and kwargs['query'].strip().lower().startswith('select'):
                raise Exception("fields option and a 'select <fields>' query provided, request cannot have both")

        kwargs['query'] = convert_dates(kwargs['query'])
        if 'cltool' in kwargs and kwargs['cltool']:
            extra = self.clSource
        else:
            extra = self.apiSource
        self.queryLogger.info('%s - %s - %s', str(kwargs['query']), str(fields), requestor, extra=extra)

        return self.pagequery(source, kwargs['query'], fields, return_count=10000, flatten=flatten)

    @restful.permissions('portal')
    @restful.validate({'fields': {'required': False, 'type': list, 'validator': {'*': {'type': str}}},
                       'query': {'type': (str, dict), 'validator': {'*:1': {'type': '*'}}}})
    def post_portalquery(self, args, kwargs):
        fields = kwargs.get('fields', None)
        # make assumptions on who is calling this, and how
        self.queryLogger.info('%s - %s - %s', str(kwargs['query']), str(fields), 'portal', extra=self.apiSource)
        return self.pagequery('file', kwargs['query'], fields, return_count=10000)

    # I believe this is the field that ensure the result of post_pagequery is a PageResponse object (ajtritt)
    post_pagequery.paged = True

    def post_query(self, args, kwargs):
        if len(args) > 0:
            return self.get_query(args, kwargs)
        if not isinstance(kwargs, dict):
            return []
        requestor = kwargs.get('requestor', None)
        fields = kwargs.get('fields', None)
        kwargs = convert_dates(kwargs)
        if 'cltool' in kwargs and kwargs['cltool']:
            extra = self.clSource
        else:
            extra = self.apiSource
        self.queryLogger.info('%s - %s - %s', str(kwargs), str(fields), requestor, extra=extra)
        return self.query('file', **kwargs)

    # Wrapper around the following function in order to bring the same query:{dict} convention to this api
    @restful.doc('Queries JAMO to see if files are safely in JAMO/Tape (either copied to the repository or fully backed up).  Returns an array of matching files: {file_name, _id, file_safe_in_jamo(T/F), file_path, file_id}.  Note that file_safe_in_jamo is a derived key/value and does not exist in the database.')
    @restful.validate({'query': {'type': dict, 'doc': 'The JSON key:value to search on',
                                 'example': '{"metadata.jat_key":"AUTO-4600"}'}})
    def post_queryfilesaved(self, args, kwargs):
        if len(args) > 0 and not isinstance(kwargs, dict):
            return []
        query = kwargs['query']
        return self.post_queryfilesafe('file', query)

    # This function has been given out to a few people. we'll need to get them to switch before combining the two post_queryfilesa* functions
    def post_queryfilesafe(self, args, kwargs):
        if len(args) > 0 and not isinstance(kwargs, dict):
            return []
        if 'limit' not in kwargs:
            # If `limit` is not explicitly passed as a keyword arg, default to fetching all records.
            kwargs['limit'] = None
        rows = self.query('file', **kwargs)
        time.sleep(self.query_penalty)
        ret = []
        for row in rows:
            record = self.getResults(row, ['_id', 'file_name', 'file_path', 'file_id'])
            record['file_safe_in_jamo'] = restful.run_internal('tape', 'get_filesafe', record['file_id'])
            ret.append(record)
        return ret

    def post_checkdata(self, args, kwargs):
        return self.processStores(kwargs)[0]

    @restful.validate({'file_type': {'type': str}, 'description': {'type': str}})
    def post_filetype(self, args, kwargs):
        return self.insert('file_type', kwargs)

    @restful.single
    # @restful.generatedhtml(title="Record {{value}}")
    @restful.template('metadata_viewer.html')
    def get_file(self, args, kwargs):
        if len(args[0]) == 24 and ObjectId.is_valid((args[0])):
            return self.query('file', _id=ObjectId(args[0]))
        else:
            # Changing to get around some abuses by internal pipelines
            return {}

    @restful.raw
    def get_download(self, args, kwargs):
        if len(args[0]) == 24 and ObjectId.is_valid((args[0])):
            data = self.query('file', _id=ObjectId(args[0]))
        data = data[0]
        if 'file_path' in data:
            file_path = os.path.join(data['file_path'], data['file_name'])
        else:
            file_path = data['file']

        if not os.path.exists(file_path):
            mount = self.config.dm_archive_mount_by_division[data['division']]
            root = self.config.dm_archive_root_by_division[data['division']]
            file_path = file_path.replace(root, mount)

        if not os.path.exists(file_path):
            raise common.HttpException(404, f'File {args[0]} not accessible to download')

        self.logger.debug(f"Serving up {file_path}")
        return cherrypy.lib.static.serve_download(file_path, name=os.path.basename(file_path))


    @restful.doc('Adds or updates a datastore. If you are attempting to update a store that doesn\'t belong to you, you will get a 403 error. When a file is added that has metadata with a key that is a data store key a GET call will be made to the datastore url replacing {{value}} with the value that was given to the metadata in the file. The results of this call will then be stored in a subdocument of the metadata document under the key that is specified by the identifier parameter')
    @restful.passreq
    @restful.validate(
        {'key': {'type': str, 'doc': 'The key that will be used to check if see if a file should use this data store.'},
         'url': {'type': str,
                 'doc': 'The url that will be called when a data store is triggered. Use {{value}} to identify where the value of the key should go.',
                 'example': 'http://geneusprod.jgi-psf.org:8180/pluss/sow-segments/{{value}}/sow-item'},
         'identifier': {'type': str, 'doc': 'The key that the data will be stored under in the metadata document.'},
         'map': {'required': False, 'type': dict, 'validator': {'*': {'type': dict, 'validator': {
             'new_key': {'type': str, 'required': False}, 'use': {'type': bool, 'required': False},
             'extract': {'type': bool, 'required': False}}}}}, 'only_use_map': {'type': bool, 'required': False},
         'conform_keys': {'type': bool, 'required': False}, 'ignore_null': {'type': bool, 'required': False}},
        allowExtra=False)
    @restful.permissions('add_store')
    def post_datastore(self, args, kwargs):
        user = kwargs['__auth']
        del kwargs['__auth']
        kwargs['owner'] = user['group']
        if kwargs['key'] in self.stores:
            stores = self.stores[kwargs['key']]
            for tmp in stores:
                if tmp['identifier'] == kwargs['identifier']:
                    kwargs['_id'] = tmp['_id']
                    current_owner = tmp['owner']
                    if kwargs['owner'] != current_owner:
                        raise common.HttpException(403, 'You are attempting to update a service you do not own. Change the key value to create a new one')
                    self.save('data_store', kwargs)
                    tmp = kwargs
                    return tmp['_id']
        id = self.save('data_store', kwargs)
        if kwargs['key'] in self.stores:
            self.stores[kwargs['key']].append(kwargs)
        else:
            self.stores[kwargs['key']] = [kwargs]
        return id

    @restful.validate({'name': {'type': str}, 'callback': {'type': str}})
    def post_dataservice(self, args, kwargs):
        # TODO: This call to `save` won't work, since kwargs is being unpacked and the method expects a dict.
        return self.save('data_service', **kwargs)

    @restful.permissions('admin')
    def post_removedatastore(self, args, kwargs):
        if kwargs['key'] in self.stores:
            nStores = []
            for store in self.stores[kwargs['key']]:
                if store['identifier'] != kwargs['identifier']:
                    nStores.append(store)
            if len(nStores) == 0:
                del self.stores[kwargs['key']]
            else:
                self.stores[kwargs['key']] = nStores
            return self.remove('data_store', {'_id': ObjectId(kwargs['_id'])})

    @restful.single
    def get_datastore(self, args, kwargs):
        return self.query('data_store', identifier=args[0])

    @restful.menu('stores')
    @restful.permissions('admin')
    @restful.table(title='Data stores',
                   map={'key': {'order': 0}, 'identifier': {'order': 1}, 'url': {'order': 2}, '_id': {'order': 3},
                        'delete': {'type': 'html',
                                   'value': restful.Button('Remove', post_removedatastore, '_id', 'key')}},
                   onlyshowmap=True)
    def get_datastores(self, args, kwargs):
        if kwargs is not None:
            return self.query('data_store', **kwargs)
        return self.query('data_store')

    @restful.permissions('admin')
    def post_togglesubscriptions(self, args, kwargs):
        if 'Enabled' in kwargs:
            enabled = kwargs['Enabled']
        elif 'enabled' in kwargs:
            enabled = kwargs['enabled']

        if enabled.lower() == 'false':
            value = False
        elif enabled.lower() == 'true':
            value = True

        try:
            self.update('subscriptions', {'_id': kwargs['_id']}, {'$set': {'Enabled': not (value)}})
        except Exception:
            raise common.HttpException(403, "Couldn't update the subscription record")
        self.createDecisionTree()

    @restful.queryResults({'title': 'file Subscriptions',
                           'table': {'columns': [['name', {}],
                                                 ['group', {}],
                                                 ['user', {}],
                                                 ['type', {}],
                                                 ['url', {}],
                                                 ['Enabled', {'type': 'toggle',
                                                              'inputs': {'method': 'POST',
                                                                         'url': '/api/metadata/togglesubscriptions',
                                                                         'data': {'_id': '_id',
                                                                                  'Enabled': 'Enabled'}}}],
                                                 ]},
                           'data': {'default_query': ''}})
    @restful.menu('subscriptions')
    @restful.permissions('admin')
    def get_subscriptions_menu(self, args, kwargs):
        if kwargs is not None:
            if kwargs.get('queryResults', None):
                ret = self.queryResults_dataChange(kwargs, 'subscriptions')
            else:
                ret = self.query('subscriptions', **kwargs)
        else:
            ret = self.query('subscriptions')
        return ret

    @restful.permissions('admin')
    def delete_file(self, args, kwargs):
        file_path, file_name = os.path.split(kwargs['file'])
        self.remove('file', {'file_name': file_name, 'file_path': file_path})
        restful.run_internal('tape', 'delete_file', file=kwargs['file'])

    @restful.template(template="find_file.html", title="File search")
    def get_search(self, args, kwargs):
        return {"fields": metadata_search_options_basic, 'services': self.process_services}

    def getSimpleDateString(self, date):
        now = datetime.datetime.now()
        if date + datetime.timedelta(hours=11) > now:
            return date.strftime("%I:%M %p").lower()
        if date + datetime.timedelta(days=365) > now:
            return date.strftime("%b %w")
        return date.strftime("%d/%m/%Y")

    def post_search(self, args, kwargs):
        query = kwargs['query']
        mQuery = toMongoObj(query)
        if len(args) == 0:
            page = self.pagequery('file', mQuery, return_count=50, sort=('modified', -1))
        else:
            page = self.get_nextpage(args, kwargs)
        ret = {"start": page['start'], 'end': page['end'], 'record_count': page['record_count'], 'records': [], 'cursor_id': page['cursor_id']}
        for record in page['records']:
            ret['records'].append({'selected': False, 'file_name': record['file_name'], 'added_date': self.getSimpleDateString(record['added_date']), 'desc': record['file_path']})
        return ret

    def add_update(self, args, kwargs):
        self.events.append(('file_update', args))
        self.startEventThread()

    def runUpdate(self):
        try:
            while 1:
                event, data = self.events.popleft()
                if event == 'file_update':
                    what, fields = data
                    if len(what) > 0 and fields is not None:
                        self.smartUpdate('file', what, {'$set': fields})
                elif event == 'add':
                    subscriptions = self.subscriptionTree.test(data)
                    if subscriptions is None:
                        continue
                    updates = {}
                    for subscription in subscriptions:
                        url = self.subscriptionMap[subscription]
                        try:
                            curl.post(url + '/', data=data)
                            updates['_subscriptions.%s.called_new' % subscription] = datetime.datetime.now()
                        except Exception:
                            self.logger.critical('failed to call subscription %s' % subscription)
                            updates['_subscriptions.%s.new_failed' % subscription] = datetime.datetime.now()

                    if len(updates) > 0:
                        self.smartUpdate('file', {'_id': data['_id']}, {'$set': updates})
        except IndexError:
            pass
        except Exception:
            self.logger.critical('something happened')
        with self.updateLock:
            self.updateThreadRunning = False

    def post_importproduction(self, args, kwargs):
        for file in kwargs['files']:
            if len(file) < 20:
                raise common.ValidationError('you have passed in a bad metadata id of %s' % file)
            newObj = curl.get('%s/api/metadata/file/%s' % (self.config.prod_url, file))
            try:
                newObj['_id'] = ObjectId(newObj['_id'])
                self.save('file', newObj)
            except Exception:
                self.logger.error("post_importproduction error processing file %s" % str(file))

    @restful.doc('Initiate subscription calls for a set of files.  Used for testing or rerunning subscription code on failed submissions.', public=False)
    @restful.permissions('admin')
    @restful.validate({'files': {'type': list, 'doc': 'The list of file _id to process.  Code will skip any file where at least one subscription has been successfully called (field _subscriptions.*.called_new)',
                                 'validator': {'*': {'type': str}},
                                 'example': "['5c12babc46d1e64a530fba95']"},
                       'override': {'type': bool,
                                    'doc': 'Fire subscriptions for all files, not just entries that are missing a called_new',
                                    'required': False}})
    @restful.passreq(True)
    def post_testsubscription(self, args, kwargs):
        # args: files - list of file._id to run subscriptions on
        #       override: true/false - run subscription regardless
        override = False
        if 'override' in kwargs:
            if kwargs['override'] is True:
                override = True
        for file in kwargs['files']:
            fileObj = self.get_file([file], None)
            if fileObj is None:
                fileObj = curl.get('%s/api/metadata/file/%s' % (self.config.prod_url, file))
                fileObj['_id'] = ObjectId(fileObj['_id'])
                self.save('file', fileObj)
            else:
                fileObj['_id'] = ObjectId(fileObj['_id'])
            # check to see if we've run before for any subscription
            run_subscription = True
            if '_subscriptions' in fileObj:
                for fired_event in fileObj['_subscriptions']:
                    if 'called_new' in fileObj['_subscriptions'][fired_event]:
                        run_subscription = False
            if run_subscription or override:
                self.addEvent('add', fileObj)

    def startEventThread(self):
        with self.updateLock:
            if self.updateThreadRunning is False:
                self.updateThreadRunning = True
                self.updateThread = threading.Thread(target=self.runUpdate)
                self.updateThread.start()

    @restful.menu('process services')
    @restful.table(title='Process Services')
    def get_processservices(self, args, kwargs):
        return self.query('process_services')

    @restful.permissions('add_service')
    @restful.passreq
    @restful.validate({'name': {'type': str}, 'callback': {'type': str}})
    def post_processservice(self, args, kwargs):
        user = kwargs['__auth']
        del kwargs['__auth']
        kwargs['owner'] = user['group']
        if kwargs['name'] in self.process_services:
            kwargs['_id'] = self.process_services[kwargs['name']]['_id']
            current_owner = self.process_services[kwargs['name']]['owner']
            if kwargs['owner'] != current_owner:
                raise common.HttpException(403, 'You are attempting to update a service you do not own. Change the key value to create a new one')
        id = self.save('process_services', kwargs)
        kwargs['_id'] = ObjectId(id)
        self.process_services[kwargs['name']] = kwargs
        return id

    @restful.doc('Save user settings for the specified user. This must be called before a user can start to use JAMO in order to establish where their files will go', public=False)
    @restful.permissions('admin')
    @restful.validate({'user': {'type': str, 'doc': 'The user name that a token has been issued to'},
                       'relative_root': {'type': str,
                                         'doc': 'The root that all of this users file will go. This path must be owned by the account running `dt-service`'}})
    def post_user(self, args, kwargs):
        if kwargs['user'] in self.userSettings:
            kwargs['_id'] = ObjectId(self.userSettings[kwargs['user']]['_id'])
        self.save('user', kwargs)
        self.userSettings[kwargs['user']] = kwargs
        return kwargs

    def get_users(self, args, kwargs):
        return self.query('user')

    def safeMerge(self, origin, new, replace_with_null=True):
        updates = 0
        for key, value in new.items():
            if isinstance(value, dict) and key in origin:
                updates += self.safeMerge(origin[key], value, replace_with_null)
            else:
                if replace_with_null or (value is not None and value != 'null' and value != 'None' and value != ''):
                    if origin.get(key, None) != value:
                        updates += 1
                        origin[key] = value
        return updates

    @restful.permissions('admin')
    @restful.validate({'where': {'type': dict}, 'keep': {'type': list, 'validator': {'*': {'type': str}}}}, allowExtra=False)
    def post_registerupdate(self, args, kwargs):
        where = kwargs['where']
        keep = kwargs['keep']
        _page = 1
        total_count = 0
        updated = 0
        self.store_cache.clear()
        while _page:
            where['_page'] = _page
            records = self.query('file', **where)
            count = 0
            for record in records:
                metadata = record['metadata']
                new_metadata = {}
                for key in keep:
                    if key in metadata:
                        new_metadata[key] = metadata[key]

                meta, failed = self.processStores(new_metadata)
                if self.safeMerge(metadata, meta):
                    record['_id'] = ObjectId(record['_id'])
                    record['modified_date'] = record['metadata_modified_date'] = datetime.datetime.now()
                    self.save('file', record)
                    updated += 1
                count += 1
            total_count += count
            if count == 500:
                _page += 1
            else:
                _page = 0
        return 'processed %d records, modified %d records' % (total_count, updated)

    @restful.menu("Jamo Keys")
    @restful.template(template="smrt_keys_dictionary.html", title="Jamo Keys")
    def get_keys_dictionary(self, args, kwargs):
        return self.query('keys_dictionary')

    @restful.validate({'data': {'type': dict, 'doc': 'Key/value pairs to update'}})
    def put_update_keys(self, args, kwargs):
        self.logger.info("data = %s" % str(kwargs['data']))
        try:
            self.db['keys_dictionary'].update({"_id": kwargs['_id']}, {'$set': kwargs['data']})
        except Exception as e:
            self.logger.critical("[put_update_keys] Error updating the document: %s" % str(e))
            raise common.HttpException(400, 'Error during updating the record %s' % str(e))
        return {'status': 'ok'}

    def get_check_keys(self, args, kwargs):
        search_id = kwargs['_id']
        if search_id == "":
            self.logger.critical("Please provide ID to check")
            result = -1
        else:
            result = self.db['keys_dictionary'].count_documents({'_id': search_id})
            self.logger.info("Id count : %s" % re)
        if result <= 0:
            raise common.HttpException(400, 'Invalid _id')
        return result

    def get_parent_keys(self, args, kwargs):
        cursor = self.db['new_keys'].find({'_id': {'$regex': r'^metadata\..*?\.'}})
        ret = {}
        i = 0
        for rec in cursor:
            id = rec['_id']
            i = i + 1
            parent = id.split('.')[1]
            if parent in ret:
                ret[parent] = ret[parent] + 1
            else:
                ret[parent] = 1
        self.logger.info('%d - number of parent level keys = %d' % (i, len(ret.keys())))
        return ret

    @restful.template(template="keys.html", title="Metadata Keys")
    def get_keys(self, args, kwargs):
        query = {}
        if len(args) > 0:
            query = {'_id': {"$regex": args[0], "$options": "i"}}
        cursor = self.db['jamo_keys'].find(query)
        ret = []
        for rec in cursor:
            ret.append(rec)
        return ret

    def get_files(self, args, kwargs):
        if 'ids' in kwargs:
            if isinstance(kwargs['ids'], str):
                ids = kwargs['ids'].split(',')
            else:
                ids = kwargs['ids']
            ids = list(map(ObjectId, ids))
            return self.query('file', **{'_id': {'$in': ids}})
        return self.query('file', **kwargs)

    def exchangeKeys(self, data, what_to):
        if isinstance(data, list):
            ret = []
            for item in data:
                ret.append(self.exchangeKeys(item, what_to))
            return ret
        if isinstance(data, dict):
            ret = {}
            for key, value in data.items():
                new_key = key
                for f, t in what_to.items():
                    new_key = new_key.replace(f, t)
                ret[new_key] = self.exchangeKeys(value, what_to)
            return ret
        return data

    @restful.passreq(True)
    @restful.validate({'name': {'type': str}, 'description': {'type': str}, 'filter': {'type': dict}, 'url': {'type': str}})
    def post_subscription(self, args, kwargs):
        record = self.db.subscriptions.find_one({'name': kwargs['name'], 'type': 'metadata'})
        kwargs['type'] = 'metadata'
        kwargs['filter'] = self.exchangeKeys(kwargs['filter'], {'$': '#', '.': '>'})
        if record is None:
            if 'Enabled' not in kwargs:  # Default for new subscription is 'Enabled = True'
                kwargs['Enabled'] = True
            id = self.save('subscriptions', kwargs)
            self.createDecisionTree()
            return {'subscription_id': id}
        else:
            kwargs['_id'] = record['_id']
            if record['user'] != kwargs['user']:
                raise common.HttpException(403, 'You are not the owner of this subscription and can not edit it')
            # subscription record already exists then default is 'Enabled'= True
            if 'Enabled' not in record and 'Enabled' not in kwargs:
                kwargs['Enabled'] = True
            else:
                kwargs['Enabled'] = record['Enabled']
            self.save('subscriptions', kwargs)
            self.createDecisionTree()
            return {'subscription_id': kwargs['_id']}

    def get_subscriptions(self, args, kwargs):
        ret = self.query('subscriptions', **kwargs)
        if ret is not None:
            return self.exchangeKeys(ret, {'#': '$', '>': '.'})
        return ret

    # @JGI specific
    @restful.validate({'files': {'type': list, 'validator': {'*': {'type': 'oid'}}}})
    def post_duid(self, args, kwargs):
        duid = str(uuid.uuid1()).replace('-', '')
        cur_time = datetime.datetime.now()
        self.update('file', {'_id': {'$in': kwargs['files']}}, {
            "$push": {"metadata.portal.duid": duid},
            '$set': {'modified_date': cur_time, 'metadata_modified_date': cur_time}
        })
        return {'url': 'http://genome.jgi-psf.org/pages/dynamicOrganismDownload.jsf?organism=duid&duid=%s' % duid, 'duid': duid}

    @restful.validate({'identifier': {'type': str}, 'values': {'type': (str, list)}, 'fields': {'type': (str, list), "required": False}})
    def post_projectfiles(self, args, kwargs):
        pq_kwargs = {"query": {kwargs['identifier']: {
            "$in": list(map(self.checkNumeric, kwargs['values'])) if isinstance(kwargs['values'], list) else [
                self.checkNumeric(kwargs['values']), ]},
            "file_type": {"$ne": "folder"},
            "obsolete": {"$exists": False}}}
        if 'fields' in kwargs:
            pq_kwargs["fields"] = kwargs['fields'] if isinstance(kwargs['fields'], list) else [kwargs['fields'], ]
        result = restful.run_internal('metadata', 'post_pagequery', **pq_kwargs)
        result = sorted(result, key=functools.cmp_to_key(lambda x, y: (x['file_name'] > y['file_name']) - (x['file_name'] < y['file_name'])))
        return {"files": result}

    # @JGI specific
    # May be obsolete after the publishing flag functions come in, should be removed along with projectfiles_old.html
    @restful.validate({'identifier': {'type': str}, 'values': {'type': str}})
    @restful.template('projectfiles.html', title='Project Files')
    @restful.passreq(True, True)
    def get_projectfiles(self, args, kwargs):
        records = restful.run_internal('metadata', 'post_pagequery',
                                       fields=['_id', 'file_id', 'metadata.sequencing_project_id',
                                               'metadata.sequencing_project.sequencing_project_name',
                                               'metadata.library_name', 'file_name',
                                               'metadata.portal.display_location',
                                               'metadata.publish_to', 'file_type', 'group'],
                                       query='%s in (%s) and file_type != "folder" and obsolete exists false' % (kwargs['identifier'],
                                                                                                                 kwargs['values']))
        perm = 0
        if 'set_portal' in restful.run_internal('core', 'get_userpermissions', kwargs['user'] + '@lbl.gov'):
            perm = 1

        return {'files': records, 'display_location_cv': self.display_location_cv,
                'publishing_flags': publishing_flags, 'perms': perm}

    def checkNumeric(self, value):
        ret = value
        try:
            ret = int(value)
        except ValueError:
            pass
        try:
            ret = float(value)
        except ValueError:
            pass
        return ret

    @restful.template('distributionsetter.html', title='Distribution Properties')
    @restful.permissions('set_portal')
    @restful.menu('Distribution Properties')
    def get_distributionproperties(self, args, kwargs):
        # These data are needed for the HTML template
        return {"search_fields": search_fields, "publishing_flags": publishing_flags, "display_location_cv": self.display_location_cv}

    @restful.doc('Sets the portal display location for a given metadata oid')
    @restful.passreq
    @restful.permissions('set_portal')
    @restful.validate({'path': {'type': list, 'example': ['Raw Data'],
                                'doc': 'The path that this file should go onto in portal. /assembly/one would be represented by ["assembly","one"], do not include the file name in this list as the file name is appended to the location automatically',
                                'validator': {'*': {'type': str}}}}, argsValidator=[
        {'name': 'file_id', 'type': 'oid', 'doc': 'The metadata id in the form of an objectid'}], allowExtra=False)
    def put_portallocation(self, args, kwargs):
        self.logger.info("[put_portallocation] got loc = %s for %s " % (str(kwargs['path']), args[0]))
        current = self.query('file', _id=ObjectId(args[0]))
        if len(current) == 0:
            raise Exception("[put_publishingflags] No such ID exists: %s" % args[0])
        current_location = current[0]['metadata']['portal']['display_location'] if 'portal' in current[0]['metadata'] and 'display_location' in current[0]['metadata']['portal'] else []
        current_loc_str = '/'.join(current_location)
        new_loc_str = '/'.join(kwargs['path'])
        if current_loc_str != new_loc_str:
            cur_time = datetime.datetime.now()
            new_update = {"user": kwargs['__auth']['user'], "on": cur_time, "display_location": {"from": current_loc_str, "to": new_loc_str}}
            self.logger.info("[put_portallocation] updating %s with %s" % (args[0], str(new_update)))
            self.update('file', {'_id': ObjectId(args[0])}, {
                '$set': {'metadata.portal.display_location': kwargs['path'], 'modified_date': cur_time, 'metadata_modified_date': cur_time},
                '$push': {'__update_publish_to': new_update}
            })

    @restful.doc('remove the portal display location from this record')
    @restful.passreq
    @restful.validate(argsValidator=[{'name': 'metadata_id', 'type': 'oid'}])
    @restful.permissions('set_portal')
    def delete_portallocation(self, args, kwargs):
        self.logger.info("[delete_portallocation] for _id = %s" % args[0])
        current = self.query('file', _id=args[0])
        if len(current) == 0:
            raise Exception("[delete_portallocation] No such ID exists: %s" % args[0])
        if 'portal' in current[0]['metadata'] and 'display_location' in current[0]['metadata']['portal']:
            current_location = current[0]['metadata']['portal']['display_location']
        else:  # nothing to delete
            return
        current_path = '/'.join(current_location)
        cur_time = datetime.datetime.now()
        update = {"user": kwargs['__auth']['user'], "on": cur_time, "display_location": {"from": current_path, "to": ""}}
        self.logger.info("[delete_portallocation] updating %s with %s" % (args[0], str(update)))
        self.update('file', {'_id': args[0]}, {
            '$unset': {'metadata.portal.display_location': ''},
            '$set': {'modified_date': cur_time, 'metadata_modified_date': cur_time},
            '$push': {'__update_publish_to': update}
        })

    @restful.doc('Sets the PMO publishing flags for an individual file (given metadata oid).  For JAT submissions, use the JAT api for setting publishing flags.')
    @restful.passreq
    @restful.permissions('set_portal')
    @restful.validate({'flags': {'type': (list, str), 'example': ['foo', 'bar'],
                                 'doc': 'The applications that will be viewing the file',
                                 'validator': {'*': {'type': str, 'options': publishing_flags}}},
                       'operation': {'type': str, 'required': False, 'example': 'add',
                                     'options': ['add', 'remove', 'update'],
                                     'doc': 'Specify how to treat existing flags. "add" will add the the specified flags to existing flags, "remove" will remove the specified flags from the existing flags, and "update" will overwrite the existing flags with the specified flags. Default is "update"'}},
                      argsValidator=[
                          {'name': 'file_id', 'type': 'oid', 'doc': 'The metadata id in the form of an objectid'}],
                      allowExtra=False)
    def put_publishingflags(self, args, kwargs):
        flags = [kwargs['flags'], ] if isinstance(kwargs['flags'], str) else kwargs['flags']
        self.logger.info("[put_publishingflags] got flags = %s for %s" % (str(flags), args[0]))
        if len(flags) == 0:
            return
        update = []
        change_list = []
        current_flags = []
        current = self.query('file', _id=ObjectId(args[0]))
        if len(current) == 0:
            raise Exception("[put_publishingflags] No such ID exists: %s" % args[0])
        elif 'obsolete' in current[0]:
            raise Exception("[put_publishingflags] ID %s is obsolete" % args[0])
        current_flags = current[0]['metadata']['publish_to'] if 'publish_to' in current[0]['metadata'] else []
        prev_flags = current_flags

        if 'operation' in kwargs and kwargs['operation'] != 'update':
            if kwargs['operation'] == 'remove':
                if len(current_flags) != 0:
                    update = list(filter(lambda x: x not in flags, current_flags))
            elif kwargs['operation'] == 'add':
                update = list(current_flags)
                update.extend(filter(lambda x: x not in current_flags, flags))
        else:
            update = list(flags)
            current_flags = []

        for x in update:
            if (x in prev_flags):
                prev_flags.remove(x)
            else:
                change_list.append("+" + x)
        if (len(prev_flags)):
            change_list.extend(map(lambda x: "-" + x, prev_flags))
        cur_time = datetime.datetime.now()
        update_publishto = {"user": kwargs['__auth']['user'], "on": cur_time, "publish_to": change_list}
        if len(update) != len(current_flags):
            if len(update) == 0:
                self.logger.info("[put_publishingflags] deleting publish_to from %s" % (args[0]))
                restful.run_internal('metadata', 'delete_publishingflags', args[0], permissions=['admin'], __auth={'user': 'sdm', 'group': 'sdm'})
            else:
                self.logger.info("[put_publishingflags] updating %s with %s" % (args[0], str(update_publishto)))
                self.update('file', {'_id': ObjectId(args[0])}, {
                    '$set': {'metadata.publish_to': update, 'modified_date': cur_time, 'metadata_modified_date': cur_time},
                    '$push': {'__update_publish_to': update_publishto}
                })

    @restful.doc('remove the portal display location from this record')
    @restful.validate(argsValidator=[{'name': 'metadata_id', 'type': 'oid'}])
    @restful.passreq
    @restful.permissions('set_portal')
    def delete_publishingflags(self, args, kwargs):
        # self.logger.info('[delete_publishingflags] for %s'%args[0])
        current = self.query('file', _id=args[0])
        if len(current) == 0:
            raise Exception("[delete_publishingflags] No such ID exists: %s" % args[0])
        if 'publish_to' in current[0]['metadata']:
            current_flags = current[0]['metadata']['publish_to']
        else:
            return
        cur_time = datetime.datetime.now()
        change_list = list(map(lambda x: "-" + x, current_flags))
        update = {"user": kwargs['__auth']['user'], "on": cur_time, "publish_to": change_list}
        self.logger.info("[delete_publishingflags] updating %s with %s" % (args[0], str(update)))
        self.update('file', {'_id': args[0]}, {
            '$unset': {'metadata.publish_to': ''},
            '$set': {'modified_date': cur_time, 'metadata_modified_date': cur_time},
            '$push': {'__update_publish_to': update}
        })

    @restful.permissions('metadata_update')
    @restful.validate({'*': {'type': '*', 'doc': 'Any metadata to update'}}, argsValidator=[{'name': 'id', 'type': 'oid', 'doc': 'The metadata id in the form of an objectid'}])
    def put_safeupdate(self, args, kwargs):
        # TODO: delete this method?
        print(args, kwargs)

    # @JGI specific
    search2_options = {'file_name': {'label': 'File Name', 'type': 'string'},
                       'metadata.analysis_project_id': {'label': 'Analysis Project Id', 'type': 'number'},
                       'metadata.final_deliv_project_id': {'label': 'Final Deliv Project Id', 'type': 'number'},
                       'metadata.gold_data.gpts_proposal_id': {'label': 'GPTS Proposal ID', 'type': 'number'},
                       'metadata.gold_data.img_oid': {'label': 'IMG OID', 'type': 'number'},
                       'metadata.gold_data.its_proposal_id': {'label': 'ITS Proposal ID', 'type': 'number'},
                       'metadata.img.taxon_oid': {'label': 'Taxon OID', 'type': 'number'},
                       'metadata.library_name': {'label': 'Library Name', 'type': 'string'},
                       'metadata.parent_library_name': {'label': 'Parent Library Name', 'type': 'string'},
                       'metadata.portal.identifier': {'label': 'Portal Identifier', 'type': 'string'},
                       'metadata.sample_name': {'label': 'Sample Name', 'type': 'string'},
                       'metadata.sequencing_project_id': {'label': 'SPID', 'type': 'number'},
                       }

    @restful.queryResults({'title': 'file search',
                           'table': {'columns': [['file_name', {'type': 'link', 'title': 'Name',
                                                                'inputs': {'text': '{{file_name}}',
                                                                           'title': 'Metadata for {{file_name}}\n\nMetadata Id: {{_id}}',
                                                                           'url': '/metadata/file/{{_id}}'}}],
                                                 ['metadata.jat_key', {'type': 'link', 'title': 'JAT Key',
                                                                       'inputs': {'url': '/analysis/analysis/{{metadata.jat_key}}'}}],
                                                 ['metadata.library_name', {'title': 'Library'}],
                                                 ['metadata.sequencing_project_id', {'title': 'SPID'}],
                                                 ['metadata.sequencing_project.sequencing_project_name', {'title': 'Project Name'}],
                                                 ['file_path', {'title': 'Path'}],
                                                 ['file_type', {'title': 'Type'}],
                                                 ['group', {'title': 'Group'}],
                                                 ['added_date', {'title': 'Date Added'}],
                                                 ['file_size', {'title': 'Size (B)', 'type': 'number'}]],
                                     'multi_select': {'actions': [{'name': 'Restore files',
                                                                   'callback': '/api/tape/grouprestore',
                                                                   'id_return': 'files',
                                                                   'user_return': 'requestor'}],
                                                      'additional_info': '[[file_size]] / 1000000 MB'}},
                           'filter': {'options': search2_options, 'saved_queries': 'metadata-search2'},
                           'data': {}})
    @restful.menu('search')
    def get_search2(self, args, kwargs):
        if kwargs and kwargs.get('queryResults', None):
            return self.queryResults_dataChange(kwargs, 'file')
        else:
            return None

    @restful.raw
    def get_htmltemplate(self, args, kwargs):
        path = os.path.realpath(os.path.join(self.location, 'templates', '/'.join(args)))
        if not path.startswith(os.path.join(self.location, 'templates')):
            raise common.HttpException(404, 'You have gone to an invalid url')
        cherrypy.response.headers['Content-Type'] = "text/html"
        with open(path, 'rb') as f:
            return f.read()

    @restful.doc('Returns the distinct list of non-null values of key.  Note that this is an expensive operation and should be used sparingly.  Also the result-set is limited to 16MB')
    @restful.validate(argsValidator=[{'name': 'metadata_key', 'type': str, 'doc': 'The metadata key to search for'}], allowExtra=False)
    def get_distinct(self, args, kwargs):
        self.logger.info("Calling distinct on %s" % (args[0]))
        return self.db['file'].distinct(args[0], filter={args[0]: {"$ne": None}})


    # @JGI specific
    @restful.doc('Adds new elements to portal location, this will only work if portal location is already a dict and not a list')
    @restful.permissions('mod_portal')
    @restful.validate({'*': {'doc': 'A dictonary whose keys are the map of identifier and the value is a list of the location path', 'type': list, 'validator': {'*': {'type': str}}}}, [{'name': 'file_id', 'type': 'oid'}])
    def put_portalpath(self, args, kwargs):
        setting = {}
        for key, value in kwargs.items():
            if key == '':
                raise common.HttpException(400, 'Sorry you can not pass a blank key')
            setting['metadata.portal.display_location.%s' % key] = value
        return self.smartUpdate('file', {'_id': args[0]}, {'$set': setting, '$addToSet': {'metadata.portal.identifier': {'$each': list(kwargs)}}})

    # @JGI specific
    @restful.doc('Removes an element from portal location, the portal location must be used as a dict and not a list in this case.')
    @restful.permissions('mod_portal')
    @restful.validate(argsValidator=[{'name': 'file_id', 'type': 'oid'}, {'name': 'portal_identifier', 'type': str}])
    def delete_portalpath(self, args, kwargs):
        return self.smartUpdate('file', {'_id': args[0]}, {'$unset': {'metadata.portal.display_location.%s' % args[1]: ''}})

    @restful.validate({'query': {'type': dict}, 'update': {'type': dict}})
    @restful.passreq(True, True)
    def post_safeupdate(self, args, kwargs):
        if 'admin' not in kwargs['permissions']:
            raise common.HttpException(403, 'you do not have access to run this method')
        return self.smartUpdate('file', kwargs['query'], kwargs['update'])

    @restful.permissions('admin')
    @restful.validate({'query': {'type': (str, dict)}}, allowExtra=False)
    def post_delete(self, args, kwargs):
        records = restful.run_internal('metadata', 'post_pagequery', **kwargs)
        tape_records = 0
        for record in records:
            data = restful.run_internal('tape', 'delete_file', record['file_id'])
            tape_records += data['tape_records']
            record['_tape_data'] = data['tape_data']
            record['file_status'] = 'PURGED'
            self.save('deleted_file', record)
            self.remove('file', {'_id': record['_id']})
        return {'file_records': len(records), 'tape_records': tape_records}

    @restful.permissions('admin')
    @restful.validate({'query': {'type': (str, dict)}}, allowExtra=False)
    def post_undelete(self, args, kwargs):
        kwargs['source'] = 'deleted_file'
        records = restful.run_internal('metadata', 'post_pagequery', **kwargs)
        tape_records = 0
        for record in records:
            if '_tape_data' in record:
                data = restful.run_internal('tape', 'post_undelete_file', record['_tape_data'])
                tape_records += data['tape_records']
                del record['_tape_data']
            self.save('file', record)
            self.remove('deleted_file', {'_id': record['_id']})
        return {'file_records': len(records), 'tape_records': tape_records}

    # @JGI specific
    @restful.cron('5', '0', '*', '*', '*')
    def queue_wip_metadata_refresh(self) -> None:
        """Queue WIP update tasks to the database. These tasks will be executed by `update_queued_metadata_refresh`.
        """
        lastupdate = self.getSetting('wip_update', datetime.datetime.today().strftime('%Y-%m-%d'))
        curdate = datetime.datetime.today().strftime('%Y-%m-%d')
        for service in self.config.wip_updates:
            try:
                ids_to_update = curl.get(
                    f'{service.get("service")}?entity={service.get("entity")}&updated_since={lastupdate}')
            except Exception as e:
                self.logger.error(f'refresh wip failed while working on entity {service.get("entity")}: {str(e)}')
                return
            keep = service.get('keep')
            for key in ids_to_update:
                self._queue_metadata_update_task(service.get('key'), key, keep, 'wip')
        self.saveSetting('wip_update', curdate)

    # @JGI specific
    @restful.cron('5', '1', '*', '*', '*')
    def queue_dus_metadata_refresh(self) -> None:
        """Queue DUS update tasks to the database. These tasks will be executed by `update_queued_metadata_refresh`.
        """
        lastupdate = self.getSetting('dus_update', datetime.datetime.today().strftime('%Y-%m-%d'))
        curdate = datetime.datetime.today().strftime('%Y-%m-%d')
        for service in self.config.dus_updates.get('services'):
            try:
                url = self.config.dus_updates.get('url').replace('{{entity}}', service.get('entity')).replace(
                    '{{date}}', lastupdate)
                records_to_update = curl.get(url)
            except Exception as e:
                self.logger.error(
                    f'refresh data utilization status failed while working on entity {service.get("entity")}: {str(e)}')
                return
            keep = service.get('keep')
            for rec in records_to_update:
                self._queue_metadata_update_task(service.get('key'), rec.get(service.get('entity_key')), keep, 'dus')
        self.saveSetting('dus_update', curdate)

    # @JGI specific
    @restful.cron('5', '2', '*', '*', '*')
    def queue_mycocosm_metadata_refresh(self) -> None:
        """Queue Mycocosm update tasks to the database. These tasks will be executed by
        `update_queued_metadata_refresh`.
        """
        lastupdate = self.getSetting('mycocosm_update', datetime.datetime.today().strftime('%Y-%m-%d'))
        curdate = datetime.datetime.today().strftime('%Y-%m-%d')
        for service in self.config.mycocosm_updates:
            try:
                ids_to_update = [entry.get('portal_id') for entry in curl.get(f'{service.get("service")}/{lastupdate}')]
            except Exception as e:
                self.logger.error(f'refresh mycocosm failed: {str(e)}')
                return
            keep = service.get('keep')
            for key in ids_to_update:
                self._queue_metadata_update_task(service.get('key'), key, keep, 'mycocosm')
        self.saveSetting('mycocosm_update', curdate)

    # @JGI specific
    @restful.cron('5', '3', '*', '*', '*')
    def update_queued_metadata_refresh(self) -> None:
        """Execute queued metadata refresh tasks. Delete tasks from the database as tasks get completed.
        """
        records = self.query('metadata_refresh', limit=None)
        for record in records:
            where = {record.get('key_name'): record.get('key_value')}
            keep = record.get('keep')
            update_results = self.post_registerupdate(args=None, kwargs={'keep': keep, 'where': where})
            self.logger.info(
                f'refresh metadata: key_name({record.get("key_name")}) key_value({record.get("key_value")}) source_process({record.get("source_process")}) - {update_results}')
            self.remove('metadata_refresh', {'_id': record.get('_id')})

    # @JGI specific
    # @restful.cron('5', '0', '*', '*', '*')
    def refresh_wip_data(self):
        """
        Refresh Work In Progress system data.
        Order of services in the config is intentional, as the relationship between lower and higher entities in the hierarchy might change.
        """
        lastupdate = self.getSetting('wip_update', datetime.datetime.today().strftime("%Y-%m-%d"))
        curdate = datetime.datetime.today().strftime('%Y-%m-%d')
        for service in self.config.wip_updates:
            try:
                ids_to_update = curl.get('%s?entity=%s&updated_since=%s' % (service['service'], service['entity'], lastupdate))
            except Exception as e:
                self.logger.error('refresh wip failed while working on entity %s: %s' % (service['entity'], str(e)))
                return
            keep = service['keep']
            n = len(ids_to_update)
            i = 1
            self.logger.info('refresh wip: start %s, %s ids to update' % (service['entity'], n))
            for key in ids_to_update:
                where = {service['key']: key}
                retvalue = self.post_registerupdate(args=None, kwargs={'keep': keep, 'where': where})
                self.logger.info('refresh wip: rec %d/%d - %s %s: %s' % (i, n, service['entity'], key, retvalue))
                i += 1
        self.saveSetting('wip_update', curdate)

    # @JGI specific
    # @restful.cron('5', '3', '*', '*', '*')
    def refresh_dus_data(self):
        """
        Refresh Data Utilization Status data.
        Run after WIP, as refresh_wip_data may change the relationship between SPID/APID and the FD.
        If these do change in WIP, the data store calls made above will trigger calls to the DUS data stores so there so there shouldn't be any inconsistencies.
        """
        lastupdate = self.getSetting('dus_update', datetime.datetime.today().strftime("%Y-%m-%d"))
        curdate = datetime.datetime.today().strftime('%Y-%m-%d')
        for service in self.config.dus_updates['services']:
            try:
                url = self.config.dus_updates['url'].replace('{{entity}}', service['entity']).replace('{{date}}', lastupdate)
                records_to_update = curl.get(url)
            except Exception as e:
                self.logger.error('refresh data utilization status failed while working on entity %s: %s' % (service['entity'], str(e)))
                return
            n = len(records_to_update)
            i = 1
            self.logger.info('refresh dus: start %s, %s ids to update' % (service['entity'], n))
            keep = service['keep']
            for rec in records_to_update:
                key = rec[service['entity_key']]
                where = {service['key']: key}
                retvalue = self.post_registerupdate(args=None, kwargs={'keep': keep, 'where': where})
                self.logger.info('refresh dus: rec %d/%d - %s %s: %s' % (i, n, service['entity'], key, retvalue))
                i += 1
        self.saveSetting('dus_update', curdate)

    def _extract_keys(self, data, store_map, conform_keys, original_doc):
        """Extract any keys that are not associated with datastore keys (key if simple, first element if composite) so
        that we can properly call the datastores without depending on dictionary iteration order, which in PY2 is considered
        arbitrary, so if the iteration order changes with Python implementation (as in PY3), it breaks the code since not
        all key parts may have been extracted.

        :param dict data: Data retrieved associated with datastore identifier
        :param dict store_map: Mapping of keys to extract associated with datastore
        :param bool conform_keys: Whether to conform the keys extracted
        :param dict original_doc: Original data
        :return: dict with extracted keys
        """
        extracted_keys = {}
        for map_name in store_map:
            name = map_name
            if map_name.count(">") > 0:
                dir = map_name.split(">")[:-1]
                for level in dir:
                    if level not in data:
                        break
                    data = data[level]
                name = map_name.split('>')[-1]

            if name in data:
                mapped_key = name if 'new_key' not in store_map.get(map_name) else store_map.get(map_name).get(
                    'new_key')
                mapped_value = data.get(name)
                if conform_keys:
                    mapped_key, mapped_value = self.conform(mapped_key, data.get(name))
                if 'extract' in store_map.get(map_name) and store_map.get(map_name).get('extract'):
                    # Skip the extract if skip_if_exists key exists in the original metadata doc
                    if 'skip_extract_if_exists' in store_map.get(map_name) and store_map.get(map_name).get(
                            'skip_extract_if_exists') in original_doc:
                        pass
                    else:
                        extracted_keys[mapped_key] = mapped_value
        return extracted_keys

    def _queue_metadata_update_task(self, key_name: str, key_value: str, keep: List[str], source_process: str) -> None:
        """Add metadata update task to `metadata_refresh` table.

        :param key_name: Key name to be used for querying documents to have metadata updated
        :param key_value: Key value to be used for querying document to have metadata updated
        :param keep: List of attribute names to persist from original metadata
        :param source_process: Process that is queuing the update task
        """
        payload = {'key_name': key_name, 'key_value': key_value, 'keep': keep,
                   'dt_modified': datetime.datetime.today(), 'source_process': source_process}
        record = self.query('metadata_refresh', key_name=key_name, key_value=key_value)
        if not record:
            self.insert('metadata_refresh', payload)
