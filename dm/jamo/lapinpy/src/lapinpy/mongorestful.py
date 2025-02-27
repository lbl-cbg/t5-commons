from . import restful
from dateutil import parser
from bson import ObjectId
import cherrypy
from . import common
import re
import datetime
import pymongo
import string
import random
import urllib

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future.utils import iteritems
from future.standard_library import install_aliases
install_aliases()
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


MAXRETURN = 10000


def convertToOID(obj):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(obj, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(obj, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        # _id has come in as a string "ObjectId('<id>')", extract out the OID to convert
        if 'ObjectId' in obj:
            try:
                found = re.search('ObjectId..(.*)..', obj).group(1)
                obj = found
            except Exception:
                pass
        try:
            return ObjectId(obj)
        except Exception:
            return obj
    if isinstance(obj, list):
        return list(map(convertToOID, obj))
    if isinstance(obj, dict):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        return {k: convertToOID(v) for k, v in iteritems(obj)}
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup
        # return {k: convertToOID(v) for k, v in obj.items()}
        ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
    return obj


def set_key(doc, key, value):
    for subkey in key.split('.'):
        if isinstance(doc, dict):
            if subkey not in doc:
                return True
            else:
                doc = doc[subkey]
        elif isinstance(doc, list):
            try:
                doc = doc[int(subkey)]
            except Exception:
                return True
        else:
            return True
    return doc != value


def unset_key(doc, key, value):
    for subkey in key.split('.'):
        if isinstance(doc, dict):
            if subkey not in doc:
                return False
            else:
                doc = doc[subkey]
        elif isinstance(doc, list):
            try:
                doc = doc[int(subkey)]
            except Exception:
                return False
        else:
            return False
    return True


def rename_key(doc, key, value):
    return unset_key(doc, key, value) if key != value else False


def push_value(doc, key, value):
    return True


def add_to_set(doc, key, value):
    for subkey in key.split('.'):
        if isinstance(doc, dict):
            if subkey not in doc:
                return True
            else:
                doc = doc[subkey]
        elif isinstance(doc, list):
            try:
                doc = doc[int(subkey)]
            except Exception:
                return False
        else:
            return False
    if not isinstance(doc, list):
        return False
    if isinstance(value, dict) and '$each' in value:
        for val in value['$each']:
            if val not in doc:
                return True
        return False
    else:
        return value not in doc


update_functions = {
    '$set': set_key,
    '$unset': unset_key,
    '$rename': rename_key,
    '$push': push_value,
    '$addToSet': add_to_set,
}


class MongoRestful(restful.Restful):
    '''A restful class that handles the communication to a mongo server
    '''

    # Thread parameter should not be used (other than by the changes in core
    # This was done to prevent a thread leak.   Ideally core.post_queryResults_dataChange
    # and core.generate_data_info should be moved up the stack so new MongoRestful objects
    # do not need to be created (or insure MongoRestful fully releases all resources on
    # garbage collect).
    # MongoClient opens up a thread pool that persists (close() doesn't seem to address this currently).
    # Minting multiple instances of the class will result in an open file leak and should not be done.
    def __init__(self, host, user, password, db, options=None, thread=True, host_port=None):
        restful.Restful.__init__(self, host, user, password, db, host_port=host_port)
        mongo_options = '?' + urllib.parse.urlencode(options) if isinstance(options, dict) else ''
        if host_port is None:
            host_port = 27017
        if host_port is not None:
            host = "%s:%s" % (host, host_port)
        self.client = pymongo.MongoClient('mongodb://%s:%s@%s/%s%s' % (user, password, host, db, mongo_options))
        self.db = self.client[db]
        self.cursors = {}
        self.thread = thread
        if self.thread:
            self.cleanThread = cherrypy.process.plugins.BackgroundTask(60, self.cleanCursors)
            self.cleanThread.start()
        self.backup_location = None

    def save(self, collection, data):
        '''Saves a record in the collection. If the record contains _id then no new record will be created
        otherwise a new record will be created and the ObjectId will be returned.

        Args:
            collection (str): The name of the collection to save the record in.
            data (dict): The record that you wish to save.

        Returns:
            A string of that represents the ObjectId.
        '''
        return str(self.db[collection].save(data))

    def smartSave(self, collection, data):
        if '_id' in data and collection in ('file', 'analysis'):
            docs = self.query(collection, **{'_id': data['_id']})
            if len(docs) > 0:
                doc = docs[0]
                timestamps = {}
                for key in ('modified_date', 'metadata_modified_date'):
                    if key in doc:
                        if key != 'modified_date':
                            timestamps[key] = doc[key]
                        del doc[key]
                    if key in data:
                        del data[key]
                if doc == data:
                    return str(data['_id'])
                data['modified_date'] = datetime.datetime.now()
                if collection == 'file':
                    if doc.get('metadata') != data.get('metadata'):
                        data['metadata_modified_date'] = data['modified_date']
                    else:
                        data.update(timestamps)
        return self.save(collection, data)

    def stop(self):
        if self.thread:
            self.cleanThread.cancel()
        if self.client:
            self.client.close()

    def get_howami(self, args, kwargs):
        return {'mongo_connection_alive': self.client.alive()}

    def cleanCursors(self):
        ids_to_remove = []
        now = datetime.datetime.now()
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for cursor_id, data in iteritems(self.cursors):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for cursor_id, data in self.cursors.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if (now - data['last_accessed']).total_seconds() > data['timeout']:
                ids_to_remove.append(cursor_id)
        for id in ids_to_remove:
            self.cursors[id]['cursor'].close()
            del self.cursors[id]

    def find(self, collection, what, **kwargs):
        '''Runs a find on the given collection.

        Args:
            collection (str): The collection that contains the records you wish to search
            what (dict): A dictionary that represents a mongo query.

        Returns:
            a cursor

        Example:
            >> self.update('file', {'file_id':123})
        '''
        if '_id' in what:
            what['_id'] = convertToOID(what['_id'])
        return self.db[collection].find(what, **kwargs)

    def update(self, collection, what, data, **kwargs):
        '''Runs an update on the given collection.

        Args:
            collection (str): The collection that contains the records you wish to update.
            what (dict): A dictionary that represents a mongo query.
            data (dict): A dictionary of keys that will get updated.

        Returns:
            A document that contains the status of the operation.

        Example:
            >> self.update('file', {'file_id':123}, {'file_name':'hello'})
        '''
        if '_id' in what:
            what['_id'] = convertToOID(what['_id'])
        return self.db[collection].update(what, data, multi=True, **kwargs)

    def remove(self, collection, what):
        '''Remove all records that match the query in what.

        Args:
            collection (str): The collection that contains the records you wish to update.
            what (dict): A dictionary that represents a mongo query.

        Example:
            >> self.remove('file', {'file_id':123})
        '''
        if len(what) == 0:
            raise Exception('attempt remove all data in collection %s was made' % collection)
        if '_id' in what:
            what['_id'] = convertToOID(what['_id'])
        self.db[collection].remove(what)

    def insert(self, collection, data):
        '''Inserts a new record in the collection.

        Args:
            collection (str): The name of the collection to save the record in.
            data (dict): The record that you wish to save.

        Returns:
            A string of that represents the ObjectId.

        Example:
            >> self.insert('file', {'file_name':'hello'})
        '''
        ret = self.db[collection].insert(data)
        if isinstance(ret, list):
            ret = list(map(str, ret))
        else:
            ret = str(ret)
        return ret

    def getRandomId(self, digits=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(digits))

    def get_nextpage(self, args, kwargs):
        cursor_id = args[0]
        if cursor_id not in self.cursors:
            return None
        session_data = self.cursors[cursor_id]
        return_count = session_data['return_count']
        session_data['start'] = session_data['end'] + 1
        session_data['end'] = min(session_data['start'] + return_count - 1, session_data['record_count'])
        ret = {'start': session_data['start'], 'end': session_data['end'], 'cursor_id': session_data['cursor_id'],
               'record_count': session_data['record_count'], 'records': []}
        i = 0
        modifiers = session_data['modifiers'] if 'modifiers' in session_data else {}
        for record in session_data['cursor']:
            i += 1
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for modifier, func in iteritems(modifiers):
                if isinstance(func, basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for modifier, func in modifiers.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                    # if isinstance(func, str):
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    self.__setvalue(record, modifier, func.replace('{{value}}', str(self.__getvalue(record, modifier))))
                else:
                    self.__setvalue(record, modifier, func(record, self.__getvalue(record, modifier)))
            if session_data['flatten']:
                record = self.flatten(record)
            ret['records'].append(record)
            if i >= return_count:
                break

        if session_data['end'] >= ret['record_count']:
            session_data['cursor'].close()
            del self.cursors[session_data['cursor_id']]
        else:
            session_data['last_accessed'] = datetime.datetime.now()
            self.cursors[session_data['cursor_id']] = session_data
        return ret

    def post_page(self, args, kwargs):
        record_count = kwargs['record_count']
        start = kwargs['start']
        end = min(start + record_count - 1, record_count)
        what = kwargs['query']
        select = kwargs['fields']
        sort = None
        flatten = kwargs['flatten']
        if 'sort' in kwargs:
            sort = kwargs['sort']

        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(what, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(what, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            what = common.toMongoObj(what)
        if what is not None and '_id' in what:
            what['_id'] = convertToOID(what['_id'])
        key_map = None
        # How is this code ever executed?
        if key_map is not None:
            key_types = {}
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(key_map):
                if 'type' in value and isinstance(value['type'], basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in key_map.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                # if 'type' in value and isinstance(value['type'], str):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    key_types[key] = value['type']
            if len(key_types) > 0 and what is not None:
                what = self.encode_values(what, key_types)

        cursor = self.db[kwargs['collection']].find(what, select).skip(start).limit(record_count)
        if sort is not None:
            cursor.sort(*sort)
        record_count = self._count_documents(self.db[kwargs['collection']], what)
        # Why are we comparing the same value?
        return_count = min(MAXRETURN, record_count, record_count)
        # TODO
        ret = {'start': start, 'end': end, 'record_count': record_count, 'records': []}
        i = 0
        for record in cursor:
            i += 1
            '''
            for modifier,func in modifiers.iteritems():
                if isinstance(func, basestring):
                    self.__setvalue(record, modifier, func.replace('{{value}}', str(self.__getvalue(record, modifier)) ))
                else:
                    self.__setvalue(record, modifier, func(record, self.__getvalue(record, modifier) ))
            '''
            if flatten:
                record = self.flatten(record)
            ret['records'].append(record)
            if i >= return_count:
                break
        return ret

    def __getvalue(self, doc, field):
        doc2 = doc
        for sf in field.split('.'):
            if sf in doc2:
                doc2 = doc2[sf]
            else:
                return None
        return doc2

    def __setvalue(self, doc, field, value):
        doc2 = doc
        for sf in field.split('.')[:-1]:
            if sf in doc2:
                doc2 = doc2[sf]
            else:
                doc2[sf] = {}
                doc2 = doc2[sf]
        doc2[field.split('.')[-1]] = value

    def encode_value(self, obj, to_type):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(obj, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(obj, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if to_type == 'oid':
                obj = ObjectId(obj)
            elif to_type == 'date':
                obj = parser.parse(obj)
        elif isinstance(obj, list):
            ret = []
            for value in obj:
                ret.append(self.encode_value(value, to_type))
            return ret
        elif isinstance(obj, dict):
            ret = {}
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(obj):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in obj.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ret[key] = self.encode_value(value, to_type)
            return ret
        return obj

    def encode_values(self, obj, key_types):
        ret = {}
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, value in iteritems(obj):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, value in obj.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if key in key_types:
                value = self.encode_value(value, key_types[key])
            elif isinstance(value, dict):
                value = self.encode_values(value, key_types)
            ret[key] = value
        return ret

    def getUserQueries(self, args, kwargs, page):
        return self.query('queries', **{'page': page, 'user': kwargs['user']})
        # records = self.query('queries', **{'$or':[{'access.public':True},{'access.groups':kwargs['group']},{'access.users':kwargs['user']},{'user':kwargs['user']}]})

    def postUserQuery(self, args, kwargs):
        remove = kwargs['remove_query']
        del kwargs['remove_query']
        if remove:
            return self.remove('queries', kwargs)
        else:
            return self.save('queries', kwargs)

    def update_csv_list(self, records):
        updated_records = []
        # index = 0
        for record in records:
            # if index == 2:
            #    raise Exception(record)
            # else:
            #    index += 1

            for key, value in record.items():
                if isinstance(value, list):
                    record[key] = ['"{}"'.format(','.join(map(str, value)))]
            updated_records.append(record)
        return updated_records

    def queryResults_dataChange(self, parameters, collection):
        what = parameters['query']
        if 'return_count' in parameters:
            return_count = parameters['return_count']
        else:
            return_count = MAXRETURN

        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(what, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(what, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            what = common.toMongoObj(what)
        if what is not None and '_id' in what:
            what['_id'] = convertToOID(what['_id'])

        # TODO: If mongo sort with -1 (desc) starts working, it should replace
        #        this descending related code.
        sort = None
        if 'sort' in parameters and parameters['sort'].strip() != '':
            sort_opt = parameters['sort'].strip().split(' ')
            if sort_opt[1] == 'desc':
                order = -1
            else:
                order = 1
            sort = [(sort_opt[0], order)]

        cursor = self.db[collection].find(what, parameters['fields'], sort=sort)

        record_count = self._count_documents(self.db[collection], what)
        return_count = min(MAXRETURN, return_count, record_count)

        cursor = cursor.skip((parameters['page'] - 1) * return_count).limit(return_count)

        data = []
        for record in cursor:
            data.append(self.flatten(record))

        return {'record_count': record_count,
                'return_count': return_count,
                'data': data}

    def pagequery(self, collection, what=None, select=None, return_count=100,
                  sort=None, modifiers={}, key_map=None, flatten=False, timeout=540):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(what, basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(what, str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            what = common.toMongoObj(what)
        if what is not None and '_id' in what:
            what['_id'] = convertToOID(what['_id'])
        if key_map is not None:
            key_types = {}
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(key_map):
                if 'type' in value and isinstance(value['type'], basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - remove this noqa comment after migration cleanup
            # for key, value in key_map.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                # if 'type' in value and isinstance(value['type'], str):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    key_types[key] = value['type']
            if len(key_types) > 0 and what is not None:
                what = self.encode_values(what, key_types)

        cursor = self.db[collection].find(what, select)
        if sort is not None:
            cursor.sort(*sort)
        cursor_id = self.getRandomId()
        while cursor_id in self.cursors:
            cursor_id = self.getRandomId()
        record_count = self._count_documents(self.db[collection], what)
        return_count = min(MAXRETURN, return_count, record_count)
        session_data = {'start': 1, 'end': return_count, 'cursor_id': cursor_id, 'cursor': cursor,
                        'record_count': record_count, 'return_count': return_count, 'modifiers': modifiers,
                        'fields': select, 'flatten': flatten, 'timeout': timeout}
        ret = {'start': session_data['start'], 'end': session_data['end'], 'cursor_id': cursor_id,
               'record_count': session_data['record_count'], 'records': [], 'fields': select, 'timeout': timeout}
        i = 0
        for record in session_data['cursor']:
            i += 1
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for modifier, func in iteritems(modifiers):
                if isinstance(func, basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for modifier, func in modifiers.iteritems():  # noqa: E115 - remove this noqa comment after migration cleanup
                # if isinstance(func, str):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    self.__setvalue(record, modifier, func.replace('{{value}}', str(self.__getvalue(record, modifier))))
                else:
                    self.__setvalue(record, modifier, func(record, self.__getvalue(record, modifier)))
            if flatten:
                record = self.flatten(record)
            ret['records'].append(record)
            if i >= return_count:
                break

        if session_data['end'] >= ret['record_count']:
            session_data['cursor'].close()
        else:
            session_data['last_accessed'] = datetime.datetime.now()
            self.cursors[cursor_id] = session_data
        return ret

    def flatten(self, record, subkey=None):
        ret = {}
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, value in iteritems(record):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, value in record.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if subkey is not None:
                key = subkey + '.' + key
            if isinstance(value, dict):
                ret.update(self.flatten(value, key))
            else:
                ret[key] = value
        return ret

    def findOne(self, collection, **what):
        '''Find one record that matches the query.

        Args:
            collection (str): The collection that contains the record you are looking for.

        kwargs:
            **what: Any key with a value that will be used in the mongo query.

        Example:
            >> self.findOne('file', file_name='hello')
        '''
        return self.db[collection].find_one(what)

    def query(self, collection, **what):
        """Runs a query and return a list of records. For calls where `tqx` is not passed as a keyword argument, if the
        `limit` keyword is passed it will return up to `limit` records, otherwise defaults to 500. If the argument
        is explicitly set to `None`, then it will return all records.

        Args:
            collection (str): The collection that contains the records you are looking for.

        Keyword Arguments:
            **what: Any key with a value that you are searching for.

        Example:
            >> self.query('file', file_size=10)
        """
        coll = self.db[collection]
        if '_read_preference' in what:
            coll = coll.with_options(read_preference=what['_read_preference'])
            del what['_read_preference']

        if '_id' in what:
            what['_id'] = convertToOID(what['_id'])

        if 'tqx' in what:
            tq = what['tq'].strip()
            arr = tq.split(' ')
            del what['tqx']
            del what['tq']
            tokens = {'limit': 0, 'offset': 0, 'by': None, 'desc': '', 'asc': ''}
            # this would come from the restful.table def on first pass
            # will be replaced by values in tq/tqx
            if 'sort' in what:
                if 'by' not in arr:
                    tokens['by'] = what['sort'][0]
                    if what['sort'][1] == -1:
                        tokens['desc'] = 'True'
                del what['sort']
            for i in range(len(arr)):
                key = arr[i]
                if key in tokens:
                    if len(arr) >= i:
                        tokens[key] = arr[i + 1].strip("'`")
                    else:
                        tokens[key] = 'True'

            offset = int(tokens['offset'])
            limit = int(tokens['limit'])
            cursor = coll.find(what)
            if tokens['by'] is not None:
                cursor = cursor.sort([(tokens['by'], -1 if tokens['desc'] != '' else 1)])
            cursor = cursor[offset:offset + limit]

        else:
            start = 0
            limit = what.pop('limit', 500)
            if '_page' in what:
                if limit is None:
                    raise common.HttpException(400, 'Pagination is not supported if `limit` is set to `None`')
                page = int(what['_page']) - 1
                if page < 0:
                    raise common.HttpException(400, 'page must be a number greater than 0')
                start = page * limit
                del what['_page']
            records = self._count_documents(coll, what)
            end = start + limit if limit is not None else records
            cursor = coll.find(what)
            if end > records:
                end = records
            if start > records:
                return []
            cursor = cursor[start:end]
        ret = []
        for rec in cursor:
            ret.append(rec)
        return ret

    @restful.validate(argsValidator=[{'type': 'oid', 'name': 'update_id'}])
    @restful.permissions('admin')
    def post_approveupdate(self, args, kwargs):
        pass

    def smartUpdate(self, collection, what, update):
        """This method should be used only for updates of 'file' and 'analysis' collections."""
        ret = {'ok': 1.0, 'nModified': 0, 'n': 0}
        ids = []
        update_metadata = False

        for doc in self.find(collection, what):
            ret['n'] += 1
            should_update = False
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(update):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in update.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                if key in update_functions:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    for field, new_value in iteritems(value):
                    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                    # for field, new_value in value.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        if update_functions[key](doc, field, new_value):
                            should_update = True
                            if field == 'metadata' or field.startswith('metadata.'):
                                update_metadata = True
                else:
                    should_update = True
                    update_metadata = True
            if should_update:
                ids.append(doc['_id'])
        if len(ids) > 0:
            if collection in ('file', 'analysis'):
                cur_time = datetime.datetime.now()
                if '$set' not in update:
                    update['$set'] = {}
                update['$set']['modified_date'] = cur_time
                if update_metadata and collection == 'file':
                    update['$set']['metadata_modified_date'] = cur_time
            ret['nModified'] = self.update(collection, {'_id': {'$in': ids}}, update)['nModified']
        return ret

    def exchangeKeys(self, data, what_to):
        if isinstance(data, list):
            ret = []
            for item in data:
                ret.append(self.exchangeKeys(item, what_to))
            return ret
        if isinstance(data, dict):
            ret = {}
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(data):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in data.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                new_key = key
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                for f, t in iteritems(what_to):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # for f, t in what_to.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    new_key = new_key.replace(f, t)
                ret[new_key] = self.exchangeKeys(value, what_to)
            return ret
        return data

    def _count_documents(self, collection, what):
        """Counts the number of documents in the database given the `what` filter. If `what` is `None`, convert it to an
        empty dict (to play nice with the db).

        :param pymongo.database.Database collection: MongoDB database collection
        :param dict[str, str] what: Filter to use when counting number of records
        """
        if what is None:
            what = {}
        return collection.count_documents(what)


# class PageList:
#    '''
#    Pass the result of a pagequery into this to make an iterable over a pagequery result
#
#    e.g:
#        files =  PageList(curl.post('api/metadata/pagequery',query=my_query, fields=my_fields), curl)
#        for f in files:
#            # do something
#    '''
#    def __init__(self, page, service, **methods):
#        '''
#        page: the pagequery result to iterate over
#        curl: the Curl object from which the pagequery was called
#        service: the application the pagequery was called on. This must also have a get_nextpage method
#        methods: I am not sure what this is (ajtritt)
#        '''
#
#        self.page = page
#        self.service = service
#        self.current_list = page['records']
#        self.cursor_id = page['cursor_id']
#        self.total_record_count = page['record_count']
#        self.methods = methods
#
#    def __getitem__(self,item):
#        return customtransform(self.current_list[item],**self.methods)
#
#    def __iter__(self):
#        class Iterat():
#            def __init__(iself):
#                iself.i = -1
#                iself.si = -1
#            def __iter__(iself): return iself
#            def next(iself):
#                if iself.i<self.total_record_count-1:
#                    iself.i += 1
#                    if iself.si+2 > len(self.current_list):
#                        iself.si = -1
#                        self.current_list = restful.run_internal(self.service, 'get_nextpage', self.cursor_id)['records']
#                    iself.si += 1
#                    return customtransform(self.current_list[iself.si],**self.methods)
#                raise StopIteration
#        return Iterat()
