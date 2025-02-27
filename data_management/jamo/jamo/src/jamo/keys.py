from __future__ import print_function
from pymongo import MongoClient
import os
import sys


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + '[].')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def usage():
    print("python2 keys.py [file|analysis]")
    print("environment variables 'database', 'host', 'user', and 'password' also need to be set")
    exit()


def main():
    if not all(name in os.environ for name in ('host', 'user', 'password', 'database')):
        print("Environment variables host, user, password, and database must be defined")
        exit()

    file_keys = {}
    database = os.environ['database']
    client = MongoClient(os.environ['host'])
    client[database].authenticate(os.environ['user'], os.environ['password'])
    db = client[database]

    if len(sys.argv) < 2 or ('file' not in sys.argv and 'analysis' not in sys.argv):
        usage()
    else:
        if 'file' in sys.argv:
            source_db = 'file'
            dest_db = 'jamo_keys'
            date_string = 'added_date'
        elif 'analysis' in sys.argv:
            source_db = 'analysis'
            dest_db = 'jat_keys'
            date_string = 'added_date'
        else:
            usage()

    for doc in db[source_db].find():
        added_date = doc[date_string]
        group = doc['group']
        if 'template_name' in doc['metadata']:
            template = doc['metadata']['template_name']
        else:
            template = 'None'

        for key, value in flatten_json(doc).items():
            if '[' in key and key.endswith('.[]'):
                key = key[:len(key) - 3]
            val_type = type(value).__name__
            if val_type == 'unicode':
                val_type = 'string'
            if val_type == 'NoneType':
                val_type = 'None'
            if key not in file_keys:
                file_keys[key] = {"record_count": 1, "first_seen": added_date, "last_seen": added_date, "groups": [group], "types": [val_type], "templates": [template]}
            else:
                file_keys[key]['record_count'] += 1
                if val_type not in file_keys[key]['types']:
                    file_keys[key]['types'].append(val_type)
                if group not in file_keys[key]['groups']:
                    file_keys[key]['groups'].append(group)
                if template not in file_keys[key]['templates']:
                    file_keys[key]['templates'].append(template)
                if added_date > file_keys[key]['last_seen']:
                    file_keys[key]['last_seen'] = added_date
                if added_date < file_keys[key]['first_seen']:
                    file_keys[key]['first_seen'] = added_date

    # Add in the indexes
    for key, value in db[source_db].index_information().items():
        field = value['key'][0][0]
        if field in file_keys:
            file_keys[field]['is_indexed'] = 1
    # Save the work
    db.temp_keys.delete_many({})
    for key, value in file_keys.iteritems():
        db.temp_keys.insert_one({'_id': key, 'value': value})
    db.temp_keys.rename(dest_db, dropTarget=True)


if __name__ == '__main__':
    main()
