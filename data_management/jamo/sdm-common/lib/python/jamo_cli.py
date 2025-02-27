#!/usr/bin/env python
from __future__ import print_function
from builtins import map
from builtins import range
from builtins import object
import difflib
import getpass
import importlib
import json
import os
import sdm_curl
import sys
import yaml
import time
from bson import ObjectId
from jamo_common import expose, customtransform, toMongoObj, tokenize, parse_jamo_query, PageList
from mathparser import NumericStringParser

nsp = NumericStringParser()
file_types = {
    'raw_normal': {'query': {'metadata.fastq_type': 'sdm_normal', 'user': 'sdm'},
                   'description': 'Filters for only sdm\'s normal fastqs'},
    'all': {'query': {}, 'description': 'Does not filter for any file type'},
    'filtered': {'query': {'metadata.fastq_type': 'filtered', 'user': 'rqc'},
                 'description': 'Filters for only rqc\'s filtered fastqs'}
}

queries = {
    'library': {'query': {'metadata.library_name': {'$in': '$args'}}, 'description': 'Searches for library name'},
    'plibrary': {'query': {'metadata.parent_library_name': {'$in': '$args'}},
                 'description': 'Searches for the children of a library name'},
    'filename': {'query': {"file_name": {"$in": "$args"}}, 'description': 'Match against the file names'},
    'custom': {'query': None, 'description': 'Run a custom mongodb query, pass in a json string'},
    'org': {'query': {"$or": [{"metadata.gold_data.display_name": {"$regex": ".*$args.0.*", "$options": "i"}}, {
        "metadata.sow_segment.sequencing_project_name": {"$regex": ".*$args.0.*", "$options": "i"}}]},
            'description': 'Search for an organism'},  # noqa: E126
    'ntid': {'query': {"metadata.gold_data.ncbi_taxon_id": {'$in': 'int($args)'}},
             'description': 'Search for a ncbi taxon id'},
    'spid': {'query': {'metadata.sequencing_project_id': {'$in': 'int($args)'}},
             'description': 'Searches for files for given sequencing project id'},
    'apid': {'query': {'metadata.analysis_project_id': {'$in': 'int($args)'}},
             'description': 'Searches for files for given ITS analysis project id'},
    'atid': {'query': {'metadata.analysis_task_id': {'$in': 'int($args)'}},
             'description': 'Searches for files for given ITS analysis task id'},
    'ncbip': {'query': {'metadata.gold_data.ncbi_project_name': {'$in': '$args'}},
              'description': 'Searches for files for given ncbi project name'},
    'sample': {'query': {'metadata.sample_name': {'$in': '$args'}},
               'description': 'Searches for files for given sample name'},
    'goldid': {'query': {'metadata.gold_id': {'$in': '$args'}}, 'description': 'Searches for files for given gold id'},
    'pdir': {'query': {'metadata.project_dir_number': {'$in': 'int($args)'}},
             'description': 'Searches for files for given venonat project directory number'},
    'pmoid': {'query': {'metadata.pmo_project_id': {'$in': 'int($args)'}},
              'description': 'Searches for files for given pmo project id'},
    'pbjob': {'query': {'metadata.sdm_smrt_cell.secondary_analysis_job_id': {'$in': 'int($args)'}},
              'description': 'Searches for files for given pacbio secondary analysis job id'},
    'run': {'query': {'metadata.lane': 'int($args.1)', 'metadata.illumina_physical_run_id': 'int($args.0)'},
            'description': 'Searches for all files that were part of the run x lane y'},
    'illumina_run': {'query': {"metadata.illumina_physical_run_id": {"$in": "int($args)"}},
                     'description': 'Find all files that are associated to a given illumina physical run'},
    'id': {'query': {"_id": {"$in": "$args"}}, 'description': 'Find the file that is associated to this metadata_id'}

}
fieldFunctions = {
    'concat': lambda x: ''.join(map(str, x)),
    'length': lambda args: 1 if isinstance(args[0], (int, float)) else len(args[0]),
    'math': lambda args: nsp.eval(' '.join(map(str, args))),
    'int': lambda args: int(args[0]),
    'get': lambda args: getFieldValue(args[1], customtransform(sdm_curl.get(args[0], cache=True)))
}


def getFieldValue(key, values):
    if isinstance(key, tuple):
        args = [getFieldValue(x, values) for x in key[1]]
        return fieldFunctions[key[0]](args)
    if key.startswith('^'):
        return key[1:]
    return values[key]


class JAMO(object):

    def __init__(self):
        self.curl = sdm_curl.Curl(os.environ.get('JAMO_HOST', 'https://jamo.jgi.doe.gov'))
        self.methods = [attr for attr in dir(self) if hasattr(getattr(self, attr), 'expose')]
        self.loadedSettings = False
        self.reports = {}

    def printQueryHelp(self):
        self.loadUserSettings()
        sys.stderr.write('The possible file_types are:\n')
        for file_type in file_types:
            desc = file_types[file_type]['description']
            if len(file_type) < 7:
                file_type += (' ' * (7 - len(file_type)))
            sys.stderr.write(' %s\t%s\n' % (file_type, desc))
        sys.stderr.write('\nThe possible fields are:\n')
        for query in queries:
            desc = queries[query]['description']
            if len(query) < 7:
                query += (' ' * (7 - len(query)))
            sys.stderr.write(' %s\t%s\n' % (query, desc))

    def loadSettingsFile(self, path):
        ret = {}
        with open(path) as f:
            onName = None  # [name of query]
            onValue = {}  # { 'description': str, 'query': str }
            for line in f.readlines():
                line = line.strip()
                if line.startswith('['):
                    if onName is not None:
                        ret[onName] = onValue
                    onName = line[1:-1].strip()
                    onValue = {}
                elif line.count('=') == 1:
                    name, value = line.split('=', 1)
                    onValue[name.strip()] = value.strip()
            if onName is not None:
                ret[onName] = onValue
        return ret

    def loadUserSettings(self):
        if self.loadedSettings:
            return
        self.loadedSettings = True
        jamohome = os.path.join(os.path.expanduser('~'), '.jamo')
        if os.path.isdir(jamohome):
            if os.path.isfile(os.path.join(jamohome, 'filetypes')):
                for name, value in self.loadSettingsFile(os.path.join(jamohome, 'filetypes')).items():
                    if 'query' not in value:
                        sys.stderr.write('warning: your file filter named %s is missing the query parameter\n' % name)
                        continue
                    if 'description' not in value:
                        sys.stderr.write('warning: your file filter named %s is missing the description parameter\n' % name)
                        continue
                    try:
                        value['query'] = json.loads(value['query'])
                    except ValueError as e:  # noqa: F841
                        sys.stderr.write('error: failed to parse the query value "%s" as a JSON value\n' % value['query'])
                        continue

                    file_types[name] = value
            '''Copy the code above for now.. I will probably want these to be different since they could have different options...'''
            if os.path.isfile(os.path.join(jamohome, 'queries')):
                for name, value in self.loadSettingsFile(os.path.join(jamohome, 'queries')).items():
                    if 'query' not in value:
                        sys.stderr.write('warning: your query named %s is missing the query parameter\n' % name)
                        continue
                    if 'description' not in value:
                        sys.stderr.write('warning: your query named %s is missing the description parameter\n' % name)
                        continue
                    try:
                        value['query'] = json.loads(value['query'])
                    except ValueError as e:  # noqa: F841
                        sys.stderr.write('error: failed to parse the query value "%s" as a JSON value\n' % value['query'])
                        continue
                    queries[name] = value
            if os.path.isfile(os.path.join(jamohome, 'reports')):
                with open(os.path.join(jamohome, 'reports')) as f:
                    for appPath in f.readlines():
                        appPath = appPath.strip()
                        if os.path.isfile(appPath):
                            try:
                                with open(appPath):
                                    spec = importlib.util.spec_from_file_location(os.path.basename(appPath), appPath)
                                    app = importlib.util.module_from_spec(spec)
                                    spec.loader.exec_module(app)
                            except Exception as e:  # noqa: F841
                                sys.stderr.write('''warning: the report '%s' failed to load: %s\n''' % (appPath, e))
                            else:
                                for name in dir(app):
                                    method = getattr(app, name)
                                    if hasattr(method, 'expose'):
                                        self.reports[name] = method

            if os.path.isfile(os.path.join(jamohome, 'report.py')):
                file = os.path.join(jamohome, 'report.py')
                with open(file):
                    try:
                        spec = importlib.util.spec_from_file_location('report.py', file)
                        app = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(app)
                    except ValueError as e:  # noqa: F841
                        sys.stderr.write('''warning: %s/report.py in failed to load\n''' % jamohome)
                        #   raise
                    else:
                        for name in dir(app):
                            method = getattr(app, name)
                            if hasattr(method, 'expose'):
                                self.reports[name] = method

    def getAnalysisRoot(self):
        cwd = os.getcwd()
        while not os.path.exists(os.path.join(cwd, '.jamo/id')):
            cwd = os.path.dirname(cwd)
            if cwd == '/':
                return None
        return cwd

    @expose('Prints info for the files that are returned for a query')
    def info(self, args):
        if len(args) == 0 or args[0] == 'help' or (len(args) == 1 and args[0] in file_types):
            sys.stderr.write('Usage: jamo info [file_type] field <args>\n\n')
            self.printQueryHelp()
            sys.exit(2)

        md5loc = args.index('-md5') if '-md5' in args else 0
        if md5loc > 0:
            args.remove('-md5')
        query = self.getQuery(args)
        modifier = self.getQueryModifier(args, 'all')
        if modifier is None:
            modifier = "_id"
        files = PageList(self.curl.post('api/metadata/pagequery', query=query, cltool=True,
                                        fields=[modifier, 'file_name', 'file_path', 'file_status', 'current_location', 'md5sum'],
                                        requestor=getpass.getuser()),
                         self.curl, link=self.link_single)
        for file in files:
            if 'current_location' in file:
                file_location = file['current_location']
            else:
                file_location = str(file.get('file_path', '')) + '/' + str(file.get('file_name', ''))
            print(self.getMetadataValue(modifier, file), file_location, file['file_status'], file['_id'], file['md5sum'] if md5loc > 0 else '')
        if files.total_record_count == 0:
            print('No matching records found')

    @expose('Retrieves files from jamo. Max number of files per call is 500')
    def fetch(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('Usage: jamo fetch [-d days] [-w] [-s source_data_center] [file_type] field <args>\n')
            sys.stderr.write('\t-d: the number of days you would like this file to be restored for. Optional must come right after fetch\n'
                             '\t-w: Wait till the files are restored.\n'
                             '\t-s source_data_center: source data center identifier (e.g., igb, dori) - optional\n'
                             '\tNote: there is a maximum of 500 files that may be fetched per call.\n')
            self.printQueryHelp()
            sys.exit(2)
        days = 90
        isBusyWait = False
        source = None
        if args[0] == '-d':
            days = int(args[1])
            args = args[2:]
        if '-w' in args:
            isBusyWait = True
            args.remove('-w')
        if '-s' in args:
            idx = args.index('-s')
            source = args[idx + 1]
            # Remove the argument flag for data center source and its corresponding value.
            args.pop(idx + 1)
            args.pop(idx)
        query = self.getQuery(args)
        files = PageList(self.curl.post('api/metadata/pagequery', query=query, cltool=True, requestor=getpass.getuser()),
                         self.curl, link=self.link_single)
        ids = []
        n_files = 0
        for file_data in files:
            n_files += 1
            if n_files <= 500:
                ids.append(file_data['_id'])
                if 'current_location' in file_data:
                    file_location = file_data['current_location']
                else:
                    file_location = file_data['file_path'] + '/' + file_data['file_name']
                print(file_location, file_data['file_status'], file_data['_id'])
        self.curl.post('api/tape/grouprestore', files=ids, days=days, requestor=getpass.getuser(), source=source)
        while len(ids) != 0 and isBusyWait:
            print("Waiting on " + str(len(ids)) + " file(s) to restore. Will check in 5 minutes")
            time.sleep(5 * 60)
            files = self.curl.post('api/metadata/query', data={'_id': {'$in': ids}})
            for file_data in files:
                if file_data['file_status_id'] != 12:
                    if 'current_location' in file_data:
                        file_location = file_data['current_location']
                    else:
                        file_location = file_data['file_path'] + '/' + file_data['file_name']
                    ids.remove(file_data['_id'])
                    print("completed restore for : " + file_location, file_data['file_status'], file_data['_id'])
        if n_files > 500:
            print('Only first 500 files restored')
        elif n_files == 0:
            print('No matching records found')
        return

    def getMetadataValue(self, key, data):
        if key is None:
            return ''
        for depth in key.split('.'):
            if isinstance(data[depth], dict):
                data = data[depth]
            else:
                return str(data[depth]).replace(' ', '_')

    def getKeys(self, tokens):
        ret = []
        for token in tokens:
            if isinstance(token, tuple):
                ret.extend(self.getKeys(token[1]))
            else:
                ret.append(token)
        return ret

    @expose('Runs a custom report with the returned metadata')
    def report(self, args):
        self.loadUserSettings()
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('Usage: jamo report name [file_type] field <args>\n\n')
            sys.stderr.write('The possible reports are:\n')
            for report in self.reports:
                sys.stderr.write(' %s\t%s\n' % (report, self.reports[report].description))
            sys.stderr.write('\n')
            self.printQueryHelp()
            sys.exit(2)
        if args[0] == 'select':
            where_loc = args.index('where') if 'where' in args else None
            if where_loc is None or where_loc >= len(args) - 1:
                sys.stderr.write('Sorry you must provide a query after the \'where\' keyword\n')
                sys.exit(1)
            outt = 'txt'
            if args[-2] == 'as':
                outt = args[-1]
                args = args[:-2]
            fields = tokenize(''.join(args[1:where_loc]))
            queries = self.getQueries(['custom'] + args[where_loc + 1:])
            for query in queries:
                files = PageList(
                    self.curl.post('api/metadata/pagequery', query=query, cltool=True, fields=self.getKeys(fields), requestor=getpass.getuser()),
                    self.curl)
                if outt in ('json', 'yaml'):
                    outlis = []
                    for file_data in files:
                        tmp = {}
                        for field in fields:
                            tmp[field] = getFieldValue(field, file_data)
                        outlis.append(tmp)
                    if outt == 'yaml':
                        print(yaml.safe_dump(outlis, default_flow_style=False))
                    elif outt == 'json':
                        print(json.dumps(outlis))
                else:
                    # This currently doesn't work well for fields with embedded commas
                    deli = "," if outt == 'csv' else '\t'
                    if outt in ('csv', 'tab'):
                        sys.stdout.write(deli.join(fields) + '\n')
                    for file in files:
                        ptab = False
                        for field in fields:
                            if ptab:
                                sys.stdout.write(deli)
                            else:
                                ptab = True
                            sys.stdout.write(u'{}'.format(getFieldValue(field, file)))
                        sys.stdout.write('\n')
            return
        if args[0] not in self.reports:
            sys.stderr.write('Sorry there is no report named %s\n' % args[0])
            sys.exit(2)
        reportMethod = self.reports[args[0]]
        query = self.getQuery(args[1:])
        files = PageList(self.curl.post('api/metadata/pagequery', query=query, cltool=True, requestor=getpass.getuser()), self.curl, link=self.link_single)
        reportMethod(files)

    @expose('list all the keys that match your query')
    def keys(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jamo keys [-v | -vv] <search string>\n')
            sys.stderr.write('\t-v: verbose, provides stats about keys, (list is tab-delimited)\n')
            sys.stderr.write('\t-vv: same as above, also shows where keys are contained in arrays of sub-documents, (shows as [])\n')
            sys.exit(2)
        if args[0] == '-v' or args[0] == '-vv':
            if len(args) == 2:
                query = args[1]
            else:
                query = ""
            if args[0] == '-vv':
                verbose = 2
            else:
                verbose = 1
            print("%-80s\t%s\t%s\t%s\t%-20s\t%-20s\t%s" % ('Key', '# Records', 'First Seen', 'Last Seen', 'Data Types', 'Groups', 'Templates'))
        else:
            query = args[0]
            verbose = 0
        for row in sorted(self.curl.get('api/metadata/keys/%s' % query), key=lambda d: d['_id']):
            key = row['_id']
            if 'is_indexed' in row['value']:
                key += " *"
            if verbose:
                if verbose == 1:
                    key = key.replace('.[]', '')
                value = row['value']
                print("%-80s\t%8d\t%s\t%s\t%-20s\t%-20s\t%s" % (key, value['record_count'],
                                                                value['first_seen'][:10],
                                                                value['last_seen'][:10],
                                                                ", ".join(sorted(value['types'])),
                                                                ", ".join(sorted(value['groups'])),
                                                                ", ".join(sorted(value['templates']))))
            else:
                print(key.replace('.[]', ''))

    @expose('Shows all the metadata for a specific metadata id')
    def show(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jamo show <metadata_id | path_to_file | [file_type] field <args>>\n\n')
            sys.exit(2)
        if len(args) == 1:
            try:
                ObjectId(args[0])
                data = self.curl.get('api/metadata/file/%s' % args[0])
            except Exception:
                file = os.path.realpath(args[0])
                info = self.curl.get('api/tape/latestfile', file=file)
                if info is None or info['metadata_id'] is None:
                    sys.stderr.write('sorry no record was found in jamo for file: \'%s\'\n' % file)
                    sys.exit(2)
                data = self.curl.get('api/metadata/file/%s' % info['metadata_id'])
            print(yaml.safe_dump(data, default_flow_style=False))
        else:
            query = self.getQuery(args)
            files = self.curl.post('api/metadata/pagequery', query=query, cltool=True, requestor=getpass.getuser())
            if files['record_count']:
                for record in files['records']:
                    print(yaml.safe_dump(record, default_flow_style=False))
            else:
                print("No matching records found")

    def link_single(self, file, destination, autoRestore=True, source=None):
        if 'current_location' in file:
            rFilePath = file['current_location']
        else:
            rFilePath = os.path.join(file['file_path'], file['file_name'])
        rFilePath = self._replace_nersc_dm_archive_root(source, rFilePath)
        if not os.path.exists(destination):
            os.symlink(rFilePath, destination)
        self.curl.post('api/tape/grouprestore', files=[file['_id']], requestor=getpass.getuser(), source=source)

    @expose('Retrieves files from jamo and links them in the current folder')
    def link(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jamo link [-s source_data_center] [file_type] field <args>\n'
                             '\tNote: there is a maximum of 500 files that may be fetched/linked per call.\n')
            self.printQueryHelp()
            sys.exit(2)
        source = None
        if '-s' in args:
            idx = args.index('-s')
            source = args[idx + 1]
            # Remove the argument flag for data center source and its corresponding value.
            args.pop(idx + 1)
            args.pop(idx)
        query = self.getQuery(args)
        files = PageList(self.curl.post('api/metadata/pagequery', query=query, cltool=True, requestor=getpass.getuser()),
                         self.curl, link=self.link_single)
        ids = []
        modifier = self.getQueryModifier(args)
        purged_ids = []
        analysis_root = self.getAnalysisRoot()
        if analysis_root is not None:
            inputFile = open(os.path.join(analysis_root, '.jamo/inputs'), 'a')
        n_files = 0
        links = []
        for file_data in files:
            n_files += 1
            if n_files <= 500:
                ids.append(file_data['_id'])
                mValue = self.getMetadataValue(modifier, file_data)
                if modifier is not None and modifier not in ('file_name'):
                    to_file = mValue + '.' + file_data['file_name']
                else:
                    to_file = file_data['file_name']
                if to_file in links:
                    to_file = file_data['_id'] + '_' + to_file
                links.append(to_file)
                self.link_single(file_data, to_file, False, source)
                if 'current_location' in file_data:
                    file_location = file_data['current_location']
                else:
                    file_location = file_data['file_path'] + '/' + file_data['file_name']
                print(mValue + ' ' + file_location, file_data['file_status'], file_data['_id'])
                purged_ids.append(file_data['_id'])
                if analysis_root is not None:
                    cwd = os.getcwd()
                    inputFile.write(cwd[len(analysis_root) + 1:] + '/' + to_file + ',' + file_data['_id'] + '\n')
        if analysis_root is not None:
            inputFile.close()
        if n_files > 500:
            print("Only first 500 files fetched/linked")
        elif n_files == 0:
            print('No matching records found')
        self.curl.post('api/tape/grouprestore', files=purged_ids, requestor=getpass.getuser(), source=source)

    def toInt(args):
        if isinstance(args, list):
            return list(map(int, args))
        return int(args)

    def toObjectId(args):
        if isinstance(args, list):
            return list(map(ObjectId, args))
        return ObjectId(args)

    def openFile(args):
        with open(args[0]) as f:
            return [line.strip() for line in f.readlines()]

    def stdin(args):
        return [line.strip() for line in sys.stdin.readlines()]

    useableMethods = {
        'int': toInt,
        'oid': toObjectId,
        'file': openFile,
        'stdin': stdin,
    }

    def parse(self, what, args):
        curr = ''
        on = 0
        ret = None
        for x in range(len(what)):
            char = what[x]
            if char == '(':
                subCurr = ''
                depth = 0
                for i in range(on + 1, len(what)):
                    schar = what[i]
                    if schar == ')' and depth == 0:
                        break
                    else:
                        subCurr += schar
                        if schar == '(':
                            depth += 1
                        elif schar == ')':
                            depth -= 1
                method = ''
                for i in range(len(curr)):
                    on = len(curr) - i - 1
                    if curr[on] in (',', ' ', ')') or on == 0:
                        method = curr[on:]
                        break
                if method not in self.useableMethods:
                    raise Exception('method: %s is not a valid method which was called in a query' % method)
                mArgs = self.parse(subCurr, args)
                ret = self.useableMethods[method](mArgs)
                return ret
            elif char == ',':
                if not isinstance(ret, list):
                    ret = [ret]
                ret.append(curr)
            elif char == '$':
                var = ''
                pCount = 0
                for i in range(on + 1, len(what)):
                    cChar = what[i]
                    if cChar in (' ', ','):
                        break
                    if cChar == '.':
                        pCount += 1
                        if pCount > 1:
                            break
                    var += cChar
                if var == 'args':
                    return args
                elif var.startswith('args.'):
                    index = int(var.split('.')[-1])
                    if len(args) <= index:
                        sys.stderr.write(
                            'error: The query you have selected does not have enough arguments passed into it\n')
                        sys.exit(1)
                    return args[index]
                continue

            else:
                curr += char
            on += 1
        if ret is None:
            return curr
        elif isinstance(ret, list):
            ret.append(curr)
        else:
            return ret

    def replaceQuery(self, query, args):
        if isinstance(query, str):
            if query.count('$args') > 0:
                return self.parse(query, args)
            else:
                return query
        elif isinstance(query, bool):
            return query
        elif isinstance(query, dict):
            ret = {}
        elif isinstance(query, list):
            ret = []
        for key in query:
            if isinstance(query, dict):
                ret[key] = self.replaceQuery(query[key], args)
            elif isinstance(query, list):
                ret.append(self.replaceQuery(key, args))
        return ret

    def getQuery(self, args):
        self.loadUserSettings()
        query = {}
        if args[0] in file_types:
            query = file_types[args[0]]['query']
            args = args[1:]
        elif args[0] != 'id':
            query = file_types['raw_normal']['query']
        if len(args) == 0:
            sys.stderr.write('error: you need to pass in a query to search for\n')
            sys.exit(2)
        if args[0] == 'custom':
            jsonD = parse_jamo_query(args[1:])
            if jsonD.startswith('{'):
                jsonD = json.loads(jsonD)
                return jsonD
            else:
                return toMongoObj(jsonD)
        if args[0] not in queries:
            sys.stderr.write('''jamo info: '%s' is not an option.  Run 'jamo info help' for a list of all queries\n''' % args[0])
            sys.exit(2)
        query.update(self.replaceQuery(queries[args[0]]['query'], args[1:]))
        return query

    def detokenize(self, token):
        if isinstance(token, tuple):
            funcname, args = token
            if funcname not in self.useableMethods:
                raise Exception('Method: %s is not a valid function' % funcname)
            return self.useableMethods[funcname](self.detokenize(args))
        elif isinstance(token, list):
            ret = []
            for tok in token:
                val = self.detokenize(tok)
                if isinstance(val, list):
                    ret.extend(val)
                else:
                    ret.append(val)
            return ret
        else:
            return token

    def getQueries(self, args):
        self.loadUserSettings()
        if args[0] in file_types:
            args = args[1:]
        if len(args) == 0:
            sys.stderr.write('error: you need to pass in a query to search for\n')
            sys.exit(2)
        if args[0] == 'custom':
            jsonD = parse_jamo_query(args[1:])
            if jsonD.startswith('{'):
                jsonD = json.loads(jsonD)
                return [jsonD]
            else:
                tokens = tokenize(jsonD)
                new_tokens = []
                for token in tokens:
                    if isinstance(token, tuple):
                        funcname, args = token
                        if funcname not in self.useableMethods:
                            raise Exception('Method: %s is not a valid function' % funcname)
                        token = self.useableMethods[funcname](self.detokenize(args))
                    new_tokens.append(token)
                for i in range(len(new_tokens)):
                    if isinstance(new_tokens[i], list) and len(new_tokens[i]) > 60:
                        ret = []
                        items = new_tokens[i]
                        for start in range(0, len(items), 50):
                            ret.append(toMongoObj(new_tokens[:i] + [items[start:start + 50]] + new_tokens[i + 1:]))
                        return ret
                return [toMongoObj(new_tokens)]

    def getQueryModifier(self, args, default_filter='raw_normal'):
        if args[0] in file_types:
            args = args[1:]
        if args[0] not in queries:
            sys.stderr.write('''jamo info: '%s' is not an option.  Run 'jamo info help' for a list of all queries\n''' % args[0])
            sys.exit(64)
        if args[0] == 'custom':
            if args[1].startswith('{'):
                temp = json.loads(args[1])
            else:
                temp = toMongoObj(parse_jamo_query(args[1:]))
        else:
            temp = queries[args[0]]['query']
        for key, value in temp.items():
            if isinstance(value, dict):
                for key1, value1 in value.items():
                    if isinstance(value1, str) and value1.count('args') >= 0:
                        return key
            elif isinstance(value, str) and value.count('args') >= 0:
                return key

    def _replace_nersc_dm_archive_root(self, source, path):
        """Replace NERSC's `dm_archive_root` with the remote source's `dm_archive_root` if available. Otherwise this is
        a no-op.

        :param str source: Data center source name (e.g., igb, dori)
        :param str path: Path to replace NERSC's `dm_archive_root` with source's `dm_archive_root`
        """
        if source is None:
            return path
        if not hasattr(self, 'dm_archive_roots'):
            # Cache dm_archive_roots so that we don't have to query the server multiple times
            self.dm_archive_roots = self.curl.get('/api/tape/dm_archive_roots')
        if source not in self.dm_archive_roots:
            return path
        return path.replace(self.dm_archive_roots.get('nersc'), self.dm_archive_roots.get(source))

#   resources = {
#       'report': {'description': 'saves a query to be used later'},
#    }
#
#    @expose('Saves resources like default settings and reports')
#    def save(self, args):
#        if len(args) == 0 or args[0] == 'help':
#            sys.stderr.write('usage: jamo save <resource> <as> <what>\n')
#            sys.stderr.write('The following resources are available to use:\n')
#            for resource in self.resources:
#                sys.stderr.write('  %s:\t%s\n' % (resource, self.resources[resource]['description']))
#            sys.exit(2)
#        if len(args) < 3:
#            sys.stderr.write('invalid usage\n')
#            self.save([])
#        else part never implemented, commenting out function

    @expose('Prints this message')
    def help(self, args):
        sys.stderr.write('usage: jamo <command> [<args>]\n')
        sys.stderr.write('\nThe jamo commands are:\n')
        for method in self.methods:
            sys.stderr.write('%s\t%s\n' % (method, getattr(self, method).description))
        sys.exit(2)

    def run(self, args):
        method = args[0]
        args = args[1:]
        if method not in self.methods:
            sys.stderr.write('''jamo: '%s' is not a jamo command.  Run 'jamo help' for more options\n''' % method)
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes) > 0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n' % meth)
            sys.exit(64)
        if len(args) > 2 and args[-2] == '-f' and os.path.isfile(args[-1]):
            file_name = args[-1]
            args = args[:-2]
            new_args = []
            with open(file_name) as fi:
                for line in fi.readlines():
                    new_args.append(line.rstrip())
            if len(new_args) > 0:
                if len(new_args) > 50:
                    for start in range(0, len(new_args), 50):
                        getattr(self, method)(args + new_args[start:start + 50])
                    return
                else:
                    args.extend(new_args)
        getattr(self, method)(args)


def main():
    args = sys.argv[1:]
    jamo = JAMO()
    if len(args) == 0:
        jamo.help(args)
    else:
        try:
            jamo.run(args)
        except KeyboardInterrupt as e:  # noqa: F841
            sys.stderr.write("Keyboard interrupt, exiting...")
            sys.exit()
#       except Exception as e:
#           sys.stderr.write(", ".join(e))
#           sys.exit()

if __name__ == '__main__':
    main()
