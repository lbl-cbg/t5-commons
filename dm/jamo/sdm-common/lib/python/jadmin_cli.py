#!/usr/bin/env python
from __future__ import print_function
from builtins import input
from builtins import range
from builtins import object
import difflib
import json
import pwd
import os
import sdm_curl
import sys
import stat
import string
import readline
import random
import yaml
import requests
from jamo_common import expose, editJson, editYaml, toMongoSet, getQuery, PageList

readline.set_completer_delims('/')


class FileCompleter(object):
    def __init__(self, rootDir):
        self.rootDir = rootDir
        readline.parse_and_bind("tab: complete")

    def complete(self, text, state):
        if state == 0:
            folder, match = os.path.split(readline.get_line_buffer())
            self.options = [file + ('/' if os.path.isdir(os.path.join(self.rootDir, folder, file)) else '') for file in os.listdir(os.path.join(self.rootDir, folder)) if file.startswith(match) or file == '']
            # print '\n',self.options
            # print readline.get_line_buffer()
        if len(self.options) == 1:
            if self.options[0].endswith('/'):
                self.options.append(self.options[0] + ' ')
        if state < len(self.options):
            return self.options[state]


def loadSettingsFile(path):
    ret = {}
    with open(path) as f:
        onName = None
        onValue = {}
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


class JAdmin(object):
    def __init__(self, kwargs, skipAuth=False):
        self.jamohost = os.environ.get('JAMO_HOST', 'https://jamo.jgi.doe.gov')
        self.curl = sdm_curl.Curl(self.jamohost, retry=0)
        self.methods = []
        self.methodMap = {}
        for attr in dir(self):
            method = getattr(self, attr)
            if hasattr(method, 'expose'):
                if method.name is not None:
                    self.methodMap[method.name] = attr
                    self.methods.append(method.name)
                else:
                    self.methods.append(attr)
        self.user = pwd.getpwuid(os.getuid())[0]
        self.lblUser = None
        tokenFile = os.path.expanduser('~/.jamo/token')
        if skipAuth:
            return
        if not os.path.exists(tokenFile):
            sys.stderr.write('error: Your idenity has not been identifed. I will now attempt to set that up\n')
            self.auth()
            sys.exit(1)
        with open(tokenFile) as f:
            tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
        if isinstance(tokenMap, str):
            tokenMap = {'https://sdm-dev.jgi-psf.org': tokenMap}
            with open(tokenFile, 'w') as f:
                f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        if self.jamohost == 'https://sdm-dev.jgi-psf.org' and 'https://sdm-dev.jgi-psf.org:8034' in tokenMap:
            tokenMap[self.jamohost] = tokenMap['https://sdm-dev.jgi-psf.org']
            with open(tokenFile, 'w') as f:
                f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        if self.jamohost not in tokenMap:
            sys.stderr.write('error: Your idenity has not been identifed. I will now attempt to set that up\n')
            self.auth()
            sys.stderr.write('info: Your idenity has been identifed.\n')
            with open(tokenFile) as f:
                tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
        token = tokenMap[self.jamohost]
        if len(token) != 32:
            sys.stderr.write('error: Your identity has yet to be validated, you should have received and email with a link in it.\nIf you have not received it, run: "jadmin reset auth" to resend an email\n')
            sys.exit(2)
        self.curl.userData = 'Bearer %s' % token
        if 'admin' not in self.curl.get('api/core/whoami')['permissions']:
            sys.stderr.write('error: You do not have the correct access level to run this utility\n')
            sys.exit(2)

    def editData(self, data=None, fileLoc=None):
        dFormat = 'yaml' if 'format' in self.defaults and self.defaults['format'] == 'yaml' else 'json'
        if fileLoc is not None:
            with open(fileLoc) as tf:
                if tf.readline().count('{') > 0:
                    dFormat = 'json'
                else:
                    dFormat = 'yaml'
        if dFormat == 'yaml':
            return editYaml(data, fileLoc)
        else:
            return editJson(data, fileLoc)

    def getUser(self):
        if self.lblUser is None:
            self.lblUser = self.curl.get('api/core/self')['user'].split('@')[0]
        return self.lblUser

    def getLocation(self, path):
        if path.startswith('/'):
            return path
        else:
            return os.path.join(os.getcwd(), path)

    @expose('Updates records in mongodb')
    def update(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jadmin update < metadata | analysis > <set <field=val, ...> | unset <field, ...> | rename <old field=new field, ...>> where < clause >\n')
            sys.exit(2)
        if ('set' not in args and 'unset' not in args and 'rename' not in args) or 'where' not in args:
            sys.stderr.write('error: did not detect a (set, unset, rename) or where clause in update statement. run jadmin update help for usage.\n')
            sys.exit(2)
        if args[0] not in ('metadata', 'analysis'):
            sys.stderr.write('error: Currently the only supported update databases are: metadata and analysis. run jadmin update help for usage.\n')
            sys.exit(2)

        datasource = args[0]
        fields = []
        unset_fields = []
        rename_fields = []
        where = []
        current_set = None
        for arg in args:
            if arg == 'set':
                current_set = 'fields'
            elif arg == 'rename':
                current_set = 'rename_fields'
            elif arg == 'where':
                current_set = 'where'
            elif arg == 'unset':
                current_set = 'unset_fields'
            elif current_set == 'where':
                where.append(arg)
            elif current_set == 'fields':
                fields.append(arg)
            elif current_set == 'rename_fields':
                rename_fields.append(arg)
            elif current_set == 'unset_fields':
                unset_fields.extend(arg.split(','))
        where = (' '.join(where)).strip()
        query = getQuery(where)
        q_fields = ['file_path', 'file_name', 'file_status', '_id'] if datasource == 'metadata' else ['key', 'user', 'template', '_id']
        files = PageList(self.curl.post('api/%s/pagequery' % datasource, query=query, fields=q_fields), self.curl)

        def runUpdate():
            updateDict = toMongoSet(' '.join(fields)) if len(fields) > 0 else {}
            if len(unset_fields) > 0:
                updateDict['$unset'] = {field: '' for field in unset_fields if len(field) > 0}
            if len(rename_fields) > 0:
                updateDict['$rename'] = toMongoSet(' '.join(rename_fields))['$set'] if len(rename_fields) > 0 else {}
            print(updateDict)
            print(self.curl.post('api/%s/safeupdate' % datasource, query=query, update=updateDict))
        self.askForAction('update', files, runUpdate, q_fields)

    @expose('deletes a record in jamo or jat that matches the supplied query')
    def delete(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jadmin delete < metadata | analysis > where < clause >\n')
            sys.exit(2)
        if len(args) < 3 or 'where' != args[1]:
            sys.stderr.write('error: did not detect a where clause in delete statement. run jadmin delete help for usage.\n')
            sys.exit(2)
        if args[0] not in ('metadata', 'analysis'):
            sys.stderr.write('error: Currently the only supported update databases are: metadata and analysis. run jadmin update help for usage.\n')
            sys.exit(2)
        datasource = args[0]
        query = ' '.join(args[2:])
        query = getQuery(query)
        records = PageList(self.curl.post('api/%s/pagequery' % datasource, query=query), self.curl)

        def runDelete():
            print(self.curl.post('api/%s/delete' % datasource, query=query))
        q_fields = ['file_path', 'file_name', 'file_status', '_id'] if datasource == 'metadata' else ['key', 'user', 'template', '_id']
        self.askForAction('delete', records, runDelete, q_fields)

    @expose('undeletes a record in jamo or jat that matches the supplied query')
    def undelete(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jadmin undelete < metadata | analysis > where < clause >\n')
            sys.exit(2)
        if len(args) < 3 or 'where' != args[1]:
            sys.stderr.write('error: did not detect a where clause in undelete statement. run jadmin delete help for usage.\n')
            sys.exit(2)
        if args[0] not in ('metadata', 'analysis'):
            sys.stderr.write('error: Currently the only supported update databases are: metadata and analysis. run jadmin update help for usage.\n')
            sys.exit(2)
        datasource = args[0]
        query = ' '.join(args[2:])
        query = getQuery(query)
        source = 'deleted_file' if datasource == 'metadata' else 'deleted_analysis'
        records = PageList(self.curl.post('api/%s/pagequery' % datasource, query=query, source=source), self.curl)

        def runUnDelete():
            print(self.curl.post('api/%s/undelete' % datasource, query=query))
        q_fields = ['file_path', 'file_name', 'file_status', '_id'] if datasource == 'metadata' else ['key', 'user', 'template', '_id']
        self.askForAction('undelete', records, runUnDelete, q_fields)

    def askForAction(self, action, records, continueF, fields=['_id', 'file_name']):
        rec_count = len(records)
        if rec_count == 0:
            sys.stderr.write('warning: Looks like there are no records that match your query. No update will be issued\n')
            sys.exit(2)
        print('There are %d records that will be updated' % rec_count)
        print('  what would you like to do?')
        print('  v : view all the files that will be %s' % action)
        print('  a : abort %s' % action)
        print('  c : continue %s' % action)
        choice = input('?')
        if choice == 'v':
            for record in records:
                for field in fields:
                    sys.stdout.write(record[field] + '\t')
                sys.stdout.write('\n')
            cont = input('\nContinue with %s (y/n)?' % action)
            if cont == 'y':
                continueF()
            else:
                sys.exit(2)
        elif choice == 'c':
            ''' issue update'''
            continueF()
        elif choice == 'a':
            print('%s aborted' % action)
            sys.exit(1)
        else:
            sys.stderr.write('invalid option provided, %s aborted\n' % action)
            sys.exit(1)

    @expose('replace a file in jamo with a different one keeping the metadata and user info the same')
    def replace(self, args):
        # TODO: Consider using `argparse` instead of parsing args manually...
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jadmin replace <src file> <file in jamo> [<source_data_center>]\n')
            sys.exit(2)
        if len(args) < 2 or len(args) > 3:
            sys.stderr.write('''error: Invalid number of arguments passed. run 'jadmin replace help' for usage\n''')
            sys.exit(2)

        src, jamo_file = args[:2]
        source = args[2] if len(args) == 3 else None

        file_info = self.curl.get('api/tape/latestfile', file=requests.utils.quote(jamo_file))
        if file_info is None:
            sys.stderr.write('''error: destination file '%s' does not exist in JAMO \n''' % jamo_file)
            sys.exit(2)

        self.curl.post('api/tape/replacefile', src=src, dest=file_info['metadata_id'], source=source)

    def auth(self):
        user = pwd.getpwuid(os.getuid())[0]
        tokenFile = os.path.expanduser('~/.jamo/token')
        if not os.path.exists(os.path.expanduser('~/.jamo')):
            os.makedirs(os.path.expanduser('~/.jamo'))
        token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(50))

        if os.path.exists(tokenFile):
            with open(tokenFile) as f:
                tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
            if isinstance(tokenMap, str):
                tokenMap = {'https://sdm-dev.jgi-psf.org:8034': tokenMap}
        else:
            tokenMap = {}
        tokenMap[self.jamohost] = token
        with open(tokenFile, 'w') as f:
            f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        os.chmod(tokenFile, stat.S_IRUSR | stat.S_IWUSR)
        if self.curl.post('api/core/associate', user=user, token=token) is None:
            sys.stderr.write('You will receive an email asking you to validate your account, you must click on the link before you can use this tool.\nIf an email doesn\'t appear make sure to check your spam folder\n')
            sys.exit(1)

    @expose('Prints this message')
    def help(self, args):
        sys.stderr.write('usage jadmin <command> [<args>]\n')
        sys.stderr.write('\nThe jadmin commands are:\n')
        for method in self.methods:
            if method in self.methodMap:
                sys.stderr.write(' %-15s %s\n' % (method, getattr(self, self.methodMap[method]).description))
            else:
                sys.stderr.write(' %-15s %s\n' % (method, getattr(self, method).description))
        sys.exit(2)

    @expose('Imports a single file from the tape system into jamo, useful for data before jamo')
    def importtapefile(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat importtapefile <tag_templates,...> <file_id> metadatakey=value...\n  Ex: jat importtapefile fastq 123 sequencing_project_id=123465\n\nThe possible templates are:\n')
            templates = PageList(self.curl.post('api/analysis/tags', fields=['name', 'description']), self.curl, service='analysis')
            chars = len(max([t['name'] for t in templates], key=len))
            for template in templates:
                sys.stderr.write((' %-' + str(chars) + 's: %s\n') % (template['name'], template['description']))
            sys.exit(2)
        elif len(args) == 1:
            sys.stderr.write('invalid usage: please provide a file and metadata\n\nthe required metadata for template %s is:\n' % args[0])
            template = self.curl.get('api/analysis/templatesmetadata/%s' % args[0].replace(",", "/"))
            sys.stderr.write(self.printRequiredKeys(template, {}))
            sys.exit(2)
        else:
            metadata = {}
            if len(args) > 2:
                for arg in args[2:]:
                    if arg.count('=') > 0:
                        key, value = arg.split('=', 1)
                        metadata[key] = value

            nmetadata = self.curl.get('api/analysis/templatesmetadata/%s' % ('/'.join(args[0].split(','))))
            neededKeysMsg = self.printRequiredKeys(nmetadata, metadata)
            if len(neededKeysMsg) > 1:
                sys.stderr.write('error: not all keys have been provided, please provide the following keys or remove a tag\n' + neededKeysMsg)
                sys.exit(1)
            else:
                try:
                    metadata = self.curl.post('api/analysis/validatetags', metadata=metadata, tags=args[0].split(','))
                    ret = self.curl.post('api/metadata/importfromtape', file_type=args[0].split(','), metadata=metadata, file_id=int(args[1]))
                    print('''imported file '%s'  as %s''' % (args[1], ret['metadata_id']))
                except sdm_curl.CurlHttpException as e:
                    sys.stderr.write('Failed to import file due to the following errors:\n')
                    sys.stderr.write('  ' + ('\n  '.join(json.loads(''.join(e.response))['errors'])) + '\n')
                    sys.exit(2)

    @expose('Queue file for MD5 calculation')
    def update_md5(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jadmin update_md5 < clause >\n')
            sys.exit(2)
        query = ' '.join(args)
        query = getQuery(query)
        records = list(PageList(self.curl.post('api/metadata/pagequery', query=query), self.curl))

        def runPostMD5():
            for f in records:
                self.curl.post('api/tape/md5',
                               file_path=os.path.join(f['file_path'], f['file_name']),
                               file_size=f['file_size'],
                               callback='local://put_file/%d' % f['file_id'])
        q_fields = ['file_path', 'file_name', 'file_status', '_id', 'md5sum']
        self.askForAction('update_md5', records, runPostMD5, q_fields)

    def printRequiredKeys(self, keys, values):
        ret = ''
        require_sets = {}
        for key in keys[0]:
            if 'required' in key and not isinstance(key['required'], bool):
                require_set_k = key['required']
                if require_set_k not in require_sets:
                    require_sets[require_set_k] = []
                require_sets[require_set_k].append(key)
            elif key['key'] not in values:
                line = '  *' if 'required' not in key or key['required'] else '  '
                line += '%s : (%s)-%s' % (key['key'], key['type'], key['description'])
                ret += line + "\n"
        for v in keys[1]:
            has_a_key = False
            for key in v:
                if key['key'] in values:
                    has_a_key = True
                    break
            if has_a_key:
                continue
            ret += 'at least one of the following:\n'
            chars = len(max([t['key'] for t in v], key=len))
            for key in v:
                ret += ('  %-' + str(chars) + 's : (%s)- %s\n') % (key['key'], key['type'], key['description'])
        return ret

    def run(self, args):
        method = args[0]
        args = args[1:]
        if method not in self.methods:
            sys.stderr.write('''jadmin: '%s' is not a jadmin command. run 'jadmin help' for more options\n''' % method)
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes) > 0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n' % meth)
            sys.exit(2)
        if method in self.methodMap:
            method = self.methodMap[method]
        getattr(self, method)(args)


def main():
    args = sys.argv[1:]
    newArgs = []
    options = []
    for arg in args:
        if arg.startswith('-'):
            options.append(arg)
        else:
            newArgs.append(arg)
    if args == ['reset', 'auth']:
        jadmin = JAdmin(options, True)
    else:
        jadmin = JAdmin(options)
    if len(args) == 0:
        jadmin.help(args)
    else:
        jadmin.run(args)

if __name__ == '__main__':
     main()
