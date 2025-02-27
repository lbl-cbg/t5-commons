#!/usr/bin/env python
from __future__ import print_function
from builtins import str
from builtins import range
from past.builtins import basestring
from builtins import object
import difflib
import getpass
import json
import pwd
import glob
import os
import sdm_curl
import sys
import stat
import string
import readline
import random
import yaml
import grp
from jamo_common import expose, editJson, editYaml, toMongoObj, parse_jamo_query, PageList

readline.set_completer_delims('/')


class Jira(object):
    pass


class FileCompleter(object):
    def __init__(self, rootDir):
        self.rootDir = rootDir
        readline.parse_and_bind("tab: complete")

    def complete(self, text, state):
        if state == 0:
            folder, match = os.path.split(readline.get_line_buffer())
            self.options = [file + ('/' if os.path.isdir(os.path.join(self.rootDir, folder, file)) else '') for file in
                            os.listdir(os.path.join(self.rootDir, folder)) if file.startswith(match) or file == '']
            # print '\n',self.options
            # print readline.get_line_buffer()
        if len(self.options) == 1:
            if self.options[0].endswith('/'):
                self.options.append(self.options[0] + ' ')
        if state < len(self.options):
            return self.options[state]


def validate_files(args, file_list=False):
    # Note that this makes this very JGI-specific
    # We need to validate that the dt_service process (jgi_dna/genome) will be able to read
    # the files.  This is to check the path and the files to make sure they have the other bits
    # set, or the group bit if it is one of the groups jgi_dna has access to.
    group_list = ['genome', 'm342', 'metatlas']

    def check_file(filename):
        readable = 0
        try:
            stats = os.stat(filename)
            mode = stats.st_mode
            if mode & 0o04:
                readable = 1
            if stat.S_ISDIR(mode):
                if not (mode & 0o01):
                    readable = 0
            if not readable:
                group = stats[stat.ST_GID]
                group_name = grp.getgrgid(group).gr_name
                if group_name in group_list:
                    if mode & 0o040:
                        readable = 1
                    if stat.S_ISDIR(mode):
                        if not (mode & 0o010):
                            readable = 0
            return readable
        except Exception as e:  # noqa: F841
            return readable

    reported = {}
    errors = []
    for filename in args:
        realpath = os.path.abspath(filename)
        while realpath and realpath not in reported:
            # print("check %s" % realpath)
            reported[realpath] = 1
            if not check_file(realpath):
                if file_list:
                    errors.append(realpath)
                else:
                    errors.append("File/directory %s does not exist or can't be accessed by JAT/JAMO" % realpath)
            realpath, filename = os.path.split(realpath)
    return errors


def validate_token_file_permissions(token_file):
    """Validate and updates permissions on token file/directory if needed. Permissions need to be 0600 for the file
    (user only r/w) and the parent directory needs to not have write permissions for group and other (other permission
    bits are allowed)

    :param str token_file: Path to token file
    :raises: ValueError if either the token file or parent directory don't have the right permissions
    """
    file_stats = os.stat(token_file)
    file_perms = file_stats[stat.ST_MODE]
    dir_stats = os.stat(os.path.dirname(token_file))
    dir_perms = dir_stats[stat.ST_MODE]
    home_stats = os.stat(os.path.expanduser('~'))
    home_perms = home_stats[stat.ST_MODE]

    if file_perms != (stat.S_IRUSR | stat.S_IWUSR | stat.S_IFREG):
        # Set token file permission to 0600
        print('Changing `~/.jamo/token` permission to 0600')
        os.chmod(token_file, (stat.S_IRUSR | stat.S_IWUSR))
    if dir_perms & (stat.S_IWGRP | stat.S_IWOTH):
        # Remove group/other write, keeping other bits as set in either jamo directory or home directory
        new_perms = (dir_perms | home_perms) & (~stat.S_IWGRP & ~stat.S_IWOTH)
        print('Changing `~/.jamo` permission to {}'.format(oct(new_perms & 0o777)))
        os.chmod(os.path.dirname(token_file), new_perms)


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


def get_template_data(folder):
    error = None
    skip_folder = False
    if os.path.isdir(folder):
        json_file = os.path.join(folder, 'metadata.json')
        if os.path.isfile(json_file):
            with open(json_file, encoding='utf-8') as f:
                try:
                    template_data = json.loads(f.read())
                except ValueError as ve:
                    error = ('error: Your metadata.json in: %s failed to parse as json with the following error.:\n' % folder) + str(ve) + '\n'
        else:
            yaml_file = os.path.join(folder, 'metadata.yaml')
            if os.path.isfile(yaml_file):
                with open(yaml_file, encoding='utf-8') as f:
                    try:
                        template_data = yaml.load(f.read(), Loader=yaml.SafeLoader)
                    except ValueError as ve:
                        error = ('error: Your metadata.yaml in: %s failed to parse as yaml with the following error.:\n' % folder) + str(ve) + '\n'
            else:
                error = "error: There is no metadata.json or metadata.yaml file in the folder '%s'\n" % folder
    elif folder.endswith('.json') and os.path.isfile(folder):
        with open(folder, encoding='utf-8') as f:
            try:
                template_data = json.loads(f.read())
                folder = os.path.dirname(folder)
                skip_folder = True
            except ValueError as ve:
                error = ('error: Your metadata.json in: %s failed to parse as json with the following error.:\n' % folder) + str(ve) + '\n'
    else:
        error = "error: You have passed in an invalid directory '%s'\n" % folder
    if error is not None:
        sys.stderr.write(error)
        sys.exit(2)

    # Check the file permissions
    file_check = [folder]
    for output in template_data.get('outputs', None):
        in_file = output.get('file', None)
        check_file = in_file if in_file.startswith('/') else '/'.join((folder, in_file))
        file_check.append(check_file)
    error = validate_files(file_check, file_list=True)
    if error:
        sys.stderr.write('The following files/directories have permission issues or were not found\n')
        for e in error:
            sys.stderr.write('  - %s\n' % e)
        sys.stderr.write('\nPlease make sure that files and directories are readable by group genome or by other if group is not genome\n')
        sys.stderr.write('Analysis not imported\n')
        ""
        sys.exit(2)

    return template_data, folder, skip_folder


class JTT(object):
    def __init__(self, options, skipAuth=False):
        self.jamohost = os.environ.get('JAMO_HOST', 'https://jamo.jgi.doe.gov')
        self.curl = sdm_curl.Curl(self.jamohost, retry=0)
        self.jiraCurl = sdm_curl.Curl('https://issues.jgi.doe.gov/rest/api/2')
        self.jiraCurl.userData = 'Basic amFtbzpaMkpBN011Ug=='
        self.methods = []
        self.options = {}
        for option in options:
            key, value = option[1:], True
            if option.count('=') > 0:
                key, value = option[1:].split("=", 1)
            self.options[key] = value
        self.methodMap = {}
        for attr in dir(self):
            method = getattr(self, attr)
            if hasattr(method, 'expose'):
                if method.name is not None:
                    self.methodMap[method.name] = attr
                    self.methods.append(method.name)
                else:
                    self.methods.append(attr)
        self.loadedSettings = False
        self.user = pwd.getpwuid(os.getuid())[0]
        self.lblUser = None
        self.loadedTemplates = False
        self.loadedMacros = False
        tokenFile = os.path.expanduser('~/.jamo/token')
        defaultsFile = os.path.expanduser('~/.jamo/settings')
        if os.path.exists(defaultsFile):
            self.defaults = loadSettingsFile(defaultsFile)
            if 'defaults' in self.defaults:
                self.defaults = self.defaults['defaults']
            else:
                self.defaults = {}
        else:
            self.defaults = {}
        self.interactive = True if '-i' in options else False
        if skipAuth:
            return
        if not os.path.exists(tokenFile):
            sys.stderr.write('error: Your identity has not been confirmed.  I will now attempt to set that up\n')
            self.auth()
            sys.exit(1)
        try:
            validate_token_file_permissions(tokenFile)
        except ValueError as e:
            sys.stderr.write(e.message)
            sys.exit(2)
        with open(tokenFile) as f:
            tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)

        # Tmmporary code to copy over the tokens from jgi-psf.org to jgi.doe.gov XX
        # if isinstance(tokenMap, basestring):
        #    tokenMap = {'https://sdm-dev.jgi-psf.org': tokenMap}
        #    with open(tokenFile, 'w') as f:
        #        f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        #########
        rewrite = False
        if isinstance(tokenMap, basestring):
            tokenMap = {'https://sdm-dev.jgi-psf.org': tokenMap}
            rewrite = True
        if self.jamohost not in tokenMap:
            if 'jgi.doe.gov' in self.jamohost:
                if self.jamohost == 'https://jamo.jgi.doe.gov' and 'https://sdm2.jgi-psf.org' in tokenMap:
                    tokenMap['https://jamo.jgi.doe.gov'] = tokenMap['https://sdm2.jgi-psf.org']
                    rewrite = True
                if self.jamohost == 'https://jamo-dev.jgi.doe.gov' and 'https://sdm-dev.jgi-psf.org' in tokenMap:
                    tokenMap['https://jamo-dev.jgi.doe.gov'] = tokenMap['https://sdm-dev.jgi-psf.org']
                    rewrite = True
        if rewrite:
            with open(tokenFile, 'w') as f:
                f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        # End Tmmporary code

        # if self.jamohost == 'https://sdm-dev.jgi-psf.org' and 'https://sdm-dev.jgi-psf.org:8034' in tokenMap:
        #    tokenMap[self.jamohost] = tokenMap['https://sdm-dev.jgi-psf.org:8034']
        #    with open(tokenFile, 'w') as f: f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        if self.jamohost not in tokenMap:
            sys.stderr.write('error: Your identity has not been confirmed.  I will now attempt to set that up\n')
            self.auth()
            sys.stderr.write('info: Your identity has been confirmed.\n')
            with open(tokenFile) as f:
                tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
        token = tokenMap[self.jamohost]
        # User token request generation token
        if len(token) == 50:
            try:
                # Get the reserved token from the server
                token = self.curl.get('api/core/reserved_token', token=token)
                tokenMap[self.jamohost] = token
                with open(tokenFile, 'w') as f:
                    f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
            except Exception as e:  # noqa: F841
                sys.stderr.write(
                    'error: Your identity has yet to be validated, you should have received and email with a link in it.\nIf you have not received it, run: "jat reset auth" to resend an email\n')
                sys.exit(2)
        if len(token) != 32:
            sys.stderr.write('error: Your identity has yet to be validated, you should have received and email with a link in it.\nIf you have not received it, run: "jat reset auth" to resend an email\n')
            sys.exit(2)
        self.curl.userData = 'Bearer %s' % token

        self.start_setupers = {'jira': self.startJiraAnalysis}

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

    def startJiraAnalysis(self, name):
        try:
            jiraInfo = self.jiraCurl.get('issue/%s' % name)
            return {'summary': jiraInfo['fields']['summary'],
                    'description': jiraInfo['fields']['description'],
                    'jira_id': name,
                    'key': name
                    }
        except Exception:
            # TODO: Why are we catching the exception if we're just re-raising it without any additional processing???
            raise

    def getWorkingAnalysisFolder(self):
        cwd = os.getcwd()
        while not os.path.exists(os.path.join(cwd, '.jamo/id')):
            cwd = os.path.dirname(cwd)
            if cwd == '/':
                sys.stderr.write('error: There is no known analysis for this folder\n')
                sys.exit(2)
        return cwd

    def getWorkingAnalysis(self):
        cwd = self.getWorkingAnalysisFolder()
        id = None
        with open(os.path.join(cwd, '.jamo/id')) as f:
            id = f.read()
        if id is None:
            sys.stderr.write('error: There is no known analysis for this folder\n')
            sys.exit(2)
        return id

    def getLocation(self, path):
        if path.startswith('/'):
            return path
        else:
            return os.path.join(os.getcwd(), path)

    @expose('Validates against different things')
    def validate(self, args):
        resources = {'template': ''}
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat validate <resource> <file>\n\nAvailable resources are:\n')
            for resource in resources:
                sys.stderr.write(' %-10s %s\n' % (resource, resources[resource]))
            sys.exit(2)
        if args[0] in resources:
            files = args[1:]
            hasErrors = False
            for file_data in files:
                try:
                    with open(file_data) as f:
                        data = yaml.load(f.read(), Loader=yaml.SafeLoader)
                    errors = self.curl.post('api/analysis/validate%s' % args[0], data=data)
                except sdm_curl.CurlHttpException as e:
                    hasErrors = True
                    errors = json.loads(''.join(e.response))['errors']
                if len(errors) == 0:
                    sys.stdout.write('''file: '%s' as resource:'%s' validates\n''' % (file_data, args[0]))
                else:
                    sys.stdout.write('''file: '%s' as resource:'%s' fails to validate due to:\n\n%s\n''' % (file_data, args[0], '\n '.join(errors)))
                    hasErrors = True
            sys.exit(1 if hasErrors else 0)

    @expose('Shows the status of all your analyses')
    def status(self, args):
        if len(args) == 1 and args[0] == 'help':
            sys.stderr.write('status help pending...\n')
            sys.exit(2)
        analyses = self.curl.get('api/analysis/myanalyses')
        for analysis in analyses:
            sys.stdout.write('%-15s %-50s %-10s %s\n' % (
                analysis['key'], analysis['location'], analysis.get('status', ''),
                analysis['summary'] if 'summary' in analysis else ''))

    @expose('Prints the specified resources to the screen')
    def get(self, args):
        resources = {'key_locations': {'description': 'Print the templates that use a given key'},
                     'template_keys': {'description': 'Print all keys in for a given template'},
                     'macro': {'description': 'A metadata pattern that can be used in templates'},
                     'template': {'description': 'A guideline and validator for submitting analyses'},
                     'resolvedtemplate': {
                         'description': 'like template, but includes portal display location and publishing flags'},
                     'metadata.json': {
                         'description': 'recreates the contents of the metadata.json for a specific analysis'}}
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat get <resource> <name>\n\n')
            sys.stderr.write('possible resources are:\n')
            for resource in resources:
                sys.stderr.write(' %-15s %s\n' % (resource, resources[resource]['description']))
            sys.exit(2)
        if args[0] not in resources:
            sys.stderr.write('''error: '%s' is not a valid resource. run 'jat get help' for a list of valid resources ''' % args[0])
            sys.exit(2)
        if len(args) == 1:
            sys.stderr.write('error: you have not provided a name to fetch\n')
            sys.exit(2)
        if args[0] == 'metadata.json':
            data = self.curl.get('api/analysis/analysis/%s' % args[1])
            data.update(data['options'])
            for remove_key in ('options', 'skip_folder', 'user', 'group', 'key', '_id', 'status', 'added_date', 'modified_date', 'location'):
                if remove_key in data:
                    del data[remove_key]

            print(json.dumps(data, indent=4, separators=(',', ': ')))
        else:
            if args[0] == 'template_keys':
                result = self.curl.get('api/analysis/resolvedtemplate/%s' % args[1])
                keys = dict()
                if 'required_metadata_keys' in result:
                    keys['required_metadata_keys'] = [x['key'] for x in result['required_metadata_keys']]
                if 'outputs' in result:
                    keys['outputs'] = dict()
                    for output in result['outputs']:
                        keys['outputs'][output['label']] = list()
                        for key in output['required_metadata_keys']:
                            keys['outputs'][output['label']].append(key['key'])

                output = keys

            elif args[0] == 'key_locations':
                result = self.curl.get('api/analysis/keylocations/%s' % args[1])
                if result['location'] is None:
                    output = "%s does not exist in any template or macro\n" % args[1]
                else:
                    del result['key']
                    output = result
            else:
                output = self.curl.get('api/analysis/%s/%s' % (args[0], args[1]))

            sys.stdout.write(yaml.safe_dump(output, default_flow_style=False))

    def getKeywords(self, string):
        ret = []
        for value in string.replace(',', '').split():
            if len(value) == 4:
                ret.append(('metadata.library_name', value))
            elif len(value) == 7 and value.isdigit() and value.startswith('10'):
                ret.append(('metadata.sequencing_project_id', int(value)))
        return ret

    @expose('Updates an existing analysis metadata only')
    def update(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat update <analysis key> <path to metadata.json>\n')
            sys.exit(2)
        if len(args) != 2:
            sys.stderr.write('error: invalid amount of arguments was passed. run jat update help for usage.\n')
            sys.exit(2)
        analysis, jsonFile = args
        with open(jsonFile, encoding='utf-8') as f:
            try:
                jsonData = json.loads(f.read())
            except ValueError as e:
                sys.stderr.write('error: invalid json found in %s (Error %s)\n' % (jsonFile, e))
                sys.exit(2)
        ret = self.curl.put('api/analysis/import/%s' % analysis, data=jsonData)
        if ret:
            for message in ret:
                sys.stderr.write('\n'.join(ret[message]))

    @expose('Adds a new file to an existing analysis')
    def addfile(self, args):
        # TODO: Consider using `argparse` instead of parsing args manually...
        if len(args) == 0 or args[0] == 'help':
            msg = 'usage: jat addfile <analysis key> <label> <file> <json metadata file> [<source_data_center>]\n' \
                  'The metadata file can be an updated version of the original JSON,\n' \
                  'or a JSON that contains only the metadata for the new file.\n' \
                  '`source_data_center` is the source data center identifier (e.g., igb, dori) - optional'
            # sys.stderr.write('usage: jat addfile <analysis key> <label> <file> <json metadata file>\n')
            sys.stderr.write('%s\n' % msg)
            sys.exit(2)
        if len(args) < 4 or len(args) > 5:
            sys.stderr.write('error: invalid amount of arguments was passed. run jat addfile help for usage.\n')
            sys.exit(2)
        analysis, label, new_file, json_file = args[:4]
        source = args[4] if len(args) == 5 else None
        relpath = new_file
        new_file = os.path.realpath(new_file)
        with open(json_file, encoding='utf-8') as f:
            jsonData = json.loads(f.read())
        oMetadata = None
        # check to see if they passed in the original metadata.json, but added the new output
        if 'outputs' in jsonData:
            if isinstance(jsonData['outputs'], list):
                for output in jsonData['outputs']:
                    if 'file' in output and output['file'] == relpath:
                        if 'metadata' in output:
                            oMetadata = output['metadata']
                            break
                        else:
                            sys.stderr.write(
                                'error: found %s in %s but did not find any metadata\n' % (relpath, json_file))
                            sys.exit(2)
                if oMetadata is None:
                    sys.stderr.write('error: did not find any output with name "%s" in %s\n' % (relpath, json_file))
                    sys.exit(2)
            else:
                sys.stderr.write('error: did not find a list of outputs in %s\n' % (json_file))
                sys.exit(2)
        else:
            oMetadata = jsonData
        try:
            response = self.curl.post('api/analysis/addfile/%s/%s' % (analysis, label), file=new_file,
                                      metadata=oMetadata, source=source)
            if response.get('warnings'):
                sys.stderr.write('\n'.join(response.get('warnings')) + '\n')
        except sdm_curl.CurlHttpException as e:
            sys.stderr.write('Failed to add file due to the following errors:\n')
            msg = json.loads(''.join(e.response))['errors']
            if isinstance(msg, str):
                sys.stderr.write('  ' + msg + '\n')
            else:
                sys.stderr.write('  ' + ('\n  '.join(msg) + '\n'))
            sys.exit(2)

    @expose('Adds multiple files to an existing analysis')
    def addfiles(self, args):
        # TODO: Consider using `argparse` instead of parsing args manually...
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat addfiles <analysis key> <json metadata file> [<source_data_center>]\n')
            sys.exit(2)
        if len(args) < 2 or len(args) > 3:
            sys.stderr.write('error: invalid amount of arguments was passed. run jat addfiles help for usage.\n')
            sys.exit(2)
        analysis, json_file = args[:2]
        source = args[2] if len(args) == 3 else None
        with open(json_file, encoding='utf-8') as f:
            jsonData = json.loads(f.read())

        if isinstance(jsonData, dict):
            if 'outputs' in jsonData:
                jsonData = jsonData['outputs']

        failures = list()
        warnings = []
        for output in jsonData:
            oMetadata = output['metadata']
            new_file = output['file']
            rel_path = new_file
            label = output['label']
            sys.stderr.write("Adding file %s\n" % new_file)
            new_file = os.path.realpath(new_file)
            # check to see if they passed in the original metadata.json, but added the new output
            try:
                response = self.curl.post('api/analysis/addfile/%s/%s' % (analysis, label), file=new_file,
                                          metadata=oMetadata, source=source)
                if response.get('warnings'):
                    warnings += response.get('warnings')
            except sdm_curl.CurlHttpException as e:
                msg = json.loads(''.join(e.response))['errors']
                if isinstance(msg, str):
                    error = '  ' + msg + '\n'
                else:
                    error = '  ' + ('\n  '.join(msg) + '\n')
                failures.append((rel_path, error))

        if len(failures) > 0:
            for failure in failures:
                sys.stderr.write("Failed to add %s for the following error:\n" % failure[0])
                sys.stderr.write(failure[1])
        else:
            if warnings:
                sys.stderr.write('\n'.join(warnings) + '\n')
            sys.stderr.write("Successfully added %d new files to analysis %s\n" % (len(jsonData), analysis))

    @expose('Imports a single file into jamo, useful for external data')
    def importfile(self, args):
        # TODO: Consider using `argparse` instead of parsing args manually...
        source = None
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat importfile <tag_templates,...> <file> [<source_data_center>] metadatakey=value...\n  Ex: jat importfile fastq test.fastq sequencing_project_id=123465\n\nThe possible templates are:\n')
            templates = PageList(self.curl.post('api/analysis/tags', fields=['name', 'description']), self.curl,
                                 service='analysis')
            chars = len(max([t['name'] for t in templates], key=len))
            for template in templates:
                sys.stderr.write((' %-' + str(chars) + 's: %s\n') % (template['name'], template['description']))
            sys.exit(2)
        elif len(args) == 1:
            sys.stderr.write('invalid usage: please provide a file and metadata\n\nthe required metadata for template %s is:\n' %
                             args[0])
            template = self.curl.get('api/analysis/templatesmetadata/%s' % args[0].replace(",", "/"))
            sys.stderr.write(self.printRequiredKeys(template, {}))
            sys.exit(2)
        else:
            metadata = {}
            if len(args) > 2:
                index = 2
                if args[index].count('=') == 0:
                    # The argument is the optional source value, not key/value pairs...This is so ugly and messy, we
                    # shouldn't be manually parsing command line arguments...
                    source = args[index]
                    index += 1
                for arg in args[index:]:
                    if arg.count('=') > 0:
                        key, value = arg.split('=', 1)
                        metadata[key] = value

            # Check validate_files
            e = validate_files([args[1]])
            if e:
                sys.stderr.write('error: %s\n' % '\n'.join(e))
                sys.exit(1)
            nmetadata = self.curl.get('api/analysis/templatesmetadata/%s' % ('/'.join(args[0].split(','))))
            neededKeysMsg = self.printRequiredKeys(nmetadata, metadata)
            if len(neededKeysMsg) > 1:
                sys.stderr.write('error: not all keys have been provided, please provide the following keys or remove a tag\n' + neededKeysMsg)
                sys.exit(1)
            else:
                tape_options = {}
                if 'local_purge' in self.options:
                    tape_options['local_purge_days'] = int(self.options['local_purge'])
                if 'no_local_copy' in self.options:
                    tape_options['local_purge_days'] = 0
                try:
                    ret = self.curl.post('api/analysis/importfile', tags=args[0].split(','), metadata=metadata,
                                         file=os.path.realpath(args[1]), tape_options=tape_options, source=source)
                    if ret.get('warnings'):
                        sys.stderr.write('\n'.join(ret.get('warnings')) + '\n')
                    print('''imported file '%s'  as %s''' % (os.path.realpath(args[1]), ret['metadata_id']))
                except sdm_curl.CurlHttpException as e:
                    sys.stderr.write('Failed to import file due to the following errors:\n')
                    sys.stderr.write('  ' + ('\n  '.join(json.loads(''.join(e.response))['errors'])) + '\n')
                    sys.exit(2)

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

    @expose('Imports a legacy analysis run folder into jamo', 'import')
    def importa(self, args):
        # TODO: Consider using `argparse` instead of parsing args manually...
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat importa <analysis template> <folder> [<source_data_center>]\n\nThe possible templates are:\n')
            chars = len(max([t['name'] for t in self.getTemplates()], key=len))
            for template in self.getTemplates():
                sys.stderr.write((' %-' + str(chars) + 's %s\n') % (template['name'], template['description']))
            sys.exit(2)
        if len(args) < 2 or len(args) > 3:
            sys.stderr.write('error: invalid amount of arguments was passed. run jat importa help for usage.\n')
            sys.exit(2)
        source = args[2] if len(args) == 3 else None
        self.submit_analysis(args[0], *get_template_data(os.path.realpath(args[1])), source)

    def submit_analysis(self, template_name, template_data, folder, skip_folder, source):
        try:
            data = self.curl.post('api/analysis/analysisimport',
                                  template_name=template_name,
                                  template_data=template_data,
                                  location=folder,
                                  skip_folder=skip_folder,
                                  source=source)
            if len(data['warnings']) > 0:
                sys.stdout.write('\n'.join(data['warnings']) + '\n')
            sys.stdout.write('successfully imported as : %s\n' % data['jat_key'])
        except sdm_curl.CurlHttpException as e:
            sys.stderr.write('Failed to import due to the following errors\n')
            print(e)
            errors = json.loads(''.join(e.response))['errors']
            sys.stderr.write(('\n'.join(errors) if isinstance(errors, list) else errors) + '\n')
            sys.exit(1)

    def findFile(self, rootDir, pattern):
        return glob.glob(rootDir + '/' + pattern)

    def processRootJson(self, rootDir, template):
        found = []
        options = {}
        if rootDir.endswith('.json'):
            with open(rootDir) as f:
                try:
                    userMetadata = json.loads(f.read())
                except ValueError as ve:
                    sys.stderr.write(('error: Your metadata.json in: %s failed to parse as json with the following error.:\n ' % rootDir) + str(ve) + '\n')
                    sys.exit(2)

        elif os.path.exists(os.path.join(rootDir, 'metadata.json')):
            with open(os.path.join(rootDir, 'metadata.json')) as f:
                try:
                    userMetadata = json.loads(f.read())
                except ValueError as ve:
                    sys.stderr.write(('error: Your metadata.json in: %s failed to parse as json with the following error.:\n ' % rootDir) + str(ve) + '\n')
                    sys.exit(2)
        elif os.path.exists(os.path.join(rootDir, 'metadata.yaml')):
            with open(os.path.join(rootDir, 'metadata.yaml')) as f:
                try:
                    userMetadata = yaml.load(f.read(), Loader=yaml.SafeLoader)
                except ValueError as ve:
                    sys.stderr.write(('error: Your metadata.yaml in: %s failed to parse as yaml with the following error.:\n ' % rootDir) + str(ve) + '\n')
                    sys.exit(2)
        else:
            sys.stderr.write('error: There is no metadata.json file in the folder \'%s\'\n' % rootDir)
            sys.exit(2)

        # look for invalid keys in the global metadata
        known_keys = {}
        error_key = False
        if 'required_metadata_keys' in template:
            for key in template['required_metadata_keys']:
                known_keys[key['key']] = 1
        if 'metadata' in userMetadata:
            for key in userMetadata['metadata']:
                if key not in known_keys:
                    error_key = True
                    sys.stderr.write('''warning: Metadata key '%s' not found in analysis template\n''' % (key))

        tOutputs = {}
        error_label = False
        for key in userMetadata:
            if key not in ('metadata', 'outputs'):
                options[key] = userMetadata[key]
        for output in template['outputs']:
            tOutputs[output['label']] = output
        for output in userMetadata['outputs']:
            if 'label' not in output:
                sys.stderr.write('''warning: Output file '%s' does not have a label tag in the submission file\n''' % (output['file']))
                error_label = True
                continue
            if output['label'] not in tOutputs:
                sys.stderr.write('''warning: Output file '%s' does not have a matching label of '%s' in the analysis template\n''' % (output['file'], output['label']))
                error_label = True
                continue
            matchingOutput = tOutputs[output['label']]
            metadata = matchingOutput['metadata'] if 'metadata' in matchingOutput else {}
            if 'metadata' in output:
                metadata.update(output['metadata'])
            output['tags'] = matchingOutput['tags']
            output['metadata'] = metadata
            for var in ('required', 'description', 'required_metadata_keys', 'default_metadata_values'):
                if var in matchingOutput:
                    output[var] = matchingOutput[var]
            found.append(output)

        # look for invalid keys in the outputs
        for output in userMetadata['outputs']:
            label = output['label']
            known_keys_file = {}
            if label in tOutputs and 'required_metadata_keys' in tOutputs[label]:
                for key in tOutputs[label]['required_metadata_keys']:
                    known_keys_file[key['key']] = 1
            for key in output['metadata']:
                if key not in known_keys and key not in known_keys_file:
                    error_key = True
                    sys.stderr.write('''warning: Metadata key '%s' for output %s not found in analysis template\n''' % (key, output['file']))

        if error_key or error_label:
            if error_key:
                sys.stderr.write('''warning: You have Metadata keys that are not defined in the template.  Processing will continue for now.  In a future version of jat, your import will be aborted.\n''')
            if error_label:
                sys.stderr.write('''warning: You have outputs with invalid labels.  Processing will continue for now.  In a future version of jat, your import will be aborted.\n''')
            # sys.exit(2)
            pass

        aMetadata = {}
        if 'metadata' in userMetadata:
            aMetadata = userMetadata['metadata']
        return found, aMetadata, options

    def checkType(self, type, value):
        if type == 'string' and isinstance(value, basestring):
            return True
        if type == 'number' and isinstance(value, (int, float)):
            return True
        if type == 'boolean' and isinstance(value, bool):
            return True
        if type.startswith('list'):
            lType = type.split(':', 1)[1]
            if not isinstance(value, list):
                return self.checkType(lType, value)
            for sValue in value:
                if not self.checkType(lType, sValue):
                    return False
            return True
        return False

    @expose('jira command line interface. Type "jat jira help" for more options.')
    def jira(self, args):
        if len(args) > 0 and args[0] == 'help':
            sys.stderr.write('Usage: jat jira <date range> <completed tickets> <users>\n')
            sys.stderr.write('Arguments: All arguments are optional and order independent.\n')
            sys.stderr.write('           <date range> Date range of the tickets to be returned.\n')
            sys.stderr.write('                        If <completed tickets> is set to 0, the createdDate will be used.\n')
            sys.stderr.write('                        If <completed tickets> is set to 1, the resolutiondate will be used.\n')
            sys.stderr.write('                        Options: day, week, month, year\n')
            sys.stderr.write('                        Default: No date restriction\n')
            sys.stderr.write('           <completed tickets> Indicates if tickets should be in a completed state.\n')
            sys.stderr.write('                               Options: 0 or 1\n')
            sys.stderr.write('                               Default: 0 (false)\n')
            sys.stderr.write('           <users> A comma separated list of Jira users.\n')
            sys.stderr.write('                   Default: Current user\n')
            sys.exit(2)

        date_range_options = ['day', 'week', 'month', 'year']
        complete_options = ['0', '1', 0, 1]
        # Date range: day, week, month, year
        range = None
        # If want completed results or not
        # 0 or 1
        complete = False
        # Users whose Jira tickets should be included
        users = None
        for value in args:
            if value in date_range_options:
                range = value
            elif value in complete_options:
                complete = bool(int(value))
            else:
                users = value

        status = 'in' if complete else 'not in'
        if range is not None:
            range = ' AND {} > {}()'.format('resolutiondate' if complete else 'createdDate',
                                            'startOf{}'.format(range.upper()))
        else:
            range = ''

        if users is None:
            users = self.getUser()

        issues = self.jiraCurl.post('search',
                                    jql='status {status} (Completed, Resolved, Closed) AND assignee in ({users}){range}'.format(
                                        status=status, users=users, range=range),
                                    maxResults='500')['issues']
        for issue in issues:
            sys.stdout.write('%-10s %s\n' % (issue['key'], issue['fields']['summary']))

    @expose('change info about this analysis')
    def put(self, args):
        id = self.getWorkingAnalysis()
        value = parse_jamo_query(args[1:])
        if value.isdigit():
            value = int(value)
        self.curl.put('api/analysis/analysis/%s' % id, data={args[0]: value})

    @expose('initializes a new analysis')
    def init(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat init [<analysis source> key] name\n')
            sys.stderr.write('\nThe possible analysis sources are:\n')
            sys.stderr.write(' jira\tCreate an analysis from a jira ticket\n')
        else:
            if len(args) == 1:
                location = args[0]
                key = location if location.count('/') == 0 else os.path.split(location)[0]
                data = {'key': key, 'name': key, 'status': 'Started'}
            else:
                if args[0] not in self.start_setupers:
                    sys.stderr.write('sorry the setup function you have provided %s is not allowed\n' % args[0])
                else:
                    if len(args) < 2:
                        sys.stderr.write('error: You failed to pass in the correct number of arguments\n')
                        sys.exit(2)
                    data = self.start_setupers[args[0]](args[1])
                    if len(args) == 2:
                        location = args[1]
                    else:
                        location = args[2]
            if location.count('/') > 0:
                if not location.startswith('/'):
                    location = os.path.join(os.getcwd(), location)
                if not os.path.exists(location):
                    os.makedirs(location)
                data['location'] = location
                analysis_id = self.curl.post('api/analysis/analysis', data)['analysis_id']
            else:
                data['name'] = location
                data['os_user'] = self.user
                ret = self.curl.post('api/analysis/analysis', data)
                location = ret['location']
                analysis_id = ret['analysis_id']

            if not os.path.exists(os.path.join(location, '.jamo')):
                os.makedirs(os.path.join(location, '.jamo'))
            with open(os.path.join(location, '.jamo', 'id'), 'w') as f:
                f.write(analysis_id)
            # files = []
            # for metadata in self.getKeywords(data['summary']):
            #    files.extend(self.curl.post('api/metadata/query',data={metadata[0]:metadata[1],'file_type':'fastq'}))
            # for file in files:
            #    sys.stderr.write( file['file_name\n'])

    @expose('Adds a file or files to be tracked')
    def add(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat add <file> <file type>\n')
            sys.exit(2)
        if len(args) != 2:
            sys.stderr.write('''error: invalid number of arguments. run 'jat add help' for help \n''')
            sys.exit(2)

    @expose('Removes a file or files from tracking')
    def rm(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat rm files..')
            sys.exit(2)

    @expose('Changes the users working directory to the tasks folder')
    def cd(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat cd task_id')
            sys.stderr.write(' run jat status to see all your current tasks\n')
            sys.exit(2)
        analysis = self.curl.get('api/analysis/analysis/%s' % args[0])
        if analysis is None:
            sys.stderr.write('error: You have specified an invalid task id: %s\n' % args[0])
            sys.exit(1)
        else:
            print(analysis['location'])

    def reset_group(self, group=None):
        if group is None:
            sys.stderr.write('usage: jat reset group <group>\nThe following groups are available:\n ')
            sys.stderr.write(' \n'.join(self.curl.get('api/core/groups')) + '\n')
            sys.exit(1)
        self.curl.put('api/core/user', group=group)

    @expose('resets some settings')
    def reset(self, args):
        reset_functions = {
            'auth': {'desc': 'Resets your tie to the ui', 'function': self.auth},
            'group': {'desc': 'Sets your current group to first argument', 'function': self.reset_group},
        }
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat reset <resource> [args...]\n\nThe available resources are:\n')
            for func in reset_functions:
                sys.stderr.write(' %s\t %s\n' % (func, reset_functions[func]['desc']))

            sys.exit(2)
        if args[0] not in reset_functions:
            sys.stderr.write('''error: Sorry resource '%s' is not a valid resource\n''' % args[0])
            sys.exit(2)
        try:
            reset_functions[args[0]]['function'](*args[1:])
        except TypeError:
            sys.stderr.write(
                'error: You have passed in the wrong number of arguments to this resource. %d passed and %d expected\n' % (
                    len(args) - 1, reset_functions[args[0]]['function'].__code__.co_argcount - 1))
            sys.exit(2)

    def getTemplates(self):
        if not self.loadedTemplates:
            self.loadTemplates()
        return self.templates

    def loadTemplates(self):
        self.templates = self.curl.get('api/analysis/templates')
        self.loadedTemplates = True

    def getTemplate(self, name, returnTemp=False, resolved=False):
        if resolved:
            template = self.curl.get('api/analysis/resolvedtemplate/%s' % name)
        else:
            template = self.curl.get('api/analysis/template/%s' % name)
        if template is None:
            if returnTemp:
                return {'name': name, 'description': '', 'tags': [],
                        'outputs': [{'label': '', 'required': True, 'description': ''}], 'public': False,
                        'group_public': False}
        else:
            return template

    def getQuery(self, args):
        if args[0] == 'custom':
            jsonD = parse_jamo_query(args[1:])
            if jsonD.startswith('{'):
                jsonD = json.loads(jsonD)
                return jsonD
            else:
                return toMongoObj(jsonD)

    def getMacros(self):
        if not self.loadedMacros:
            self.loadMacros()
        return self.macros

    def loadMacros(self):
        self.macros = self.curl.get('api/analysis/macros')
        self.loadedMacros = True

    def getMacro(self, name, returnTemp=False):
        template = self.curl.get('api/analysis/macro/%s' % name)
        if template is None:
            if returnTemp:
                return {'name': name, 'description': '',
                        'metadata': [{'key': '', 'required': True, 'type': '', 'description': ''}]}
        else:
            return template

    def manage_macro(self, name=None, file=None):
        if name is None:
            sys.stderr.write('usage: jat mange macro <name> [file]\n\nThe possible macros to edit are:\n')
            for macro in self.getMacros():
                sys.stderr.write(' %-15s %s\n' % (macro['name'], macro['description']))
            sys.exit(2)
        if file is not None:
            newTemplate = self.editData(fileLoc=file)
        else:
            macro = self.getMacro(name, True)
            newTemplate = self.editData(macro)
        if newTemplate is not None:
            try:
                self.curl.post('api/analysis/macro/%s' % name, data=newTemplate)
            except sdm_curl.CurlHttpException as e:
                sys.stderr.write('Failed to save this macro due to the following errors:\n')
                sys.stderr.write('  ' + ('\n  '.join(json.loads(''.join(e.response))['errors'])) + '\n')
                sys.exit(2)

    def manage_template(self, name=None, file=None):
        sys.stderr.write('sorry managing template has been disabled, use the repository\n')
        sys.exit(1)
#        if name is None:
#            sys.stderr.write('usage: jat mange template <name> [file]\n\nThe possible templates to edit are:\n')
#            for template in self.getTemplates():
#                sys.stderr.write(' %-15s %s\n' % (template['name'], template['description']))
#            sys.exit(2)
#        if file is not None:
#            newTemplate = self.editData(fileLoc=file)
#        else:
#            analysis_template = self.getTemplate(name, True)
#            newTemplate = self.editData(analysis_template)
#        if newTemplate is not None:
#            newTemplate['name'] = name
#            try:
#                self.curl.post('api/analysis/template/%s' % name, data=newTemplate)
#            except sdm_curl.CurlHttpException as e:
#                sys.stderr.write('Failed to save this template due to the following errors:\n')
#                sys.stderr.write('  ' + ('\n  '.join(json.loads(''.join(e.response))['errors'])) + '\n')
#                sys.exit(2)

    @expose('Runs a custom report with the returned metadata')
    def report(self, args):
        if len(args) == 0 or args[0] == 'help' or args[0] != 'select':
            sys.stderr.write('Usage: jat report select <args>\n\n')
            sys.stderr.write('\n')
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

            fields = (''.join(args[1:where_loc])).split(',')
            query = self.getQuery(['custom'] + args[where_loc + 1:])
            files = PageList(self.curl.post('api/analysis/pagequery', query=query, fields=fields, cltool=True,
                                            requestor=getpass.getuser()), self.curl, service='analysis')

            if outt in ('json', 'yaml'):
                outlis = []
                for file in files:
                    tmp = {}
                    for field in fields:
                        tmp[field] = file[field]
                    outlis.append(tmp)

                if outt == 'yaml':
                    print(yaml.safe_dump(outlis, default_flow_style=False))
                elif outt == 'json':
                    print(json.dumps(outlis))
            else:
                for file in files:
                    for field in fields:
                        sys.stdout.write('%s\t' % file[field])
                    sys.stdout.write('\n')
            return

    @expose('manage a resource')
    def manage(self, args):
        resources = {
            'template': {'desc': 'Modifies or creates a new analysis template', 'function': self.manage_template},
            'macro': {'desc': 'Modifies or creates a new metadata macro', 'function': self.manage_macro}
        }
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat manage <resource> [args...]\n\nThe available resources are:\n')
            for func in resources:
                sys.stderr.write(' %-15s %s\n' % (func, resources[func]['desc']))
            sys.exit(2)
        if args[0] not in resources:
            sys.stderr.write('''error: Sorry resource '%s' is not a valid resource\n''' % args[0])
            sys.exit(2)
        try:
            resources[args[0]]['function'](*args[1:])
        except TypeError:
            sys.stderr.write(
                'error: You have passed in the wrong number of arguments to this resource. %d passed and %d expected\n' % (
                    len(args) - 1, resources[args[0]]['function'].__code__.co_argcount - 1))
            raise
            sys.exit(2)

    def auth(self):
        """Starts the authorization flow to generate a user token that can be used from `jat` to identify the user.
        Will call `api/core/associate` in JAMO to begin the flow.
        """
        user = pwd.getpwuid(os.getuid())[0]
        tokenFile = os.path.expanduser('~/.jamo/token')
        if not os.path.exists(os.path.expanduser('~/.jamo')):
            jamo_dir = os.path.expanduser('~/.jamo')
            os.makedirs(jamo_dir)
            home_stats = os.stat(os.path.expanduser('~'))
            home_perms = home_stats[stat.ST_MODE]
            # Change directory permissions based on user's home permissions, minus write for group/other
            os.chmod(jamo_dir, home_perms & (~stat.S_IWGRP & ~stat.S_IWOTH))
        token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(50))

        if os.path.exists(tokenFile):
            with open(tokenFile) as f:
                tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
            if isinstance(tokenMap, basestring):
                tokenMap = {'https://sdm-dev.jgi-psf.org:8034': tokenMap}
        else:
            tokenMap = {}
        tokenMap[self.jamohost] = token
        with open(tokenFile, 'w') as f:
            f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
        os.chmod(tokenFile, stat.S_IRUSR | stat.S_IWUSR)

        user_token = self.curl.post('api/core/associate', user=user, token=token)
        if user_token is None:
            sys.stderr.write(
                'You will receive an email asking you to validate your account, you must click on the link before you can use this tool.\nIf an email doesn\'t appear make sure to check your spam folder\n')
            sys.exit(1)
        else:
            tokenMap[self.jamohost] = user_token
            with open(tokenFile, 'w') as f:
                f.write(yaml.safe_dump(tokenMap, default_flow_style=False))
            sys.stderr.write('Your identity has been confirmed. `jat` is ready for use.\n')

    @expose('list all the keys that match your query')
    def keys(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jamo keys [-v|-vv] <search string>\n')
            sys.stderr.write('\t-v: verbose, provides stats about keys, (list is tab-delimited)\n')
            sys.stderr.write('\t-vv: same as above, also shows where where keys are contained in arrays of sub-documents (shows as [])\n')
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
        for row in sorted(self.curl.get('api/analysis/keys/%s' % query), key=lambda d: d['_id']):
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

    @expose('Shows all the jat metadata for a specific jat key')
    def show(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: jat show jat-key\n\n')
            sys.exit(2)
        query = {'key': args[0]}
        files = self.curl.post('api/analysis/pagequery', query=query, cltool=True, requestor=getpass.getuser())
        if files['record_count']:
            for record in files['records']:
                print(yaml.safe_dump(record, default_flow_style=False))
        else:
            print("No matching records found")

    @expose('Prints this message')
    def help(self, args):
        sys.stderr.write('usage jat <command> [<args>]\n')
        sys.stderr.write('\nThe jat commands are:\n')
        for method in self.methods:
            if method in self.methodMap:
                sys.stderr.write(' %-15s %s\n' % (method, getattr(self, self.methodMap[method]).description))
            else:
                sys.stderr.write(' %-15s %s\n' % (method, getattr(self, method).description))
        sys.exit(2)

    def run(self, args):
        method = args[0]
        args = args[1:]
        if method not in self.methods:
            sys.stderr.write('''jat: '%s' is not a jat command. run 'jat help' for more options\n''' % method)
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes) > 0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n' % meth)
            sys.exit(2)
        # if len(args)>1 and os.path.isfile(args[-1]):
        #    file_name = args[-1]
        #    args = args[:-1]
        #    with open(file_name) as fi:
        #        for line in fi.readlines():
        #            args.append(line.rstrip())
        if method in self.methodMap:
            method = self.methodMap[method]
        getattr(self, method)(args)


def main():
    args = sys.argv[1:]
    newArgs = []
    options = []
    passed_options = False
    for arg in args:
        if arg.startswith('-') and not passed_options:
            options.append(arg)
        else:
            passed_options = True
            newArgs.append(arg)
    args = newArgs
    if args == ['reset', 'auth']:
        jat = JTT(options, True)
    else:
        jat = JTT(options)
    if len(args) == 0:
        jat.help(args)
    else:
        jat.run(args)

if __name__ == '__main__':
    main()
