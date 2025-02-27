#!/usr/bin/env python
from __future__ import print_function
from builtins import range
from past.builtins import basestring
from builtins import object
import difflib
import json
import pwd
import os
import sdm_curl
import sys
import stat
import string
import random
import yaml
from jamo_common import expose


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


class JU(object):
    def __init__(self):
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
        if not os.path.exists(tokenFile):
            sys.stderr.write('error: Your idenity has not been identifed. I will now attempt to set that up\n')
            self.auth()
            sys.exit(1)
        with open(tokenFile) as f:
            tokenMap = yaml.load(f.read(), Loader=yaml.SafeLoader)
        if isinstance(tokenMap, basestring):
            tokenMap = {'https://sdm-dev.jgi-psf.org:8034': tokenMap}
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
            sys.stderr.write('error: Your identity has yet to be validated, you should have received and email with a link in it.\nIf you have not received it, run: "ju reset auth" to resend an email\n')
            sys.exit(2)
        self.curl.userData = 'Bearer %s' % token

    @expose('Adds a file with metadata into JAMO')
    def add(self, args):
        if len(args) == 0 or args[0] == 'help':
            sys.stderr.write('usage: ju add <file> <file type> <metadata json> <relative destination>\n')
            sys.exit(2)
        if len(args) != 4:
            sys.stderr.write('''error: invalid number of arguments. run 'ju add help' for help \n''')
            sys.exit(2)

        file, file_type, metadata_file, destination = args
        file = os.path.realpath(file)
        with open(metadata_file) as mf:
            metadata = json.loads(mf.read())
        if not destination.endswith('/'):
            destination += '/'
        print(self.curl.post('api/metadata/file', file=file, file_type=file_type, metadata=metadata, destination=destination))

    def reset_group(self, group=None):
        if group is None:
            sys.stderr.write('usage: ju reset group <group>\nThe following groups are available:\n ')
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
            sys.stderr.write('usage: ju reset <resource> [args...]\n\nThe available resources are:\n')
            for func in reset_functions:
                sys.stderr.write(' %s\t %s\n' % (func, reset_functions[func]['desc']))

            sys.exit(2)
        if args[0] not in reset_functions:
            sys.stderr.write('''error: Sorry resource '%s' is not a valid resource\n''' % args[0])
            sys.exit(2)
        try:
            reset_functions[args[0]]['function'](*args[1:])
        except TypeError:
            sys.stderr.write('error: You have passed in the wrong number of arguments to this resource. %d passed and %d expected\n' % (len(args) - 1, reset_functions[args[0]]['function'].__code__.co_argcount - 1))
            sys.exit(2)

    def auth(self):
        user = pwd.getpwuid(os.getuid())[0]
        tokenFile = os.path.expanduser('~/.jamo/token')
        if not os.path.exists(os.path.expanduser('~/.jamo')):
            os.makedirs(os.path.expanduser('~/.jamo'))
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
        if self.curl.post('api/core/associate', user=user, token=token) is None:
            sys.stderr.write('You will receive an email asking you to validate your account, you must click on the link before you can use this tool.\nIf an email doesn\'t appear make sure to check your spam folder\n')
            sys.exit(1)

    @expose('Prints this message')
    def help(self, args):
        sys.stderr.write('JAMO uploader\n Use this utility to upload one off files into jamo with certian metadata')
        sys.stderr.write('usage: ju <command> [<args>]\n')
        sys.stderr.write('\nThe ju commands are:\n')
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
            sys.stderr.write('''ju: '%s' is not a ju command. run 'ju help' for more options\n''' % method)
            closeOnes = difflib.get_close_matches(method, self.methods)
            if len(closeOnes) > 0:
                sys.stderr.write('\nDid you perhaps mean to call one of the following?\n')
                for meth in closeOnes:
                    sys.stderr.write('\t%s\n' % meth)
            sys.exit(2)
        if method in self.methodMap:
            method = self.methodMap[method]
        getattr(self, method)(args)


if __name__ == '__main__':
    args = sys.argv[1:]
    newArgs = []
    jamo = JU()
    if len(args) == 0:
        jamo.help(args)
    else:
        jamo.run(args)
