import os
from subprocess import Popen, PIPE
import random


class CopyException(Exception):
    pass


# TODO: Is this still being used? Only references I see are commented out.
class FileCheck:

    def __init__(self, scratch='/tmp'):
        self.scratch = scratch

    # Cori remote file system hacks
    # return_type = 1: stdout
    # return_type = 0: command returncode
    def _run_command(self, cmd, return_type=1):
        child = Popen(cmd.split(' '), stdout=PIPE)
        stdout, stderr = child.communicate()
        if return_type:
            return stdout.rstrip()
        else:
            return child.returncode

    def isdir(self, file):
        if 'cscratch' in file:
            return self._run_command('ssh -q cori.nersc.gov test -d %s' % file, return_type=0) == 0
        else:
            return os.path.isdir(file)

    def isfile(self, file):
        if 'cscratch' in file:
            return self._run_command('ssh -q cori.nersc.gov test -f %s' % file, return_type=0) == 0
        else:
            return os.path.isfile(file)

    def realpath(self, file):
        if 'cscratch' in file:
            return self._run_command('ssh -q cori.nersc.gov realpath %s' % file)
        else:
            return os.path.realpath(file)

    def exists(self, file):
        if 'cscratch' in file:
            return self._run_command('ssh -q cori.nersc.gov test -e %s' % file, return_type=0) == 0
        else:
            return os.path.exists(file)

    def access(self, file):
        if 'cscratch' in file:
            return self._run_command('ssh -q cori.nersc.gov test -r %s' % file, return_type=0) == 0
        else:
            return os.access(file, os.R_OK)

    def stat(self, file):
        class dotdict(dict):
            __getattr__ = dict.get
            __setattr__ = dict.__setitem__
            __delattr__ = dict.__delitem__

        if 'cscratch' in file:
            stats = self._run_command('ssh -q cori.nersc.gov stat -c %%a,%%g,%%u,%%s,%%Y %s' % file)
            stat = dotdict({})
            (stat.st_mode, stat.st_gid, stat.st_uid, stat.st_size, stat.st_mtime) = [int(x) for x in stats.split(',')]
            return stat
        else:
            return os.stat(file)

    '''
    returns:
    original_file: bool
    new_file: original file or copy of file from remote system
    '''
    def copy_if_remote(self, file):
        if 'cscratch' in file:
            new_file = self.scratch + "/" + (''.join(random.choice('qwertyuiopasdfghjklzxcvbnm1234567890') for x in range(10))) + "." + os.path.basename(file)
            if self._run_command('scp -q cori.nersc.gov:%s %s' % (file, new_file), return_type=0) == 0:
                return False, new_file
            else:
                raise CopyException
        else:
            return True, file
    # End Cori remote file system hacks
