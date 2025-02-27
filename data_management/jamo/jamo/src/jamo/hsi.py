import datetime
import lapinpy.sdmlogger as logger
import select
import subprocess
import threading
import time
from lapinpy.curl import Curl


class HSIError(Exception):
    def __init(self, message):
        self.response = message
        Exception.__init__(self, message)

    def __repr__(self):
        return self.response


class HSI(object):
    timeout = 100

    def __init__(self, server):
        # I should probably open up a process instead of opening one every call
        self.server = server
        self.timeLock = threading.Lock()
        self.output = ''
        self.stop = False
        self.lastPut = time.time()
        #self.curl = Curl('https://newt.nersc.gov')   # TODO: AJTRITT - I think this is vestigial
        self.__connect()

    def __connect(self):
        logger.getLogger('hsi').info(f'connecting to hsi server {self.server}')
        self.process = subprocess.Popen(f'hsi -h {self.server}', shell=True, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        universal_newlines=True, bufsize=0)
        self.readT = threading.Thread(target=self.__read)
        self.readT.start()
        time.sleep(1)
        self.check_output('pwd')

    def __del__(self):
        self.exit()
        self.readT.join()

    def __read(self):
        fd = self.process.stdout
        try:
            poll = select.poll()
            poll.register(fd.fileno(), select.POLLIN or select.POLLPRI)
            while True:
                if self.stop:
                    self.process.communicate()
                    break
                fde = poll.poll(self.timeout)
                if len(fde):
                    f = fde[0]
                    if f[1] > 0:
                        # Data is available asynchronously, so instead of reading by char, we wait until a line is
                        # available to read the whole output, otherwise we may have partial reads since we update
                        # `self.lastPut` after each character read and following events may be empty before the
                        # whole output is read (in Python3).
                        line = fd.readline()
                        # char = fd.read(1)
                        with self.timeLock:
                            # self.output += char
                            self.output = line
                            self.lastPut = time.time()
        except Exception:
            pass

    def get_output(self):
        ret = None
        with self.timeLock:
            if self.output != '':
                ret = self.output
                self.output = ''
        return ret

    def check_output(self, command):
        string = self.get_output()
        while string is not None:
            string = self.get_output()
            time.sleep(.01)
        now_time = time.time()
        self.write(command)
        while True:
            with self.timeLock:
                lastPut = self.lastPut
            if lastPut > now_time:
                return self.get_output()
            time.sleep(.2)

    def exit(self):
        if self.process is not None:
            try:
                self.stop = True
            except Exception:
                raise

    def write(self, string):
        try:
            self.process.stdin.write(string + "\n")
        except IOError:
            self.__connect()
            time.sleep(3)
            self.get_output()
            self.process.stdin.write(string + '\n')

    def put_file(self, from_path, to):
        return subprocess.check_output(['hsi', '-h', self.server, f'put -p -P {from_path} : {to}'], stderr=subprocess.STDOUT)

    def getAllFileInfo(self, tape_file):
        # returns a tuple of permissions, user, group, filesize, modified date
        response = self.check_output(f'ls -D {tape_file}')
        if response.startswith('***'):
            raise HSIError(response)
        ret = response.split()
        return ret[0], ret[2], ret[3], int(ret[4]), datetime.datetime.strptime(' '.join(ret[6:10]), '%b %d %H:%M:%S %Y')

    def getFileInfo(self, tape_file):
        response = self.check_output(f'ls -D {tape_file}')
        if response.startswith('***'):
            raise HSIError(response)
        ret = response.split()
        return ret[5], datetime.datetime.strptime(' '.join(ret[6:10]), '%b %d %H:%M:%S %Y')

    def getTapeInfo(self, tape_file):
        response = self.check_output(f'ls -P {tape_file}')
        if response.startswith('***'):
            raise HSIError(response)
        resp = response.split()
        return resp[5], resp[4]

    def isontape(self, tape_file):
        response = self.check_output(f'ls -U {tape_file}')
        if response.startswith('***'):
            raise HSIError(response)

        if response.split()[7] != 'TAPE':
            return False
        return True

    def removefile(self, tape_file):
        response = self.check_output(f'rm {tape_file}')
        if response.startswith('***'):
            raise HSIError(response)
        return True

    def purge(self, tape_file):
        return self.check_output(f'purge {tape_file}')

    def movefile(self, source, dest):
        self.check_output(f'mv -v {source} {dest}')
        return True

    def runHtarCommand(self, command):
        cmd = f'htar -H server={self.server} {command}'
        subprocess.check_output(cmd, shell=True)
        return True

    def removeHtarEntity(self, htarRecord, tape_file):
        relativeFile = tape_file.replace(htarRecord['root_path'] + '/', '')
        return self.runHtarCommand(f'-Df {htarRecord["remote_path"]} {relativeFile}')


class HSI_status(object):
    def __init__(self):
        self.curl = Curl('https://api.nersc.gov/api/v1.2/status')
        self.name_map = {'hpss': 'regent', 'archive': 'archive'}
        self.use_api = 1
        self.cache_for_seconds = 30
        self.server_state = dict()
        for item in self.name_map:
            self.server_state[item] = {"time": 0, "state": False}

    def isup(self, server):
        # Just get the name, in case we get a qualified name such as hpss.nersc.gov
        server = server.split('.')[0]
        #  Do we know about this server
        if server not in self.server_state:
            return False
        #  get the time in seconds
        now_time = time.time()

        # return the cached value
        if now_time > self.server_state[server]['time']:
            ret = False
            if self.use_api:
                try:
                    service = self.curl.get(self.name_map[server])
                    if service['status'] in ('Up', 'active', 'up', 'degraded'):
                        ret = True
                except Exception as e:  # noqa: F841
                    pass

            self.server_state[server]['time'] = now_time + self.cache_for_seconds
            self.server_state[server]['state'] = ret
        return self.server_state[server]['state']


if __name__ == '__main__':
    test = HSI_status()
    print("Call service")
    print(test.isup('archive'))
    print(test.isup('hpss'))
    print("\ntest cache")
    print(test.isup('archive'))
    print(test.isup('hpss'))
    print("\ntest names")
    print(test.isup('archive.nersc.gov'))
    print(test.isup('hpss.nersc.gov'))
