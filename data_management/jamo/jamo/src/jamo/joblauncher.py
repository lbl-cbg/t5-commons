from __future__ import print_function
import rpyc
from hsi import HSI
from sdm_curl import Curl
from rpyc.utils.server import OneShotServer


class DNAService(rpyc.Service):

    def on_connect(self):
        self.curl = Curl('https://jamo.jgi.doe.gov')
        self.hsi_sessions = {}
        print('something connected!')

    def on_disconnect(self):
        print('disconnected')

    def exposed_getTapeLocation(self, service, file):
        serviceS = str(service)
        if serviceS not in self.hsi_sessions:
            serviceInfo = self.curl.get('api/tape/backupservice/%s' % serviceS)
            self.hsi_sessions[serviceS] = HSI(serviceInfo['server'])
        return self.hsi_sessions[serviceS].getTapeInfo(file)


if __name__ == '__main__':
    s = OneShotServer(DNAService, port=1337)
    s.start()
