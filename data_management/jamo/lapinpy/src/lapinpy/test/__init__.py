from lapinpy.curl import Curl
from lapinpy.lapinpy import RestServer
import socket
import os
from random import randrange

'''
    test every ui page
    perhaps buttons

    profile permissions
    profile retful calls

    test dependacies

'''


class Struct:
    def __init__(self, **entries):
        self.entries = entries
        self.__dict__.update(entries)

    def repr__(self):
        return '<%s>' % str('\n '.join('%s : %s' % (k, repr(v)) for (k, v) in self.__dict.iteritems()))


def get_port():
    port = None
    for i in range(10):
        port = randrange(8034, 9034)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', port))
        if result == 61:
            return port
    raise Exception('Failed to find an open port')


def generate_config(**kwargs):
    port = get_port()
    config = {
        'core_db': 'core_%d.db' % os.getpid(),
        'site_name': 'Test instance',
        'url': 'localhost:%d' % port,
        'port': port,
    }
    config.update(kwargs)
    return Struct(**config)


def start_instance(config, apps=[]):
    return TestInstance(config, apps)


class TestInstance():

    def __init__(self, config, apps):
        self.config = config
        self.server = RestServer.Instance()
        self.server.start(self.config, apps, block=False)
        self.core = self.server.core

    def get_curl(self, user=None, app=None, permissions=[]):
        ret = Curl('http://' + self.config.url, retry=0)
        if user is not None:
            user_info = self.core._post_user(None, {'name': user, 'email': user})
            for perm in permissions:
                self.core.post_userpermission(None, {'user_id': user_info['user_id'], 'permission': perm})
            tok_info = self.core.associate_user_token(user_info['user_id'], None)
            ret.userData = 'Bearer %s' % tok_info[0]['token']
        if app is not None:
            app_info = self.core.post_app(None, {'name': app})
            for perm in permissions:
                self.core.post_apppermission(None, {'id': app_info['id'], 'permission': perm})
            ret.userData = 'Application %s' % app_info['token']
        return ret

    def exit(self):
        self.server._exit(1, 1)
        os.remove(self.config.core_db)

    def loadApp(self, app_path):
        self.server.loadApp(app_path)

    def unloadApp(self, app):
        pass

    def mount(self, app, path):
        self.server.loadApp(path + '.py', app)
        self.server.apps[path] = app
        self.server.reloadUrls()
