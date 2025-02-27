from .common import Struct
import datetime
import os
from . import sdmlogger
import sys
import yaml


class ConfigManager:

    def __init__(self, loc, settings=None, pass_files=True):
        self.files = {}
        self.loc = loc
        if settings is not None:
            self.settings = settings
            return
        self.logger = sdmlogger.getLogger('ConfigManager')
        self.settings = {}
        if os.path.isdir(loc):
            self._load_folder(loc)
        else:
            self._load_file(loc)
        if pass_files:
            self.check_for_file(self.settings)

    @classmethod
    def check_for_file(cls, val):
        if isinstance(val, dict):
            for k in list(val.keys()):
                if '_pass_file' in k or '_key_file' in k:
                    path = val[k]
                    if not os.path.exists(path):
                        raise ValueError(f"{path} given for key {k} does not exist")
                    with open(path, 'r') as f:
                        val[k[:-5]] = f.read().strip()
                else:
                    cls.check_for_file(val[k])
        elif isinstance(val, (list, tuple)):
            for sub in val:
                cls.check_for_file(sub)
        else:
            return

    def check_for_changes(self):
        changed_files = []
        if os.path.isdir(self.loc):
            for file in os.listdir(self.loc):
                if file.endswith('.config'):
                    app = file.replace('.config', '')
                    full_file = os.path.join(self.loc, file)
                    c_time = datetime.datetime.fromtimestamp(os.path.getmtime(full_file))
                    if full_file not in self.files or c_time > self.files[full_file]['last_modified']:
                        self.logger.info('Application settings file %s has been modified', app)
                        try:
                            with open(full_file) as f:
                                settings = yaml.load(f.read(), Loader=yaml.SafeLoader)
                            self.files[full_file] = {'app': app, 'last_modified': c_time}
                        except Exception:
                            if full_file in self.files:
                                self.files[full_file]['last_modified'] = c_time
                            sys.stderr.write('failed to load yaml file "{}"\n'.format(full_file))
                        self.settings[app] = settings
                        changed_files.append(app)
        else:
            c_time = datetime.datetime.fromtimestamp(os.path.getmtime(self.loc))
            if c_time > self.files[self.loc]['last_modified']:
                self.logger.info('main settings file has been updated')
                with open(self.loc) as f:
                    self.settings = yaml.load(f.read(), Loader=yaml.SafeLoader)
                self.files[self.loc] = {'last_modified': c_time}
                changed_files.append('lapinpy')
        return changed_files

    def _load_folder(self, folder):
        for file in os.listdir(folder):
            if file.endswith('.config'):
                full_file = os.path.join(folder, file)
                try:
                    with open(full_file) as f:
                        settings = yaml.load(f.read(), Loader=yaml.SafeLoader)
                    self.files[full_file] = {'app': file.replace('.config', ''), 'last_modified': datetime.datetime.fromtimestamp(os.path.getmtime(full_file))}
                except Exception:
                    sys.stderr.write('failed to load yaml file "%s"\n' % full_file)
                    raise
                self.settings[file.replace('.config', '')] = settings
        if 'lapinpy' not in self.settings:
            raise Exception('config file for lapinpy not found, cannot start')

    def _load_file(self, loc):
        with open(loc) as f:
            self.settings = yaml.load(f.read(), Loader=yaml.SafeLoader)
        self.files[loc] = {'last_modified': datetime.datetime.fromtimestamp(os.path.getmtime(loc))}

    def get_settings(self, application):
        ret = self.settings['lapinpy']['shared'].copy() if 'shared' in self.settings['lapinpy'] else {}
        if application in self.settings:
            ret.update(self.settings[application])
        return Struct(**ret)
