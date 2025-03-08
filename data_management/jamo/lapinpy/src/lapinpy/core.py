import getpass
import os
import random
import string
import psutil
import cherrypy
import urllib
import math
import functools

from pkg_resources import resource_filename

from lapinpy import sdmlogger, restful, common
from lapinpy.mongorestful import MongoRestful
from lapinpy.mysqlrestful import MySQLRestful
from lapinpy.oauth2 import start_session

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future.utils import iteritems
from builtins import map
from future.standard_library import install_aliases
import subprocess
install_aliases()
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


def compareVersions(x, y):
    xs = list(map(int, x.split('.')))
    ys = list(map(int, y.split('.')))
    if len(xs) < len(ys):
        xs, ys = (ys, xs)
    for i in range(len(xs)):
        if len(ys) < i:
            ys[i] = 0
        dif = xs[i] - ys[i]
        if dif != 0:
            return dif
    return 0


@restful.menu("Core", 1)
class Core(MySQLRestful):
    permission_tree = {}
    chart_mappings = {}
    auto_reload = True
    permissions = {}

    def __init__(self, config):
        self.config = config
        MySQLRestful.__init__(self, self.config.mysql_host, self.config.mysql_user, self.config.mysql_pass, self.config.core_db, host_port=getattr(self.config, 'mysql_port', None))
        currentVersion = None
        try:
            currentVersion = self.getSetting('core', 'db_version', '1.0.2')
        except Exception as e:  # noqa: F841
            # TODO: Why are getting the path for the table script if it's unused (code using it is commented out, we
            #  should consider deleting this block...
            tableScript = resource_filename('lapinpy', 'db/tables.sql')  # noqa: F841
            # self.runScript(tableScript)
        if currentVersion is not None:
            migrateVersions = [file[:-4] for file in
                               os.listdir(resource_filename('lapinpy', 'db/migrate')) if
                               file.endswith('.sql')]
            newVersion = None
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            try:
                sVersions = sorted(migrateVersions, cmp=compareVersions)
            except TypeError as e:  # noqa: F841
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                sVersions = sorted(migrateVersions, key=functools.cmp_to_key(compareVersions))
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            for version in sVersions:
                if compareVersions(currentVersion, version) < 0:
                    # self.runScript(os.path.realpath(os.path.dirname(__file__) + '/../db/migrate/%s.sql' % version))
                    newVersion = version
            if newVersion is not None:
                self.saveSetting('core', 'db_version', newVersion)

        self.logger = sdmlogger.getLogger('core', level=None)

        self.scratchDir = os.path.join(getattr(self.config, 'scratch', '/tmp'), 'lapin')
        self.set_chartMappings()
        perms = self.get_permissions(None, None)
        self.tempTokens = {}
        self.token_reservations = {}
        self.cron_enabled = True
        for row in perms:
            self.permissions[str(row['id'])] = row['name']
        # Temporary fix for open file leak caused by generate_data_info
        self.temp_mongo = MongoRestful(self.config.mongoserver, self.config.mongo_user, self.config.mongo_pass, self.config.meta_db, thread=False)

    def get_runningas(self, args, kwargs):
        return {'user': getpass.getuser()}

    def put_permission_tree(self, args, kwargs):
        rows = self.query('select p1.name as parent,p2.name as child from permission_group g left join permission p1 on p1.id=g.permission left join permission p2 on p2.id=has_permission')
        for row in rows:
            parent = row['parent']
            child = row['child']
            if parent not in self.permission_tree:
                self.permission_tree[parent] = []
                self.permission_tree[parent].append(parent)
            if child not in self.permission_tree:
                self.permission_tree[child] = []
                self.permission_tree[child].append(child)
            self.permission_tree[parent].append(self.permission_tree[child])

    def get_modules(self, args=None, kwargs=None):
        # Load any module configurations from the database
        modules = self.query('select * from modules')
        # Append any module configurations from the config file (if defined).
        if hasattr(self.config, 'modules_to_load'):
            modules += self.config.modules_to_load
        return modules

    def addModule(self, module):
        # Skip add functionality for modules defined in the configuration as per JAMO-1416
        if hasattr(self.config, 'modules_to_load') and module.appname in [mod.get('name') for mod in
                                                                          self.config.modules_to_load]:
            self.logger.info(
                'Skipping adding module for {} since it is defined in config `modules_to_load`'.format(module.appname))
            return
        if len(self.query('select * from modules where name=%s', [module.appname])) > 0:
            self.modify('update modules set path=%s where name=%s', module.location, module.appname)
        else:
            self.modify('insert into modules values (%s,%s)', module.appname, module.location)

    def generate_apptoken(self, name, permissions):
        app_info = self.get_app([name], None)
        if app_info is None:
            app_info = self.post_app(None, {'name': name})
        appperms = self.get_apppermissions([app_info['token']], None)
        for perm in permissions:
            if perm not in appperms and perm is not None:
                self.post_apppermission(None, {'id': app_info['id'], 'permission': perm})
        return app_info

    @restful.permissions('admin')
    def post_togglemethodcron(self, args, kwargs):
        methods = self.restserver.apps[kwargs['app']].cronMethods
        for method in methods:
            if method.__name__ == kwargs['method_name']:
                method.__func__.enabled = not method.enabled

    @restful.permissions('admin')
    def post_triggercron(self, args, kwargs):
        methods = self.restserver.apps[kwargs['app']].cronMethods
        for method in methods:
            if method.__name__ == kwargs['method_name']:
                self.restserver.run_cron_method(method, True)

    @restful.menu('Cron Jobs')
    @restful.permissions('admin')
    @restful.table(title='Cron Jobs', map={
        'toggle': {'type': 'html', 'value': restful.Button('Toggle', post_togglemethodcron, 'method_name', 'app')},
        'trigger': {'type': 'html', 'value': restful.Button('Trigger', post_triggercron, 'method_name', 'app')}})
    def get_crons(self, args, kwargs):
        applications = self.restserver.apps
        ret = []
        for application in applications:
            cronMethods = applications[application].cronMethods
            for method in cronMethods:
                ret.append({'app': application, 'method_name': method.__name__, 'next_event': method.nextEvent,
                            'last_ran': method.lastRan, 'enabled': method.enabled,
                            'parameters': ' : '.join(method.cron)})
        return ret

    @restful.table(title='permissions')
    def get_permissions(self, args, kwargs):
        return self.query('select * from permission')

    def post_permission(self, args, kwargs):
        id = self.smart_insert('permission', kwargs)
        if id > 0:
            self.permissions[str(id)] = kwargs['name']
        return id

    def get_childpermissions(self, args, kwargs):
        if args[0] in self.permission_tree:
            children = self.permission_tree[args[0]]
            return self.get_list_items(children)
        return args

    @restful.passreq
    def get_whoami(self, args, kwargs):
        auth = kwargs['__auth']
        auth['permissions'] = self.get_userpermissions([auth['user'] + '@lbl.gov'], None)
        return auth

    def get_userinfo_from_user_token(self, token):
        ret = self.query('select email as user, `group`, division from user_tokens t left join user u on u.user_id=t.user_id where token=%s', [token])
        if len(ret) > 0:
            ret = ret[0]
            ret['user'] = ret['user'].split('@')[0]
            return ret

    def get_appinfo_from_token(self, token):
        ret = self.query('select name as user, `group`, division from application_tokens t left join application a on a.id=t.application_id where token=%s', [token])
        if len(ret) > 0:
            ret = ret[0]
            if ret['group'] is None:
                ret['group'] = ret['user']
            return ret

    def get_list_items(self, list):
        ret = []
        for item in list:
            if isinstance(item, str):
                ret.append(item)
            else:
                ret.extend(self.get_list_items(item))
        return ret

    def get_groups(self, args, kwargs):
        appNames = [x['name'] for x in self.query('select name from application')]
        distinct_group_names = self.query('select distinct `group` from user')
        for name in distinct_group_names:
            if name['group'] is not None and name['group'] not in appNames:
                appNames.append(name['group'])
        return appNames

    @restful.permissions('admin')
    def _post_user(self, args, kwargs):
        self.modify("insert into user (user_id,email,name,`group`) values (null,%s,%s,null)", kwargs['email'].lower(), kwargs['name'])
        return self.query("select * from user where email=%s", [kwargs['email'].lower()])[0]

    @restful.permissions('admin')
    @restful.validate({'name': {'type': str}})
    def post_app(self, args, kwargs):
        token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
        self.modify('insert into application values (null, %s ,%s, %s)', kwargs['name'], kwargs['name'], self.config.default_division)
        ret = self.get_app([kwargs['name']], None)
        self.modify('insert into application_tokens (application_id, token) values (%s, %s)', ret['id'], token)
        ret['token'] = token
        return ret

    @restful.permissions('admin')
    @restful.validate({'app': {'type': str}, 'permission': {'type': str}})
    def put_apppermission(self, args, kwargs):
        pass

    @restful.menu("Apps")
    @restful.permissions('admin')
    @restful.table(title='Apps', map={'name': {'value': '<a href="/core/app/{{value}}">{{value}}</a>'}})
    def get_apps(self, args, kwargs):
        return self.query('select * from application a left join application_tokens t on t.application_id=a.id', extras=kwargs)

    @restful.permissions('admin')
    def set_chartMappings(self):
        results = self.query('select * from chart_conf')
        for row in results:
            self.chart_mappings[row['url']] = row['conf']

    def associate_user_token(self, userid, token):
        if token is None:
            token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
        # self.modify("delete from user_tokens where user_id = %s",userid)
        self.modify("insert into user_tokens values (null,%s,%s)", userid, token)
        return self.query("select * from user_tokens where token=%s", [token])

    def associate_app_token(self, appid, token):
        if token is None:
            token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
        self.modify("insert into application_tokens (application_id, token) values (%s,%s)", appid, token)
        return self.query("select * from application_tokens where token=%s", [token])

    # TODO This should be a delete rather than a get.  Also the redirect doesn't seem to work correctly
    @restful.permissions('admin')
    def get_removeuserpermission(self, args, kwargs):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(kwargs['permission'], basestring) and not kwargs['permission'].isdigit():
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(kwargs['permission'], str) and not kwargs['permission'].isdigit():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            kwargs['permission'] = self.query('select id from permission where name=%s', [kwargs['permission']])[0]['id']
        if len(self.query('select * from user_permissions where user_id=%s and permission=%s', [kwargs['user_id'], kwargs['permission']])) == 1:
            self.delete('delete from user_permissions where user_id=%s and permission=%s', kwargs['user_id'], kwargs['permission'])
        return {}

    @restful.permissions('admin')
    @restful.table(title='User permissions', map={'Delete': {'type': 'html',
                                                             'value': restful.Button('Remove', get_removeuserpermission,
                                                                                     'permission', 'user_id')}})
    def get_userpermissions2(self, args, kwargs):
        perms = self.query("select p.name, up.user_id from user_permissions up left join permission p on p.id=up.permission left join user u on u.user_id=up.user_id where u.email=%s order by p.name", [args])
        ret = []
        for perm in perms:
            ret.append({'permission': self.get_childpermissions([perm['name']], None)[0], 'user_id': perm['user_id']})
        return ret

    def get_userpermissions(self, args, kwargs):
        perms = self.query("select p.name from user_permissions up left join permission p on p.id=up.permission left join user u on u.user_id=up.user_id where u.email=%s order by p.name", [args])
        ret = []
        for perm in perms:
            ret.extend(self.get_childpermissions([perm['name']], None))
        return ret

    @restful.permissions('admin')
    def get_permissions_from_user_token(self, token):
        perms = self.query("select p.name from user_tokens ut left join user_permissions up on up.user_id=ut.user_id left join permission p on p.id=up.permission left join user u on u.user_id=up.user_id where ut.token=%s", [token])
        ret = []
        for perm in perms:
            if perm['name'] is not None:
                ret.extend(self.get_childpermissions([perm['name']], None))
        return ret

    # TODO This should be a delete rather than a get.  Also the redirect doesn't seem to work correctly
    @restful.permissions('admin')
    def get_removeapppermission(self, args, kwargs):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(kwargs['permission'], basestring) and not kwargs['permission'].isdigit():
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(kwargs['permission'], str) and not kwargs['permission'].isdigit():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            kwargs['permission'] = self.query('select id from permission where name=%s', [kwargs['permission']])[0]['id']
        if len(self.query('select * from application_permissions where application=%s and permission=%s', [kwargs['application_id'], kwargs['permission']])) == 1:
            self.delete('delete from application_permissions where application=%s and permission=%s', kwargs['application_id'], kwargs['permission'])
        return {}

    @restful.permissions('admin')
    @restful.table(title='App permissions', map={'Delete': {'type': 'html',
                                                            'value': restful.Button('Remove', get_removeapppermission,
                                                                                    'permission', 'application_id')}})
    def get_apppermissions2(self, args, kwargs):
        perms = self.query("select a.application, p.name from application_tokens at left join application_permissions a on a.application=at.application_id left join permission p on p.id=a.permission  where at.token=%s order by p.name", [args])
        ret = []
        for perm in perms:
            ret.append({'permission': self.get_childpermissions([perm['name']], None)[0], 'application_id': perm['application']})
        return ret

    @restful.permissions('admin')
    def get_apppermissions(self, args, kwargs):
        perms = self.query("select p.name from application_tokens at left join application_permissions a on a.application=at.application_id left join permission p on p.id=a.permission  where at.token=%s", [args])
        ret = []
        for perm in perms:
            ret.extend(self.get_childpermissions([perm['name']], None))
        return ret

    def post_apppermission(self, args, kwargs):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(kwargs['permission'], basestring) and not kwargs['permission'].isdigit():
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(kwargs['permission'], str) and not kwargs['permission'].isdigit():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            kwargs['permission'] = self.query('select id from permission where name=%s', [kwargs['permission']])[0][
                'id']
        if len(self.query('select * from application_permissions where application=%s and permission=%s',
                          [kwargs['id'], kwargs['permission']])) == 0:
            self.modify('insert into application_permissions (application, permission) values (%s, %s)', kwargs['id'],
                        kwargs['permission'])

    def post_userpermission(self, args, kwargs):
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(kwargs['permission'], basestring) and not kwargs['permission'].isdigit():
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(kwargs['permission'], str) and not kwargs['permission'].isdigit():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            kwargs['permission'] = self.query('select id from permission where name=%s', [kwargs['permission']])[0][
                'id']
        if len(self.query('select * from user_permissions where user_id=%s and permission=%s',
                          [kwargs['user_id'], kwargs['permission']])) == 0:
            self.modify('insert into user_permissions (user_id, permission) values (%s, %s)', kwargs['user_id'],
                        kwargs['permission'])

    @restful.permissions('admin')
    @restful.generatedhtml(title='App {{value}}')
    @restful.ui_link(restful.Selection('Add permission', 'permission', permissions, post_apppermission, 'id'))
    @restful.link(get_apppermissions2, 'token', 'permissions')
    @restful.single
    def get_app(self, args, kwargs):
        return self.query(
            'select a.id,a.name, t.token, t.created from application a left join application_tokens t on t.application_id=a.id where a.name=%s',
            args[0])

    @restful.permissions('admin')
    @restful.generatedhtml(title='User {{value}}')
    @restful.ui_link(restful.Selection('Add permission', 'permission', permissions, post_userpermission, 'user_id'))
    @restful.link(get_userpermissions2, 'email', 'permissions')
    @restful.single
    def get_user(self, args, kwargs):
        email = args[0]
        if email.count("@") == 0:
            email += '@lbl.gov'
        email = email.lower()
        return self.query('select * from user where email=%s', [email])

    @restful.passreq
    def get_self(self, args, kwargs):
        return kwargs['__auth']

    @restful.permissions('admin')
    @restful.menu('users')
    @restful.table(title='Users', map={'email': {'value': '<a href="/core/user/{{value}}">{{value}}</a>'}})
    def get_users(self, args, kwargs):
        return self.query("select * from user", extras=kwargs)

    @restful.permissions('admin')
    def get_user_from_id(self, usertoken):
        return self.query('select * from user_tokens t left join user u on u.user_id=t.user_id where token=%s', [usertoken])

    @restful.permissions('admin')
    def post_chart(self, args, kwargs):
        wrapper = kwargs['wrapper']
        chart_url = kwargs['chart']
        chart_url = "/".join(chart_url.split("/")[:4])
        self.modify('insert into chart_conf (url, conf) values (%s,%s) on duplicate key update conf=%s', chart_url, wrapper, wrapper)
        self.chart_mappings[chart_url] = wrapper

    @restful.permissions('admin')
    def get_settings(self, args, kwargs):
        if args is None or len(args) == 0:
            args = ['lapinpy']
        return self.restserver.configManager.get_settings(args[0]).entries

    @restful.permissions('admin')
    @restful.menu('modules')
    @restful.table(title='modules',
                   map={'name': {'order': 0, 'value': '<a href="/core/application/{{value}}">{{value}}</a>'}})
    def get_applications(self, args, kwargs):
        apps = self.restserver.apps
        ret = []
        for app in apps:
            application = apps[app]
            ret.append({'name': app, 'file_location': application.location, 'cron_enabled': application.cron_enabled,
                        'auto_reload': application.auto_reload, 'last_modified': application.last_modified,
                        'loaded': application.loaded_time})
        return ret

    @restful.permissions('admin')
    @restful.table(title='Cron events', map={
        'toggle': {'type': 'html', 'value': restful.Button('Toggle', post_togglemethodcron, 'method_name', 'app')},
        'trigger': {'type': 'html', 'value': restful.Button('Trigger', post_triggercron, 'method_name', 'app')}})
    def get_applicationcron(self, args, kwargs):
        apps = self.restserver.apps
        application = apps[args[0]]
        cronMethods = application.cronMethods
        ret = []
        for method in cronMethods:
            ret.append({'app': args[0], 'method_name': method.__name__, 'enabled': method.enabled,
                        'last_job_name': method.lastJobName, 'last_ran': method.lastRan, 'next_event': method.nextEvent,
                        'parameters': ' : '.join(method.cron)})
        return ret

    # @restful.permissions('admin')
    def post_reloadapp(self, args, kwargs):
        if 'name' in kwargs:
            file = os.path.join(self.restserver.apps[kwargs['name']].location, '%s.py' % kwargs['name'])
        else:
            file = kwargs['file']
        return self.restserver.reloadApp(file)

    @restful.permissions('admin')
    def post_togglecron(self, args, kwargs):
        application = self.restserver.apps[kwargs['name']]
        application.cron_enabled = not application.cron_enabled
        for cronMethod in application.cronMethods:
            cronMethod.__func__.enabled = application.cron_enabled

    @restful.permissions('admin')
    @restful.table(title='Imported libs')
    def get_applicationimports(self, args, kwargs):
        apps = self.restserver.apps
        application = apps[args[0]]
        ret = []
        if not hasattr(application, 'loaded_imports'):
            return []
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for lib, path in iteritems(application.loaded_imports):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for lib, path in application.loaded_imports.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ret.append({'library': lib, 'path': path})
        return ret

    applicationjobs_table = {'title': 'Jobs',
                             'table': {'columns': [['status', {}],
                                                   ['job_id', {'title': 'id'}],
                                                   ['process_id', {'title': 'Process ID',
                                                                   'type': 'link',
                                                                   'inputs': {'text': '{{process_id}}',
                                                                              'title': 'Job Details',
                                                                              'url': '/api/core/job/{{process_id}}'}}],
                                                   ['job_name', {'title': 'name'}],
                                                   ['job_path', {'title': 'path'}],
                                                   ['minutes', {}],
                                                   ['cores', {}]],
                                       'sort': {'enabled': False,
                                                'default': {'column': 'job_id', 'direction': 'desc'}}},
                             'data': {'url': 'applicationjobs',
                                      'default_query': 'pipeline = {{value}}'}}

    @restful.queryResults(applicationjobs_table)
    def get_applicationjobs(self, args, kwargs):
        return self.get_jobs2((), {'pipeline': args[0], 'record_id': None})

    @restful.permissions('admin')
    @restful.single
    @restful.ui_link(restful.Button('Reload App', post_reloadapp, 'name'))
    @restful.ui_link(restful.Button('Toggle Cron', post_togglecron, 'name'))
    @restful.table_link(get_applicationjobs, 'name', applicationjobs_table, 'jobs')
    @restful.link(get_applicationcron, 'name', 'cron_events')
    @restful.link(get_applicationimports, 'name', 'imports')
    @restful.generatedhtml(title='Module: {{value}}')
    def get_application(self, args, kwargs):
        apps = self.restserver.apps
        application = apps[args[0]]
        return [{'name': args[0], 'file_location': application.location, 'cron_enabled': application.cron_enabled,
                 'auto_reload': application.auto_reload, 'last_modified': application.last_modified,
                 'loaded': application.loaded_time}]

    job_map = {'submitted_date': {'order': 0},
               'status': {'order': 1},
               'job_id': {'order': 2},
               'job_name': {'order': 3},
               'job_path': {'order': 4},
               'exit_code': {'order': 5},
               'process_id': {'order': 6},
               'started_date': {'order': 7},
               'ended_date': {'order': 8},
               'machine': {'order': 9},
               'platform': {'order': 10},
               'minutes': {'order': 11},
               'cores': {'order': 12},
               'pipeline': {'order': 13},
               'process': {'order': 14},
               'record_id': {'order': 15},
               'record_id_type': {'order': 16},
               'sge_id': {'order': 17}
               }

    def post_job(self, _args, kwargs):
        data = {'status': 'Submitted', 'submitted_date': 'now()'}
        data.update({field: kwargs[field] for field in self.job_map if field in kwargs})
        return self.smart_insert('job', data)

    @restful.table(title='Jobs', map=job_map)
    @restful.menu('Jobs')
    @restful.permissions('admin')
    def get_jobs(self, _args, kwargs):
        extras = kwargs
        query = 'select * from job'
        if hasattr(self.config, 'job_monitor'):
            query += ' where pipeline in ({})'.format(','.join(['"' + pipeline + '"' for pipeline in self.config.job_monitor]))
        if 'tq' in kwargs and 'order by' not in kwargs['tq']:
            extras['tq'] = 'order by submitted_date DESC ' + extras['tq']
        return self.query(query, extras=extras)

    @restful.table(title='Jobs', map=job_map)
    def get_jobs2(self, _args, kwargs):
        extra = []
        where = ''
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, value in iteritems(kwargs):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, value in kwargs.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if value is None or value == 'null':
                extra.append('({key} is null or {key} = "")'.format(key=key))
            else:
                if isinstance(value, int):
                    extra.append('%s = %d' % (key, value))
                else:
                    extra.append('%s = "%s"' % (key, str(value)))

        if extra:
            where = "where " + " and ".join(extra)

        return self.query('select * from job %s order by job_id desc' % where)

    def put_job(self, _args, kwargs):
        try:
            job_id = int(kwargs['job_id'])
            del kwargs['job_id']
            data = {}
            if 'status' in kwargs:
                if kwargs['status'] == 'Started':
                    field = 'started_date'
                else:
                    field = 'ended_date'
                data[field] = 'now()'
            data.update({field: kwargs[field] for field in self.job_map if field in kwargs})
            self.smart_modify('job', 'job_id=%d' % job_id, data)
        except Exception:
            pass

    @restful.single
    def get_job(self, args, kwargs):
        self.logger.debug('args for get_job = {}'.format(args))
        return self.query('select * from job where job_id=%s', args)

    @restful.cron('*/15', '*', '*', '*', '*')
    def monitorjobs(self):
        where_pipeline = ''
        if hasattr(self.config, 'job_monitor'):
            where_pipeline = 'and pipeline in ({})'.format(','.join(['"' + pipeline + '"' for pipeline in self.config.job_monitor]))

        if hasattr(self.config, 'use_slurm') and self.config.use_slurm:
            lines = subprocess.check_output('sacct -bnP', shell=True).split('\n')
            sge = {}
            for line in lines:
                if '|' in line:
                    (_id, state, error) = line.split('|')
                    if '.' not in _id:
                        (error1, error2) = error.split(':')
                        sge[_id] = {'state': state, 'error': error2}

            # we are looking at the sacct list, so we only want jobs that got submitted to slurm
            running_jobs = self.query('select * from job where status = "Started" and sge_id is not null')
            for job in running_jobs:
                sge_id = str(job['sge_id'])
                status = None
                exit_code = -1
                if sge_id not in sge:
                    status = 'Lost'
                else:
                    sge_state = sge[sge_id].get('state', None)
                    if sge_state == 'COMPLETED':
                        status = 'Finished'
                        exit_code = 0
                    elif sge_state == 'OUT_OF_MEMORY':
                        status = 'Killed'
                    elif sge_state == 'TIMEOUT':
                        status = 'Timeout'
                    elif sge_state == 'FAILED':
                        status = 'Failed'

                if status is not None:
                    query = 'update job set status="{status}", exit_code={exit_code}, ended_date=now() where sge_id={sge_id} {where_pipeline}'\
                            .format(status=status, exit_code=exit_code, sge_id=sge_id, where_pipeline=where_pipeline)
                    self.modify(query)
        else:
            if hasattr(self.config, 'job_platform') and 'localhost' in self.config.job_platform:
                try:
                    pids = psutil.pids()
                    query = 'select * from job where status = "Started" {where_pipeline}'.format(where_pipeline=where_pipeline)
                    running_jobs = self.query(query)
                    for job in running_jobs:
                        job_id = job['process_id']
                        if job_id not in pids:
                            self.modify('update job set status="Lost", exit_code = -1, ended_date=now() where process_id = {}'.format(job_id))
                except Exception as e:
                    self.logger.error(repr(e))

    @restful.menu('add app')
    @restful.permissions('admin')
    @restful.rawHTML(urlpath='/addapp', title='Add Application')
    def appForm(self, args, kwargs):
        return '<form method="post" action="/api/core/app"><div>Application:</div><input id="name" name="name" class="element text medium" type="text" maxlength="255" value=""/> <input id="saveForm" class="button_text" type="submit" name="submit" value="Submit"/></form>'

    def get_status(self, args, kwargs):
        apps = self.restserver.apps
        ret = {}
        for app in apps:
            application = apps[app]
            if hasattr(application, '__test__'):
                ret[app] = application.__test__()

        return ret

    def getSetting(self, application, setting, default):
        ret = self.query('select value from setting where application=%s and setting=%s', [application, setting])
        if len(ret) == 0:
            return default
        return ret[0]['value']

    def saveSetting(self, application, setting, value):
        ret = self.query('select value from setting where application=%s and setting=%s', [application, setting])
        if len(ret) == 0:
            self.modify('insert into setting (application,setting,value) values (%s,%s,%s)', application, setting,
                        str(value))
        else:
            self.modify('update setting set value = %s where application=%s and setting=%s', str(value), application,
                        setting)

    def fetch_nersc_user_info(self, username):
        """Fetch user info for NERSC users via Superfacility account API.

        :param str username: NERSC user name to fetch information for
        :return: JSON encoded response
        """
        session = start_session(self.config.client_id, self.config.private_key, self.config.token_url)
        response = session.get(self.config.api_url + 'account/?username={}'.format(username))
        # If the status code != 200, then the user does not exist, or we don't share a project with the user, so the
        # request will be forbidden (this effectively only allows JGI users to get a token via the API).
        if response.status_code != 200:
            self.logger.warning(
                'Call to Superfacility account API returned an error ({}): {}'.format(response.status_code,
                                                                                      response.reason))
            return None
        else:
            return response.json()

    @restful.validate({
        'to': {'type': str, 'doc': 'The email address of the person to send this email to'},
        'subject': {'type': str, 'doc': 'The subject of the email'},
        'body': {'type': str, 'doc': 'The message to send'}})
    @restful.permissions('send_email')
    def post_sendmail(self, args, kwargs):
        sdmlogger.sendEmail(kwargs['to'], kwargs['subject'], kwargs['body'])

    @restful.validate({'user': {'type': str}, 'token': {'type': str}})
    def post_associate(self, args, kwargs):
        """Associate a user with a new user token. If the user is a NERSC user and a JGI user (via project association),
        fetch the user info via the Superfacility API. Otherwise, generate a new token via OAuth flow.

        :param list args: Unused
        :param dict[str, str] kwargs: Dictionary should contain values for the following keys:
            ['user']: User name
            ['token']: Request token to start the new token generation flow
        """
        user_info = self.fetch_nersc_user_info(kwargs.get('user'))
        if user_info is not None:
            local_user_info = self.get_user([user_info.get('email')], None)
            if local_user_info is None:
                name = '{} {}'.format(user_info.get('firstname'), user_info.get('lastname'))
                user_info['name'] = name
                local_user_info = self._post_user(None, user_info)
            token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
            self.associate_user_token(local_user_info.get('user_id'), token)
            return token
        while True:
            token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
            if token not in self.tempTokens:
                break
        auth_reset_request_token = kwargs.get('token')
        self.tempTokens[token] = auth_reset_request_token
        sdmlogger.sendEmail('{}@nersc.gov'.format(kwargs.get('user')), 'Validate your jamo account',
                            '''In order for you to use jat on the command line you need to associate your user account with your ui account. \nPlease click here: {} to validate your account'''
                            .format(self.restserver.get_authorization_url(('core', 'associate', token))))

    @restful.rawHTML
    @restful.passreq
    def get_associate(self, args, kwargs):
        """Callback after OAuth authentication to cache the user info, which can then later be used to generate a new
        token for the caller. Caches the information with the original user token generation request token.

        :param list[str] args: args[0] should contain the original token association request token
        :param dict[str, str] kwargs: Dictionary should contain values for the following keys:
            ['__auth']['user']: User name
        :return: str to be displayed in the UI
        """
        if args[0] not in self.tempTokens:
            raise common.HttpException(400, 'You have gone to an expired page sorry')
        auth_reset_request_token = self.tempTokens[args[0]]
        # We cache the user information so that we can delay token association until `get_reserved_token` is
        # called, to prevent adding token entries to the database when the token is unclaimed (i.e., `jat` is not
        # called after oauth authorization.
        self.token_reservations[auth_reset_request_token] = kwargs.get('__auth').get('user')
        # Remove token from tempTokens
        del self.tempTokens[args[0]]
        return 'Authentication complete, you can close this tab and run `jat` again.'

    def get_reserved_token(self, args, kwargs):
        """Generates a new user token and associates it with the user. This can only be called after the user completes
        the OAuth flow.

        :param list args: Unused
        :param dict[str, str] kwargs: Dictionary should contain values for the following keys:
            ['token']: Original user token generation request token
        :return: str containing newly created user token
        """
        token = kwargs.get('token')
        if token not in self.token_reservations:
            raise common.HttpException(400, 'You are using an expired token sorry')
        user = self.token_reservations[token]
        user_token = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32))
        self.associate_user_token(self.get_user((user, ), None)['user_id'], user_token)
        # Remove token from token_reservations
        del self.token_reservations[token]
        return user_token

    @restful.passreq
    def put_user(self, args, kwargs):
        self.modify('update user set `group`=%s where email=%s', kwargs['group'], kwargs['__auth']['user'] + '@lbl.gov')

    def get_howami(self, args, kwargs):
        applications = self.restserver.apps
        ret = {}
        for application in applications:
            if application == 'core':
                continue
            if hasattr(applications[application], 'get_howami'):
                ret[application] = applications[application].get_howami(None, None)
            else:
                ret[application] = 'unknown'
        return ret

    @restful.raw
    def get_htmltemplate(self, args, kwargs):
        template_path = resource_filename(__package__, 'templates')
        path = os.path.realpath(os.path.join(template_path, '/'.join(args)))
        if not path.startswith(template_path):
            raise common.HttpException(404, 'You have gone to an invalid url')
        cherrypy.response.headers['Content-Type'] = "text/html"
        with open(path, 'rb') as f:
            return f.read()

    @restful.raw
    def get_jsscript(self, args, kwargs):
        js_file = urllib.parse.unquote(args[0])

        if js_file != '':
            cherrypy.response.headers['Content-Type'] = "text/html"
            with open(js_file, 'rb') as f:
                return f.read()

    def is_numeric_for_table(self, value):
        try:
            float(value)
            return True
        except Exception:
            return False

    def generate_data_from_func(self, args, kwargs):
        path_parts = [p for p in kwargs['db_address'].split('/') if p.strip() != '']
        args = [a for a in args if a not in path_parts]
        if len(path_parts) > 2:
            args += [p for p in path_parts[2:] if p not in args]
        return restful.run_internal(path_parts[0], 'get_{}'.format(path_parts[1]), *args, **kwargs)

    def filter_data(self, data, args, kwargs):
        if 'query' in kwargs and kwargs['query'] != '':
            filters = kwargs['query'].split('and')
            for filt in filters:
                prop, operator, comparison = filt.strip().split(' ', 2)

                if 'in' in operator:
                    filter_string = None
                    if operator == 'nin':
                        operator = 'not in'

                    comparisons = comparison.replace('(', '').replace(')', '').split(',')
                    for comp in comparisons:
                        if self.is_numeric_for_table(comp):
                            filter_string = 'float(d[prop]) {} [{}]'.format(operator, ','.join(comparisons))
                            break

                    if filter_string is None:
                        filter_string = 'str(d[prop]).lower() {} [{}]'.format(operator, comparison)
                elif operator == 'like':
                    filter_string = 'str({}).lower() in str(d[prop]).lower()'.format(comparison)
                else:
                    if operator == '=':
                        operator = '=='

                    if comparison in ['true', 'false']:
                        filter_string = 'str(d[prop]).lower() == \'{}\' or d[prop] == {}'.format(comparison,
                                                                                                 int(comparison == 'true'))
                    elif self.is_numeric_for_table(comparison):
                        filter_string = 'float(d[prop]) {} float({})'.format(operator, comparison)
                    else:
                        filter_string = 'str(d[prop]).lower() {} str({}{}{}).lower()'.format(operator,
                                                                                             '' if comparison.startswith('"') else '"',
                                                                                             comparison,
                                                                                             '' if comparison.endswith('"') else '"')

                filtered_data = []
                for d in data:
                    if ((d[prop] is None and comparison in [None, 'None', '', '0', 0, 'false'] and '=' in operator)
                            or (d[prop] is not None and eval(filter_string))):
                        filtered_data.append(d)
                data = filtered_data
        return data

    def sort_data(self, data, args, kwargs):
        if 'sort' in kwargs and kwargs['sort'] != '':
            sort_opt = kwargs['sort'].strip().split(' ')
            data = sorted(data, key=lambda k: k[sort_opt[0]], reverse=sort_opt[1] == 'desc')
        return data

    def page_data(self, data, record_count, return_count, kwargs):
        if record_count > return_count:
            page = int(kwargs['page'])
            data = data[(return_count * (page - 1)) + 1: (return_count * page)]
        return data

    def process_data(self, data, args, kwargs):
        return_count = kwargs['return_count']
        record_count = len(data)

        if record_count > 0:
            data = self.filter_data(data, args, kwargs)
            record_count = len(data)

            if record_count > 0:
                data = self.page_data(self.sort_data(data, args, kwargs),
                                      record_count, return_count, kwargs)

        return {'record_count': record_count,
                'return_count': return_count,
                'data': data}

    def post_queryResults_dataChange(self, args, kwargs):
        kwargs['queryResults'] = True

        process = True
        data = kwargs.get('data', None)
        if data is None:
            data_info = self.generate_data_from_func(args, kwargs)
            if isinstance(data_info, list):
                data = data_info
            elif isinstance(data_info, dict):
                if data_info.get('tbody', None):
                    return data_info
                else:
                    data = data_info.get('data', None)

                if data is None:
                    data = [data_info]
                else:
                    process = False

        if data is None:
            print('Data in unexpected form | db_address={} | {}'.format(kwargs.get('db_address', None), data_info))
            return {'total': 0, 'total_formatted': 0, 'last_page': 1,
                    'tbody': '<tbody><td><td/><tbody/>', 'first_row': None}

        if process:
            data_info = self.process_data(data, args, kwargs)

        record_count = data_info['record_count']

        return {'total': record_count,
                'total_formatted': common.format_int(record_count),
                'last_page': (math.ceil(record_count / float(data_info['return_count'])) if record_count > 0 else 1),
                'tbody': restful.QueryResults().create_tbody(kwargs['id_field'], data_info.get('data', []),
                                                             kwargs['columns'], kwargs['multi_select']),
                'first_row': data[0] if len(kwargs.get('columns', [])) == 0 else None}

    def post_userquery(self, args, kwargs):
        return self.temp_mongo.postUserQuery(args, kwargs)

    def get_python_version(self, args, kwargs):
        """Get the Python version being used.

        :param list args: Unused
        :parm dict kwargs: Unused
        """
        import sys
        return {'python_version': '{}.{}.{}'.format(sys.version_info[0],
                                                    sys.version_info[1],
                                                    sys.version_info[2])}
