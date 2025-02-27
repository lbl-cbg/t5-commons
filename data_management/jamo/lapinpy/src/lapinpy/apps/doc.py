### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
import os
from lapinpy.common import HttpException
from lapinpy import restful


@restful.menu('Doc', 2)
class Doc(restful.Restful):

    def __init__(self, config):
        self.config = config
        self.auto_reload = True

    @restful.onFinishLoad
    def finishLoading(self):
        self.modules = {}
        self.ui_mappings = {}
        self.apps = restful.RestServer.Instance().apps
        self.env = restful.RestServer.Instance().env
        for app in self.apps:
            if hasattr(self.apps[app], 'moduleName'):
                moduleName = self.apps[app].moduleName
                if moduleName in self.modules:
                    self.modules[moduleName].append(app)
                    continue
                else:
                    self.modules[moduleName] = [app]

                @restful.template('module_doc.html')
                @restful.prepend(moduleName)
                def newMethod(modName, args, kwargs):
                    return self.getApp(modName, args)

                # newMethod = restful.template('module_doc.html')(methClass.call)
                setattr(self, 'get_' + moduleName, newMethod)
                url = '/%s/%s' % (self.appname, moduleName)
                found = False
                for menu in self.menus:
                    if menu['title'] == moduleName:
                        found = True
                if not found:
                    self.menus.append(
                        {'href': '/doc/%s' % self.apps[app].moduleName, 'title': self.apps[app].moduleName,
                         'permissions': ''})
                self.ui_mappings[url] = {'rest_url': url, 'method': newMethod}
        restful.RestServer.Instance().reloadUrls()

    def createNav(self, folder, linkprefix):
        ret = []
        for file_name in os.listdir(folder):
            if file_name in ('index.html', 'examples') or file_name.startswith('.'):
                continue
            fullPath = os.path.join(folder, file_name)
            link_path = linkprefix + '/' + file_name
            item = {'title': file_name.replace('.html', '').replace('_', ' ')}
            if os.path.isdir(fullPath):
                item['children'] = self.createNav(fullPath, link_path)
            # if os.path.isfile(fullPath) or os.path.isfile(fullPath+'/index.html'):
            item['link'] = link_path
            ret.append(item)
        return ret

    def getApp(self, moduleName, args):
        apps = self.modules[moduleName]
        content = ''
        public, private = 0, 0
        apiReference = {'title': 'Public API Reference', 'children': [], 'link': '/doc/%s/api_reference' % moduleName}
        privApiReference = {'title': 'Private API Reference', 'children': [], 'link': '/doc/%s/priv_api_reference' % moduleName}
        import pymongo
        for appName in apps:
            app = self.apps[appName]
            apiApplication = {'title': appName, 'children': [],
                              'link': '/doc/%s/api_reference/%s' % (moduleName, appName)}
            privApiApplication = {'title': appName, 'children': [],
                                  'link': '/doc/%s/priv_api_reference/%s' % (moduleName, appName)}
            resources = {}
            priv_resources = {}
            for method in dir(app):
                method_ref = getattr(app, method)
                # Skip Mongo objects as they don't support truth testing
                if isinstance(method_ref, (pymongo.MongoClient, pymongo.database.Database)):
                    continue
                if hasattr(method_ref, 'restful') and hasattr(method_ref,
                                                              'description') and method_ref.restful and method.count(
                        '_') > 0:
                    html_method, call = method.split('_', 1)
                    if html_method not in ('get', 'post', 'put', 'delete'):
                        continue
                    if method_ref.public:
                        public += 1
                        if call in resources:
                            resources[call].append(html_method)
                        else:
                            resources[call] = [html_method]
                    else:
                        private += 1
                        if call in priv_resources:
                            priv_resources[call].append(html_method)
                        else:
                            priv_resources[call] = [html_method]
            for resource in sorted(resources):
                children = []
                for html_method in resources[resource]:
                    children.append({'title': html_method, 'link': '/doc/%s/api_reference/%s/%s/%s' % (moduleName, appName, resource, html_method)})
                apiApplication['children'].append({'title': resource, 'children': children,
                                                   'link': '/doc/%s/api_reference/%s/%s' % (moduleName, appName, resource)})
            if resources:
                apiReference['children'].append(apiApplication)

            for resource in sorted(priv_resources):
                children = []
                for html_method in priv_resources[resource]:
                    children.append({'title': html_method, 'link': '/doc/%s/priv_api_reference/%s/%s/%s' % (moduleName, appName, resource, html_method)})
                privApiApplication['children'].append({'title': resource, 'children': children,
                                                       'link': '/doc/%s/priv_api_reference/%s/%s' % (moduleName, appName, resource)})
            if priv_resources:
                privApiReference['children'].append(privApiApplication)
        folder = app.location
        path = os.path.join(folder, 'doc', '/'.join(args))
        navItems = self.createNav(os.path.join(folder, 'doc'), '/' + self.appname + '/' + moduleName)
        if public:
            navItems.append(apiReference)
        if private:
            navItems.append(privApiReference)

        currentUrl = '/doc/%s' % moduleName
        currentLevel = navItems
        for arg in args:
            currentUrl += '/%s' % arg
            for item in currentLevel:
                if item['link'] == currentUrl:
                    item['selected'] = True
                    if 'children' not in item:
                        item['highlight'] = True
                        break
                    currentLevel = item['children']

        if len(args) > 3 and (args[0] == 'api_reference' or args[0] == 'priv_api_reference'):
            method_code = self.get_method([args[1], args[2], args[3]], None)
            tmpl = self.env.get_template(self.get_method.template)
            content = tmpl.render(data=method_code)
        elif os.path.isfile(path):
            with open(path) as f:
                content = f.read().replace('<aside>', '<aside class="note"><b>Note:</b>')
        elif os.path.isfile(os.path.join(path, 'index.html')):
            with open(os.path.join(path, 'index.html')) as f:
                content = f.read().replace('<aside>', '<aside class="note"><b>Note:</b>')

        return {"moduleName": moduleName, "navigation": navItems, "path": args, 'content': content}

    def convertType(self, ptype):
        if isinstance(ptype, (list, tuple)):
            ret = []
            for pType in ptype:
                ret.append(self.convertType(pType))
            return ret
        if not hasattr(ptype, '__name__'):
            return 'unknown :('
        return ptype.__name__

    def getExampleData(self, parameter):
        type = parameter['type']
        if 'example' in parameter:
            return [parameter['example']]
        if type is bool:
            return [False, True]
        elif type is int:
            return [1, -5]
        elif type is str:
            return ['a string']
        elif type is list:
            return [self.getExampleData(parameter['validator']['*'])]
        elif type is dict:
            ret = {}
            if 'validator' not in parameter:
                return [{'any_key': 'any_value'}]
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            if isinstance(parameter['validator'], basestring):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # if isinstance(parameter['validator'], str):  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                if parameter['validator'].startswith('*'):
                    return [{'any_key': 'any_value'}]
            for arg in parameter['validator']:
                ret[arg] = self.getExampleData(parameter['validator'][arg])[0]
            return [ret]
        else:
            return ["unknown"]

    def generateValidatorCode(self, validator):
        ret = {}
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if isinstance(validator, basestring) and validator.startswith('*'):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if isinstance(validator, str) and validator.startswith('*'):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            expArgs = validator.split(':')[1] if validator.count(':') > 0 else 0
            return {'*': {'type': 'any type', 'required': int(expArgs) > 0,
                          'doc': 'Can pass in any number of keys that have any type',
                          'examples': ['string', 4, {'blah': 'hello'}]}}
        if 'type' in validator and isinstance(validator['type'], type):
            ret['type'] = self.convertType(validator['type'])
            if 'validator' in validator:
                return self.generateValidatorCode(validator['validator'])
            return ret
        for param in validator:
            if param == '*':
                continue
            parameter = validator[param]
            ret[param] = parameter.copy()
            pType = self.convertType(parameter['type'])
            ret[param]['type'] = pType
            ret[param]['required'] = ('required' not in parameter and 'default' not in parameter) or (
                'required' in parameter and parameter['required'])
            ret[param]['examples'] = self.getExampleData(parameter)
            if 'validator' in parameter and isinstance(parameter['validator'], dict) and (
                    'type' in parameter['validator'] and parameter['validator']['type'] in (list, dict)):
                items = self.generateValidatorCode(validator[param]['validator'].copy())
                for item in items:
                    ret[param + '.' + item] = items[item]

        return ret

    def shorten(self, string, length):
        if len(string) > length:
            retLength = length
            for i in range(length):
                if string[length - i] == ' ':
                    break
                retLength -= 1
            return string[:retLength] + '...'
        return string

    def getappC(self, application):
        app = self.apps[application]
        if not hasattr(app, 'description'):
            return
        methods = []
        for method in dir(app):
            method_ref = getattr(app, method)
            if hasattr(method_ref, 'restful') and hasattr(method_ref,
                                                          'description') and method_ref.restful and method.count(
                    '_') > 0:
                html_method, call = method.split('_')
                methods.append(
                    {'name': call, 'method': html_method, 'description': self.shorten(method_ref.description, 230)})
        if len(methods) > 0:
            return {'name': app.appname, 'description': self.shorten(app.description, 300), 'methods': methods}

    def createArgsPath(self, validator):
        ret = ''
        for arg in validator:
            ret += '/{%s}' % arg['name']
        return ret

    @restful.template('method_doc.html')
    def get_method(self, args, kwargs):
        app = args[0]
        application = self.apps[args[0]]
        example_dir = os.path.join(application.location, 'doc/examples')
        args = args[1:]
        if len(args) == 0:
            return
        method_name = '%s_%s' % (args[1], args[0])
        if not hasattr(application, method_name):
            raise HttpException('404', 'sorry the module %s doesn\'t have documentation method %s' % (app, method_name))
        method = getattr(application, method_name)
        response = {'name': args[0], 'application': app, 'html_method': args[1], 'url': '/api/%s/%s' % (app, args[0])}
        if hasattr(method, 'returns') and method.returns is not None:
            response['returns'] = method.returns

        if hasattr(method, 'permissions') and method.permissions is not None:
            response['permissions'] = method.permissions

        # data_str = None
        if hasattr(method, 'description'):
            response['description'] = method.description
        if hasattr(method, 'validator') and method.validator is not None:
            tmp = self.generateValidatorCode(method.validator)
            parameters = []
            for param in tmp:
                tmp[param]['name'] = param
                if tmp[param]['required']:
                    parameters.insert(0, tmp[param])
                else:
                    parameters.append(tmp[param])
            response['parameters'] = parameters
        # data_str = self.getJSONHtml(method.validator)
        if hasattr(method, 'argsvalidator') and method.argsvalidator is not None:
            response['args'] = self.createArgsPath(method.argsvalidator)
            # path += self.createArgsPath(method.argsvalidator)

        methodExample = os.path.join(example_dir, method_name)
        if os.path.exists(methodExample):
            arrr = []
            with open(methodExample) as exF:
                expData = {}
                lines = exF.readlines()
                currentValue = ''
                currentKey = None
                for line in lines:
                    if line.startswith('@'):
                        line = line[1:].rstrip()
                        if line.startswith('example'):
                            if currentKey is not None:
                                expData[currentKey] = currentValue
                                arrr.append(expData)
                                expData = {}
                            continue
                        if currentKey is not None:
                            expData[currentKey] = currentValue
                        currentKey, currentValue = line.split(' ', 1) if line.count(' ') > 0 else (line, '')
                    else:
                        currentValue += line
                expData[currentKey] = currentValue
                arrr.append(expData)
            response['examples'] = arrr

        return response

    @restful.template('application_doc.html')
    def get_app(self, args, kwargs):
        return [self.getappC(args[0])]

    @restful.template('applications.html', '/doc')
    def get_apps(self, args, kwargs):
        ret = []
        for application in self.apps:
            temp = self.getappC(application)
            if temp is not None:
                ret.append(temp)
        return ret
