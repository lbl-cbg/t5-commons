### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future.utils import iteritems
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from bson.objectid import ObjectId
from dateutil import parser
from .lapinpy_core import RestServer
from .lapinpy_core import Curl
from threading import Lock
from . import common
from . import sdmlogger
import datetime
import os
import uuid
import subprocess
from pkg_resources import resource_filename
### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
try:
    from urllib import quote_plus
except Exception:
### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    from urllib.parse import quote_plus
### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup


class Restful(object):
    '''
    The baseclass that is requested to be extended in order for
    the application to be recognized by LapinPy
    '''
    __app__ = True
    auto_reload = False
    '''tell lapin to watch for changes and reload app if found'''

    cron_enabled = False
    '''tells lapin to run the methods decorated by cron. See restful.Restful.doc'''

    def __init__(self, host=None, user=None, password=None, database=None, config=None, host_port=None):
        self.mod_mappings = {}
        self.address = '/' + self.__module__
        if config is not None:
            self.config = config
        if host is None:
            return
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.host_port = host_port
        self.connections = []
        self.connection_mutex = Lock()
        self.core = None

    def map_submod(self, name, application):
        application.address = self.address + '/' + name
        self.mod_mappings[name] = application
        for meth in ('post', 'put', 'get', 'delete'):
            setattr(self, meth + '_' + name, self.call_submodule(meth, name))

    def call_submodule(self, hmethod, name):
        def ret(a, k):
            print(a)
            method_name = a[0]
            a = a[1:]
            return getattr(self.mod_mappings[name], hmethod + '_' + method_name)(a, k)

        return ret

    def getrestmethod(self, httpmethod, args):
        if args[0] in self.mod_mappings and hasattr(self.mod_mappings[args[0]], httpmethod + '_' + args[1]):
            ret = getattr(self.mod_mappings[args[0]], httpmethod + '_' + args[1])
            ret.__func__.address = self.mod_mappings[args[0]].address + '/' + args[1]
            return ret, 1
        if hasattr(self, httpmethod + '_' + args[0]):
            return getattr(self, httpmethod + '_' + args[0]), 0

    def stop(self):
        pass

    def getCore(self):
        if self.core is None:
            self.core = RestServer.Instance().core
        return self.core

    def getSetting(self, setting, default):
        return self.getCore().getSetting(self.appname, setting, default)

    def saveSetting(self, setting, value):
        return self.getCore().saveSetting(self.appname, setting, value)

    def get_connection(self):
        ret = None
        with self.connection_mutex:
            if len(self.connections) > 0:
                ret = self.connections.pop()
        if ret is None:
            return self.connect()
        return ret

    def put_connection(self, conn):
        with self.connection_mutex:
            self.connections.append(conn)

    def error(self, errorcode, messages):
        return RestServer.Instance().error(errorcode, messages)

    def getCvs(self, *tables, **fields):
        """This function assumes the method 'query' exists on self"""
        ret = {}
        tempStructs = {}
        prepend = False
        ints = False
        if 'prepend' in fields:
            prepend = fields['prepend']
        if 'ints' in fields:
            ints = fields['ints']

        for table in tables:
            temp = {}
            if prepend:
                rows = self.query('select %s_id, %s_name as status from %s_cv' % (table, table, table))
            else:
                rows = self.query('select %s_id, status from %s_cv' % (table, table))
            for row in rows:
                temp[row['status']] = row['%s_id' % table]
                temp[str(row['%s_id' % table])] = row['status']
                if ints:
                    temp[row['%s_id' % table]] = row['status']
            ret[table] = temp
            tempStructs[table] = common.Struct(**ret[table])
        self.__dict__.update(tempStructs)
        return ret


class UIComponent:
    pass


class ExternalButton(UIComponent):
    def __init__(self, name, url, named_values={}, test_url=False, values=(), extras={}):
        self.name = name
        self.named_values = named_values
        self.values = values
        self.extras = extras
        self.test_url = test_url
        self.method = url

    def produce(self, data=None, currentPage=None, method_address=None):
        return Button(self.name, self.method, *self.values, **self.extras).produce(data=data,
                                                                                   currentPage=currentPage,
                                                                                   method_address=method_address,
                                                                                   named_values=self.named_values,
                                                                                   test_url=self.test_url)


class Button(UIComponent):
    def __init__(self, name, method, *values, **extras):
        self.name = name
        self.values = values
        self.extras = extras

        if isinstance(method, str):
            self.method = None
            self.url = method
        else:
            self.method = method
            self.url = None

    def produce(self, data=None, currentPage=None, method_address=None, named_values={}, test_url=False):
        url = self.url
        method = 'get'
        if url is None:
            # if method_address is None:
            method_address = self.method.address
            methodName = self.method.__name__
            htmlMethod = methodName.split('_')[0]

            url = '/api{}'.format(method_address)
            method = htmlMethod

        try:
            button_data = {}

            for value in self.values:
                button_data[value] = data[value]

            for value in named_values:
                button_data[value] = data[named_values[value]]

            for extra in self.extras:
                button_data[extra] = self.extras[extra]

            if test_url:
                if method == 'get':
                    Curl(url).get('', data=button_data)
                else:
                    Curl(url).post('', data=button_data)

            html = '<form action="{}" method="{}">{}'.format(url, method,
                                                             (('<input type="hidden" name="XXredirect_internalXX" value="%s">' % currentPage)
                                                              if self.url is None and 'XXredirect_internalXX' not in self.extras
                                                              else ''))

            for value in button_data:
                html += '<input type="hidden" name="%s" value="%s">' % (value, button_data[value])
            return html + '<input type="submit" value="%s"></form>' % self.name
        except Exception as e:  # noqa: F841
            return ''


class Selection(UIComponent):
    def __init__(self, name, valueName, items, method, *values):
        self.name = name
        self.valueName = valueName
        self.items = items
        self.values = values
        self.method = method

    def produce(self, data, currentPage, method_address=None):
        # if method_address is None:
        method_address = self.method.address
        methodName = self.method.__name__
        htmlMethod = methodName.split('_')[0]
        html = '<form action="/api%s" method="%s">' % (method_address, htmlMethod)
        html += '<input type="hidden" name="XXredirect_internalXX" value="%s">' % currentPage
        html += '<select class="selection" name="%s">' % self.valueName
        for item in sorted(list(self.items.items()), key=lambda item: item[1]):
            html += '<option value="%s">%s</option>' % (item[0], item[1])
        html += '</select>'
        for value in self.values:
            html += '<input type="hidden" name="%s" value="%s">' % (value, data[value])
        return html + '<input type="submit" value="%s"></form>' % self.name


class QueryResults(Restful):
    def __init__(self):
        self.filter = False
        self.download = False
        self.multiSelect = False
        self.template_path = resource_filename(__package__, 'templates')

    def create_html(self, columns, field_count, title='', id_field='_id', generated_data=None, result_desc=None,
                    filter='', select_actions='', download=None, paging=False, multi_select=None,
                    table_sort={'enabled': False}, edit=None, actions='', first_row=None):
        generated_dataset = generated_data is not None
        if result_desc is None:
            total = 0
            if generated_dataset:
                total = len(generated_data)
            result_desc = {'total': total, 'total_formatted': common.format_int(total), 'last_page': 1}

        if title != '':
            title = '<h3 class="sub-table">{}</h3>'.format(title)

        if 'tbody' in result_desc:
            tbody = result_desc['tbody']
            del result_desc['tbody']
        else:
            tbody = self.create_tbody(id_field, generated_data, columns, multi_select, generated_dataset, edit)

        html_header, headers = self.create_thead(columns, multi_select, table_sort, first_row)

        template = self.get_template('query_results.html')
        html = (template.replace('[[title]]', title)
                .replace('[[filter]]', filter)
                .replace('[[results_info]]',
                         self.create_results_info(download, paging, multi_select, result_desc['total_formatted']))
                .replace('[[thead]]', html_header)
                .replace('[[tbody]]', tbody)
                .replace('[[noResultsClass]]', 'hideElement' if result_desc['total'] > 0 else '')
                .replace('[[field_count]]', str(field_count))
                .replace('[[select_actions]]', select_actions)
                .replace('[[actions]]', actions)
                # .replace('[[id]]', str(uuid.uuid4()))
                )

        return {'html': html,
                'headers': headers,
                'result_desc': result_desc,
                'tbody': tbody}

    def __build_select_options(self, options, selected='', empty_option=True):
        if isinstance(options, list):
            select_options = [[o, o] for o in options]
        else:
            select_options = []
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(options):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in options.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                if 'label' not in value:
                    select_options.append([key, key])
                else:
                    select_options.append([key, value['label']])

        options = '<option value=""></option>' if empty_option else ''

        return options + ''.join(
            ['<option value="{}"{}>{}</option>'.format(option[0],
                                                       ' selected' if option[0] == selected.strip() else '',
                                                       option[1])
             for option in sorted([[opt[0].strip(), ' '.join(v[0].upper() + v[1:]
                                                             for v in opt[1].split() if v != '').strip()]
                                   for opt in select_options], key=lambda x: x[1])])

    def __build_select(self, key, options, value='', edit=False, empty_option=True):
        return '<select name="{}"{}>{}</select>'.format(key,
                                                        ' onBlur="save_edit()"' if edit else '',
                                                        self.__build_select_options(options, value, empty_option))

    def create_filter(self, filter, saved_queries):
        if filter is not None:
            options = filter['options']
            html_saved_queries = ''
            html_saved_queries_actions = ''
            if saved_queries is not None:
                html_saved_queries = (self.get_template('query_results_filter_savedqueries.html')
                                      .replace('[[saved_queries]]', self.__create_saved_queries(saved_queries)))
                html_saved_queries_actions = self.get_template('query_results_filter_savedqueries_actions.html')

            # Filter key values
            html_key_options = self.__build_select_options(options, empty_option=False)

            # Comparisions
            html_comparison_options = ''
            for op in ['=', '&ne;', '<', '<=', '>', '>=', 'like', 'in', 'not in']:
                html_comparison_options += '<option value="' + op + '">' + op + '</option>'

            html = self.get_template('query_results_filter.html')
            return (html.replace('[[saved_queries]]', html_saved_queries)
                    .replace('[[key_options]]', html_key_options)
                    .replace('[[comparison_options]]', html_comparison_options)
                    .replace('[[saved_queries_actions]]', html_saved_queries_actions))
        else:
            return ''

    def create_results_info(self, download, paging, multi_select, total):
        try:
            html = self.get_template('query_results_resultinfo.html')

            html_download = ''
            if download is not None and download:
                html_download = self.get_template('query_results_resultinfo_download.html')

            html_multi_select = ''
            if multi_select is not None:
                html_multi_select = self.get_template('query_results_resultinfo_multiselect.html')

            html_pager = ''
            if paging:
                html_pager = self.get_template('query_results_pager.html')

            return (html.replace('[[total]]', str(total))
                    .replace('[[download]]', html_download)
                    .replace('[[multi_select]]', html_multi_select)
                    .replace('[[pager]]', html_pager))
        except Exception:
            return ''

    def _process_common_th(self, html, name, header, headers, sorting, default):
        th_class = ''
        sort_span = ''
        if sorting:
            th_class = 'class="sort"'
            sort_span = '<span class="sort_indicator '
            if default is not None and name == default['column']:
                if default['direction'] == 'desc':
                    sort_span += 'arrow down'
                else:
                    sort_span += 'arrow up'
            sort_span += '"></span>'

        html += '<th name="{}" {}>{}{}</th>'.format(name, th_class, header, sort_span)
        headers.append(header)
        return html, headers

    def create_thead(self, columns, multi_select, sort, first_row=None):
        headers = []
        html = ''
        sorting = sort['enabled']
        default = None
        if 'default' in sort:
            default = sort['default']

        # TODO: Consider best way
        grouped_columns = [c for c in columns if 'header_group' in c[1]]
        if len(grouped_columns) > 0:
            html += '<tr class="group">'
            group = ''
            for column in columns:
                if 'header_group' in column[1]:
                    if group != column[1]['header_group']:
                        group = column[1]['header_group']
                        html += '<th colspan="{}">{}</th>'.format(len([c for c in grouped_columns
                                                                       if c[1]['header_group'] == group]),
                                                                  group)
                else:
                    html += '<th></th>'

            html += '</tr>'

        html += '<tr class="header">'
        if multi_select is not None:
            html += '<th align="left" class="q_select"><label><input type="checkbox" ng-model="select_all" ng-change="select_all_tr()"><div class="custom-checkbox"></div></label></th>'

        if len(columns) == 0:
            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
            for key, value in iteritems(first_row):
            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
            # for key, value in first_row.items():  # noqa: E115 - remove this noqa comment after migration cleanup
            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                html, headers = self._process_common_th(html, key, key.replace('_', ' '), headers, sorting, default)
        else:
            for column in columns:
                name = column[0]
                value = column[1]
                header = ''

                if 'type' in value and value['type'] in ['percent', 'percent_frac']:
                    header = '% '

                if 'title' in value:
                    header += value['title']
                else:
                    header += name.replace('_', ' ')

                if 'units' in value:
                    header += ' ({})'.format(value['units'])

                html, headers = self._process_common_th(html, name, header, headers, sorting, default)
        return html + '</tr>', headers

    def create_tbody(self, id_field, dataset, columns, multi_select=None, generated_data=False, edit=None):
        html = ''
        if dataset is not None and len(dataset) > 0:
            for rowIndex, row in enumerate(dataset):
                # if not generated_data and id_field in row:
                if not generated_data and row.get(id_field, None):
                    id = row[id_field]
                else:
                    id = uuid.uuid4().hex

                html += '<tr data-id="{}" data-name="{}">'.format(id, id_field if id_field is not None else '_id')

                if multi_select is not None:
                    html += '<td align="left" class="q_select"><label><input type="checkbox"><div class="custom-checkbox"></div></label></td>'

                # No columns defined
                # This should only be used if we are using an external api call and the results may change with time
                if len(columns) == 0:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    for key, value in iteritems(row):
                    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                    # for key, value in row.items():  # noqa: E115 - remove this noqa comment after migration cleanup
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        html += '<td>{}</td>'.format(value)
                else:
                    for index, column in enumerate(columns):
                        value = column[1]
                        key = column[0]

                        if generated_data:
                            type = 'string'
                            inputs = None
                            data_value = row[index]
                        else:
                            type = value.get('type', 'string')
                            inputs = value.get('inputs', None)
                            data_value = row.get(key, None)

                        route_info = None
                        if type == 'func':
                            if 'route_info' in value:
                                route_info = value['route_info']
                            else:
                                raise Exception('route_info is required for type = func')

                        html += '<td>{}</td>'.format(', '.join(map(str, data_value)) if isinstance(data_value, list)
                                                     else str(self.__convert_for_type(key, data_value, type, row,
                                                                                      inputs, route_info)))
                html += '</tr>'
        return html

    def create_select_actions(self, multi_select):
        html = ''
        if multi_select is not None and 'actions' in multi_select:
            html += '<div class="select_actions"><div class="action_buttons">'
            actions = multi_select['actions']
            for action in actions:
                html += '<button disabled="disabled" type="button" class="button" title="{name}" ng-click="confirm_select_action({action})">{name}</button>'.format(
                    action=action, name=action['name'])
            html += '</div></div>'
        return html

    def create_actions(self, actions=None, args=[]):
        # TODO
        html = ''
        if actions is not None:
            html = '<div class="actions"><div class="action_buttons">'
            for action in actions:
                html += action
                '''method = action.get('method', 'get')
                url = '{}?'.format(action.get('url', ''))
                if url == '?':
                    raise Exception('URL must be defined for a queryResult button')
                else:
                    for key, value in action.get('data', {}).items():
                        url += '{}={}&'.format(key, value.replace('{{value}}', args[0]))

                if method == 'post':
                    url += 'XXredirect_internalXX={}'.format('jazz')

                html += '<form action="/api{url}" method="{method}">' \
                        '<input type="submit" value="{title}"></form>'.format(url=url, method=method,
                                                                              title=action.get('title', 'button'))'''
            html += '</div></div>'
        return html

    def __get_input_value(self, row, input):
        if input in row:
            # Set based on data value
            value = row[input]
        elif '{{' in input:
            # Set based on dynamically declared data values
            value = ''
            while '{{' in input:
                index = input.index('{{')
                end_index = input.index('}}')
                value += input[:index] + str(row[input[index + 2: end_index]])
                input = input[end_index + 2:]
            value += input
        else:
            # Set as a constant value
            value = input
        return value

    def __create_button(self, value, title, method, url, data_value, is_bool=False):
        btn_class = 'button'
        if is_bool:
            return self.__generate_bool(title, value, btn_class, data_value + ' method="{}" url="{}"'.format(method, url))
        else:
            return '<input class="{}" type="submit" value="{}" title="{}" method="{}" url="{}"{}/>'.format(
                btn_class, value, title, method, url, data_value)

    def __get_bool_text(self, value):
        check = (value or value == 'True' or value == 'true' or value == '1' or value == 1)
        if check:
            text = '&#10003;'
        else:
            text = 'x'
        return check, text

    def __generate_bool(self, check, text, bool_class='', props=''):
        if check == 1:
            check = True
        elif check == 0:
            check = False
        return '<div class="bool {}" value="{}" title="{}" {}>{}</div>'.format(bool_class, check, check, props, text)

    def __get_value_cell_action(self, value, type, row, inputs=None):
        if type == 'toggle':
            check, text = self.__get_bool_text(value)
        elif 'text' in inputs:
            text = self.__get_input_value(row, inputs['text'])
        else:
            text = value

        if 'condition' in inputs and not eval(self.__get_input_value(row, inputs['condition'])):
            value = text
        elif text is not None and text != 'None':
            if 'title' in inputs:
                title = self.__get_input_value(row, inputs['title'])
            elif type == 'toggle':
                title = check
            else:
                title = text

            data_value = ''
            if 'method' in inputs:
                method = inputs['method']
                if method == 'POST' and 'data' in inputs:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    for k, v in iteritems(inputs['data']):
                    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                    # for k, v in inputs['data'].items():  # noqa: E115 - remove this noqa comment after migration cleanup
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        data_value += ' data-{}="{}"'.format(k, self.__get_input_value(row, v))
            else:
                method = 'GET'

            if 'url' in inputs:
                url = self.__get_input_value(row, inputs['url'])
            else:
                url = 'js'

            if type in ['button', 'toggle']:
                value = self.__create_button(text, title, method, url, data_value, type == 'toggle')
            elif type == 'link':
                value = '<a title="{}" href="{}">{}</a>'.format(title, url, text)
        else:
            value = ''

        return value

    def __get_value_cell_edit(self, key, value, type, row, inputs=None):
        if type == 'textbox':
            value = ''
        elif type == 'textarea':
            value = ''
        elif type == 'select':
            value = self.__build_select(key, inputs.get('options', {}), value,
                                        edit=True, empty_option=inputs.get('empty_option', True))
        return value

    def __convert_for_type(self, key, value, type, row, inputs=None, route_info=None):
        # 'string, number, float, percent, date, datetime, raw, bool, button, func (custom processing)'
        if type in ['button', 'link', 'toggle']:
            value = self.__get_value_cell_action(value, type, row, inputs)
        elif type in ['textbox', 'select', 'textarea']:
            value = self.__get_value_cell_edit(key, value, type, row, inputs)
        elif value is None or value == 'None':
            return ''
        elif type == 'number' or type == 'int':
            value = '<div type="int">{}</div>'.format(common.format_int(value))
        elif type == 'float':
            if inputs and 'decimal_pnts' in inputs:
                value = '<div type="int">{}</div>'.format(common.format_float(value, decimal_pnts=inputs['decimal_pnts']))
            else:
                value = '<div type="int">{}</div>'.format(common.format_float(value))
        elif type == 'percent':
            value = '<div type="int">{}</div>'.format(common.format_percent(value, include_symbol=False))
        elif type == 'percent_frac':
            value = '<div type="int">{}</div>'.format(common.format_percent(value, is_fraction=True, include_symbol=False))
        elif type == 'bool' or type == 'boolean':
            check, text = self.__get_bool_text(value)
            value = self.__generate_bool(check, text)
        elif type == 'date':
            value = value.strftime("%m/%d/%Y")
        elif type == 'func':
            value = run_internal(route_info['page'], route_info['function'], **row)
            pass
        # elif type == 'datetime':

        return value

    def __create_delete(self, ng_click, title=None):
        if title is None:
            title = 'Delete'
        return '<div class="delete" ng-click="' + ng_click + '" title="' + title + '">X</div>'

    def __create_saved_queries(self, saved_queries):
        html = ''
        for query in saved_queries:
            description = query['description']
            name = query['name']
            html += ('<li>'
                     + '<div class="delete" title="Delete">X</div>'
                     + '<a href="#" title="' + description + '">' + name + '</a></li>')
        return html

    def get_template(self, page):
        try:
            with open(os.path.join(self.template_path, page), 'r') as f:
                return f.read()
        except Exception as e:
            return str(e)


def copyattrs(funcfrom, functo):
    properties = dir(funcfrom)
    for property in properties:
        if not (property.startswith('__') or property.startswith('func')):
            setattr(functo, property, getattr(funcfrom, property))


def menu(name, order=None):
    '''Place a link to this page in the main menu of the templated webpage

    Args:
        name (str): The name of the menu item.

    Keyword Arguments:
        order (int): Where to put this item on its parent.
    '''

    def inner(func):
        func.menuname = name.lower()
        func.order = order
        return func

    return inner


def chart(urlpath=None, title=None, map=None, onlyshowmap=False, display_as='chart'):
    def inner(func):
        func.display_type = display_as
        func.is_ui = True
        func.title = title
        func.onlyshowmap = onlyshowmap
        func.map = map
        func.restful = True
        func.url_path = urlpath
        if type(urlpath) == type(func):
            func.url_path = None
        return func

    if type(urlpath) == type(inner):
        return inner(urlpath)
    return inner


def pagevalidator(func):
    return validate({
        'query': {'type': (dict, str), 'doc': 'a query string or a mongodb query document'},
        'return_count': {'type': int, 'doc': 'the number of records to return in a page, max is 500'},
    })(func)


def get_page_script(location, file_path):
    if file_path is not None:
        try:
            paths = [line for line in subprocess.check_output(
                "find {} -iname '{}.js'".format(location, file_path[file_path.rfind('/') + 1:]),
                shell=True).splitlines()]
            if len(paths) != 0:
                return quote_plus(paths[0])
            else:
                return ''
        except Exception as e:  # noqa: F841
            return ''

    return ''


# Title and table are required
def queryResults(queryResults):
    def inner(func):
        func.display_type = 'queryResults'
        func.is_ui = True
        func.restful = True
        func.title = queryResults['title']

        if 'data' not in queryResults:
            queryResults['data'] = {}

        if 'filter' not in queryResults:
            queryResults['filter'] = None

        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        try:
            queryResults['file'] = getattr(func.func_code, 'co_filename')
        except Exception as e:  # noqa: F841
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
            queryResults['file'] = getattr(func.__code__, 'co_filename')
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        if hasattr(func, 'queryResults'):
            func.queryResults.append(queryResults)
        else:
            func.queryResults = [queryResults]
        return func

    return inner


def pagetable(title, collection, map, sort=None, actions=None, return_count=200, allow_empty_query=False):
    def inner(function):
        modifiers = {}
        cols = len(map)
        headers = [None] * cols
        ret_count = return_count
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for field, value in iteritems(map):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for field, value in map.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            place = value['order']
            if 'value' in value:
                mod_value = value['value']
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                if hasattr(mod_value, '__call__') or isinstance(mod_value, basestring):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # if hasattr(mod_value, '__call__') or isinstance(mod_value, str):  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    modifiers[field] = value['value']
            if place >= cols:
                headers.append([field, value['title'] if 'title' in value else field,
                                value['type'] if 'type' in value else 'string'])
            else:
                headers[place] = [field, value['title'] if 'title' in value else field,
                                  value['type'] if 'type' in value else 'string']

        def func(self, args, kwargs):
            if kwargs is None:
                kwargs = {}
            return_count = min(kwargs['return_count'] if 'return_count' in kwargs else 200, ret_count)
            query = kwargs['query'] if 'query' in kwargs else None
            if not allow_empty_query and (query is None or len(query) == 0):
                return {'start': 0, 'record_count': 0, 'end': 0, 'records': []}

            if '__ui' in kwargs:
                return self.pagequery(collection, select=(list(map) if map is not None else None), sort=sort,
                                      return_count=return_count, modifiers=modifiers, what=query, key_map=map)
            select = None
            if 'fields' in kwargs:
                select = kwargs['fields']
            return self.pagequery(collection, select=select, return_count=return_count, what=query)

        copyattrs(function, func)
        func.display_type = 'template'
        func.is_ui = True
        func.template = 'page_table.html'
        func.title = title
        func.sort = sort
        func.actions = actions
        func.restful = True
        func.map = map
        func.headers = headers
        return func

    return inner


def table(title=None, urlpath=None, map=None, onlyshowmap=False, sort=None, order=None):
    def inner(func):
        func.display_type = 'table'
        func.is_ui = True
        func.title = title
        func.sort = sort
        func.onlyshowmap = onlyshowmap
        func.restful = True
        func.url_path = urlpath
        func.map = map
        if order is not None:
            if func.map is None:
                func.map = {}
            on = 0
            for field in order:
                if field not in func.map:
                    func.map[field] = {}
                func.map[field]['order'] = on
                on += 1

        if type(urlpath) == type(func):
            func.url_path = None
        return func

    if type(urlpath) == type(inner):
        return inner(urlpath)
    return inner


def form(title=None, urlpath=None, map=None, onlyshowmap=False, submitTo=None, method='post'):
    def inner(func):
        func.display_type = 'form'
        func.is_ui = True
        func.title = title
        func.method = method
        func.onlyshowmap = onlyshowmap
        func.restful = True
        func.submitto = submitTo
        func.url_path = urlpath
        func.map = map
        if type(urlpath) == type(func):
            func.url_path = None
        return func

    if type(urlpath) == type(inner):
        return inner(urlpath)
    return inner


def prepend(*args, **kwargs):
    def outer(func):
        func.args = args
        func.kwargs = kwargs

        def inner(*iargs, **ikwargs):
            kwargs.update(ikwargs)
            return func(*(args + iargs), **kwargs)

        return inner

    return outer


def link(function, col_header, key):
    def outer(func):
        def inner(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            if isinstance(response, list):
                for row in response:
                    if col_header in row:
                        value = row[col_header]
                        row[key] = function(self, (value, None), None)
            elif response is not None:
                if col_header in response:
                    value = response[col_header]
                    response[key] = function(self, (value,), {})
            return response

        copyattrs(func, inner)
        if 'links' not in dir(inner):
            inner.links = []
        inner.links.append({'function': function, 'key': key})
        return inner

    return outer


def permissions(needs):
    '''Require that the user has these set of permissions.

    Args:
        needs (str,list:str): The required permission's that a user will need to access this api.
    '''

    def inner(func):
        if isinstance(needs, str):
            func.permissions = []
            func.permissions.append(needs)
        else:
            func.permissions = needs
        return func

    return inner


def raw(func):
    func.raw = True
    return func


def ui_link(method):
    def inner(func):
        if 'ui_links' not in dir(func):
            func.ui_links = []
        func.ui_links.append(method)
        return func

    return inner


def table_link(function, key, query_results, api_key):
    def outer(func):
        def inner(self, args, kwargs):
            # Called when the table is actually generated for UI and api
            response = func(self, args, kwargs)
            if isinstance(response, list):
                response = response[0]

            if isinstance(response, dict):
                response_key = str(response.get(key, None))
                if response_key:
                    response[api_key] = function(self, (response_key,), {})
            else:
                sdmlogger.getLogger('table_link').error('Unexpected response from function {}'.format(args))

            return response

        # Setup and cached
        copyattrs(func, inner)
        if 'table_links' not in dir(inner):
            inner.table_links = []

        inner.table_links.append({'key': key, 'queryResults': query_results,
                                  'api_key': api_key, 'function': function})
        return inner

    return outer


def generatedhtml(urlpath=None, title=None, map=None, onlyshowmap=False):
    '''Attempt to auto generate a webpage for the data that is returned.

    Keyword Arguments:
        urlpath (str): The url that this method should be accessed from the web by (this will overwrite the generated url).
        title (str): The title that will be displayed on this page.
        map (dic): A dictionary of key to dic that will be used in table generation.
        onlyshowmap (bool): Only show keys that are in the map.
    '''

    def inner(func):
        func.display_type = 'generated'
        func.is_ui = True
        func.title = title
        func.map = map
        func.onlyshowmap = onlyshowmap
        func.url_path = urlpath
        func.restful = True
        if type(urlpath) == type(func):
            func.url_path = None
        return func

    return inner


def ui(func):
    func.is_ui = True
    return func


def run_internal(__module, __name, *args, **kwargs):
    return RestServer.Instance().run_method(__module, __name, *args, **kwargs)


def async_func(description, resources=['internal']):
    def outer(function):
        def func(self, *args, **kwargs):
            return function.queue.add({'args': args, 'kwargs': kwargs})

        func.is_async = True
        func.async_description = description
        func.resources = resources
        copyattrs(function, func)
        func.old_function = function
        return func

    return outer


def single(func):
    '''Force this method to return the first item of the list.
    This is also used to tell the ui generator to display it a little differently.
    '''

    def inner(self, *args, **kwargs):
        response = func(self, *args, **kwargs)
        if len(response) == 0:
            return None

        if isinstance(response, list):
            return response[0]
        elif isinstance(response, dict):
            return response

    copyattrs(func, inner)
    inner.is_single = True
    return inner


def validateValidator(validator, pre_key=''):
    for key in list(validator):
        validator_val = validator[key]
        full_key = pre_key + '.' + key if pre_key != '' else key
        if 'validator' in validator_val:
            sub_val = validator_val['validator']
            validateValidator(sub_val, full_key)
        if key.startswith('*'):
            if len(key) > 1:
                if key[1] != ':' or not key[2:].isdigit():
                    raise Exception(
                        '''Invalid key: '%s' in validator, keys starting with '*' must be in the form '*:int' ''' % full_key)
            if len(validator) > 1:
                raise Exception('''Keys mixed with '*' is not allowed''')
        elif key.count('|') > 0:
            new_keys = key.split('|')
            validator_val['required'] = False
            for new_key in new_keys:
                validator[new_key] = validator_val
            del validator[key]
        for key_v in validator_val:
            if key_v not in ('doc', 'validator', 'options', 'type', 'allow_extra', 'required', 'default', 'example', 'values'):
                raise Exception('''Invalid key '%s' provided in options''' % key_v)
        if 'required' not in validator_val:
            validator_val['required'] = True
        else:
            if not isinstance(validator_val['required'], bool):
                raise Exception('''option 'required' must be a boolean''')
        if 'allow_extra' not in validator_val:
            validator_val['allow_extra'] = False
        else:
            if not isinstance(validator_val['allow_extra'], bool):
                raise Exception('''option 'allow_extra' must be a boolean''')
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if 'type' in validator_val and validator_val['type'] not in (bool, int, basestring, str, '*', 'oid', 'date', float, list, dict, 'cv'):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if 'type' in validator_val and validator_val['type'] not in (bool, int, str, '*', 'oid', 'date', float, list, dict, 'cv'):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if isinstance(validator_val['type'], tuple):
                newT = []
                for val in validator_val['type']:
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    if val not in (bool, int, basestring, str, '*', 'oid', 'date', float, list, dict, 'cv'):
                    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                    # if val not in (bool, int, str, '*', 'oid', 'date', float, list, dict, 'cv'):  # noqa: E115 - remove this noqa comment after migration cleanup
                    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        raise Exception('''Invalid type provided in tuple for key '%s' ''' % full_key)
                    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                    elif val == str:
                        newT.append(basestring)
                    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
                    else:
                        newT.append(val)
                validator_val['type'] = tuple(newT)
            else:
                raise Exception('''Invalid type provided for key: '%s' ''' % full_key)
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        else:
            if validator_val['type'] == str:
                validator_val['type'] = basestring
        ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
        if 'options' in validator_val and not isinstance(validator_val['options'], list):
            raise Exception('''Invalid options have been passed''')
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        if 'doc' in validator_val and not isinstance(validator_val['doc'], basestring):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # if 'doc' in validator_val and not isinstance(validator_val['doc'], str):  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            raise Exception('''Invalid doc provided''')
        if 'values' in validator_val:
            if not isinstance(validator_val['values'], (dict, list)):
                raise Exception('''Invalid values provided, must be a dict''')
            else:
                if isinstance(validator_val['values'], list):
                    validator_val['values'] = {x: x for x in validator_val['values']}
                else:
                    updates = {}
                    for key, val in validator_val['values'].items():
                        # We're modifying the dict size as we traverse it, raises an exception in Python3
                        # validator_val['values'][val] = val
                        updates[val] = val
                    validator_val.get('values').update(updates)


def checkdata(data, validator, pre_key='', allow_extra=False):
    errors = []
    if not isinstance(data, dict):
        return ['Must pass in a dictionary']

    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    for key, validator_info in iteritems(validator):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # for key, validator_info in validator.items():  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        full_key = pre_key + '.' + key if pre_key != '' else key
        required = validator_info['required']
        if key.startswith('*'):
            count = int(key.split(':')[1]) if key.count(':') else 0
            if len(data) < count:
                return ['Must pass in at least %s keys to dictionary %s' % (count, pre_key)]
            else:
                continue

        if (key not in data or data[key] is None):
            if 'default' in validator_info:
                data[key] = validator_info['default']
            elif required:
                errors.append('''Required key '%s' not provided''' % full_key)
        elif key in data:
            value = data[key]
            if value is None:
                del data[key]
                continue
            if 'type' in validator_info:
                exp_type = validator_info['type']
                type_check = checkType(value, exp_type)
                if exp_type == 'cv':
                    if value not in validator_info['values']:
                        errors.append('''Key '%s' has an incorrect value, please look at the docs. ''' % full_key)
                    else:
                        value = validator_info['values'][value]
                        data[key] = value
                if not type_check:
                    errors.append('''Key '%s' has the incorrect type, '%s' passed, but expected '%s' ''' % (full_key, str(type(value)), str(exp_type)))
                elif 'validator' in validator_info:
                    sub_validator = validator_info['validator']
                    if exp_type == list:
                        list_validator = sub_validator['*']
                        for i in range(len(value)):
                            sub_value = value[i]
                            sub_check = checkType(sub_value, list_validator['type'])
                            if not sub_check:
                                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                                string_type = list_validator['type'] if isinstance(list_validator['type'], basestring) else list_validator['type'].__name__
                                ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
                                ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                                # TODO: uncomment code below during cleanup
                                # string_type = list_validator['type'] if isinstance(list_validator['type'], str) else list_validator['type'].__name__
                                ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
                                errors.append('''List '%s' contains an invalid entry at index %d. value passed '%s' expected '%s' ''' % (full_key, i, str(sub_value), string_type))
                            ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                            elif isinstance(list_validator['type'], basestring):
                            ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                            ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                            # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                            # elif isinstance(list_validator['type'], str):  # noqa: E115 - remove this noqa comment after migration cleanup
                            ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                                value[i] = sub_check[1]
                            elif list_validator['type'] == dict:
                                errors.extend(checkdata(sub_value, list_validator['validator'], full_key + '.%d' % i, validator_info['allow_extra']))
                    elif exp_type == dict:
                        errors.extend(checkdata(value, sub_validator, full_key, validator_info['allow_extra']))
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                elif isinstance(value, basestring):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # elif isinstance(value, str):  # noqa: E115 - remove this noqa comment after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    data[key] = type_check[1]
    if not allow_extra:
        for key in data:
            if key not in validator and '*:1' not in validator and key not in ('group', 'user', '__auth', 'permissions'):
                if pre_key:
                    errors.append("Invalid key '%s' passed to dictionary '%s'" % (key, pre_key))
                else:
                    errors.append("Invalid parameter '%s' passed" % key)
    return errors


def validateArgsValidator(argsValidator):
    for validator in argsValidator:
        ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
        for key, value in iteritems(validator):
        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
        # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
        # for key, value in validator.items():  # noqa: E115 - remove this noqa comment after migration cleanup
        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
            if key not in ('name', 'doc', 'type'):
                raise Exception('''Key '%s' is an invalid keyname for args validator''' % key)
            if key == 'type':
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                if value not in (bool, int, basestring, str, '*', 'oid', 'date', float):
                ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
                # if value not in (bool, int, str, '*', 'oid', 'date', float):  # noqa: E266,E115 - to be removed after migration cleanup
                ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                    if isinstance(value, type):
                        value = value.__name__
                    raise Exception('''Invalid type '%s' passed into argsvalidator''' % value)
                ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
                if value == str:
                    validator['type'] = basestring
                ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


def checkType(value, exp_type):
    if exp_type == 'cv':
        return (True, value)
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(exp_type, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(exp_type, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        if exp_type == 'oid':
            if isinstance(value, ObjectId):
                return (True, value)
            else:
                try:
                    return (True, ObjectId(value))
                except Exception:
                    return False
        elif exp_type == 'date':
            if isinstance(value, datetime.datetime):
                return (True, value)
            else:
                try:
                    return (True, parser.parse(value))
                except Exception:
                    return False
        elif exp_type == '*':
            return (True, value)
    elif isinstance(exp_type, (list, tuple)):
        for sub_type in exp_type:
            subCall = checkType(value, sub_type)
            if subCall:
                return subCall
        return False
    elif isinstance(value, exp_type):
        return (True, value)
    elif exp_type in (float, int):
        try:
            return (True, exp_type(value))
        except Exception as e:  # noqa: F841
            return False
    elif exp_type == bool:
        try:
            return isinstance(value, bool)
        except Exception as e:  # noqa: F841
            return False
    return False


def checkArgs(args, validators):
    if isinstance(args, tuple):
        args = [x for x in args]
    errors = []
    i = 0
    for validator in validators:
        if 'required' not in validator or validator['required']:
            if len(args) <= i:
                errors.append('missing a required argument "%s" at position %d' % (validator['name'], i + 1))
                continue
            arg = args[i]
            resp = checkType(arg, validator['type'])
            if not resp:
                errors.append('''Incorrect value has been passed at position %d, '%s' is not of type %s''' % (i + 1, str(arg), validator['type'].__name__))
            else:
                args[i] = resp[1]
        i += 1
    return errors


def validate(validator=None, argsValidator=None, allowExtra=True):
    '''This will validate the args and kwargs that are passed by users. It will even take care of conversion of string->type
    This should be used on any post/put method where arguments should be validated against.

    Keyword Arguments:
        validator (dic): The validator structure to use for the kwargs.
        argsValidator (list): A list of validator structures to validate against.
        allowExtra (bool): Don't throw an error if there are keys that are passed that aren't in the validator.

    Raises:
        HttpException if validation fails.
    '''
    if validator is not None:
        validateValidator(validator)
    if argsValidator is not None:
        validateArgsValidator(argsValidator)

    def inner(func):
        def functioncall(self, args, kwargs):
            errors = []
            if validator is not None:
                errors = checkdata(kwargs, validator, allow_extra=allowExtra)
            if argsValidator is not None:
                errors.extend(checkArgs(args, argsValidator))
            if len(errors) > 0:
                sdmlogger.getLogger('validator').error(func.__qualname__ + " : " + str(errors))
                raise common.ValidationError(errors)
            return func(self, args, kwargs)

        copyattrs(func, functioncall)
        functioncall.restful = True
        functioncall.validator = validator
        functioncall.argsvalidator = argsValidator
        return functioncall

    return inner


def search(name):
    def inner(func):
        func.search = name
        return func

    return inner


def doc(description, returns=None, public=True):
    '''Auto generate documentation for the decorated RESTFul method.

    Args:
        description (str): The html that will be displayed on the doc page.

    Keyword Arguments:
        returns : An example structure of what the decorated method returns.
    '''

    def inner(func):
        func.description = description
        func.returns = returns
        func.restful = True
        func.public = public
        return func

    return inner


def onFinishLoad(func):
    '''Tells LapinPy to call this decorated method after any application has loaded.

    The decorated method can't have any arguments in its signature.

    >>@onFinishLoad
    >>def callme(self):
    >>    print 'some application just loaded'

    '''
    func.call_on_finish = True
    return func


def onload(func):
    '''Tells LapinPy to call this decorated method after this application is done loading.

    The decorated method can't have any arguments in its signature.

    >>@onload
    >>def callme(self):
    >>    print 'finished loading'

    '''
    func.call_on_finish_single = True
    return func


def template(template=None, urlpath=None, title=None):
    '''Use a custom jinja2 template to display this data.

    Keyword Arguments:
      template (str): The name of the template to use. This template should be in the same folder this application is under a folder called templates.
      urlpath (str): The url that this method should be accessed from the web by (this will overwrite the generated url).
      title (str): The title that will be displayed on this page.
    '''

    def inner(func):
        func.display_type = 'template'
        func.is_ui = True
        func.template = template
        func.title = title
        func.url_path = urlpath
        if type(urlpath) == type(func):
            func.__func__.url_path = None
        return func

    if type(urlpath) == type(inner):
        return inner(urlpath)
    return inner


def customTemplate(template):
    def inner(func):
        func.custom_template = template
        return func

    return inner


def cron(min, hour, dom, mon, dow='*'):
    '''Runs the decorated function in a cron manner. All cron syntax should be accepted.

    Args:
        min (str): The minute at which this method should be ran. 0-59.
        hour (str): The hour at which this method should be ran. 0-23.
        dom (str): The day of the month this method should be ran. 0-30.
        mon (str): The months that this method should be ran. 0-11.

    Keyword Arguments:
        dow (str): The day of the week this method should run. 0-6.

    Print 'Hello world' every 10 minutes ::
    >>    @cron('*/10','*','*','*')
    >>    def foo(self):
    >>        print 'Hello world'
    '''

    def inner(func):
        func.cron = (min, hour, dom, mon, dow)
        func.lastRan = None
        func.lastJobName = None
        func.nextEvent = None
        return func

    return inner


def rawHTML(urlpath=None, title=None):
    '''Tells LapinPy to display the content that the decorated method returns as html

    Keyword Arguments:
        urlpath (str): The url that this method should be accessed from the web by (this will overwrite the generated url).
        title (str): The title that will be displayed on this page.
    '''

    def inner(func):
        func.display_type = 'raw'
        func.is_ui = True
        func.title = title
        func.url_path = urlpath
        if type(urlpath) == type(func):
            func.url_path = None
        return func

    if type(urlpath) == type(inner):
        return inner(urlpath)
    return inner


def passreq(join=False, include_perms=False):
    '''Pass in the user information to kwargs as __auth. This will also force a user to be authenticated before calling this method
    In the kwargs under the __auth key, a dictionary will be passed with the keys: user, group. If join is True then user, group will be in the root of kwargs.

    Keyword Arguments:
        join (bool) - Instead of passing the user info into __auth put it in the root of the kwargs dict.
        include_perms (bool) - In addition to user and group add the permissions this users has in the permissions key.

    '''

    def inner(func):
        func.joinauth = join
        func.passreq = True
        func.include_perms = include_perms
        return func

    if type(join) == type(inner):
        meth = join
        join = False
        return inner(meth)
    return inner


def passuser(func):
    '''Pass in the user information if available
    '''

    def inner(func):
        func.passuser = True
        return func

    return inner(func)


def sm(key, type, **kwargs):
    ret = {'key': key, 'type': type}
    ret.update(kwargs)
    return ret


def usewhen(value, configKey='instance_type'):
    '''Tells LapinPy to use this method only when a certain configuration value is set.

    Args:
        value (str): The value of the configuration key that must be set for this to load.

    Keyword Arguments:
        configKey (str): The configuration key to use.

    '''

    def inner(func):
        func.__usewhen = (configKey, value)
        return func

    return inner
