### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from __future__ import print_function
from past.builtins import basestring
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from builtins import next
from builtins import object
import base64
import os
import subprocess
from lapinpy.curl import Curl, CurlHttpException
from lapinpy import sdmlogger


class Issue(object):
    def __init__(self, **kwargs):
        self.issue_id = kwargs.get('issue_id')
        self.jiracurl = kwargs.get('jiracurl')
        self.logger = kwargs.get('logger')

    @property
    def allfielddata(self):
        try:
            return self._allfielddata
        except Exception:
            self._allfielddata = self.jiracurl.get('field')
            return self._allfielddata

    @property
    def metadata(self):
        try:
            return self._metadata
        except Exception:
            self._metadata = self.jiracurl.get('issue/{issue}/editmeta'.format(
                issue=self.issue_id))
            return self._metadata

    def value_fields(self, field):
        try:
            return self.metadata['fields'][field]
        except KeyError:
            return self.metadata['fields'][self.get_field_name(field)]

    def get_custom_id(self, fieldid):
        fieldid = fieldid.lower().replace(' ', '')
        try:
            for f in self.allfielddata:
                if f['id'] == fieldid or f['name'].lower().replace(' ', '') == fieldid:
                    return f['id']
        except Exception as e:  # noqa: F841
            return None

    @property
    def issue_fields(self):
        try:
            return self._issue_fields
        except Exception:
            try:
                self._issue_fields = {value['name'].lower(): key for key, value in
                                      list(self.metadata['fields'].items())}
            except Exception:
                return None
            return self._issue_fields

    def get_field_id(self, fieldname):
        fieldname = fieldname.lower()
        try:
            return self.issue_fields[fieldname]
        except Exception as e:
            print(repr(e))
            return None

    def get_field_name(self, fieldid):
        try:
            return dict((v, k) for k, v in
                        list(self.issue_fields.items()))[fieldid]
        except Exception as e:  # noqa: F841
            return None

    def get_field_value(self, fieldid):
        try:
            fields = self.jiracurl.get('issue/{}'.format(self.issue_id))['fields']
        except Exception:
            return
        try:
            return fields[fieldid]
        except KeyError:
            return fields[self.get_field_id(fieldid)]

    def get_field_keys(self, fieldid):
        pass

    def format_set_field(self, value):
        return [{"set": "{}".format(value)}]

    def comment_with_edit(self, comment):
        return [{"add": {"body": "{}".format(comment)}}]

    def format_field(self, value, key=None, op='set'):
        if not key:
            return {"{}".format(op): "{}".format(value)}
        return {
            "{}".format(op):
                {
                    "{}".format(key): "{}".format(value)
                }  # noqa: E123 (flake8 wrongly reporting error)
        }

    def prepare_opts(self, fieldid, value):
        try:
            value = value.title()
        except Exception:
            pass
        opts = self.value_fields(fieldid)
        if 'allowedValues' in opts:
            for av in opts['allowedValues']:
                for k, v in list(av.items()):
                    if v.lower() == value.lower():
                        return self.format_field(key=k, value=value)
        return self.format_field(value=value)

    def add_comment(self, comment=None):
        d1 = {"update":
            {  # noqa: E128 (flake8 wrongly reporting error)
                "comment": [self.format_field(key="body",
                                              value="{}".format(comment),
                                              op='add')]
            }
        }
        self.put_issue(d1)

    def set_field(self, fieldid, value=None, comment=None, fieldopts=None):
        if fieldopts:
            setdic = self.format_field(**fieldopts)
        else:
            setdic = self.prepare_opts(fieldid, value)
        if comment:
            d1 = {
                "update":
                    {
                        "{}".format(fieldid): [
                            setdic
                        ],
                        "comment": [
                            self.format_field(key="body",
                                              value="{}".format(comment),
                                              op='add')
                        ]
                    }
            }
        else:
            d1 = {
                "update":
                    {
                        "{}".format(fieldid): [setdic]
                    }
            }
        self.put_issue(d1)
        return

    # JIRA issue transition states as defined for JAMO/SDM workflows
    JIRA_ISSUE_TRANSITION_STATES = {
        10002: 33,   # needs info -> provide info
    }
    JIRA_COMPLETED_STATUS = {'status_id': 10007, 'internal_status_id': 43}

    def close(self, comment=None):
        """Close the issue.

        :param str comment: Optional comment to add
        """

        status_id = int(self.get_field_value('status').get('id'))
        if comment:
            self.add_comment(comment)
        while status_id not in self.JIRA_COMPLETED_STATUS.values():
            # Get the next transition state
            new_status_id = self.JIRA_ISSUE_TRANSITION_STATES.get(status_id,
                                                                  self.JIRA_COMPLETED_STATUS.get('internal_status_id'))
            self.logger.info('Transitioning from state {} to state {} for {}'.format(status_id, new_status_id,
                                                                                     self.issue_id))
            status_id = new_status_id
            try:
                self.__set_transition_state(status_id)
            except Exception as e:
                response = ' - {}'.format(e.response) if isinstance(e, CurlHttpException) else ''
                self.logger.error(
                    'Error transitioning to state {} for {} - {}{}'.format(status_id, self.issue_id,
                                                                           repr(e),
                                                                           response))
                # If the transition update doesn't succeed, don't continue trying to update the states
                break

    def __set_transition_state(self, status_id):
        """Set the transition state for the issue.

        :param int status_id: Status id for the transition state
        """

        request = {
            "transition": {
                "id": "{}".format(status_id)
            },
        }
        self.jiracurl.post('issue/{}/transitions'.format(self.issue_id),
                           output=None, **request)

    def put_issue(self, data):
        try:
            self.jiracurl.put('issue/{}'.format(self.issue_id),
                              output=None, **data)
        except Exception as e:
            print(repr(e))


class SDMJira(object):
    def __init__(self, **kwargs):
        # TODO: move version to config, either as part of url or as new parameter
        version = '2'

        try:
            plainname = kwargs['plainname']
            authkey = base64.b64encode(plainname)
        except KeyError:
            authkey = kwargs['authkey']

        jiraurl = os.path.join(kwargs.get('url', 'https://issues-test.jgi-psf.org/'), 'rest', 'api', version)
        self.jiracurl = Curl(jiraurl)
        self.jiraurl = jiraurl
        self.authkey = authkey
        self.jiracurl.userData = 'Basic {}'.format(authkey)
        self.logger = sdmlogger.getLogger('SDMJira')

    def create_issue(self, **kwargs):
        try:
            project = {'key': '{}'.format(kwargs['project'])}
            fields = {'project': project,
                      'summary': '{}'.format(kwargs['summary']),
                      'description': '{}'.format(kwargs['description']),
                      'issuetype': {'name': '{}'.format(kwargs['issuetype'])}, }
        except Exception:
            print("Project,summary,description and issuetype are required fields. Received: {}".format(kwargs))
            return None

        try:
            fields['assignee'] = {'name': '{}'.format(kwargs['assignee'])}
        except Exception:
            pass

        known = ['project', 'summary', 'description', 'issuetype', 'assignee', 'watchers']
        extras = [key for key in list(kwargs.keys()) if key not in known]
        if extras:
            try:
                example = self.exissue(project)
            except Exception as e:
                example = None
                print("exception : {}".format(repr(e)))

            for key, value in list(kwargs.items()):
                if key not in known:
                    try:
                        if (example and 'textfield' in next(iter([field for field in example.allfielddata
                                                                  if field.get('id', None) == key]),
                                                            {}).get('schema', {}).get('custom', '')):
                            fields[key] = value
                        ### PYTHON2_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                        elif example and isinstance(value, basestring):
                            fields[example.get_custom_id(key)] = {'value': value}
                        ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
                        # TODO: uncomment code below during cleanup
                        # elif example and isinstance(value, str):
                        #     fields[example.get_custom_id(key)] = {'value': value}
                        ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
                        else:
                            fields[key] = value
                    except Exception as e:
                        print("key={} | value={} | exception : {}".format(key, value, repr(e)))

        create = {'fields': fields}
        ticket_id = self.jiracurl.post('issue/', **create)
        try:
            for watcher in kwargs['watchers']:
                self.post_watcher(ticket_id['key'], watcher)
        except Exception as e:  # noqa: F841
            pass
        return ticket_id

    def update_issue(self, **kwargs):
        return self.jiracurl.put('issue/{}'.format(kwargs.get('key')),
                                 data={'fields': kwargs.get('data')})  # , output='raw')

    def exissue(self, project):
        '''Get an example issue for a project.
        :param dict project: {'key': 'project_key'}
        :return: Example issue for the project
        '''
        try:
            return self.get_issues(None, **project)[0]
        except Exception as e:  # noqa: F841
            return None

    def format_field(self, value, key=None, op='set'):
        if not key:
            return {"{}".format(op): "{}".format(value)}
        return {
            "{}".format(op):
                {
                    "{}".format(key): "{}".format(value)
                }  # noqa: E123 (flake8 wrongly reporting error)
        }

    def get_issue(self, issue_id):
        return Issue(issue_id=issue_id, jiracurl=self.jiracurl, logger=self.logger)

    def bykey(self, args, kwargs):
        try:
            key = "cf[{issueid}]".format(issueid=self.customkeys[kwargs['key']])
        except Exception:
            key = kwargs['key']
        try:
            value = self.customvalues[kwargs['value']]
        except Exception:
            value = kwargs['value']
        return '{k}="{v}"'.format(k=key,
                                  v=value)

    def argbuilder(self, _args, kwargs):
        """Builds a list of argument strings to be passed to JQL search queries.

        :param list _args: Unused
        :param dict kwargs: Key/value arguments to create arguments
        :return: List of string arguments
        """

        return ['"{key}"{operator}"{value}"'.format(
            key=key.replace(' ', '%20'),
            value=value.replace(' ', '%20'),
            # We need to use the `~` operator for queries on summary
            operator='~' if key == 'summary' else '=') for
            key, value in list(kwargs.items())]

    def jql_builder(self, _args, kwargs):
        '''Builds a JQL query string from the arguments passed to the function.
        Grouping of ANDs and ORs is not supported.

        kwargs is either in the format
            {'AND': {'key1': 'value1', 'key2': 'value2'}}
        or
            {'OR': {'key1': 'value1', 'key2': 'value2'}}
        or
            {'key1': 'value1'}

        :param _args: Unused
        :param dict kwargs: key/value arguments to create arguments
        :return: JQL query string
        '''
        arg_string = ''
        if kwargs.get('kwargs'):
            kwargs = kwargs['kwargs']
        if kwargs.get('OR'):
            arg_list = self.argbuilder(_args, kwargs['OR'])
            arg_string = '+OR+'.join(arg_list)
        elif kwargs.get('AND'):
            arg_list = self.argbuilder(_args, kwargs['AND'])
            arg_string = '+AND+'.join(arg_list)
        elif len(kwargs) == 1:
            arg_string = self.argbuilder(_args, kwargs)[0]
        if arg_string:
            return {'jql': '{jqlstring}'.format(jqlstring=arg_string)}
        else:
            return None

    def return_fields(self, args, kwargs):
        pass

    def get_issues(self, args, **kwargs):
        data = self.jql_builder(args, kwargs)
        res = self.jiracurl.get('search/', data=data)
        return [self.get_issue(issue_id=issue['key']) for issue in res['issues']]

    def post_issue(self, args, kwargs):
        self.create_issue(kwargs)

    def put_issue(self, args, kwargs):
        if len(args) == 1 and kwargs.get('key', None) is None:
            kwargs['key'] = args[0]

        self.update_issue(kwargs)

    def post_comment(self, args, kwargs):
        pass

    def put_comment(self, args, kwargs):
        pass

    def get_comment(self, args, kwargs):
        pass

    def post_watcher(self, ticket_id, watcher):
        cmd = ('curl -i '
               ' -H "Authorization: Basic {token}" '
               ' -H "Content-Type: application/json" '
               ' -H "Accept: application/json" -X POST -d '
               '\'\"{watcher}\"\''
               ' {jiraurl}/issue/{issue}'
               '/watchers -k').format(token=self.authkey,
                                      jiraurl=self.jiraurl,
                                      watcher=watcher,
                                      issue=ticket_id)
        subprocess.call(cmd, shell=True)

    def put_watcher(self, args, kwargs):
        pass

    def get_watchers(self, args, kwargs):
        pass

    def get_attachment(self, args, kwargs):
        pass

    def post_attachment(self, args, kwargs):
        pass


def main():
    return


if __name__ == '__main__':
    main()
