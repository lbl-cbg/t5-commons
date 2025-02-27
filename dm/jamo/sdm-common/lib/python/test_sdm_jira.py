import unittest
from parameterized import parameterized
try:
    ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import Mock, call, patch
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import Mock, call, patch
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
from sdm_jira import SDMJira, Issue


class TestSdmJira(unittest.TestCase):

    jira_curl = None
    sdm_jira = None
    issue = None
    logger = None

    def setUp(self):
        self.jira_curl = Mock()
        self.sdm_jira = SDMJira(authkey='some_key')
        self.sdm_jira.jiracurl = self.jira_curl
        self.logger = Mock()
        self.issue = Issue(issue_id='my_issue', jiracurl=self.jira_curl, logger=self.logger)

    def test_sdm_jira_argbuilder(self):
        kwargs = {
            'project': 'my_project',
            'summary': 'my_summary',
            'issue_id': 'my_issue_id',
        }
        expected_args = ['"project"="my_project"',
                         # summary should use the `~` operator
                         '"summary"~"my_summary"',
                         '"issue_id"="my_issue_id"']
        expected_args.sort()

        actual_args = self.sdm_jira.argbuilder([], kwargs)
        actual_args.sort()
        self.assertEqual(actual_args, expected_args)

    def test_sdm_jira_create_issue_no_extras(self):
        self.jira_curl.post.return_value = 1

        self.assertEqual(
            self.sdm_jira.create_issue(project='some_project', summary='summary', description='description',
                                       issuetype='bug', assignee='me'), 1)

        self.jira_curl.post.assert_called_with('issue/',
                                               fields={'project': {'key': 'some_project'}, 'issuetype': {'name': 'bug'},
                                                       'assignee': {'name': 'me'}, 'description': 'description',
                                                       'summary': 'summary'})

    def test_sdm_jira_create_issue_with_extras_textfield(self):
        self.jira_curl.post.return_value = 1
        self.jira_curl.get.side_effect = [
            {'issues': [{'key': 'my_issue'}]},
            [{'id': 'foo', 'schema': {'custom': {'textfield': 'field'}}}],
        ]

        self.assertEqual(
            self.sdm_jira.create_issue(project='some_project', summary='summary', description='description',
                                       issuetype='bug', assignee='me', foo='bar'), 1)

        self.jira_curl.post.assert_called_with('issue/', fields={'description': 'description', 'summary': 'summary',
                                                                 'project': {'key': 'some_project'},
                                                                 'assignee': {'name': 'me'},
                                                                 'issuetype': {'name': 'bug'}, 'foo': 'bar'})

    def test_sdm_jira_create_issue_with_extras_string(self):
        self.jira_curl.post.return_value = 1
        self.jira_curl.get.side_effect = [
            {'issues': [{'key': 'my_issue'}]},
            [{'id': 'foo', 'schema': {'custom': {}}}],
        ]

        self.assertEqual(
            self.sdm_jira.create_issue(project='some_project', summary='summary', description='description',
                                       issuetype='bug', assignee='me', foo='bar'), 1)

        self.jira_curl.post.assert_called_with('issue/', fields={'project': {'key': 'some_project'},
                                                                 'summary': 'summary',
                                                                 'description': 'description',
                                                                 'issuetype': {'name': 'bug'},
                                                                 'assignee': {'name': 'me'},
                                                                 'foo': {'value': 'bar'}})

    def test_sdm_jira_create_issue_with_extras_other(self):
        self.jira_curl.post.return_value = 1
        self.jira_curl.get.side_effect = [
            {'issues': [{'key': 'my_issue'}]},
            [{'id': 'foo', 'schema': {'custom': {}}}],
        ]

        self.assertEqual(
            self.sdm_jira.create_issue(project='some_project', summary='summary', description='description',
                                       issuetype='bug', assignee='me', foo=2), 1)

        self.jira_curl.post.assert_called_with('issue/', fields={'description': 'description', 'summary': 'summary',
                                                                 'project': {'key': 'some_project'},
                                                                 'assignee': {'name': 'me'},
                                                                 'issuetype': {'name': 'bug'}, 'foo': 2})

    @patch('sdm_jira.subprocess')
    def test_sdm_jira_create_issue_with_watchers(self, subprocess):
        self.jira_curl.post.return_value = {'key': 1}

        self.assertEqual(
            self.sdm_jira.create_issue(project='some_project', summary='summary', description='description',
                                       issuetype='bug', assignee='me', watchers=['foo']), {'key': 1})
        self.jira_curl.post.assert_called_with('issue/',
                                               fields={'project': {'key': 'some_project'}, 'issuetype': {'name': 'bug'},
                                                       'assignee': {'name': 'me'}, 'description': 'description',
                                                       'summary': 'summary'})
        subprocess.call.assert_called_with(
            'curl -i  -H "Authorization: Basic some_key"  -H "Content-Type: application/json"  -H "Accept: application/json" -X POST -d \'"foo"\' https://issues-test.jgi-psf.org/rest/api/2/issue/1/watchers -k',
            shell=True)

    def test_sdm_jira_update_issue(self):
        self.sdm_jira.update_issue(key=1, data={'foo': 'bar'})

        self.jira_curl.put.assert_called_with('issue/1', data={'fields': {'foo': 'bar'}})

    def test_sdm_jira_exissue(self):
        self.jira_curl.get.side_effect = [
            {'issues': [{'key': 'my_issue'}]},
            [{'id': 'foo'}],
        ]

        issue = self.sdm_jira.exissue({'key': 'some_project'})

        self.assertEqual(issue.allfielddata, [{'id': 'foo'}])
        self.assertEqual(issue.issue_id, 'my_issue')

    def test_sdm_jira_format_field_with_key(self):
        self.assertEqual(self.sdm_jira.format_field('value', key='key', op='update'), {'update': {'key': 'value'}})

    def test_sdm_jira_format_field_without_key(self):
        self.assertEqual(self.sdm_jira.format_field('value', op='update'), {'update': 'value'})

    def test_sdm_jira_get_issue(self):
        self.jira_curl.get.return_value = [{'id': 'foo'}]

        issue = self.sdm_jira.get_issue('my_issue')

        self.assertEqual(issue.allfielddata, [{'id': 'foo'}])
        self.assertEqual(issue.issue_id, 'my_issue')

    def test_sdm_jira_bykey_custom(self):
        self.sdm_jira.customkeys = {'key': 'custom_key'}
        self.sdm_jira.customvalues = {'value': 'custom_value'}

        self.assertEqual(self.sdm_jira.bykey(None, {'key': 'key', 'value': 'value'}), 'cf[custom_key]="custom_value"')

    def test_sdm_jira_bykey_no_custom(self):
        self.assertEqual(self.sdm_jira.bykey(None, {'key': 'key', 'value': 'value'}), 'key="value"')

    @parameterized.expand([
        ('orlist', {'OR': {'foo': 'bar', 'foo2': 'bar2'}}, {'jql': '"foo"="bar"+OR+"foo2"="bar2"'}),
        ('orlist', {'kwargs': {'OR': {'foo': 'bar', 'foo2': 'bar2'}}}, {'jql': '"foo"="bar"+OR+"foo2"="bar2"'}),
        ('andlist', {'AND': {'foo': 'bar', 'foo2': 'bar2'}}, {'jql': '"foo"="bar"+AND+"foo2"="bar2"'}),
        ('andlist', {'kwargs': {'AND': {'foo': 'bar', 'foo2': 'bar2'}}}, {'jql': '"foo"="bar"+AND+"foo2"="bar2"'}),
        ('simplelist', {'foo': 'bar'}, {'jql': '"foo"="bar"'}),
        ('simplelist', {'kwargs': {'foo': 'bar'}}, {'jql': '"foo"="bar"'}),
    ])
    def test_sdm_jira_jql_builder(self, _description, conditions, expected):
        self.assertEqual(self.sdm_jira.jql_builder(None, conditions), expected)

    @unittest.skip('Code seems broken (should expand dict)')
    def test_sdm_jira_post_issue(self):
        self.sdm_jira.post_issue(None,
                                 {'project': 'some_project', 'summary': 'summary', 'description': 'description',
                                  'issuetype': 'bug', 'assignee': 'me'})

        self.jira_curl.post.assert_called_with('issue/',
                                               fields={'project': {'key': 'some_project'}, 'issuetype': {'name': 'bug'},
                                                       'assignee': {'name': 'me'}, 'description': 'description',
                                                       'summary': 'summary'})

    @unittest.skip('Code seems broken (should expand dict)')
    def test_sdm_jira_put_issue(self):
        self.sdm_jira.put_issue([], {'key': 1, 'data': {'foo': 'bar'}})

        self.jira_curl.put.assert_called_with('issue/1', data={'fields': {'foo': 'bar'}})

    def test_sdm_jira_post_comment(self):
        # Currently unimplemented method
        self.sdm_jira.post_comment([], {})

    def test_sdm_jira_put_comment(self):
        # Currently unimplemented method
        self.sdm_jira.put_comment([], {})

    def test_sdm_jira_get_comment(self):
        # Currently unimplemented method
        self.sdm_jira.get_comment([], {})

    @patch('sdm_jira.subprocess')
    def test_sdm_jira_post_watcher(self, subprocess):
        self.sdm_jira.post_watcher(1, 'foo')

        subprocess.call.assert_called_with(
            'curl -i  -H "Authorization: Basic some_key"  -H "Content-Type: application/json"  -H "Accept: application/json" -X POST -d \'"foo"\' https://issues-test.jgi-psf.org/rest/api/2/issue/1/watchers -k',
            shell=True)

    def test_sdm_jira_put_watcher(self):
        # Currently unimplemented method
        self.sdm_jira.put_watcher([], {})

    def test_sdm_jira_get_watchers(self):
        # Currently unimplemented method
        self.sdm_jira.get_watchers([], {})

    def test_sdm_jira_get_attachment(self):
        # Currently unimplemented method
        self.sdm_jira.get_attachment([], {})

    def test_sdm_jira_post_attachment(self):
        # Currently unimplemented method
        self.sdm_jira.post_attachment([], {})

    @parameterized.expand([
        ('open with comment', '1', True, ('43',)),
        ('open without comment', '1', False, ('43',)),
        ('in progress with comment', '3', True, ('43',)),
        ('in progress without comment', '3', False, ('43',)),
        ('needs info with comment', '10002', True, ('33', '43')),
        ('needs info without comment', '10002', False, ('33', '43')),
        ('defer with comment', '10003', True, ('43',)),
        ('defer without comment', '10003', False, ('43',)),
    ])
    def test_issue_close(self, _description, status_id, add_comment, expected_status_transitions):
        # _description is only used for showing the test case that failed in a more descriptive way
        # Issue is in `status_id` state
        self.jira_curl.get.return_value = {'fields': {'status': {'id': status_id}}}
        # Expected calls to jira_curl (in order of transition state updates)
        expected_calls = [call('issue/my_issue/transitions', output=None,
                               transition={'id': expected_status_id}) for expected_status_id in
                          expected_status_transitions]

        self.issue.close('my comment' if add_comment else None)

        self.jira_curl.post.assert_has_calls(expected_calls)
        # Verify whether there's an add comment call
        if add_comment:
            self.jira_curl.put.assert_called_with('issue/my_issue', output=None,
                                                  update={'comment': [{'add': {'body': 'my comment'}}]})
        else:
            self.jira_curl.put.assert_not_called()

    def test_issue_close_jira_update_raises_error_stops_transitions(self):
        # Issue is in open state
        self.jira_curl.get.return_value = {'fields': {'status': {'id': '10002'}}}
        # Cause JIRA transition update request to fail
        self.jira_curl.post.side_effect = Exception("error")

        self.issue.close()

        # Make sure only 1 update call to jira_curl
        self.jira_curl.post.assert_called_once()
        self.jira_curl.post.assert_called_with('issue/my_issue/transitions', output=None, transition={'id': '33'})

    def test_issue_allfielddata_not_cached(self):
        self.jira_curl.get.return_value = [{'id': 1}, {'name': 2}]

        self.assertEqual(self.issue.allfielddata, [{'id': 1}, {'name': 2}])
        self.jira_curl.get.assert_called_with('field')

    def test_issue_allfielddata_cached(self):
        self.issue._allfielddata = [{'id': 1}, {'name': 2}]

        self.assertEqual(self.issue.allfielddata, [{'id': 1}, {'name': 2}])
        self.jira_curl.get.assert_not_called()

    def test_issue_metadata_not_cached(self):
        self.jira_curl.get.return_value = {'fields': {'id': 1}}

        self.assertEqual(self.issue.metadata, {'fields': {'id': 1}})
        self.jira_curl.get.assert_called_with('issue/my_issue/editmeta')

    def test_issue_metadata_cached(self):
        self.issue._metadata = {'fields': {'id': 1}}

        self.assertEqual(self.issue.metadata, {'fields': {'id': 1}})
        self.jira_curl.get.assert_not_called()

    def test_value_fields_matches_field_name(self):
        self.issue._metadata = {'fields': {'id': 1}}

        self.assertEqual(self.issue.value_fields('id'), 1)

    def test_value_fields_does_not_matched_field_name(self):
        self.issue._metadata = {'fields': {'name': 'foo'}}
        self.issue._issue_fields = {'name': 'id'}

        self.assertEqual(self.issue.value_fields('id'), 'foo')

    def test_get_custom_id_matches_id(self):
        self.issue._allfielddata = [{'id': 'custom_id'}]

        self.assertEqual(self.issue.get_custom_id('custom_id'), 'custom_id')

    def test_get_custom_id_matches_name(self):
        self.issue._allfielddata = [{'id': 'some_other_id', 'name': 'custom_id'}]

        self.assertEqual(self.issue.get_custom_id('custom_id'), 'some_other_id')

    def test_get_custom_id_exception_returns_none(self):
        self.issue._allfielddata = [{'name': 'custom_id'}]

        self.assertEqual(self.issue.get_custom_id('custom_id'), None)

    def test_issue_fields_cached(self):
        self.issue._issue_fields = {'name': 'id'}

        self.assertEqual(self.issue.issue_fields, {'name': 'id'})

    def test_issue_fields_not_cached(self):
        self.issue._metadata = {'fields': {'custom_field': {'name': 'foo'}}}

        self.assertEqual(self.issue.issue_fields, {'foo': 'custom_field'})

    def test_issue_fields_exception_returns_none(self):
        self.issue._metadata = {}

        self.assertEqual(self.issue.issue_fields, None)

    def test_issue_get_field_id(self):
        self.issue._issue_fields = {'name': 'id'}

        self.assertEqual(self.issue.get_field_id('name'), 'id')

    def test_issue_get_field_id_exception_returns_none(self):
        self.issue._issue_fields = {}

        self.assertEqual(self.issue.get_field_id('name'), None)

    def test_issue_get_field_name(self):
        self.issue._issue_fields = {'id': 'name'}

        self.assertEqual(self.issue.get_field_name('name'), 'id')

    def test_issue_get_field_name_exception_returns_none(self):
        self.issue._issue_fields = {}

        self.assertEqual(self.issue.get_field_name('name'), None)

    def test_issue_get_field_value(self):
        self.jira_curl.get.return_value = {'fields': {'id': 1}}

        self.assertEqual(self.issue.get_field_value('id'), 1)

    def test_issue_get_field_value_not_found_in_fields(self):
        self.jira_curl.get.return_value = {'fields': {'id': 1}}
        self.issue._issue_fields = {'name': 'id'}

        self.assertEqual(self.issue.get_field_value('name'), 1)

    def test_issue_get_field_keys(self):
        # Currently unimplemented method
        self.issue.get_field_keys('id')

    def test_issue_format_set_field(self):
        self.assertEqual(self.issue.format_set_field('foo'), [{'set': 'foo'}])

    def test_issue_comment_with_edit(self):
        self.assertEqual(self.issue.comment_with_edit('comment'), [{'add': {'body': 'comment'}}])

    def test_issue_format_field_no_key(self):
        self.assertEqual(self.issue.format_field('foo'), {'set': 'foo'})

    def test_issue_format_field_with_key(self):
        self.assertEqual(self.issue.format_field('foo', key='bar', op='update'), {'update': {'bar': 'foo'}})

    def test_issue_prepare_opts_no_allowed_values(self):
        self.issue._metadata = {'fields': {'id': {}}}

        self.assertEqual(self.issue.prepare_opts('id', 'foo'), {'set': 'Foo'})

    def test_issue_prepare_opts_allowed_values(self):
        self.issue._metadata = {'fields': {'id': {'allowedValues': [{'bar': 'foo'}]}}}

        self.assertEqual(self.issue.prepare_opts('id', 'foo'), {'set': {'bar': 'Foo'}})

    def test_issue_add_comment(self):
        self.issue.add_comment('comment')

        self.jira_curl.put.assert_called_with('issue/my_issue', output=None,
                                              update={'comment': [{'add': {'body': 'comment'}}]})

    def test_issue_set_field_no_comment(self):
        self.issue._metadata = {'fields': {'id': {}}}

        self.issue.set_field('id', 'foo')

        self.jira_curl.put.assert_called_with('issue/my_issue', output=None, update={'id': [{'set': 'Foo'}]})

    def test_issue_set_field_with_comment(self):
        self.issue._metadata = {'fields': {'id': {}}}

        self.issue.set_field('id', 'foo', 'comment')

        self.jira_curl.put.assert_called_with('issue/my_issue', output=None,
                                              update={'comment': [{'add': {'body': 'comment'}}],
                                                      'id': [{'set': 'Foo'}]})

    def test_issue_set_field_field_opts(self):
        self.issue.set_field('id', 'foo', fieldopts={'key': 'value', 'value': 'foo'})

        self.jira_curl.put.assert_called_with('issue/my_issue', output=None, update={'id': [{'set': {'value': 'foo'}}]})

    def test_issue_put_issue(self):
        self.issue.put_issue({'update': [{'set': 'Foo'}]})

        self.jira_curl.put.assert_called_with('issue/my_issue', output=None, update=[{'set': 'Foo'}])


if __name__ == '__main__':
    unittest.main()
