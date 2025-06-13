import asyncio
import json
import os
import shutil
import sqlite3
import unittest
from unittest.mock import patch, Mock

import requests

from t5common.jira.start_workflow import check_jira
from t5common.jira.check_workflow import check_jobs as checkwf_check_jobs
from t5common.jira.publish_results import check_jobs as publish_check_jobs
from t5common.jira.database import initialize_database
from t5common.jira.mark_job import mark_job


class WorkflowTests(unittest.TestCase):

    issue = 'TEST-1234'
    db = 'jobs.db'
    job_dir = '.'

    def _read_db(self):
        """Read the database tracking workflow state"""
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()

        statement = """
        SELECT
            job.issue,
            job.job_directory,
            project.name AS project,
            job_states.name AS job_state
        FROM
            job
        JOIN
            job_states ON job.job_state_id = job_states.id
        JOIN
            project ON job.project_id = project.id;
        """
        cursor.execute(statement)
        rows = cursor.fetchall()

        conn.close()

        return rows

    def tearDown(self):
        if os.path.isdir(self.issue):
            shutil.rmtree(self.issue)
        if os.path.exists(self.db):
            os.remove(self.db)

    @patch('t5common.jira.connector.requests.post')
    @patch('t5common.jira.utils.read_token')
    @patch('t5common.jira.start_workflow.read_token')
    @patch('t5common.jira.check_workflow.read_token')
    @patch('t5common.jira.publish_results.read_token')
    def test_wf_mark_job(self, mock_read_token1, mock_read_token2, mock_read_token3, mock_read_token4, mock_post):
        # Mock requests for POST to Jira search endpoint
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'issues': [{'key': self.issue}]}
        mock_post.return_value = mock_response

        # Mock token reading
        mock_read_token1.return_value = '=======FAKETOKEN======='
        mock_read_token2.return_value = '=======FAKETOKEN======='
        mock_read_token3.return_value = '=======FAKETOKEN======='
        mock_read_token4.return_value = '=======FAKETOKEN======='

        config = {
                'jira_host': "https://nonexistent.jira.site",
                'jira_user': "noone@lbl.gov",
                'jira_token_file': "nonexistent_token_file",
                'jamo_host': "https://nonexistent.jamo.site",
                'jamo_token_file': "nonexistent_token_file",
                'database': self.db,
                'job_directory': self.job_dir,
                'projects':[
                        {
                            'project': 'TEST',
                            'new_status': 'Fake Status',
                            'workflow_command': 'true',
                            'check_command': 'true',
                            'publish_command': 'true'
                        }
                    ],
                }

        # Test initializing the database with states
        with self.subTest("Initialize database"):
            initialize_database(self.db)
            conn = sqlite3.connect(self.db)
            cursor = conn.cursor()

            statement = "SELECT name FROM job_states;"
            cursor.execute(statement)
            rows = cursor.fetchall()
            self.assertListEqual(rows, [('WORKFLOW_STARTED',),
                                        ('WORKFLOW_START_FAILED',),
                                        ('WORKFLOW_CHECK_FAILED',),
                                        ('WORKFLOW_FINISHED',),
                                        ('PUBLISH_FAILED',),
                                        ('PUBLISHED',)])

        # Test checking Jira and starting jobs
        with self.subTest("Check Jira and start workflow"):
            self.assertFalse(os.path.isdir(self.issue))
            asyncio.run(check_jira(config))
            self.assertTrue(os.path.isdir(self.issue))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_STARTED')])

        # Test checking for finishing jobs and publishing
        with self.subTest("Check for running jobs"):
            asyncio.run(checkwf_check_jobs(config))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_STARTED')])

        # Test marking a job finished
        with self.subTest("Mark workflow finished"):
            rows = self._read_db()
            mark_job('WORKFLOW_FINISHED', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_FINISHED')])

        # Test checking for finishing jobs and publishing
        with self.subTest("Check for finished jobs and start publishing"):
            asyncio.run(publish_check_jobs(config))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_FINISHED')])

        # Test marking a job published
        with self.subTest("Mark job published"):
            mark_job('PUBLISHED', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'PUBLISHED')])

    @patch('t5common.jira.connector.requests.post')
    @patch('t5common.jira.utils.read_token')
    @patch('t5common.jira.start_workflow.read_token')
    @patch('t5common.jira.check_workflow.read_token')
    @patch('t5common.jira.publish_results.read_token')
    def test_wf_no_check(self, mock_read_token1, mock_read_token2, mock_read_token3, mock_read_token4, mock_post):
        # Mock requests for POST to Jira search endpoint
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'issues': [{'key': self.issue}]}
        mock_post.return_value = mock_response

        # Mock token reading
        mock_read_token1.return_value = '=======FAKETOKEN======='
        mock_read_token2.return_value = '=======FAKETOKEN======='
        mock_read_token3.return_value = '=======FAKETOKEN======='
        mock_read_token4.return_value = '=======FAKETOKEN======='

        config = {
                'jira_host': "https://nonexistent.jira.site",
                'jira_user': "noone@lbl.gov",
                'jira_token_file': "nonexistent_token_file",
                'jamo_host': "https://nonexistent.jamo.site",
                'jamo_token_file': "nonexistent_token_file",
                'database': self.db,
                'job_directory': self.job_dir,
                'projects':[
                        {
                            'project': 'TEST',
                            'new_status': 'Fake Status',
                            'workflow_command': 'true',
                            'publish_command': 'true'
                        }
                    ],
                }

        # Test initializing the database with states
        with self.subTest("Initialize database"):
            initialize_database(self.db)
            conn = sqlite3.connect(self.db)
            cursor = conn.cursor()

            statement = "SELECT name FROM job_states;"
            cursor.execute(statement)
            rows = cursor.fetchall()
            self.assertListEqual(rows, [('WORKFLOW_STARTED',),
                                        ('WORKFLOW_START_FAILED',),
                                        ('WORKFLOW_CHECK_FAILED',),
                                        ('WORKFLOW_FINISHED',),
                                        ('PUBLISH_FAILED',),
                                        ('PUBLISHED',)])

        # Test checking Jira and starting jobs
        with self.subTest("Check Jira and start workflow"):
            self.assertFalse(os.path.isdir(self.issue))
            asyncio.run(check_jira(config))
            self.assertTrue(os.path.isdir(self.issue))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_STARTED')])

        # Test checking for finishing jobs and publishing
        with self.subTest("Check for running jobs"):
            asyncio.run(checkwf_check_jobs(config))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_STARTED')])

        # Test marking a job finished
        with self.subTest("Mark workflow finished"):
            rows = self._read_db()
            mark_job('WORKFLOW_FINISHED', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_FINISHED')])

        # Test checking for finishing jobs and publishing
        with self.subTest("Check for finished jobs and start publishing"):
            asyncio.run(publish_check_jobs(config))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_FINISHED')])

        # Test marking a job published
        with self.subTest("Mark job published"):
            mark_job('PUBLISHED', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'PUBLISHED')])
