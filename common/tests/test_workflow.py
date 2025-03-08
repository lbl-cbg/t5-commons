import asyncio
import json
import os
import shutil
import sqlite3
import unittest
from unittest.mock import patch, Mock

import requests

from t5common.jira.check_jira import check_jira
from t5common.jira.check_jobs import check_jobs
from t5common.jira.database import initialize_database
from t5common.jira.mark_job import mark_job


class TestYourFunction(unittest.TestCase):

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
    @patch('t5common.jira.check_jira.read_token')
    def test_your_function(self, mock_read_token1, mock_read_token2, mock_post):
        # Mock requests for POST to Jira search endpoint
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'issues': [{'key': self.issue}]}
        mock_post.return_value = mock_response

        # Mock token reading
        mock_read_token1.return_value = '=======FAKETOKEN======='
        mock_read_token2.return_value = '=======FAKETOKEN======='

        config = {
                'host': "https://nonexistent.jira.site",
                'user': "noone@lbl.gov",
                'token_file': "nonexistent_token_file",
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
            self.assertListEqual(rows, [('WORKFLOW_STARTED',), ('WORKFLOW_FINISHED',), ('PUBLISH_STARTED',), ('PUBLISHED',)])

        # Test checking Jira and starting jobs
        with self.subTest("Check Jira and start workflow"):
            self.assertFalse(os.path.isdir(self.issue))
            asyncio.run(check_jira(config))
            self.assertTrue(os.path.isdir(self.issue))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_STARTED')])

        # Test marking a job finished
        with self.subTest("Mark workflow finished"):
            mark_job('finished', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'WORKFLOW_FINISHED')])

        # Test checking for finishing jobs and publishing
        with self.subTest("Check for finished jobs and start publishing"):
            asyncio.run(check_jobs(config))
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'PUBLISH_STARTED')])

        # Test marking a job published
        with self.subTest("Mark job published"):
            mark_job('published', self.issue)
            rows = self._read_db()
            self.assertListEqual(rows, [('TEST-1234', './TEST-1234', 'TEST', 'PUBLISHED')])
