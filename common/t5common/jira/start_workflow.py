import asyncio
import json
import os
from os.path import abspath, relpath
import re

import typer

from .connector import JiraConnector
from .database import DBConnector, JobState
from .utils import load_config, get_job_env, open_wf_file
from .mark_job import mark_job
from ..utils import get_logger, read_token


logger = get_logger()


def format_query(config):
    """Build JQL query to get issues from Jira"""
    return 'project = {project} AND status = "{new_status}"'.format(**config)


async def intiate_job(issue, project_config, config):
    """Start a job from an issue

    Args:
        issue:              The Jira issue key
        project_config:     Information on the project to pull issues from
        config:             General information about this workflow runner

    """
    logger.info(f"Initiating job for {issue}")
    env, wd = get_job_env(issue, config)

    # Make working directory for job
    if os.path.exists(wd):
        logger.error(f"Job already initiated for {issue} - {wd} exists")
        return -1, None
    else:
        os.mkdir(wd)

    # Add workflow info to the working directory for subsequent steps
    wf_info = {
            'issue': issue,
            'database': relpath(abspath(config['database']), abspath(wd)),
            }
    with open_wf_file(wd, 'w') as f:
        json.dump(wf_info, f)

    # Set up the command to run in the subprocess
    command = re.split(r'\s+', project_config['workflow_command'])
    command.append(issue)

    # Call the job command in a subprocess
    logger.info(f"Executing workflow command for {issue}: {' '.join(command)}")
    process = await asyncio.create_subprocess_exec(
        command[0], *command[1:],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        cwd=wd,
    )
    # Read the output and error streams
    stdout, _ = await process.communicate()

    if process.returncode != 0:
        logger.error(f"Workflow command for {issue} failed:\n{stdout.decode()}")
    else:
        msg = f"Workflow command for {issue} succeeded"
        if len(stdout) > 0:
            msg += f"\n{stdout.decode()}"
        logger.info(msg)

    return process.returncode, wd


async def check_jira(config):
    """Check Jira, and start a job for every issue returned

    Args:
        config: General information about this workflow runner. This should also contain
                information about the projects to check and how to run jobs for those
                projects
    """
    # Connect to Jira
    jc = JiraConnector(jira_host=config['jira_host'],
                       jira_user=config['jira_user'],
                       jira_token=read_token(config['jira_token_file']))

    database = config['database']

    if not os.path.isdir(config['job_directory']):
        os.mkdir(config['job_directory'])

    dbc = DBConnector(f"sqlite:///{database}")

    # Check each project queue, and create a new job for each new issue
    tasks = list()
    issues = list()
    for project_config in config['projects']:
        query = format_query(project_config)
        proj_issues = jc.query(query)['issues']
        for issue in proj_issues:
            key = issue['key']
            state = dbc.job_state(key)
            if state is not None:
                continue
            issues.append((project_config['project'], key))
            tasks.append(intiate_job(key, project_config, config))

    results = await asyncio.gather(*tasks)
    for (project, issue), (retcode, wd) in zip(issues, results):
        if retcode == 0:
            logger.info(f"Issue {issue} marked as workflow started")
            dbc.start_job(issue, wd, project)
        else:
            logger.error(f"Issue {issue} failed -- not marking as workflow started")
            jc.add_comment(issue, "Workflow start failed")
            mark_job(wd, JobState.WORKFLOW_START_FAILED)

    dbc.close()


def start_workflows(
    config: str = typer.Argument(..., help="Path to the YAML configuration file")
):
    """Check Jira project(s) and start workflows for new issues"""

    config = load_config(config)
    asyncio.run(check_jira(config))

if __name__ == '__main__':
    typer.run(start_workflows)
