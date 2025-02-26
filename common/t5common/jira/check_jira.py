import argparse
import asyncio
import json
import os
from os.path import abspath, relpath
import re
import sys
import subprocess
import time

import yaml

from .connector import JiraConnector
from .database import DBConnector
from .utils import load_config, WF_FILENAME
from ..utils import get_logger, read_token


logger = get_logger()


def format_query(config):
    return 'project = {project} AND status = "{new_status}"'.format(**config)


async def process_issue(issue, project_config, config):
    # Set up environment to run subprocess in
    env = os.environ.copy()
    env['JIRA_HOST'] = config['host']
    env['JIRA_USER'] = config['user']
    env['JIRA_TOKEN'] = read_token(config['token_file'])

    # Set up the command to run in the subprocess
    command = re.split(r'\s+', project_config['command'])
    command.append(issue)

    # Set up the working directory to run the job in
    wd = os.path.join(config.get('job_directory', '.'), issue)
    if os.path.exists(wd):
        raise RuntimeError(f"workflow already started for {issue} - {wd} already exists")
    else:
        os.mkdir(wd)

    # Add workflow info to the working directory for subsequence steps
    wf_info = {
            'issue': issue,
            'database': relpath(abspath(config['database']), abspath(wd)),
            }
    with open(os.path.join(wd, WF_FILENAME), 'w') as f:
        json.dump(wf_info, f)

    # Call the job command in a subprocess
    logger.info(f"Processing {issue}: {' '.join(command)}")
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
        logger.error(f"Processing {issue} failed:\n{stdout.decode()}")
    else:
        msg = f"Processing {issue} succeeded:"
        if len(stdout) > 0:
            msg += f"\n{stdout.decode()}"
        logger.info(msg)

    return process.returncode, wd


async def check_jira(config):
    # Connect to Jira
    jc = JiraConnector(jira_host=config['host'],
                       jira_user=config['user'],
                       jira_token=read_token(config['token_file']))

    database = config['database']
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
                if state != 'STARTED':
                    logger.error(f"Issue {key} still has new_status, but is not in STARTED state: state = {state}")
                continue
            issues.append(key)
            tasks.append(process_issue(key, project_config, config))

    results = await asyncio.gather(*tasks)
    for issue, (retcode, wd) in zip(issues, results):
        if retcode == 0:
            logger.info(f"Issue {issue} marked as started")
            dbc.start_job(issue, wd)
        else:
            logger.info(f"Issue {issue} failed -- not marking as started")

    dbc.close()


def main():
    parser = argparse.ArgumentParser(description="Poll Jira projects and run a script for each issue.")
    parser.add_argument('config', type=str, help='Path to the YAML configuration file')
    args = parser.parse_args()

    config = None

    config = load_config(args.config)
    asyncio.run(check_jira(config))


if __name__ == "__main__":
    main()
