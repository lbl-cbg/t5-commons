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
from .database import DBConnector, WORKFLOW_FINISHED, PUBLISH_STARTED
from .utils import load_config, get_job_env, open_wf_file
from ..utils import get_logger, read_token


logger = get_logger()


async def publish_job(issue, project_config, config):
    logger.info(f"Publishing results for {issue}")
    env, wd = get_job_env(issue, config)

    # Add workflow info to the working directory for subsequent steps
    with open_wf_file(wd, 'r') as f:
        wf_info = json.load(f)

    # Set up the command to run in the subprocess
    command = re.split(r'\s+', project_config['publish_command'])
    command.append(wd)

    # Call the job command in a subprocess
    logger.info(f"Executing publish command for {issue}: {' '.join(command)}")
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
        logger.error(f"Publish command for {issue} failed:\n{stdout.decode()}")
    else:
        msg = f"Publish command for {issue} succeeded"
        if len(stdout) > 0:
            msg += f"\n{stdout.decode()}"
        logger.info(msg)

    return process.returncode, wd


async def check_jobs(config):
    database = config['database']
    dbc = DBConnector(f"sqlite:///{database}")

    # Check each project queue, and create a new job for each new issue
    tasks = list()
    issues = list()
    for project_config in config['projects']:
        jobs = dbc.get_jobs(WORKFLOW_FINISHED, project_config['project'])
        for job in jobs:
            issues.append(job.issue)
            tasks.append(publish_job(job.issue, project_config, config))

    results = await asyncio.gather(*tasks)
    for issue, (retcode, wd) in zip(issues, results):
        if retcode == 0:
            logger.info(f"Issue {issue} marked as publishing started")
            dbc.transition_job(issue, PUBLISH_STARTED)
        else:
            logger.error(f"Issue {issue} publishing start failed")

    dbc.close()


def main():
    parser = argparse.ArgumentParser(description="Poll Jira projects and run a script for each issue.")
    parser.add_argument('config', type=str, help='Path to the YAML configuration file')
    args = parser.parse_args()

    config = None

    config = load_config(args.config)
    asyncio.run(check_jobs(config))


if __name__ == "__main__":
    main()
