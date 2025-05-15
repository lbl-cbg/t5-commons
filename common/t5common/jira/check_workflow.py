import argparse
import asyncio
from importlib.resources import files
import json
import re
import os
import sys
from enum import Enum
from pathlib import Path

import typer
import yaml

from .connector import JiraConnector
from .database import DBConnector, JobState
from .utils import load_config, get_job_env, WF_FILENAME
from .mark_job import mark_job
from ..utils import get_logger


logger = get_logger()


async def check_workflow(issue, project_config, config):
    """Start a job from an issue

    Args:
        issue:              The Jira issue key
        project_config:     Information on the project to pull issues from
        config:             General information about this workflow runner

    """
    logger.info(f"Checking job for {issue}")
    env, wd = get_job_env(issue, config)

    # Make sure working directory for job exists
    if not os.path.exists(wd):
        logger.error(f"Job not initiated for {issue} - {wd} exists")
        return -1, None

    # Set up the command to run in the subprocess
    command = re.split(r'\s+', project_config['check_command'])
    command.append(wd)

    # Call the job command in a subprocess
    logger.info(f"Executing check workflow command for {issue}: {' '.join(command)}")
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
        logger.error(f"Check command for {issue} failed:\n{stdout.decode()}")
    else:
        msg = f"Workflow command for {issue} succeeded"
        if len(stdout) > 0:
            msg += f"\n{stdout.decode()}"
        logger.info(msg)

    return process.returncode, wd


async def check_jobs(config):
    """Check workflow for in-progress jobs

    Args:
        config: General information about this workflow runner. This should also contain
                information about the projects to check and how to run jobs for those
                projects
    """
    database = config['database']
    dbc = DBConnector(f"sqlite:///{database}")

    # Check each project queue, and create a new job for each new issue
    tasks = list()
    issues = list()
    for project_config in config['projects']:
        if 'check_command' not in project_config:
            continue
        jobs = dbc.get_jobs(JobState.WORKFLOW_STARTED, project_config['project'])
        for job in jobs:
            issues.append(job.issue)
            tasks.append(check_workflow(job.issue, project_config, config))

    results = await asyncio.gather(*tasks)
    for issue, (retcode, wd) in zip(issues, results):
        if retcode == 0:
            logger.info(f"Issue {issue} workflow check succeeded")
        else:
            logger.error(f"Issue {issue} workflow check failed")
            jc.add_comment(issue, "Workflow check failed")
            mark_job(wd, JobState.WORKFLOW_CHECK_FAILED)

    dbc.close()


def check_workflows(config: Path = typer.Argument(..., help="Path to the YAML configuration file")):
    """Check running workflows for completion"""
    config = load_config(config)
    asyncio.run(check_jobs(config))


if __name__ == '__main__':
    typer.run(check_workflows)
