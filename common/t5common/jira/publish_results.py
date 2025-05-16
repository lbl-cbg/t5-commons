import argparse
import asyncio
import json
import os
from os.path import abspath, relpath
import re
import sys
import subprocess
import time
from pathlib import Path

import yaml
import typer

from .connector import JiraConnector
from .database import DBConnector, JobState
from .utils import load_config, get_job_env, open_wf_file
from .mark_job import mark_job
from ..utils import get_logger, read_token


logger = get_logger()


async def publish_job(issue, project_config, config):
    """Publish results from a job

    Args:
        issue:              The Jira issue key the job was run for
        project_config:     Information on the project, namely how to publish results
        config:             General information about this workflow runner

    """
    logger.info(f"Publishing results for {issue}")
    env, wd = get_job_env(issue, config)

    # Add workflow info to the working directory for subsequent steps
    with open_wf_file(wd, 'r') as f:
        wf_info = json.load(f)

    # Set up the command to run in the subprocess
    command = re.split(r'\s+', project_config['publish_command'])
    command.append("./")

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
    """Check for finished jobs and publish the results of finished jobs.

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
        jobs = dbc.get_jobs(JobState.WORKFLOW_FINISHED, project_config['project'])
        for job in jobs:
            issues.append(job.issue)
            tasks.append(publish_job(job.issue, project_config, config))

    results = await asyncio.gather(*tasks)
    for issue, (retcode, wd) in zip(issues, results):
        if retcode == 0:
            logger.info(f"Issue {issue} publish command succeeded")
        else:
            logger.error(f"Issue {issue} publish command failed")
            jc.add_comment(issue, "Publish failed")
            mark_job(wd, JobState.PUBLISH_FAILED)

    dbc.close()


def publish_results(config: Path = typer.Argument(..., help="Path to the YAML configuration file")):
    """Publish results of finished workflows"""
    config = load_config(config)
    asyncio.run(check_jobs(config))


if __name__ == "__main__":
    main()
