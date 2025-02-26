import argparse
from importlib.resources import files
import json
import os
import sys

import yaml
from jsonschema import validate, ValidationError

from .connector import JiraConnector
from .database import DBConnector
from .utils import load_config, WF_FILENAME


def _load_schema():
    schema_path = files(__package__).joinpath('schema', 'wf_info.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema


def load_info(directory):
    schema = _load_schema()

    wf_info_path = os.path.join(directory, WF_FILENAME)
    with open(wf_info_path, 'r') as file:
        wf_info = json.load(file)

    try:
        validate(instance=wf_info, schema=schema)
    except ValidationError as ve:
        print("Invalid workflow info file {wf_info_path}: {ve}", file=sys.stderr)
        exit(3)

    # Get the configuration file for the instance of the workflow management
    # system that this job was started from
    database = os.path.join(directory, wf_info['database'])
    return wf_info['issue'], database


STATES = {
        'finished': 'WORKFLOW_FINISHED',
        'published': 'PUBLISHED'
        }


def mark_job(step, directory):
    """Mark a job as finished or published.

    This command will be invoked by wokflow and publishing scripts
    to indicate that they are finished.
    """
    issue, database = load_info(directory)
    dbc = DBConnector(f"sqlite:///{database}")
    dbc.transition_job(issue, STATES[step])
    dbc.close()


def main():
    parser = argparse.ArgumentParser(description="Mark new status on a job")
    parser.add_argument('step', type=str, choices=list(STATES.keys()), help='The job state to mark')
    parser.add_argument('dir', type=str, help='The directory the job was run from', default='./', nargs='?')
    args = parser.parse_args()

    mark_job(args.step, args.dir)


if __name__ == "__main__":
    main()
