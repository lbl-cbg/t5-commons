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


STEPS = {
        'finished': 'finish_job',
        'published': 'publish_job'
        }

def mark_job(step, directory):

    issue, database = load_info(directory)

    dbc = DBConnector(f"sqlite:///{database}")

    method = getattr(dbc, STEPS[step])

    method(issue)

    dbc.close()


def main():
    parser = argparse.ArgumentParser(description="Mark new status on a job")
    parser.add_argument('step', type=str, choices=list(STEPS.keys()), help='The job state to mark')
    parser.add_argument('dir', type=str, help='The directory the job was run from', default='./', nargs='?')
    args = parser.parse_args()


    mark_job(args.step, args.dir)


if __name__ == "__main__":
    main()
