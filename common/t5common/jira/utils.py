from importlib.resources import files
import json
import os
import sys

import yaml
from jsonschema import validate, ValidationError
from ..utils import read_token


WF_FILENAME = '.t5_jira_wf'


def load_info(directory):
    schema_path = files(__package__).joinpath('schema', 'wf_info.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    wf_info_path = os.path.join(directory, WF_FILENAME)
    with open(wf_info_path, 'r') as file:
        wf_info = json.load(file)

    try:
        validate(instance=wf_info, schema=schema)
    except ValidationError as ve:
        print(f"Invalid workflow info file {wf_info_path}: {ve}", file=sys.stderr)
        exit(3)

    # Get the configuration file for the instance of the workflow management
    # system that this job was started from
    database = os.path.join(directory, wf_info['database'])
    return wf_info['issue'], database


def load_config(config_path):
    schema_path = files(__package__).joinpath('schema', 'config.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    validate(instance=config, schema=schema)
    return config


def get_job_env(issue, config):
    # Set up environment to run subprocess in
    env = os.environ.copy()
    env['JIRA_HOST'] = config['jira_host']
    env['JIRA_USER'] = config['jira_user']
    env['JIRA_TOKEN'] = read_token(config['jira_token_file'])
    env['JAMO_HOST'] = config['jamo_host']
    env['JAMO_URL'] = config['jamo_host']
    env['JAMO_TOKEN'] = read_token(config['jamo_token_file'])

    # Set up the working directory to run the job in
    wd = os.path.join(config.get('job_directory', '.'), issue)
    return env, wd


def open_wf_file(wd, mode):
    return open(os.path.join(wd, WF_FILENAME), mode)
