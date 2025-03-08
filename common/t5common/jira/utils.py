from importlib.resources import files
import json
import os

import yaml
from jsonschema import validate, ValidationError
from ..utils import read_token


WF_FILENAME = '.t5_jira_wf'


def _load_schema():
    schema_path = files(__package__).joinpath('schema', 'config.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema


def load_config(config_path):
    schema = _load_schema()
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    validate(instance=config, schema=schema)
    return config


def get_job_env(issue, config):
    # Set up environment to run subprocess in
    env = os.environ.copy()
    env['JIRA_HOST'] = config['host']
    env['JIRA_USER'] = config['user']
    env['JIRA_TOKEN'] = read_token(config['token_file'])

    # Set up the working directory to run the job in
    wd = os.path.join(config.get('job_directory', '.'), issue)
    return env, wd


def open_wf_file(wd, mode):
    return open(os.path.join(wd, WF_FILENAME), mode)
