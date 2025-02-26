from importlib.resources import files
import json

import yaml
from jsonschema import validate, ValidationError


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
