templateValidator = {
    'name': {'type': str},
    'description': {'type': str},
    'tags': {'type': list, 'validator': {'type': str}},
    'outputs': {'type': list, 'validator': {'type': dict, 'validator': {
        'label': {'type': str, 'doc': 'The file label used solely to map submission files'},
        'tags': {'type': list, 'validator': {'type': str}, 'doc': 'The file tags that describe this file'},
        'required': {'type': bool, 'default': True},
        'metadata': {'type': dict, 'required': False, 'validator': '*:1'},
        'description': {'type': str},
        'file_name': {'type': str, 'required': False},
        'default_metadata_values': {'type': dict, 'required': False, 'validator': '*:1'},
        'required_metadata_keys': {'type': list, 'required': False, 'validator': {'type': dict, 'validator': {
            'description': {'type': str, 'required': False},
            'macro': {'type': str, 'required': False},
            'type': {'type': str, 'required': False},
            'key': {'type': str, 'required': False},
            'required': {'type': bool, 'default': True}}}}}}},
    'default_metadata_values': {'type': dict, 'required': False, 'validator': '*:1'},
    'required_metadata_keys': {'type': list, 'required': False, 'validator': {'type': dict, 'validator': {
        'description': {'type': str, 'required': False},
        'type': {'type': str, 'required': False},
        'macro': {'type': str, 'required': False},
        'key': {'type': str, 'required': False},
        'required': {'type': bool, 'default': True}}}},
    'email': {'type': dict, 'required': False, 'validator': {
        'attachments': {'type': list, 'required': False, 'validator': {'type': str}},
        'content': {'type': dict,
                    'validator': {'file': {'type': str, 'required': False}, 'string': {'type': str, 'required': False}},
                    'doc': 'The content of this email, can pass in a file or just string'},
        'reply_to': {'type': str, 'required': False,
                     'doc': 'If you want to have the response send to an other email address pass this in'},
        'subject': {'type': str,
                    'doc': 'The subject of the email address. You can pass in dynamic values from the submission data by using {metadata.key}'},
        'to': {'type': (str, list), 'doc': 'The email address(es) that this email will send to'},
        'bcc': {'type': (str, list), 'required': False,
                'doc': 'The email address(es) that this email will be blind cc\'d to'},
        'mime': {'type': str, 'required': False, 'doc': 'The mime type of the email content that is type string'},
        'cc': {'type': (str, list), 'required': False, 'doc': 'The email address(es) that this email will be cc\'d to'}
    }}}

importValidator = {
    'send_email': {'type': bool, 'required': False},
    'metadata': {'type': dict, 'required': True, 'validator': '*:1'},
    'outputs': {'type': list, 'required': True, 'validator': {
        'file': {'type': str},
        'label': {'type': str},
        'metadata': {'type': dict, 'required': False, 'validator': '*'}}},
    'email': {'type': dict, 'required': False, 'validator': {
        'attachments': {'type': list, 'required': False, 'validator': {'type': str}},
        'content': {'type': dict, 'required': False,
                    'validator': {'file': {'type': str, 'required': False}, 'string': {'type': str, 'required': False}},
                    'doc': 'The content of this email, can pass in a file or just string'},
        'reply_to': {'type': str, 'required': False,
                     'doc': 'If you want to have the response send to an other email address pass this in'},
        'subject': {'type': str, 'required': False,
                    'doc': 'The subject of the email address. You can pass in dynamic values from the submission data by using {metadata.key}'},
        'mime': {'type': str, 'required': False, 'doc': 'The mime type of the email content that is type string'},
        'to': {'type': (str, list), 'required': False, 'doc': 'The email address(es) that this email will send to'}
    }}}

macroValidator = {
    'name': {'type': str,
             'doc': 'The name to give this macro, should be the same as the file name and not contain spaces'},
    'description': {'type': str, 'doc': 'Descriptive text on what this macro is and what it should be used for'},
    'default_metadata_values': {'type': dict, 'required': False, 'validator': '*:1'},
    'required_metadata_keys': {'type': list, 'required': True, 'validator': {'type': dict, 'validator': {
        'description': {'type': str},
        'type': {'type': str},
        'key': {'type': str},
        'required': {'type': bool, 'default': True}}}},
}

cv = {
    'value': {'type': (str, int, float), 'doc': 'The value that can be passed in will be validated against'},
    'description': {'type': str, 'doc': 'The description of the value'}
}

tagTemplateValidator = {
    'name': {'type': str},
    'md5': {'type': str, 'required': False},
    'description': {'type': str},
    'default_metadata_values': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}},
    'required_metadata_keys': {'type': list, 'required': False, 'validator': {'*': {'type': dict, 'validator': {
        'description': {'type': str, 'required': False},
        'macro': {'type': str, 'required': False},
        'type': {'type': str, 'required': False},
        'key': {'type': str, 'required': False},
        'required': {'type': (bool, int), 'default': True}}}}},
}
