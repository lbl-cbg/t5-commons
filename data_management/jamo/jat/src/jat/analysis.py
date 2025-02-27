import tempfile
import shutil
import zipfile
import datetime
import hashlib
import copy
import subprocess
import threading
import yaml
import os
import glob
import pymongo
import cherrypy
from lapinpy.curl import Curl
from bson.objectid import ObjectId
from lapinpy.mongorestful import MongoRestful
from lapinpy import curl, common, restful, sdmlogger
from jamo_common import customtransform, convert_dates

from .validators.analysis import tagTemplateValidator


def checkType(type, value):
    if type == 'string' and isinstance(value, str):
        return True
    if type == 'number' and isinstance(value, (int, float)):
        return True
    if type == 'boolean' and isinstance(value, bool):
        return True
    if type.startswith('list'):
        lType = type.split(':', 1)[1]
        if not isinstance(value, list):
            return checkType(lType, value)
        for sValue in value:
            if checkType(lType, sValue) is False:
                return False
        return True
    return False


def log(func):
    def inner(*args, **kwargs):
        ret = func(*args, **kwargs)
        print(args, kwargs, ret)
        return ret

    return inner


def convertType(type, value):
    try:
        if type in ('string', 'str'):
            return str(value), True
        if type == 'number':
            val = str(value)
            if val.count('.') == 1:
                return float(val), True
            else:
                return int(val), True
        if type == 'boolean':
            value = str(value).lower()
            if value == 'true' or value == '1':
                return True, True
            elif value == 'false' or value == '0':
                return False, True
            return None, False
        if type.startswith('list'):
            lType = type.split(':', 1)[1]
            if not isinstance(value, list):
                if value.count(',') > 0:
                    value = value.split(',')
                else:
                    return convertType(lType, value)
            ret = []
            for sValue in value:
                value, success = convertType(lType, sValue)
                if not success:
                    return None, False
                ret.append(value)
            return ret, True
    except Exception:
        pass
    return None, False


def check_keys(known_keys, doc, extra_keys=None, file=None):
    warnings = []
    warning = "warning: Metadata key '%s' " + ("for output file '%s' " if file else "") + "not found in analysis template"
    for key in doc:
        if key not in known_keys and (not extra_keys or key not in extra_keys):
            warnings.append(warning % tuple([key] + ([file] if file else [])))
    return warnings


def process_template_data(template, template_data):
    # look for invalid keys in the global metadata
    known_keys = set([key['key'] for key in template.get('required_metadata_keys', [])])
    warnings = check_keys(known_keys, template_data.get('metadata', {}))
    error_key = len(warnings) > 0

    options = {}
    for key in template_data:
        if key not in ('metadata', 'outputs', 'inputs'):
            options[key] = template_data[key]

    template_outputs = {}
    for output in template['outputs']:
        template_outputs[output['label']] = output

    found = []
    error_label = False
    for output in template_data.get('outputs', []):
        # If we want to allow users to pass a dictionary of dictionaries rather than an array of dictionaries
        # Could be dangerous as duplicate entries would be lost
        # if not isinstance(output, dict):
        #     if isinstance(template_data['outputs'][output], dict):
        #         output = template_data['outputs'][output]
        #     else:
        #         warnings.append("Warning: Object %s in output not processed" % output)
        #         continue
        if 'label' not in output:
            warnings.append("warning: Output file '%s' does not have a label tag in the submission file" % (output.get('file', 'None')))
            error_label = True
            continue
        if output['label'] not in template_outputs:
            warnings.append("warning: Output file '%s' does not have a matching label of '%s' in the analysis template" % (output.get('file', 'None'), output['label']))
            error_label = True
            continue
        matching_output = template_outputs[output['label']]
        metadata = matching_output.get('metadata', {})
        metadata.update(output.get('metadata', {}))
        output['tags'] = matching_output['tags']
        output['metadata'] = metadata
        for var in ('required', 'description', 'required_metadata_keys', 'default_metadata_values'):
            if var in matching_output:
                output[var] = matching_output[var]
        found.append(output)

    # look for invalid keys in the outputs
    for output in found:
        known_keys_file = set([key['key'] for key in template_outputs[output['label']].get('required_metadata_keys', [])])
        file_warnings = check_keys(known_keys, output['metadata'], known_keys_file, output.get('file', 'None'))
        warnings.extend(file_warnings)
        if len(file_warnings) > 0:
            error_key = True

    if error_key or error_label:
        if error_key:
            warnings.append('warning: You have Metadata keys that are not defined in the template.  Processing will continue for now.  In a future version of jat, your import will be aborted.')
        if error_label:
            warnings.append('warning: You have outputs with invalid labels.  Processing will continue for now.  In a future version of jat, your import will be aborted.')
        pass

    return {'metadata': template_data.get('metadata', {}),
            'outputs': found,
            'inputs': template_data.get('inputs', []),
            'options': options,
            'warnings': warnings}


def process_template(kwargs):
    missing = []
    found_file_types = {}
    errors = []
    found_errors = False
    for output in kwargs['outputs']:
        found_file_types[output['label']] = output

    for output in kwargs['template']['outputs']:
        if output['label'] in found_file_types:
            continue
        if kwargs['location'] is not None and 'file_name' in output:
            matches = glob.glob(os.path.join(kwargs['location'], output['file_name']))
            if len(matches) == 1:
                output['file'] = matches[0]
                kwargs['outputs'].append(output)
                continue
        if output.get('required', True):
            found_errors = True
            missing.insert(0, output)
        else:
            missing.append(output)
    if len(kwargs['outputs']) == 0:
        errors.append('list of output files is empty')
        found_errors = True
    for output in kwargs['outputs']:
        output['errors'] = []
        if 'file' not in output:
            output['errors'].append('file location not found')
            found_errors = True
        if 'default_metadata_values' in output:
            if 'metadata' not in output:
                output['metadata'] = {}
            for dkey, dval in output['default_metadata_values'].items():
                if dkey not in output['metadata']:
                    output['metadata'][dkey] = dval
            del output['default_metadata_values']
        if 'required_metadata_keys' not in output:
            continue
        for info in output['required_metadata_keys']:
            key = info['key']
            if key in output.get('metadata', {}):
                use_keys_from = output
            else:
                use_keys_from = kwargs
            if key in use_keys_from.get('metadata', {}):
                if not checkType(info['type'], use_keys_from['metadata'][key]):
                    output['errors'].append('metadata key %s has the wrong type. Got %s, expected %s' % (key, type(use_keys_from['metadata'][key]).__name__, info['type']))
                    found_errors = True
            elif info.get('required', True):
                output['errors'].append('required metadata key %s not found' % key)
                found_errors = True
    if 'default_metadata_values' in kwargs['template']:
        for m in kwargs['template']['default_metadata_values']:
            if m not in kwargs['metadata']:
                kwargs['metadata'][m] = kwargs['template']['default_metadata_values'][m]

    if 'required_metadata_keys' in kwargs['template']:
        for m in kwargs['template']['required_metadata_keys']:
            if m['key'] not in kwargs['metadata'] and m.get('required', True):
                errors.append('metadata key %s was not found in the analysis metadata' % m['key'])
                found_errors = True
            elif m['key'] in kwargs['metadata'] and not checkType(m['type'], kwargs['metadata'][m['key']]):
                errors.append('metadata key %s found in the analysis metadata had the wrong type. Was %s expected %s' % (m['key'], type(kwargs['metadata'][m['key']]).__name__, m['type']))
                found_errors = True

    if found_errors:
        errors.insert(0, 'error: cannot submit this analysis, encountered the following errors:')
        for output in missing:
            if output.get('required', True):
                errors.append('missing required file of type %s' % output['label'])
        for output in kwargs['outputs']:
            if len(output['errors']) > 0:
                errors.append('The following errors were encountered for file type %s:' % output['label'])
                errors.extend(output['errors'])
        raise common.HttpException(400, errors)

    for output in kwargs['outputs']:
        for name in ('required', 'required_metadata_keys', 'errors'):
            if name in output:
                del output[name]


def _get_quarter(row, args):
    # calculate the fiscal year and quarter which starts in the previous year in October
    # and ends the following year in September
    date = row.get('added_date', None)
    if date:
        return f'FY{date.year + date.month // 10} Q{(((date.month + 2) % 12) // 3) + 1}'
    return None


def _get_publish(row, args):
    if row.get('publish', None):
        return 'True'
    return None


@restful.doc('Interface for interacting with jamo analysis')
@restful.menu('jat')
class Analysis(MongoRestful):
    publishing_flags = ['img', 'mycocosm', 'phytozome', 'portal', 'genbank']
    display_location_cv = ['QC and Genome Assembly',
                           'Combined Metatranscriptome Assembly',
                           'smRNA Analysis',
                           'smRNA Analysis/Read Length Distribution',
                           'smRNA Analysis/miRNA Detection and Expression',
                           'Sequencing QC Reports',
                           'Transcriptome Analysis',
                           'Transcriptome Analysis/BAMs',
                           'Transcriptome Analysis/Pairwise DGE Results',
                           'Transcriptome Assembly',
                           'QC Filtered Raw Data',
                           'Raw Data',
                           'Metatranscriptome Assembly',
                           'Resequencing - Multisample Analysis',
                           'Reference Alignment',
                           'Metatranscriptome Analysis/BAMs',
                           'Genome Extraction',
                           'Methylation Analysis',
                           'Resequencing']

    def __init__(self, config=None):
        # config should be required, but when config added, needed to default to None
        if config is not None:
            self.config = config
        mongo_options = getattr(self.config, 'mongo_options', None)
        MongoRestful.__init__(self, self.config.mongoserver, self.config.mongo_user, self.config.mongo_pass, self.config.meta_db, mongo_options, host_port=getattr(self.config, 'mongo_port', None))
        self.moduleName = 'JAT'
        self.auto_reload = True
        self.logger = sdmlogger.getLogger("analysis")
        self.queryLogger = sdmlogger.getQueryLogger('Query')
        self.apiSource = {'source': 'analysis_api'}
        self.clSource = {'source': 'jat_cltool'}
        self.logger.info('started analysis')
        self.macros = {}
        temp = self.query('analysis_macros')
        for te in temp:
            self.macros[te['name']] = te
        self.alias_on = None
        self.alias_lock = threading.Lock()
        # this should be moved to a config file
        if hasattr(self.config, 'bitbucket_api_url'):
            self.bb_curl = Curl(self.config.bitbucket_api_url, retry=0)
            # self.bb_curl.userData = 'Basic amdpX2RtX2ludGVncmF0aW9uOng5NTJqdHpB'
            self.bb_curl.userData = 'Basic Y2piZWVjcm9mdDo0TXNoSlBTTTVRZ0NwYWRQNnZyUQ=='
        for key in ('analysis.key', 'macro.name', 'template.name'):
            collection, key = key.split('.')
            self.db[collection].ensure_index(key, unique=True)
        self.cv = {}
        self.template_files = getattr(self.config, 'templates', list())
        self.cv_files = getattr(self.config, 'cvs', list())
        self.macro_files = getattr(self.config, 'macros', list())
        self.tag_template_files = getattr(self.config, 'tag_templates', list())
        # self.filecheck = filecheck.FileCheck(self.config.scratch)
        if self.config.instance_type == 'dev':
            self.prod_curl = curl.Curl(self.config.prod_url)
            self.post_analysisimport.__func__.permissions = []
        global publishing_flags
        if hasattr(self.config, 'publishing_flags'):
            publishing_flags = self.config.publishing_flags
        global display_location_cv
        if hasattr(self.config, 'display_location_cv'):
            display_location_cv = self.config.display_location_cv
        self.jat_key_dir_switch = 367700
        self.jat_keys = {key: value.get('jat_key_name') for (key, value) in self.config.division.items()}


    def sendEmail(self, to, subject, content,
                  fromAddress="sdm@localhost",
                  attachments=[],
                  replyTo=None,
                  key=None,
                  cc=[],
                  bcc=[],
                  mime='plain'):

        return self.insert(
            'email',
            {
                "from": fromAddress,
                "to": to,
                "subject": subject,
                "content": content,
                "cc": cc,
                "bcc": bcc,
                "attachments": attachments,
                "mime": mime,
                "email_status": "pending",
                "key": key
            })

    def get_analyses_dir(self, jat_key):
        # hpss has a 65k file limit in any dir,
        # so we are going to create the analysis dirs with a max of 10k entries
        id = int(jat_key.split('-')[1])
        if id >= self.jat_key_dir_switch:
            return 'analyses-%d' % (id // 10000)
        else:
            return 'analyses'

    @restful.onload
    def onstartup(self):
        self.repo_location = self.location.replace('/ui', '')
        self.loadAllCvs()
        self.loadAllMacros()
        self.loadAllTemplates()
        self.loadAllTagTemplates()

    def post_reloadAll(self, args, kwargs):
        self.onstartup()

    def getAliasOn(self):
        return int(self.getSetting('alias', 1000))

    def getNextAlias(self):
        alias = None
        with self.alias_lock:
            if self.alias_on is None:
                self.alias_on = self.getAliasOn()
            self.alias_on += 1
            alias = self.alias_on
            self.saveSetting('alias', alias)
        return alias

    @restful.doc('Creates a new analysis in jat')
    @restful.passreq
    @restful.validate({'location': {'type': str, 'required': False}, 'description': {'type': str, 'required': False}, 'summary': {'type': str, 'required': False}})
    def post_analysis(self, args, kwargs):
        user = kwargs['__auth']['user']
        kwargs.update(kwargs['__auth'])
        kwargs['status'] = 'Started'
        if len(self.query('analysis', **{'user': user, 'key': kwargs['key']})) > 0:
            raise common.HtmlException(400, 'Sorry you have already created an analysis with the key: %s' % kwargs['key'])
        if 'location' not in kwargs:
            kwargs['location'] = os.path.join(self.config.scratch, kwargs['name'])
            os.makedirs(kwargs['location'])
            subprocess.check_output('setfacl -m user:%s:rwx %s' % (kwargs['os_user'], kwargs['location']), shell=True)

        del kwargs['__auth']
        kwargs['added_date'] = kwargs['modified_date'] = datetime.datetime.now()
        return {'analysis_id': self.save('analysis', kwargs), 'location': kwargs['location']}

    @restful.menu('Analyses')
    @restful.table('Analyses', map={'key': {'order': 0, 'type': 'html', 'value': '<a href="/analysis/analysis/{{value}}">{{value}}</a>'}, 'location': {'order': 1}, 'status': {'order': 2}, 'summary': {'order': 3}}, onlyshowmap=True)
    @restful.passreq
    def get_myanalyses(self, args, kwargs):
        kwargs['user'] = kwargs['__auth']['user']
        del kwargs['__auth']
        return self.query('analysis', **kwargs)

    @restful.table('Analyses', map={'key': {'order': 0, 'type': 'html', 'value': '<a href="/analysis/analysis/{{value}}">{{value}}</a>'}, 'location': {'order': 1}, 'status': {'order': 2}, 'summary': {'order': 3}}, onlyshowmap=True)
    def get_analyses(self, args, kwargs):
        return self.query('analysis', **kwargs)

    def getSimpleDateString(row, args):
        if 'added_date' not in row:
            return None
        date = row['added_date']
        return date.strftime("%m/%d/%Y")
        '''
        now = datetime.datetime.now()
        if date+ datetime.timedelta(hours=11)>now:
            return date.strftime("%I:%M %p").lower()
        if date + datetime.timedelta(days=365) > now:
            return date.strftime("%b %w")
        return date.strftime("%d/%m/%Y")
        '''

    def tostrlist(row, args):
        return ','.join(map(str, args)) if isinstance(args, list) else str(args)

    report_map = {'key': {'order': 0, 'title': 'JAT Key', 'type': 'html', 'value': '<a href="/analysis/analysis/{{value}}">{{value}}</a>'},
                  'metadata.sequencing_project_id': {'order': 1, 'title': 'SPID', 'value': tostrlist},
                  'metadata.sequencing_project.sequencing_project_name': {'order': 2, 'title': 'Sequencing Project Name'},
                  'metadata.library_name': {'order': 3, 'title': 'Library', 'value': tostrlist},
                  'template': {'order': 4, 'title': 'Type'},
                  'user': {'order': 5, 'title': 'Analyst'},
                  'group': {'order': 6, 'title': 'Group'},
                  'publish': {'order': 7, 'value': _get_publish, 'title': 'Publish'},
                  'status': {'order': 8, 'title': 'Status'},
                  'quarter': {'order': 9, 'value': _get_quarter, 'title': 'FY/Q'},
                  'added_date': {'order': 10, 'type': 'date', 'title': 'Release Date', 'value': getSimpleDateString}}

    @restful.pagetable('Analyses', 'analysis', map=report_map, sort=('_id', -1), allow_empty_query=True, return_count=50)
    @restful.menu('analysis report')
    def get_ranalyses(self, args, kwargs):
        pass

    @restful.passreq(True)
    def post_deletereviewanalysis(self, args, kwargs):
        self.remove('analysis', {'options.reviewer': kwargs['user'] + '@lbl.gov', 'key': kwargs['key']})

#   @restful.permissions('admin')
#   @restful.validate(argsValidator=[{'name': 'key', 'type': str, 'doc': 'The jat key to remove'}])
#   def delete_analysis(self, args, kwargs):
#       analysis = self.get_analysis(args, kwargs)
#       for output in analysis['outputs']:
#           self.run_internal('metadata', 'delete_file', output)  #args[0])
#       self.remove('analysis', {'key': args[0]})

    @restful.passreq(True)
    def post_releaseanalysis(self, _args, kwargs):
        analysis = self.findOne('analysis', key=kwargs.get('key'))
        ignore_files = []
        for output in kwargs.get('outputs'):
            file_locations = {}
            file_format = output.get('metadata', {}).get('file_format', 'None')
            if output.get('label') in file_locations:
                file_locations.get(output.get('label')).append({'location': output.get('file'), 'format': file_format})
            else:
                file_locations[output.get('label')] = [{'location': output.get('file'), 'format': file_format}]
        skip_folder = analysis.get('options').get('skip_folder', False)
        template = self.get_template([analysis.get('template')], None)
        if analysis is not None and analysis.get('options').get('reviewer') == kwargs.get('user') + '@lbl.gov':
            analysis['status'] = 'Released'
            analysis['user'] = kwargs.get('user')
            analysis['group'] = kwargs.get('group')
            del analysis['options']['reviewer']
            analyses_dir = self.get_analyses_dir(analysis.get('key'))
            for output in analysis.get('outputs'):
                o_metadata = {}
                if 'metadata' in output:
                    o_metadata.update(output.get('metadata'))
                    o_metadata.update(analysis.get('metadata'))
                    if 'portal' in o_metadata and 'display_location' in o_metadata.get('portal'):
                        new_path = []
                        for pa in o_metadata.get('portal').get('display_location'):
                            new_path.append(eval_string(pa, {'metadata': o_metadata}))
                        o_metadata['portal']['display_location'] = new_path
                else:
                    o_metadata = analysis.get('metadata')
                o_metadata['jat_key'] = analysis.get('key')
                o_metadata['jat_label'] = output.get('label')
                metadata_id = restful.run_internal('metadata', 'post_file', metadata=o_metadata,
                                                   destination=f'{analyses_dir}/{analysis.get("key")}/',
                                                   file=output.get('file'), file_type=output.get('tags'),
                                                   __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                           'division': kwargs.get('division')},
                                                   inputs=analysis.get('inputs'))
                metadata_id = metadata_id.get('metadata_id')
                output['metadata_id'] = metadata_id
                if output.get('file').startswith(analysis.get('location')):
                    output['file'] = output.get('file')[len(analysis.get('location')) + 1:]
                ignore_files.append(output.get('file'))
            if not skip_folder:
                metadata_id = restful.run_internal('metadata', 'post_folder', metadata=analysis.get('metadata'),
                                                   ignore=ignore_files, folder=analysis.get('location'),
                                                   file_type='analysis',
                                                   __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                           'division': kwargs.get('division')})
                metadata_id = metadata_id.get('metadata_id')
                analysis['metadata_id'] = metadata_id
            analysis['added_date'] = analysis['modified_date'] = datetime.datetime.now()
            del analysis['metadata']['jat_key']
            if template is not None and 'email' in template and analysis.get('options').get('send_email', False):
                unlink_temp = []
                content = {"strings": '', "files": []}
                email = template.get('email')
                if 'email' in analysis.get('options'):
                    email.update(analysis.get('options').get('email'))
                    del analysis['options']['email']
                for line in email.get('content'):
                    if isinstance(line, str):
                        content["strings"] += f'\n{eval_string(line, analysis)}'
                    elif isinstance(line, dict) and 'file' in line and line.get('file') in file_locations:
                        files = file_locations.get(line.get('file'))
                        # htandra: NOTE- only file_format = 'text' or 'txt' can be added to email content
                        for myfile in files:
                            if myfile.get('format').lower() in ['txt', 'text']:
                                content.get('files').append(
                                    self._get_metadata_id_for_file(
                                        outputs=kwargs.get('outputs'),
                                        path=myfile.get('location'),
                                        base=kwargs.get('location')
                                    ))
                    elif isinstance(line, dict) and 'string' in line:
                        content["strings"] += f'\n{eval_string(line.get("string"), analysis)}'
                attachments = []
                if 'attachments' in email:
                    for name in email.get('attachments'):
                        if name in file_locations:
                            for myfile in file_locations.get(name):
                                attachments.append(
                                    self._get_metadata_id_for_file(
                                        outputs=kwargs.get('outputs'),
                                        path=myfile.get('location'),
                                        base=kwargs.get('location')
                                    ))
                reply_to = None
                cc = email.get('cc', [])
                bcc = email.get('bcc', [])
                if 'reply_to' in email:
                    reply_to = eval_string(email.get('reply_to'), analysis)
                response = self.sendEmail(
                    to=email.get('to'),
                    subject=eval_string(email.get('subject'), kwargs),
                    content=content,
                    attachments=attachments,
                    fromAddress=self.config.from_address,
                    replyTo=reply_to,
                    key=kwargs.get('key'),
                    cc=cc,
                    bcc=bcc
                )
                self.logger.info(
                    f'{response}; From: {self.config.from_address}; To: {email.get("to")}; Subject: {eval_string(email.get("subject"), kwargs)}')
                for new_file in unlink_temp:
                    os.unlink(new_file)
            self.save('analysis', analysis)

    @restful.ui_link(restful.Button('Remove Analysis', post_deletereviewanalysis, 'key', XXredirect_internalXX='/analysis/ranalyses'))
    @restful.ui_link(restful.Button('Release Analysis', post_releaseanalysis, 'key', XXredirect_internalXX='/analysis/ranalyses'))
    @restful.passreq(True)
    @restful.generatedhtml(title="Analysis Review")
    def get_review(self, args, kwargs):
        analysis = self.findOne('analysis', key=args[0])
        if analysis is None:
            raise common.HttpException(404, 'Sorry did not find analysis for key: %s' % args[0])
        if (kwargs['user'] + '@lbl.gov') != analysis['options']['reviewer']:
            raise common.HttpException(403, 'Sorry you can not review this analysis')
        return analysis

    @restful.raw
    @restful.passreq
    def get_download(self, args, kwargs):
        key = args[0] if len(args) > 0 else kwargs['key']
        ret = self.query('analysis', **{'key': key})
        if len(ret) > 0:
            ret = ret[0]
        else:
            raise common.HttpException(404, 'Analysis: %s was not found' % key)

        ids = list()
        for x in ret['outputs']:
            ids.append(x['metadata_id'])
        files = [os.path.join(x['file_path'], x['file_name']) for x in restful.run_internal('metadata', 'get_files', ids=ids)]

        self.logger.debug(f"Making zip file to serve files from {key} for download")
        self.logger.debug(f"Serving files {files}")
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a zip file of the directory
            zip_file_path = os.path.join(temp_dir, f"{key}.zip")  # Full path for the zip file
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for _id, file_path in zip(ids, files):
                    # Swap root path for mount path if repository is mounted into container
                    if not os.path.exists(file_path):
                        mount = self.config.dm_archive_mount_by_division[ret['division']]
                        root = self.config.dm_archive_root_by_division[ret['division']]
                        file_path = file_path.replace(root, mount)
                    zip_loc = os.path.join(key, os.path.basename(file_path))
                    self.logger.debug(f"Copying {file_path} to {zip_loc} in {zip_file_path}")
                    zipf.write(file_path, zip_loc)

            return cherrypy.lib.static.serve_download(zip_file_path, name=os.path.basename(zip_file_path))


    @restful.validate(argsValidator=[{'name': 'key', 'type': str, 'doc': 'The jat key to retrieve'}])
    @restful.doc('Gets all the analysis info for a given analysis key')
    @restful.ui_link(restful.Button('Download files', get_download, 'key'))
    @restful.generatedhtml(title='Analysis', map={'outputs': {'metadata_id': {'order': 0, 'type': 'html', 'value': '<a href="/metadata/file/{{value}}">{{value}}</a>'}, 'label': {'order': 1}, 'description': {'order': 3}, 'file': {'order': 2}}})
    def get_analysis(self, args, kwargs):
        ret = self.query('analysis', **{'key': args[0]})
        if len(ret) > 0:
            ret = ret[0]
        else:
            raise common.HttpException(404, 'Analysis: %s was not found' % args[0])
        if len(args) > 1 and args[1] == 'files':
            label_map = {}
            ids = []
            for x in ret['outputs']:
                label_map[x['metadata_id']] = x.get('label', None)
                ids.append(x['metadata_id'])
            files = restful.run_internal('metadata', 'get_files', ids=ids)
            return [{'file_name': x['file_name'], 'file_path': x['file_path'], 'file_status': x['file_status'], 'file_type': x['file_type'], 'metadata_id': x['_id'], 'label': label_map[str(x['_id'])]} for x in files]
        return ret

    @restful.permissions('modify_perms')
    def put_metadata2(self, args, kwargs):
        metadata = {}
        for key, value in kwargs.items():
            metadata['metadata.%s' % key] = value
        self.smartUpdate('analysis', {'key': args[0]}, {'$set': metadata})

    @restful.passreq
    def put_analysis(self, args, kwargs):
        user = kwargs['__auth']['user']
        del kwargs['__auth']
        self.smartUpdate('analysis', {'_id': ObjectId(args[0]), 'user': user}, {'$set': kwargs})

    templateValidator = {
        'name': {'type': str},
        'md5': {'type': str, 'required': False},
        'description': {'type': str},
        'tags': {'type': list, 'validator': {'*': {'type': str}}},
        # 'outputs': {'type': list, 'validator': {'*: 1': {'type': dict, 'validator': { # could add *:1 to require that a template always has an output. Note, this gets parsed in restful.validate
        'outputs': {'type': list, 'validator': {'*': {'type': dict, 'validator': {
            'label': {'type': str, 'doc': 'The file label used soley to map submission files'},
            'tags': {'type': list, 'validator': {'*': {'type': str}}, 'doc': 'The file tags that describe this file'},
            'required': {'type': bool, 'default': True},
            'metadata': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}},
            'description': {'type': str},
            'file_name': {'type': str, 'required': False},
            'default_metadata_values': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}},
            'required_metadata_keys': {'type': list, 'required': False, 'validator': {'*': {'type': dict, 'validator': {
                'description': {'type': str, 'required': False},
                'macro': {'type': str, 'required': False},
                'type': {'type': str, 'required': False},
                'key': {'type': str, 'required': False},
                'required': {'type': bool, 'default': True}}}}}}}}},
        'default_metadata_values': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}},
        'required_metadata_keys': {'type': list, 'required': False, 'validator': {'*': {'type': dict, 'validator': {
            'description': {'type': str, 'required': False},
            'type': {'type': str, 'required': False},
            'macro': {'type': str, 'required': False},
            'key': {'type': str, 'required': False},
            'required': {'type': bool, 'default': True}}}}},
        'email': {'type': dict, 'required': False, 'validator': {
            'attachments': {'type': list, 'required': False, 'validator': {'*': {'type': str}}},
            'content': {'type': list, 'validator': {'*': {'type': dict, 'validator': {'file': {'type': str, 'required': False}, 'string': {'type': str, 'required': False}}, 'doc': 'The content of this email, can pass in a file or just string'}}},
            'reply_to': {'type': str, 'required': False, 'doc': 'If you want to have the response send to an other email address pass this in'},
            'subject': {'type': str, 'doc': 'The subject of the email address. You can pass in dynamic values from the submission data by using {metadata.key}'},
            'mime': {'type': str, 'doc': 'The mime type of the email content, if html just pass as html', 'required': False},
            'to': {'type': (str, list), 'doc': 'The email address(es) that this email will send to'},
            'bcc': {'type': (str, list), 'doc': 'The email address(es) that this email will send to', 'required': False},
            'cc': {'type': (str, list), 'doc': 'The email address(es) that this email will send to', 'required': False},
        }}}

    @restful.doc('creates a new tag template')
    @restful.validate(tagTemplateValidator, allowExtra=False)
    @restful.passreq(join=True)
    def post_tagtemplate(self, args, kwargs):
        results = self.query('analysis_tag_template', name=kwargs['name'])
        if len(results) > 0:
            template = results[0]
            if template['user'] != kwargs['user'] and template['group'] != kwargs['group']:
                raise common.HttpException(401,
                                           f'Sorry you can not edit a template that you do not own or a member of the group, this template is owned by user {template.get("user")}, group {template.get("group")}')
            kwargs['_id'] = ObjectId(template['_id'])
            if 'md5' in kwargs and 'md5' in template:
                if kwargs['md5'] == template['md5']:
                    return
        kwargs['last_modified'] = datetime.datetime.now()
        return self.save('analysis_tag_template', kwargs)

    @restful.doc('creates a new template')
    @restful.validate(templateValidator, allowExtra=False)
    @restful.passreq(join=True)
    def post_template(self, args, kwargs):
        results = self.query('analysis_template', name=kwargs['name'])
        if len(results) > 0:
            template = results[0]
            if template['user'] != kwargs['user'] and template['group'] != kwargs['group']:
                raise common.HttpException(401,
                                           f'Sorry you can not edit a template that you do not own or a member of the group, this template is owned by user {template.get("user")}, group {template.get("group")}')
            errors = self.post_validatetemplate(None, kwargs)
            if len(errors) > 0:
                raise common.ValidationError('\n'.join(errors))
            kwargs['_id'] = ObjectId(template['_id'])
            if 'md5' in kwargs and 'md5' in template:
                # if templates has not changed, don't do anything
                if kwargs['md5'] == template['md5']:
                    return
            orig_outputs = set(o['label'] for o in template['outputs'])
            new_outputs = set(o['label'] for o in kwargs['outputs'])
            intersection = new_outputs & orig_outputs
            if len(intersection) < len(new_outputs):
                self.__notify_template_change__(kwargs)
            if len(intersection) < len(orig_outputs):
                self.__update_distribution_properties__(kwargs)
        else:
            # this is a new template
            self.__notify_template_change__(kwargs)
        kwargs['last_modified'] = datetime.datetime.now()
        return self.save('analysis_template', kwargs)

    def __update_distribution_properties__(self, template):
        name = template['name']
        new_outputs = set([output['label'] for output in template['outputs']])
        flags = self.query('analysis_publishingflags', template=name)
        if len(flags) > 0:
            # if publishingflags exist for this template,
            # replace the outputs that currently exist
            flags = flags[0]['outputs']
            to_be_deleted = []
            for output in flags.keys():
                if output not in new_outputs:
                    # Cannot modify dict during iteration...
                    # del flags[output]
                    to_be_deleted.append(output)
            for output in to_be_deleted:
                del flags[output]
            self.save('analysis_publishingflags', flags)

        plocs = self.query('analysis_plocations', template=name)
        if len(plocs) > 0:
            # if portallocations exist for this template,
            # replace the outputs that currently exist
            plocs = plocs[0]['outputs']
            to_be_deleted = []
            for output in plocs.keys():
                if output not in new_outputs:
                    # Cannot modify dict during iteration...
                    # del plocs[output]
                    to_be_deleted.append(output)
            for output in to_be_deleted:
                del plocs[output]
            self.save('analysis_publishingflags', plocs)

    def __notify_template_change__(self, kwargs):
        subject = 'Template Change: %s' % kwargs['name']
        replyTo = None
        if 'email' in kwargs:
            if 'reply_to' in kwargs['email']:
                replyTo = eval_string(kwargs['email']['reply_to'], kwargs)
        fromAddress = self.config.from_address
        # This is the to address for pmo
        to = self.config.to_address
        content = "The %s template has been changed in %s.\n\nGo to %s/%s/distributionproperties/%s to see changes" % (kwargs['name'], self.config.instance_type, self.config.url, self.appname, kwargs['name'])
        if self.config.instance_type == 'prod':
            content = "The %s template has been changed in production.\n\nGo to %s/%s/distributionproperties/%s to update Portal Display Location" % (kwargs['name'], self.config.url, self.appname, kwargs['name'])
        elif self.config.instance_type == 'dev':
            content = "The %s template has been changed in dev.\n\nGo to %s/%s/distributionproperties/%s to see changes" % (kwargs['name'], self.config.url, self.appname, kwargs['name'])
        sdmlogger.sendEmail(to, subject, content, fromAddress=fromAddress, replyTo=replyTo)
        self.logger.info("Email sent; From: %s; To: %s; Subject: %s" % (fromAddress, to, subject))

    @restful.doc('validates a template, will return a list of all the errors if there are any')
    @restful.validate(templateValidator, allowExtra=False)
    def post_validatetemplate(self, args, kwargs):
        errors = []
        for tag in kwargs['tags']:
            if 'tags' in self.cv and tag not in self.cv['tags']:
                errors.append('''Analysis level tag: '%s' is not valid''' % tag)
        for output in kwargs['outputs']:
            for tag in output['tags']:
                if 'outputs.tags' in self.cv and tag not in self.cv['outputs.tags']:
                    errors.append('''Output level tag: '%s' on label: '%s' is not valid''' % (tag, output['label']))
        return errors

    @restful.queryResults({'title': 'Analysis templates',
                           'table': {'columns': [['name', {'type': 'link',
                                                           'inputs': {'title': 'Template {{name}} Details',
                                                                      'url': '/analysis/template/{{name}}'}}],
                                                 ['description', {}],
                                                 ['edit distribution properties', {'type': 'link',
                                                                                   'inputs': {'text': '{{name}}',
                                                                                              'title': 'Edit Distribution Properties',
                                                                                              'url': '/analysis/distributionproperties/{{name}}'}}],
                                                 ['last_modified', {}]]},
                           'data': {'default_query': ''}})
    @restful.menu('templates')
    def get_templates(self, args, kwargs):
        if kwargs and kwargs.get('queryResults', None):
            return self.queryResults_dataChange(kwargs, 'analysis_template')
        else:
            return self.query('analysis_template', **kwargs)

    @restful.doc('search email collection based on the values passed in')
    @restful.validate({'*': {'type': '*'}})
    def get_emails(self, args, kwargs):
        return self.query('email', **kwargs)

    @restful.doc('update email collection based on the values passed in')
    @restful.validate({'*': {'type': '*'}})
    def post_emailstatus(self, args, kwargs):
        return self.update('email', kwargs['what'], {'$set': kwargs['data']})

    @restful.validate(argsValidator=[{'name': 'key', 'type': str}])
    @restful.doc('Gets all the analysis info for a given analysis key')
    @restful.generatedhtml(title='Template: {{name}}')
    @restful.passreq(True)
    def get_template(self, args, kwargs):
        ret = self.query('analysis_template', **{'name': args[0]})
        if len(ret) == 0:
            template = '/'.join(args)
            if template.count('/') <= 0:
                raise common.HttpException(404, 'template %s was not found' % args[0])
            branch = '/'.join(template.split('/')[:-1])
            template = template.split('/')[-1]
            ret = yaml.load(self.bb_curl.get('berkeleylab/jgi-jat/src/%s/templates/%s.yaml' % (branch, template), output='raw'), Loader=yaml.SafeLoader)
            return ret
        ret = ret[0]
        del ret['user']
        del ret['group']
        del ret['_id']
        return ret

    @restful.doc('Returns information about where a key is used in the templates')
    @restful.generatedhtml(title='Key: {{key}}')
    @restful.passreq(True)
    def get_keylocations(self, args, kwargs):
        macro = self.query('analysis_macros', **{"required_metadata_keys.key": args[0]})
        defined_in = "template"
        if macro:
            defined_in = "macro"
            if len(macro) > 1:
                value = {"$in": [x['name'] for x in macro]}
            else:
                value = macro[0]['name']
            key = "required_metadata_keys.macro"
        else:
            value = args[0]
            key = "required_metadata_keys.key"

        templates = self.query('analysis_template', **{key: value})
        location = "analysis"
        if not templates:
            location = "output"
            key = "outputs.%s" % key
            templates = self.query('analysis_template', **{key: value})
        if len(templates) == 0:
            defined_in = None
            location = None

        return {"key": args[0], "location": location, "defined_in": defined_in, "templates": [x['name'] for x in templates]}

    @restful.validate(argsValidator=[{'name': 'key', 'type': str}])
    @restful.doc('Gets all the analysis info for a given analysis key')
    @restful.passreq(True)
    def get_resolvedtemplate(self, args, kwargs):
        def getmacro(macro_name):
            return self.macros[macro_name]

        def resolveMetadata(metadata):
            ret = []
            for met in metadata:
                if isinstance(met, str):
                    ret.extend(getmacro(met)['required_metadata_keys'])
                else:
                    if 'macro' in met:
                        ret.extend(getmacro(met['macro'])['required_metadata_keys'])
                    else:
                        ret.append(met)
            return ret

        macros = self.macros
        on_dev = self.config.instance_type != 'prod'
        ret = None
        publishingflags = {}

        # TODO: code must have passed this as an array of elements for branch/template invocations.  Seems to have changed
        if len(args) == 1 and '/' in args[0]:
            args = args[0].split('/')

        if len(args) > 1 and on_dev:
            macros = {}
            branch = '/'.join(args[:-1])
            template = args[-1]
            ret = yaml.load(self.bb_curl.get('berkeleylab/jgi-jat/src/%s/templates/%s.yaml' % (branch, template), output='raw'), Loader=yaml.SafeLoader)

            def gm(macro):
                if macro in macros:
                    return macros[macro]
                macro_o = yaml.load(self.bb_curl.get('berkeleylab/jgi-jat/src/%s/macros/%s.yaml' % (branch, macro), output='raw'), Loader=yaml.SafeLoader)
                macros[macro] = macro_o
                return macro_o

            getmacro = gm  # noqa: F811
        else:
            ret = self.query('analysis_template', **{'name': args[0]})
            if len(ret) == 0:
                raise common.HttpException(404, 'sorry failed to find template %s' % args[0])
            ret = ret[0]
            # portals = self.get_portallocations(args, None)
            publishingflags = self.get_publishingflags(args, None)
        if 'required_metadata_keys' in ret:
            ret['required_metadata_keys'] = resolveMetadata(ret['required_metadata_keys'])
        for output in ret['outputs']:
            if 'required_metadata_keys' in output:
                output['required_metadata_keys'] = resolveMetadata(output['required_metadata_keys'])
            label = output['label']
            # if label in portals and portals[label] is not None and portals[label] != '':
            #    portal_loc = [x for x in portals[label].split('/') if len(x) > 0]
            #    if 'default_metadata_values' not in output:
            #        output['default_metadata_values'] = {}
            #    output['default_metadata_values']['portal'] = {'display_location': portal_loc}
            # elif 'default_metadata_values' in output and 'portal' in output['default_metadata_values']:
            #    # if no portal location is set, remove it from default_metadata_values if it exists
            #    del output['default_metadata_values']['portal']
            # This should be removed at some point too
            if label in publishingflags and publishingflags[label] is not None and len(publishingflags[label]) > 0:
                if 'default_metadata_values' not in output:
                    output['default_metadata_values'] = {}
                output['default_metadata_values']['publish_to'] = publishingflags[label]
            elif 'default_metadata_values' in output and 'publish_to' in output['default_metadata_values']:
                # if publish_to is set, remove it from default_metadata_values if it exists
                del output['default_metadata_values']['publish_to']
        return ret

    @restful.passreq
    @restful.validate({'file': {'type': str}, 'file_type': {'type': (str, list)}, 'metadata': {'type': dict, 'required': False}})
    def post_output(self, args, kwargs):
        user = kwargs['__auth']['user']
        del kwargs['__auth']
        self.smartUpdate('analysis', {'_id': ObjectId(args[0]), 'user': user}, {'$addToSet': {'outputs': kwargs}})

    @restful.doc('Updates the release_to for a given analysis key')
    @restful.validate({'key': {'type': str, 'example': 'AUTO-1234'},
                       'set': {'type': list, 'required': False, 'example': ['img', 'mycocosm'], 'doc': 'Pass in values to add to the existing release_to list.  Passing in ["img"] to an existing release_to that contains ["genbank", "portal"] will result in a release_to of ["genbank", "portal", "img"].  Passing in an empty list ("set":[]) will have no effect.'},
                       'unset': {'type': list, 'required': False, 'example': ['img'], 'doc': 'Pass in values to remove from the existing release_to list'}})
    @restful.passreq(True, True)
    @restful.permissions('import_jat')
    def post_releaseto(self, args, kwargs):
        key = kwargs.get('key', None)
        prevAnalysis = self.query('analysis', **{'key': key})[0]
        if not kwargs['group'] == prevAnalysis['group']:
            if 'admin' not in kwargs['permissions']:
                raise common.HttpException(401, 'Sorry you can not update an analysis that is not in your group, this analysis is owned by group ' + prevAnalysis['group'])
        if 'options' in prevAnalysis and 'release_to' in prevAnalysis['options']:
            newReleaseTo = prevAnalysis['options']['release_to']
        else:
            newReleaseTo = []
        set = kwargs.get('set', [])
        unset = kwargs.get('unset', [])
        global publishing_flags
        publishingflags = [x.lower() for x in publishing_flags]
        for to in set:
            if to not in newReleaseTo:
                if to in publishingflags:
                    newReleaseTo.append(to)
                else:
                    raise common.HttpException(401, "Error. Unknown release_to value '" + to + "' passed.")
        for to in unset:
            if to in newReleaseTo:
                newReleaseTo.remove(to)
            elif to not in publishingflags:
                raise common.HttpException(401, "Error. Unknown release_to value '" + to + "' passed.")
        if not newReleaseTo:
            if 'options' in prevAnalysis:
                if 'release_to' in prevAnalysis['options']:
                    del prevAnalysis['options']['release_to']
        else:
            if 'options' not in prevAnalysis:
                prevAnalysis['options'] = {}
            prevAnalysis['options']['release_to'] = newReleaseTo
        self.smartSave('analysis', prevAnalysis)
        return {"Status": "OK. Successfully set the release_to flags."}

    @restful.passreq(True, True)
    def put_import(self, args, kwargs):
        keep_defined = []
        user = kwargs.get('user')
        error_key = False
        prev_analysis = self.query('analysis', **{'key': args[0]})
        if len(prev_analysis) == 0:
            raise common.HttpException(404, f'Sorry no document exists with this key: {args[0]}')
        else:
            prev_analysis = prev_analysis[0]
        if prev_analysis.get('user') != kwargs.get('user') and prev_analysis.get('group') != kwargs.get('group'):
            if 'admin' not in kwargs.get('permissions'):
                raise common.HttpException(401,
                                           f'Sorry you can not update an analysis you do not own or a member of the group, this analysis is owned by user {prev_analysis.get("user")}, group {prev_analysis.get("group")}')
        for k in ("metadata_id", "template"):
            if k in kwargs and k in prev_analysis and kwargs.get(k) != prev_analysis.get(k):
                raise common.HttpException(401, 'Sorry you cannot update key = ' + k)
        if 'publish' in kwargs:
            publish = kwargs.get('publish')
            del kwargs['publish']
        else:
            publish = True
        prev_analysis['publish'] = publish
        template = self.get_resolvedtemplate([prev_analysis.get('template')], {})
        # look for invalid keys in the global metadata
        known_keys = set([key.get('key') for key in template.get('required_metadata_keys', [])])
        new_metadata = prev_analysis.get('metadata')
        if 'metadata' in kwargs:
            warnings = check_keys(known_keys, kwargs.get('metadata'))
            error_key = len(warnings) > 0
            new_metadata.update(kwargs.get('metadata'))
        new_inputs = kwargs.get('inputs', [])
        new_input_ids = []
        for input in new_inputs:
            if '/' in input:
                if self.config.instance_type == 'dev':
                    rec = self.prod_curl.get('api/tape/latestfile', file=input)
                else:
                    rec = restful.run_internal('tape', 'get_latestfile', file=input)
                if rec is not None:
                    new_input_ids.append(rec.get('metadata_id'))
                else:
                    raise common.HttpException(401, f'Sorry cannot update - Input file = {input}, is not in jamo')
            else:
                rec = restful.run_internal('metadata', 'get_file', input)
                if rec is not None:
                    new_input_ids.append(input)
                else:
                    raise common.HttpException(401,
                                               f'Sorry cannot update - Input file with id = {input}, is not in jamo')
        is_new_inputs = False
        if prev_analysis.get('inputs') != new_input_ids:
            prev_analysis['inputs'] = new_input_ids
            is_new_inputs = True
        new_options = {}
        for o in ("send_email", "release_to", "email"):
            if o in kwargs:
                new_options[o] = kwargs.get(o)
        prev_analysis.get('options').update(new_options)
        output_name_map = {}
        for output in prev_analysis.get('outputs'):
            output_name_map[os.path.basename(output.get('file'))] = output
        template_outputs = {}
        for output in template.get('outputs'):
            template_outputs[output.get('label')] = output

        folders = restful.run_internal('metadata', 'post_query', **{'file_type': 'folder', 'metadata.jat_key': args[0]})
        # AJTRITT - I think this should only evaluate to True when an update is happening
        if len(folders) > 0:
            restful.run_internal('metadata', 'put_filemetadata', metadata=new_metadata, id=str(folders[0].get('_id')),
                                 __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                         'permissions': kwargs.get('permissions')})

        for output in kwargs.get('outputs'):
            if prev_analysis.get('location') is not None and output.get('file').startswith(
                    prev_analysis.get('location') + '/'):
                output['file'] = output.get('file')[len(prev_analysis.get('location')) + 1:]
            if os.path.basename(output.get('file')) in output_name_map:
                basename = os.path.basename(output.get('file'))
                p_output = output_name_map.get(basename)
                t_file_metadata = {}
                t_file_metadata.update(new_metadata)
                # if metadata doesn't exist add it
                if 'metadata' not in output:
                    output['metadata'] = {}
                # copy the keys down from global if they aren't local
                if 'metadata' in kwargs:
                    for key in kwargs.get('metadata'):
                        if key not in output.get('metadata'):
                            output['metadata'][key] = kwargs.get('metadata').get(key)
                # if there is anything in metadata...
                if len(output.get('metadata')):
                    known_keys_file = set([key.get('key') for key in
                                           template_outputs[p_output.get('label')].get('required_metadata_keys', [])])
                    file_warnings = check_keys(known_keys, output.get('metadata'), known_keys_file, output.get('file'))
                    warnings.extend(file_warnings)
                    if len(file_warnings) > 0:
                        error_key = True
                    p_output.get('metadata').update(output.get('metadata'))
                    t_file_metadata.update(p_output.get('metadata'))
                    if 'portal' in output.get('metadata') and 'display_location' in output.get('metadata').get(
                            'portal'):
                        # User has requested a custom portal.  We'll need to let update_publish to keep it
                        keep_defined.append(p_output.get('metadata_id'))
                # adding this in here as the key check above would complain
                t_file_metadata['jat_publish_flag'] = publish
                # make the call to jamo to update metadata
                p_id = p_output.get('metadata_id')
                restful.run_internal('metadata', 'put_filemetadata', metadata=t_file_metadata, id=p_id,
                                     __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                             'permissions': kwargs.get('permissions')})
                # if there are values in metadata, do a register update
                if len(output.get('metadata')):
                    restful.run_internal('metadata', 'post_registerupdate',
                                         where={'_id': p_id, '_read_preference': pymongo.ReadPreference.PRIMARY},
                                         keep=list(output.get('metadata')))
                if is_new_inputs:
                    restful.run_internal('metadata', 'put_file', data={'inputs': prev_analysis.get('inputs')}, id=p_id,
                                         __auth={'user': kwargs.get('user'), 'group': kwargs.get('group')})
            else:
                warnings.append('warning: file ' + output.get(
                    'file') + ' not found in existing analysis, file will be ignored.  Perhaps you intended to use "jat addfiles" instead.')
        # update the publishing flags
        self.update_publish(args[0], publish, keep_defined=keep_defined, user=user)
        self.smartSave('analysis', prev_analysis)
        if error_key:
            warnings.append(
                'warning: You have Metadata keys that are not defined in the template.  Processing will continue for now.  In a future version of jat, your update will be aborted.')
        return {'warnings': warnings} if warnings else {}

    @restful.doc('Updates an existing analysis.  For previously imported files, only the metadata will be updated.  New files will be added.  template_data should contain both a metadata and an outputs section.  You can supply a full metadata.json document with the updated metadata.  Values are merged with existing metadata.  '
                 'The JAT record will be updated along with all the files listed in outputs.  If there are any files missing from outputs that are part of the analysis, these will not be updated.')
    @restful.validate({'jat_key': {'type': str, 'required': True, 'doc': 'The JAT key of the analysis to update', 'example': 'AUTO-12345'},
                       'template_data': {'type': dict, 'required': True, 'doc': "The import's metadata.json data", 'example': {"outputs": [{"file": "refseq.data", "label": "reference", "metadata": {"reference_key": "AUTO-1024"}}], "metadata": {"ncbi_taxon_id": 47425, "analysis_project_id": 1186191}}}},
                      allowExtra=False)
    @restful.permissions('analysis_update')
    @restful.passreq(True, True)
    def post_analysisupdate(self, args, kwargs):
        errors = []
        # TODO: This should never happen due to the validator
        if 'jat_key' not in kwargs:
            kwargs['jat_key'] = None
        if isinstance(kwargs['jat_key'], str):
            kwargs['jat_key'] = kwargs['jat_key'].strip()
        if not isinstance(kwargs['jat_key'], str) or len(kwargs['jat_key']) == 0:
            errors.append('missing or invalid jat_key')

        # TODO: This should never happen due to the validator
        if 'template_data' not in kwargs or not isinstance(kwargs['template_data'], dict):
            errors.append('missing or invalid template_data')

        if len(errors) > 0:
            raise common.HttpException(400, errors)

        for k in ('user', 'group', 'permissions'):
            kwargs['template_data'][k] = kwargs[k]
        return self.put_import([kwargs['jat_key']], kwargs['template_data'])

    @restful.doc('Returns the distinct list of non-null values of key.  Note that this is an expensive operation and should be used sparingly.  Also the result-set is limited to 16MB')
    @restful.validate(argsValidator=[{'name': 'metadata_key', 'type': str, 'doc': 'The metadata key to search for'}], allowExtra=False)
    def get_distinct(self, args, kwargs):
        self.logger.info("Calling distinct on %s" % (args[0]))
        return self.db['analysis'].distinct(args[0], filter={args[0]: {"$ne": None}})

    # Update publish flag, publish_to, display_location, and __update_publish_to
    def update_publish(self, key, publish, keep_defined=None, user=None):
        _page = 1
        records = []
        where = {}
        where['metadata.jat_key'] = key
        while _page:
            where['_page'] = _page
            recs = self.query('file', **where)
            records.extend(recs)
            if len(records) == 500:
                _page += 1
            else:
                _page = 0

        if publish and len(records):
            data = self.get_distributionproperties([records[0]['metadata']['template_name'], ], None)['outputs']
        for output in records:
            if not publish:
                unpublish_file(output)
            else:
                publish_file(output, data, keep_defined, user)
            self.smartSave('file', output)

    def _get_metadata_id_for_file(self, outputs, path, base):
        for output in outputs:
            if not output['file'].startswith('/') and base is not None:
                output['file'] = os.path.join(base, output['file'])
            if output['file'] == path and 'metadata_id' in output:
                return output['metadata_id']
        return None

    def import_analysis(self, kwargs):
        ignore_files = []
        errors = []
        keep_defined = []
        template = None
        skip_folder = kwargs.get('skip_folder', False)

        if kwargs.get('location') is not None:
            kwargs['location'] = os.path.dirname(kwargs.get('location') + '/')
        file_locations = {}
        reviewer = kwargs.get('options').get('reviewer')
        if 'publish' in kwargs.get('options'):
            publish = kwargs.get('options').get('publish')
            del kwargs['options']['publish']
        else:
            publish = True
        kwargs['publish'] = publish
        # XCJB
        # if kwargs['publish']:
        #    publishingflags = self.get_publishingflags([kwargs['template'], ], None)
        #    if 'template_flags' in publishingflags and len(publishingflags['template_flags']) > 0:
        #        kwargs['publish_to'] = publishingflags['template_flags']

        for output in kwargs.get('outputs'):
            # delete publish_to if publish is false
            if not kwargs.get('publish') and 'publish_to' in output.get('metadata', {}):
                del output['metadata']['publish_to']
            if not output.get('file').startswith('/') and kwargs.get('location') is not None:
                output['file'] = os.path.join(kwargs.get('location'), output.get('file'))
            if not output.get('file').startswith('/'):
                errors.append(f'relative file path {output.get("file")} is not allowed')
            # htandra : Changing file_locations to accommodate multiple outputs with same label
            # file_locations['label'] : [ {'location':<file>,'file_format':<metadata.file_format>} ]
            file_format = output.get('metadata', {}).get('file_format', 'None')
            if output.get('label') in file_locations:
                file_locations[output.get('label')].append({'location': output.get('file'), 'format': file_format})
            else:
                file_locations[output.get('label')] = [{'location': output.get('file'), 'format': file_format}]
        new_inputs = []
        # self.logger.info("[importAnalysis] file_locations = %s" % str(file_locations))
        for input in kwargs.get('inputs'):
            # did we get a mongo id
            if ObjectId.is_valid(input):
                if self.config.instance_type == 'dev':
                    rec = self.prod_curl.get(f'api/metadata/file/{input}')
                else:
                    rec = restful.run_internal('metadata', 'get_file', input)
                if rec:
                    rec['metadata_id'] = rec.get('_id')
                else:
                    input_type = 'metadata_id'
            else:
                # or a file id
                try:
                    file_id = int(input)
                    input_type = 'file_id'
                except Exception:
                    file_id = None
                    input_type = 'file'
                try:
                    if self.config.instance_type == 'dev':
                        if file_id:
                            rec = self.prod_curl.get(f'api/tape/file/{file_id}')
                        else:
                            rec = self.prod_curl.get('api/tape/latestfile', file=input)
                    else:
                        if file_id:
                            rec = restful.run_internal('tape', 'get_file', file=file_id)
                        else:
                            rec = restful.run_internal('tape', 'get_latestfile', file=input)
                except Exception:
                    rec = None
            if rec is not None:
                if rec.get('metadata_id') not in new_inputs:
                    new_inputs.append(rec.get('metadata_id'))
            else:
                errors.append(f'input {input_type} {input} was not found in jamo')
        kwargs['inputs'] = new_inputs
        if len(errors) > 0:
            raise common.HttpException(400, errors)
        if 'key' not in kwargs:
            template = self.get_template([kwargs.get('template')], None)
            kwargs['key'] = f'{self.jat_keys.get(kwargs.get("division"))}-{self.getNextAlias()}'
            kwargs['metadata']['template_name'] = kwargs.get('template')
            kwargs['status'] = 'Released' if reviewer is None else 'Under Review'
        kwargs['metadata']['jat_key'] = kwargs.get('key')
        if reviewer is not None:
            response = self.sendEmail(
                to=reviewer,
                subject='A release is available for you to review',
                content=f'You can review the release here: {self.config.url}/{self.appname}/review/{kwargs.get("key")}',
                key=kwargs.get('key')
            )
            self.logger.info(f'{response}; Review notification for={reviewer}')
        else:
            analyses_dir = self.get_analyses_dir(kwargs.get('key'))
            for output in kwargs.get('outputs'):
                metadata = {}
                metadata.update(kwargs.get('metadata'))
                keep_file = False
                if 'metadata' in output:
                    metadata.update(output.get('metadata'))
                    if 'portal' in metadata and 'display_location' in metadata.get('portal'):
                        new_path = []
                        for pa in metadata.get('portal').get('display_location'):
                            new_path.append(eval_string(pa, {'metadata': metadata}))
                        metadata['portal']['display_location'] = new_path
                        # User has requested a custom portal.  We'll need to let update_publish to keep it
                        keep_file = True
                metadata['jat_key'] = kwargs.get('key')
                metadata['jat_label'] = output.get('label')
                metadata['jat_publish_flag'] = publish
                metadata_id = restful.run_internal('metadata', 'post_file', metadata=metadata,
                                                   destination=f'{analyses_dir}/{kwargs.get("key")}/',
                                                   file=output.get('file'), file_type=output.get('tags'),
                                                   __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                           'division': kwargs.get('division')},
                                                   inputs=new_inputs, source=kwargs.get('source', None))
                metadata_id = metadata_id.get('metadata_id')
                if keep_file:
                    keep_defined.append(metadata_id)
                output['metadata_id'] = metadata_id
                if kwargs.get('location') is not None and output.get('file').startswith(kwargs.get('location') + '/'):
                    output['file'] = output.get('file')[len(kwargs.get('location')) + 1:]
                ignore_files.append(output.get('file'))
            if not skip_folder:
                metadata_id = restful.run_internal('metadata', 'post_folder', metadata=kwargs.get('metadata'),
                                                   # Append the JAT key to the folder name to be tarred to help prevent
                                                   # naming collisions when copied to the JAT analysis folder in
                                                   # `dm_archive`.
                                                   destination=f'{analyses_dir}/{kwargs.get("key")}/{os.path.basename(kwargs.get("location"))}.{kwargs.get("key")}',
                                                   local_purge_days=kwargs.get('local_purge_days', 2),
                                                   ignore=ignore_files, folder=kwargs.get('location'),
                                                   file_type='analysis',
                                                   __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                           'division': kwargs.get('division')},
                                                   source=kwargs.get('source', None))
                metadata_id = metadata_id.get('metadata_id')
                kwargs['metadata_id'] = metadata_id
            # run this all the time now.  Post_folder used to do this, but work is delayed, so we'll have to rely on
            # calling this ourselves.
            kwargs.get('metadata').update(restful.run_internal('metadata', 'post_checkdata', **kwargs.get('metadata')))
            kwargs['added_date'] = kwargs['modified_date'] = datetime.datetime.now()
            del kwargs['metadata']['jat_key']
            if template is not None and 'email' in template and 'send_email' in kwargs.get('options') and kwargs.get(
                    'options').get('send_email'):
                content = {'strings': '', 'files': []}
                email = template.get('email')
                temp_files = []
                if 'email' in kwargs.get('options'):
                    email.update(kwargs.get('options').get('email'))
                    del kwargs['options']['email']
                for line in email.get('content'):
                    if isinstance(line, str):
                        content['strings'] += f'\n{eval_string(line, kwargs)}'
                    elif isinstance(line, dict) and 'string' in line:
                        content['strings'] += f'\n{eval_string(line.get("string"), kwargs)}'
                    elif isinstance(line, dict) and 'file' in line and line.get('file') in file_locations:
                        files = file_locations[line.get('file')]
                        # htandra: NOTE- only file_format = 'text' or 'txt' can be added to email content
                        for myfile in files:
                            if myfile.get('format').lower() in ['txt', 'text']:
                                content.get('files').append(
                                    self._get_metadata_id_for_file(
                                        outputs=kwargs.get('outputs'),
                                        path=myfile.get('location'),
                                        base=kwargs.get('location')
                                    ))
                    elif isinstance(line, dict) and 'string' in line:
                        content += f'\n{eval_string(line.get("string"), kwargs)}'
                attachments = []
                if email.get('attachments') is not None:
                    for name in email.get('attachments'):
                        if name in file_locations:
                            for myfile in file_locations.get(name):
                                attachments.append(
                                    self._get_metadata_id_for_file(
                                        outputs=kwargs.get('outputs'),
                                        path=myfile.get('location'),
                                        base=kwargs.get('location')
                                    ))
                reply_to = None
                cc = email.get('cc', [])
                bcc = email.get('bcc', [])
                mime = email.get('mime', 'plain')
                if 'reply_to' in email:
                    reply_to = eval_string(email.get('reply_to'), kwargs)
                if self.config.instance_type == 'dev':
                    response = self.sendEmail(
                        to=kwargs.get('user') + '@lbl.gov',
                        subject=eval_string(email.get('subject'), kwargs),
                        content=content,
                        attachments=attachments,
                        fromAddress=self.config.from_address,
                        replyTo=reply_to,
                        cc=cc,
                        bcc=bcc,
                        key=kwargs.get('key')
                    )
                    self.logger.info(
                        f'{response}; From: {self.config.from_address}; To: {kwargs.get("user")}@lbl.gov; Subject: {eval_string(email.get("subject"), kwargs)}')
                else:
                    response = self.sendEmail(
                        to=email.get('to'),
                        subject=eval_string(email.get('subject'), kwargs),
                        content=content,
                        attachments=attachments,
                        fromAddress=self.config.from_address,
                        replyTo=reply_to,
                        key=kwargs.get('key'),
                        cc=cc,
                        bcc=bcc,
                        mime=mime
                    )
                    self.logger.info(
                        f'{response}; From: {self.config.from_address}; To: {email.get("to")}; Subject: {eval_string(email.get("subject"), kwargs)}')
                if 'email' not in kwargs:
                    kwargs['email'] = email
                # TODO: Where do we modify `temp_files`?
                for new_file in temp_files:
                    os.unlink(new_file)
            # update the publishing flags for the JAMO records
            self.update_publish(kwargs.get('key'), publish, keep_defined=keep_defined, user=kwargs.get('user'))
        return {'analysis_id': self.save('analysis', kwargs), 'location': kwargs.get('location'),
                'jat_key': kwargs.get('key')}

    @restful.doc('Imports a legacy analysis run folder into JAMO')
    @restful.validate({'template_name': {'type': str, 'required': True, 'doc': 'The name of the JAT template to use', 'example': 'reference_sequences'},
                       'template_data': {'type': dict, 'required': True, 'doc': "The import's metadata.json data", 'example': {"outputs": [{"file": "refseq.data", "label": "reference"}], "metadata": {"data_source": "Mycocosm", "description": "spid/1186191", "reference_name": "Armillaria borealis", "reference_type": "Genome", "comments": "By web ui for Mycocosm id of 1186191", "creation_date": "2018-10-19 12:43:21", "jamo_version": 1, "ncbi_taxon_id": 47425, "genus": "armillaria", "species": "armillaria borealis", "data_source_id": "1186191", "ref_size_bp": 71689880}}},
                       'location': {'type': str, 'required': False, 'doc': 'Location of the root directory of the import, any relative file paths in the template_data will be made relative to this path.', 'example': '/global/dna/projectdirs/group/import_data'},
                       'source': {'type': str, 'required': False, 'default': None, 'doc': 'Source name for data center (e.g., igb, dori)'}})
    @restful.permissions('import_jat')
    @restful.passreq(True)
    def post_analysisimport(self, args, kwargs):
        errors = []
        # TODO: This should never happen due to the validator
        if 'template_name' not in kwargs:
            kwargs['template_name'] = None
        if isinstance(kwargs.get('template_name'), str):
            kwargs['template_name'] = kwargs.get('template_name').strip()
        if not isinstance(kwargs.get('template_name'), str) or len(kwargs.get('template_name')) == 0:
            errors.append('missing or invalid template_name')

        # TODO: This should never happen due to the validator
        if 'template_data' not in kwargs or not isinstance(kwargs.get('template_data'), dict):
            errors.append('missing or invalid template_data')

        loc_error = 'invalid location'
        # TODO: This should never happen due to the validator
        if 'location' not in kwargs:
            kwargs['location'] = None
        if isinstance(kwargs.get('location'), str) and kwargs.get('location').startswith('/'):
            # TODO: Will this change the `location`?
            kwargs['location'] = os.path.dirname(kwargs.get('location') + '/')
            if kwargs.get('location').endswith('/'):
                errors.append(loc_error)
            # elif not self.filecheck.isdir(kwargs['location']) or not self.filecheck.access(kwargs['location']):
            #    errors.append('directory %s does not exist or can\'t be read my sdm' % kwargs['location'])
        elif kwargs.get('location') is not None:
            errors.append(loc_error)

        if 'skip_folder' not in kwargs or kwargs.get('location') is None:
            kwargs['skip_folder'] = True
        if not isinstance(kwargs.get('skip_folder'), bool):
            errors.append('invalid skip_folder')

        try:
            source_warning = self._validate_datacenter_source(kwargs.get('source'))
        except common.HttpException as e:
            errors.append(e.message)
        if len(errors) > 0:
            raise common.HttpException(400, errors)

        clean_kwargs = {k: kwargs.get(k) for k in ('location', 'skip_folder', 'user', 'group', 'division', 'source')}
        clean_kwargs['template'] = self.get_resolvedtemplate([kwargs.get('template_name')], {})
        clean_kwargs.update(process_template_data(clean_kwargs.get('template'), kwargs.get('template_data')))
        process_template(clean_kwargs)
        clean_kwargs['template'] = kwargs.get('template_name')
        warnings = clean_kwargs.get('warnings')
        if source_warning:
            warnings.append(source_warning)
        del clean_kwargs['warnings']
        data = self.import_analysis(clean_kwargs)
        data['warnings'] = warnings
        return data

    @restful.doc("Add metadata to another group's analysis. New values will be put under in a sub-document named after your group.  While this is mainly used to tag other group's data, it can be used to tag your own data.  "
                 "Values are added to the record under metadata.<group>.<key>.   So if your group is rqc, for example, and you supply ""{'metadata':{'usable':true}}"" the record will be updated with 'metadata.rqc.usable':true.  "
                 "To update a metadata in a jat submission you own, use the AnalysisUpdate API.")
    @restful.passreq
    @restful.permissions('analysis_update')
    @restful.validate({'metadata': {'type': dict}}, [{'name': 'key', 'type': str}])
    def put_metadata(self, args, kwargs):
        group = kwargs['__auth']['group']
        updatedata = {}
        jamoupdate = {}
        for key, value in kwargs['metadata'].items():
            updatedata[group + '.' + key] = value
            jamoupdate['metadata.%s.%s' % (group, key)] = value
        self.smartUpdate('analysis', {'key': args[0]}, {'$set': updatedata})
        self.smartUpdate('file', {'metadata.jat_key': args[0]}, {'$set': jamoupdate})

    @restful.validate({'query': {'type': dict}, 'update': {'type': dict}})
    @restful.passreq(True, True)
    def post_safeupdate(self, args, kwargs):
        if 'admin' not in kwargs['permissions']:
            raise common.HttpException(403, 'you do not have access to run this method')
        return self.smartUpdate('analysis', kwargs['query'], kwargs['update'])

    @restful.doc('Creates or modifies a new macro')
    @restful.validate({
        'name': {'type': str},
        'description': {'type': str},
        'required_metadata_keys': {'type': list, 'validator': {'*': {'type': dict, 'validator': {
            'description': {'type': str},
            'key': {'type': str},
            'type': {'type': str},
            'required': {'type': bool, 'default': True}}}}}})
    @restful.passreq(True)
    def post_macro(self, args, kwargs):
        name = args[0]
        if name in self.macros:
            kwargs['_id'] = self.macros[name]['_id']
            if self.macros[name]['user'] != kwargs['user']:
                raise common.HttpException(403, 'You can not edit someone else\'s template')
        kwargs['name'] = name
        self.macros[name] = kwargs
        self.save('analysis_macros', kwargs)

    @restful.doc('Gets all the macros based on the search terms')
    @restful.menu('macros')
    @restful.table(title='Macros')
    @restful.passreq(True)
    def get_macros(self, args, kwargs):
        # This is probably not what was originally intended, but it is a first approximation at making this useful
        data = self.query('analysis_macros')
        for item in data:
            keys = []
            for key in item['required_metadata_keys']:
                keys.append(key['key'])
            item['required_metadata_keys'] = ', '.join(sorted(keys))
        return data  # self.query('analysis_macros', **kwargs)

    @restful.doc('Gets the macro definition')
    @restful.generatedhtml(title='macro: {{name}}')
    @restful.passreq(True)
    @restful.single
    @restful.validate(argsValidator=[{'name': 'name', 'type': str, 'doc': 'The name of the macro to get'}])
    def get_macro(self, args, kwargs):
        return self.query('analysis_macros', **{'name': args[0]})

    @restful.doc('search for analyses based on the values passed in')
    @restful.validate({'*': {'type': '*'}})
    def post_query(self, args, kwargs):
        kwargs = convert_dates(kwargs)
        return self.query('analysis', **kwargs)

    @restful.doc('search for analyses based on the values passed in')
    @restful.validate({'*': {'type': '*'}})
    def get_query(self, args, kwargs):
        return self.query('analysis', **kwargs)

    @restful.passreq(True)
    @restful.validate({'url': {'type': str}, 'filter': {'type': dict, 'validator': {'*:1': {'type': '*'}}}})
    def post_subscription(self, args, kwargs):
        pass

    def loadAllMacros(self):
        for macro in self.macro_files:
            self.loadMacro(macro['name'], macro['path'])

    def loadMacro(self, name, path):
        try:
            with open(path) as f:
                data = yaml.load(f.read(), Loader=yaml.SafeLoader)
            data['user'] = 'auto'
            data['group'] = 'users'
            data['name'] = name
            self.post_macro([name], data)
        except Exception:
            self.logger.critical('Failed to validate template %s' % name)

    def loadAllCvs(self):
        for name in self.cv_files:
            self.loadCv(name, self.cv_files[name])

    def loadCv(self, key, path):
        with open(path, 'r') as f:
            data = yaml.load(f.read(), Loader=yaml.SafeLoader)
        map = {}
        for row in data:
            map[row['value']] = row['description']
        self.cv[key] = map

    def get_cv(self, args, kwargs):
        return self.cv

    # this is where we can check for new/deleted templates
    def loadAllTemplates(self):
        # put all the templates into the Mongo DB
        nin = list()
        for template in self.template_files:
            nin.append(template['name'])
            self.loadTemplate(template['name'], template['path'])
        # remove any deleted templates from the Mongo DB
        self.remove('analysis_template', {'user': 'auto', 'name': {'$nin': nin}})

    def loadTemplate(self, name, path):
        try:
            with open(path, 'r') as f:
                file_str = f.read()
            data = yaml.load(file_str, Loader=yaml.SafeLoader)
            data['user'] = 'auto'
            data['group'] = 'users'
            data['name'] = name
            m = hashlib.md5()
            m.update(file_str.encode('utf-8'))
            data['md5'] = m.hexdigest()
            # This puts the templates into the Mongo DB
            self.post_template(None, data)
        except Exception:
            self.logger.critical('Failed to validate template %s' % name)

    def loadAllTagTemplates(self):
        nin = list()
        for template in self.tag_template_files:
            nin.append(template['name'])
            self.loadTagTemplate(template['name'], template['path'])
        self.remove('analysis_tag_template', {'user': 'auto', 'name': {'$nin': nin}})

    def loadTagTemplate(self, name, path):
        try:
            with open(path, 'r') as f:
                file_str = f.read()
            data = yaml.load(file_str, Loader=yaml.SafeLoader)
            data['user'] = 'auto'
            data['group'] = 'users'
            data['name'] = name
            m = hashlib.md5()
            m.update(file_str.encode('utf-8'))
            data['md5'] = m.hexdigest()
            self.post_tagtemplate(None, data)
        except Exception:
            self.logger.critical('Failed to validate template %s' % name)

    @restful.doc('preforms a query and return the first page of results, to get the next pages call nextpage')
    @restful.validate({'fields': {'required': False, 'type': list, 'validator': {'*': {'type': str}}}, 'query': {'type': (str, dict), 'validator': {'*:1': {'type': '*'}}}})
    def post_pagequery(self, args, kwargs):
        fields = kwargs.get('fields', None)
        requestor = kwargs.get('requestor', None)
        source = kwargs.get('source', 'analysis')
        if fields is None:
            if isinstance(kwargs['query'], str) and kwargs['query'].strip().lower().startswith('select'):
                query = kwargs['query'].lower()
                i = 0
                fields = []
                for string in query.split(' '):
                    if string is None:
                        i += 1
                        continue
                    i += len(string) + 1
                    if string == 'where':
                        break
                    if string == ',' or string == 'select':
                        continue
                    fields.extend([f for f in string.split(',') if f is not None and f != ''])
                kwargs['query'] = kwargs['query'][i:]
                if 'file_name' not in fields:
                    fields.append('file_name')
        else:
            if isinstance(kwargs['query'], str) and kwargs['query'].strip().lower().startswith('select'):
                raise Exception('''fields option and a 'select <fields>' query provided, request cannot have both''')

        kwargs['query'] = convert_dates(kwargs['query'])
        if 'cltool' in kwargs and kwargs['cltool']:
            extra = self.clSource
        else:
            extra = self.apiSource
        self.queryLogger.info('%s - %s - %s', str(kwargs['query']), str(fields), requestor, extra=extra)
        return self.pagequery(source, kwargs['query'], fields)

    post_pagequery.paged = True

    @restful.permissions('set_portal')
    @restful.template(template='distributionsetter.html')
    @restful.validate(argsValidator=[{'name': 'template', 'type': str}])
    def get_distributionproperties(self, args, kwargs):
        template = self.get_template(args, None)

        flags = self.query('analysis_publishingflags', template=args[0])
        if len(flags) == 0:
            # if there is no instance of this template in the database, return no flags for each output
            flags = {}
            for output in template['outputs']:
                flags[output['label']] = []
        else:
            flags = flags[0]['outputs']
            # Added by HT;collection doesn't store false values so get them from template
            for output in template['outputs']:
                if output['label'] not in flags:
                    flags[output['label']] = []
            # End.
        plocs = self.query('analysis_plocations', template=args[0])
        if len(plocs) == 0:
            # if there is no instance of this template in the database, return no display location for each output
            plocs = {}
            for output in template['outputs']:
                plocs[output['label']] = ""
        else:
            plocs = plocs[0]['outputs']

        ret = {}
        for output in set(list(flags) + list(plocs)):
            ret[output] = {'display_location': '', 'publish_to': []}
        for output, publish_to in flags.items():
            ret[output]["publish_to"] = publish_to
        for output, display_location in plocs.items():
            ret[output]["display_location"] = display_location

        return {"publishing_flags": publishing_flags, "display_location_cv": display_location_cv, "outputs": ret}

    @restful.permissions('set_portal')
    @restful.validate(argsValidator=[{'name': 'template', 'type': str}])
    def get_portallocations(self, args, kwargs):
        ret = self.query('analysis_plocations', template=args[0])
        template = common.customtransform(self.get_template(args, None))
        template_values = {o['label']: '/'.join(o['default_metadata_values.portal.display_location']) if o['default_metadata_values.portal.display_location'] is not None else '' for o in template['outputs']}
        if len(ret) == 0:
            self.logger.info("returning template_values: %s" % str(template_values))
            return template_values
        ret = ret[0]
        for output in template_values:
            if output not in ret['outputs']:
                ret['outputs'][output] = ''
        return ret['outputs']

    @restful.permissions('set_portal')
    @restful.validate({'*': {'type': str}}, [{'name': 'template', 'type': str}])
    def put_portallocations(self, args, kwargs):
        outputs = copy.copy(kwargs)
        template = args[0]
        if 'XXredirect_internalXX' in kwargs:
            del outputs['XXredirect_internalXX']
        self.logger.info("Updating portal.display_location for template %s" % template)
        self.logger.info("[put_portallocations] kwargs = %s" % str(kwargs))
        errors = []
        current = self.query('analysis_plocations', template=template)
        # if a record for this template exists, change things accordingly
        if len(current) > 0:
            current = current[0]['outputs']
        else:
            current = {}
        ploc_update = {}
        for key in outputs:
            if key in current and current[key] == outputs[key]:
                # we don't need to update any file metadata, but make sure we keep the current state
                ploc_update[key] = current[key]
                continue
            result = self.update_file_portal_locations(template, key, outputs[key])
            if "errors" in result:
                # if a bad macro was entered, do not update the record
                if key in current:
                    # if there already exists a value for this output, use it instead of the bad one
                    ploc_update[key] = current[key]
                # record the bad macro, so we can send it back to the client i.e. to alert the user of their error
                for error in result["errors"]:
                    error_type = list(error.keys())[0]
                    error_value = error[error_type]
                    errors.append({"output": key, "type": error_type, "value": error_value})
            else:
                ploc_update[key] = outputs[key]
        if len(errors) > 0:
            return {"errors": errors, "update": str(self.update('analysis_plocations', {'template': template}, {'$set': {'outputs': ploc_update}}, upsert=True))}
        else:
            return {"update": str(self.update('analysis_plocations', {'template': template}, {'$set': {'outputs': ploc_update}}, upsert=True))}

    def update_file_portal_locations(self, template, output, new_location):
        qdata = {'metadata.jat_label': output, 'metadata.template_name': template,
                 '__update_publish_to': {'$exists': False}}
        open_count = new_location.count('{')
        close_count = new_location.count('}')
        updated_file_count = 0
        if open_count != close_count:
            return {"errors": [{"bad macro": new_location}]}
        elif open_count > 0:
            # Macros have been specified. We need to retrieve macro data and update items one at a time
            rdata = self.extractMacros(new_location)
            result = restful.run_internal('metadata', 'post_pagequery', query=qdata, fields=rdata)
            self.logger.info(
                f'Expanding macros {",".join(rdata)} for {output} files. Will update {len(result)} documents.')
            for record in result:
                evaluated_ploc = eval_string(new_location, record)
                # TODO: Does this actually run? `Metadata.put_file` requires `__auth` to be passed and we don't pass the
                #  payload when calling the method directly...
                restful.run_internal('metadata', 'put_file', id=record.get('_id'),
                                     data={'metadata.portal.display_location': evaluated_ploc.split('/')})
                updated_file_count += 1
        else:
            # No macros used, we can do a bulk update
            restful.run_internal('metadata', 'put_filesuper', query=qdata,
                                 data={'metadata.portal.display_location': new_location.split('/')})
        return {"num_files_updated": updated_file_count}

    @restful.permissions('set_portal')
    @restful.template(template='publishingflagssetter.html')
    @restful.validate(argsValidator=[{'name': 'template', 'type': str}])
    def get_publishingflags(self, args, kwargs):
        ret = self.query('analysis_publishingflags', template=args[0])
        if len(ret) == 0:
            # if there is no instance of this template in the database, return no visibility for each output
            template = self.get_template(args, None)
            ret = {}
            for output in template['outputs']:
                ret[output['label']] = []
            ret['template_flags'] = []
        else:
            tmp = ret[0]['outputs']
            tmp['template_flags'] = ret[0]['template_flags']
            ret = tmp
        return ret

    @restful.permissions('set_portal')
    @restful.validate({'*': {'type': str}}, [{'name': 'template', 'type': str}])
    def put_publishingflags(self, args, kwargs):
        # TODO: fix this to take in a dictionary with the structure { output: [ key1, key2, ... ] }
        update = copy.copy(kwargs)
        self.logger.info("Updating publishing flags for template %s" % args[0])
        if 'XXredirect_internalXX' in kwargs:
            del update['XXredirect_internalXX']
        template_flags = set()
        to_be_deleted = []
        for output in update.keys():
            if len(update[output]) == 0:
                # Cannot modify dict during iteration...
                # del update[output]
                to_be_deleted.append(output)
            else:
                for flag in update[output]:
                    template_flags.add(flag)
        for output in to_be_deleted:
            del update[output]
        template_flags = list(template_flags)
        return str(self.update('analysis_publishingflags', {'template': args[0]}, {'$set': {'outputs': update, 'template_flags': template_flags}}, upsert=True))

    @restful.permissions('set_portal')
    @restful.validate({'*': {'type': str}}, [{'name': 'template', 'type': str}])
    def delete_publishingflags(self, args, kwargs):
        current = self.query('analysis_publishingflags', template=args[0])
        if len(current) > 0:
            updated_outputs = {}
            current = current[0]['outputs']
            need_to_change = False
            for file_type in current:
                if not need_to_change and len(current[file_type]) != 0:
                    need_to_change = True
                updated_outputs[file_type] = []
            if need_to_change:
                return str(self.update('analysis_publishingflags', {'template': args[0]}, {'$set': {'outputs': updated_outputs}}, upsert=True))
        return {}

    def extractMacros(self, string):
        ret = list()
        inB = False
        key = ''
        for char in string:
            if char == '{' and not inB:
                inB = True
            elif char == '}' and inB:
                ret.append(key)
                key = ''
                inB = False
            elif inB:
                key += char
        return ret

    @restful.permissions('admin')
    @restful.validate({'query': {'type': (str, dict)}}, allowExtra=False)
    def post_delete(self, args, kwargs):
        records = restful.run_internal('analysis', 'post_pagequery', **kwargs)
        file_records = 0
        tape_records = 0
        for record in records:
            data = restful.run_internal('metadata', 'post_delete', query='metadata.jat_key=%s' % record['key'])
            file_records += data['file_records']
            tape_records += data['tape_records']
            self.save('deleted_analysis', record)
            self.remove('analysis', {'_id': record['_id']})
        return {'analysis_records': len(records), 'file_records': file_records, 'tape_records': tape_records}

    @restful.permissions('admin')
#   @restful.validate({'query': {'type': (str, dict)}}, allowExtra=False)
    def post_undelete(self, args, kwargs):
        kwargs['source'] = 'deleted_analysis'
        records = restful.run_internal('analysis', 'post_pagequery', **kwargs)
        file_records = 0
        tape_records = 0
        for record in records:
            self.save('analysis', record)
            data = restful.run_internal('metadata', 'post_undelete', query='metadata.jat_key=%s' % record['key'])
            file_records += data['file_records']
            tape_records += data['tape_records']
            self.remove('deleted_analysis', {'_id': record['_id']})
        return {'analysis_records': len(records), 'file_records': file_records, 'tape_records': tape_records}

    @restful.pagetable('Tags', 'analysis_tag_template', map={}, sort=('_id', -1), allow_empty_query=True, return_count=50)
    def post_tags(self, args, kwargs):
        pass

    @restful.validate(argsValidator=[{'name': 'template_name', 'type': str}])
    def get_tag(self, args, kwargs):
        return self.findOne('analysis_tag_template', name=args[0])

    @restful.passreq(True)
    @restful.validate({'metadata': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}},
                       'tags': {'type': list, 'validator': {'*': {'type': str}}}, 'file': {'type': str},
                       'tape_options': {'type': dict}, 'source': {'type': str, 'required': False, 'default': None}})
    def post_importfile(self, args, kwargs):
        templates = self.query('analysis_tag_template', name={'$in': kwargs.get('tags')})
        metadata = customtransform(kwargs.get('metadata'))
        errors = self.checkMetadata(templates, metadata)
        try:
            source_warning = self._validate_datacenter_source(kwargs.get('source'))
        except common.HttpException as e:
            errors.append(e.message)
        if len(errors) > 0:
            raise common.ValidationError(errors)
        now = datetime.datetime.now()
        # print 'metadata', 'post_file', metadata, kwargs['file'], kwargs['tags'], 'file_imports/%d/%d/'%(now.year, now.month) , {'user': kwargs['user'], 'group': kwargs['group']}, kwargs['tape_options']
        return_value = restful.run_internal('metadata', 'post_file', metadata=metadata.dic, file=kwargs.get('file'),
                                            file_type=kwargs.get('tags'),
                                            destination=f'file_imports/{now.year}/{now.month}/',
                                            __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                    'division': kwargs.get('division')},
                                            **kwargs.get('tape_options'),
                                            source=kwargs.get('source'))
        return_value['warnings'] = [source_warning] if source_warning else []
        return return_value

    # helper from metadata.py for post_registerupdate
    def safeMerge(self, origin, new, replace_with_null=True):
        updates = 0
        for key, value in new.items():
            if isinstance(value, dict) and key in origin:
                updates += self.safeMerge(origin[key], value, replace_with_null)
            else:
                if replace_with_null or (value is not None and value != 'null' and value != 'None' and value != ''):
                    if origin.get(key, None) != value:
                        updates += 1
                        origin[key] = value
        return updates

    @restful.permissions('admin')
    @restful.validate({'where': {'type': dict}, 'keep': {'type': list, 'validator': {'*': {'type': str}}}}, allowExtra=False)
    def post_registerupdate(self, args, kwargs):
        where = kwargs['where']
        keep = kwargs['keep']
        records = self.query('analysis', **where)
        total_count = 0
        updated = 0
        for record in records:
            total_count += 1
            metadata = record['metadata']
            new_metadata = {}
            for key in keep:
                if key in metadata:
                    new_metadata[key] = metadata[key]

            meta = restful.run_internal('metadata', 'post_checkdata', **new_metadata)
            if self.safeMerge(metadata, meta):
                record['_id'] = ObjectId(record['_id'])
                record['modified_date'] = datetime.datetime.now()
                self.save('analysis', record)
                updated += 1
        return 'processed %d records, modified %d records' % (total_count, updated)

    @restful.validate({'metadata': {'type': dict, 'required': False, 'validator': {'*:1': {'type': '*'}}}, 'tags': {'type': list, 'validator': {'*': {'type': str}}}})
    def post_validatetags(self, args, kwargs):
        templates = self.query('analysis_tag_template', name={'$in': kwargs['tags']})
        metadata = customtransform(kwargs['metadata'])
        errors = self.checkMetadata(templates, metadata)
        if len(errors) > 0:
            raise common.ValidationError(errors)
        return metadata.dic

    def get_templatesmetadata(self, args, kwags):
        templates = self.query('analysis_tag_template', name={'$in': args})
        return self.condenseMetadata(templates)

    def checkMetadata(self, templates, metadata):
        '''
            we will return errors
            if errors has 0 elements than all went well
            metadata if possible will be converted to the correct type
        '''
        errors = []
        required_m, sets_m = self.condenseMetadata(templates)
        for item in required_m:
            if item['key'] not in metadata and item['required']:
                errors.append('''missing required field '%s' ''' % item['key'])
            elif item['key'] in metadata:
                if not checkType(item['type'], metadata[item['key']]):
                    new_value, success = convertType(item['type'], metadata[item['key']])
                    if not success:
                        errors.append('''wrong type found for key:'%s' should have the type:'%s' ''' % (item['key'], item['type']))
                    else:
                        metadata[item['key']] = new_value
        for mset in sets_m:
            has_item = False
            for item in mset:
                if item['key'] in metadata:
                    has_item = True
                    if not checkType(item['type'], metadata[item['key']]):
                        new_value, success = convertType(item['type'], metadata[item['key']])
                        if not success:
                            errors.append('''wrong type found for key:'%s' should have the type:'%s' ''' % (item['key'], item['type']))
                        else:
                            metadata[item['key']] = new_value
                    if 'options' in item:
                        val = metadata[item['key']]
                        if isinstance(val, list):
                            incorrect = False
                            for va in val:
                                if va not in item['options']:
                                    incorrect = True
                            if incorrect:
                                errors.append(''' incorect value passed for key: '%s' the allowed values are: '%s' ''' % (item['key'], ','.join(map(str, item['options']))))
                        elif val not in item['options']:
                            errors.append(''' incorect value passed for key: '%s' the allowed values are: '%s' ''' % (item['key'], ','.join(map(str, item['options']))))
            if not has_item:
                errors.append('''missing of of the following fields: '%s' ''' % ','.join([x['key'] for x in mset]))
        return errors

    def condenseMetadata(self, templates):
        sets = {}
        required_m = {}
        optional_m = {}
        set_i = 1
        for template in templates:
            r_map = {}
            for item in template['required_metadata_keys']:
                if 'required' not in item:
                    optional_m[item['key']] = item
                elif isinstance(item['required'], bool):
                    if item['required']:
                        required_m[item['key']] = item
                    else:
                        optional_m[item['key']] = item
                else:
                    if item['required'] not in r_map:
                        r_map[item['required']] = set_i
                        sets[set_i] = []
                        set_i += 1
                    sets[r_map[item['required']]].append(item)
        to_be_deleted = []
        for i, m_set in sets.items():
            new_set = []
            for item in m_set:
                if not item['key'] in required_m:
                    new_set.append(item)
            if len(new_set) == 1:
                item['required'] = False
                optional_m[item['key']] = item
                # Cannot modify dict during iteration...
                # del sets[i]
                to_be_deleted.append(i)
            elif len(new_set) > 1:
                sets[i] = new_set
            else:
                # Cannot modify dict during iteration...
                # del sets[i]
                to_be_deleted.append(i)
        for i in to_be_deleted:
            del sets[i]
        optional_m.update(required_m)
        return list(optional_m.values()), list(sets.values())

    @restful.passreq(True, True)
    @restful.permissions('analysis_update')
    @restful.validate({'file': {'type': str, 'required': True, 'doc': 'the full path to the file to be added'},
                       'metadata': {'type': dict, 'required': True},
                       'source': {'type': str, 'required': False, 'default': None}},
                      [{'name': 'analysis_key', 'type': str}, {'name': 'label', 'type': str}])
    def post_addfile(self, args, kwargs):
        source_warning = self._validate_datacenter_source(kwargs.get('source'))
        analysis_key, label = args
        analysis = self.get_analysis([analysis_key], None)
        if 'admin' not in kwargs.get('permissions') and not ('analysis_update' in kwargs.get('permissions') and (
                kwargs.get('user') == analysis.get('user') or kwargs.get('group') == analysis.get('group'))):
            raise common.HttpException(401,
                                       f'you do not have permission to run this method (analysis_update permission needed), only an admin, the user who added it ({analysis.get("user")}), or a group member ({analysis.get("group")}) can modify it')
        template = self.get_resolvedtemplate([analysis.get('template')], None)
        analysis_metadata = analysis.get('metadata')
        output_validator = None
        for output in template.get('outputs'):
            if output.get('label') == label:
                output_validator = output
                break
        if output_validator is None:
            raise common.HttpException(400, f'label \'{label}\' was not found in the template used for this analysis')

        output_metadata = {key: output_validator.get(key) for key in ('description', 'tags', 'label')}
        output_metadata['metadata'] = kwargs.get('metadata')
        output_metadata['file'] = kwargs.get('file')
        o_metadata = {}
        if 'metadata' in kwargs:
            o_metadata.update(analysis_metadata)
            o_metadata.update(kwargs.get('metadata'))
            if 'portal' in o_metadata and 'display_location' in o_metadata.get('portal'):
                new_path = []
                for pa in o_metadata.get('portal').get('display_location'):
                    new_path.append(eval_string(pa, {'metadata': o_metadata}))
                o_metadata['portal']['display_location'] = new_path
        else:
            # TODO: This code will never be reached since the validator REQUIRES `metadata` in `kwargs`
            o_metadata = analysis_metadata

        if 'default_metadata_values' in output_validator:
            for dkey, dval in output_validator.get('default_metadata_values').items():
                if dkey not in o_metadata:
                    o_metadata[dkey] = dval

        known_keys = []
        if 'required_metadata_keys' in output_validator:
            errors = self.checkMetadata([output_validator], o_metadata)
            if len(errors) > 0:
                raise common.ValidationError(errors)

            # This should be redone to use a common check
            for key in output_validator.get('required_metadata_keys'):
                known_keys.append(key.get('key'))

            if 'required_metadata_keys' in template:
                for key in template.get('required_metadata_keys'):
                    known_keys.append(key.get('key'))

        # Check to see if the passed keys are part of our defined keys
        for key in output_metadata.get('metadata'):
            if key not in known_keys:
                raise common.ValidationError(
                    'You have Metadata keys that are not defined in the template: ' + key + ', processing is aborted')

        # check for duplicate files
        base_file = os.path.basename(kwargs.get('file'))
        for output in analysis.get('outputs'):
            if base_file == os.path.basename(output.get('file')):
                raise common.ValidationError("File " + base_file + " already exists in analysis " + analysis_key)

        o_metadata['jat_key'] = analysis_key
        o_metadata['jat_label'] = label
        analyses_dir = self.get_analyses_dir(analysis_key)
        metadata_id = restful.run_internal('metadata', 'post_file', metadata=o_metadata,
                                           destination=f'{analyses_dir}/{analysis_key}/', file=kwargs.get('file'),
                                           file_type=output_metadata.get('tags'),
                                           __auth={'user': kwargs.get('user'), 'group': kwargs.get('group'),
                                                   'division': kwargs.get('division')},
                                           source=kwargs.get('source'))
        metadata_id = metadata_id.get('metadata_id')
        output_metadata['metadata_id'] = metadata_id
        self.update('analysis', {'key': analysis_key}, {
            '$push': {'outputs': output_metadata},
            '$set': {'modified_date': datetime.datetime.now()}
        })
        return {'warnings': [source_warning] if source_warning else []}

    @restful.permissions('analysis_update')
    @restful.doc('Set the publish flag by jat_key. Usage : api/analysis/publish/{jat_key} -d \'{\'publish\':boolean}\' ')
    @restful.validate({'publish': {'type': bool, 'required': False}}, [{'name': 'key', 'type': str, 'doc': 'Jat-key of the record'}], allowExtra=False)
    @restful.passreq(include_perms=True)
    def put_publish(self, args, kwargs):
        # Get the analysis record and check if a record with the key exists
        analysis = self.query('analysis', **{'key': args[0]})
        self.logger.info("Input args passed to put_publish %s" % str(kwargs))
        if len(analysis) == 0:
            raise common.HttpException(404, 'Sorry no document exists with this key: %s' % args[0])
        else:
            analysis = analysis[0]
        if 'publish' not in kwargs:
            kwargs['publish'] = True  # default is True
        # Cannot edit publish flag on an analysis record created by other groups
        if not kwargs['__auth']['group'] == analysis['group'] and 'admin' not in kwargs['permissions']:
            raise common.HttpException(401, 'Sorry you do not have permission to change the publish field on this analysis: %s' % args[0])
        elif 'publish' in analysis and kwargs['publish'] != analysis['publish']:
            jatupdate = {'publish': kwargs['publish']}
            self.logger.info("Keys in jat record that will be updated =  %s" % str(jatupdate))
            jatupdate['modified_date'] = datetime.datetime.now()
            # Update the 'publish' flag in jat analysis record
            self.update('analysis', {'key': args[0]}, {'$set': jatupdate})
        # update files in jamo
        self.update_publish(args[0], kwargs['publish'])
        return {"Status": "OK. Successfully set the publish flag."}

    @restful.permissions('analysis_update')
    @restful.doc('Set/remove the obsolete flag to unpublish/publish a file by jat_key and file_name. Usage : api/analysis/unpublishfile/{jat_key} -d \'{\'unpublish\':boolean}\', \'file_name\':str}\'}')
    @restful.validate({'unpublish': {'type': bool, 'required': True}, 'file_name': {'type': str, 'required': True}, 'replaced_by': {'type': str, 'required': False}}, [{'name': 'key', 'type': str, 'doc': 'Jat-key of the record'}], allowExtra=False)
    @restful.passreq(include_perms=True)
    def put_unpublishfile(self, args, kwargs):
        # Get the analysis record and check if a record with the key exists
        analysis = self.query('analysis', **{'key': args[0]})
        self.logger.info("Input args passed to put_unpublishfile %s" % str(kwargs))
        if len(analysis) == 0:
            raise common.HttpException(404, 'Sorry no document exists with this key: %s' % args[0])
        else:
            analysis = analysis[0]
        # TODO: This will never be true as validation requires `unpublish` to be set in `kwargs`
        if 'unpublish' not in kwargs:
            kwargs['unpublish'] = True  # default is True
        # Cannot unpublish file in an analysis record created by other groups
        if not kwargs['__auth']['group'] == analysis['group'] and 'admin' not in kwargs['permissions']:
            raise common.HttpException(401, 'Sorry you do not have permission to unpublish file in this analysis: %s' % args[0])
        # update files in jamo
        records = self.query('file', **{'metadata.jat_key': args[0], 'file_name': kwargs['file_name']})
        if len(records) == 0:
            raise common.HttpException(404, 'Sorry file %s with jat_key %s not found' % (kwargs['file_name'], args[0]))
        record = records[0]
        if kwargs['unpublish']:
            record['obsolete'] = True
            if 'replaced_by' in kwargs:
                record['replaced_by'] = kwargs['replaced_by']
            unpublish_file(record)
        # TODO: This will never be true as validation requires `unpublish` to be set in `kwargs`
        else:
            if 'obsolete' in record:
                del record['obsolete']
            if 'replaced_by' in record:
                del record['replaced_by']
            if analysis.get('publish', False):
                publish_file(record, self.get_distributionproperties([record['metadata']['template_name'], ], None)['outputs'])
        self.smartSave('file', record)
        return {"Status": "OK. Successfully %s the obsolete flag." % ('set' if kwargs['unpublish'] else 'removed')}

    def get_keys(self, args, kwargs):
        query = {}
        if len(args) > 0:
            query = {'_id': {"$regex": args[0], "$options": "i"}}
        cursor = self.db['jat_keys'].find(query)
        ret = []
        for rec in cursor:
            ret.append(rec)
        return ret

    def _validate_datacenter_source(self, source: str):
        """Validate data center source argument from caller. If the source evaluates to `False`, then it will either
        raise a 400 or return a `str` with a warning message. This is to allow enforcement via a feature flag
        (`require_datacenter_source`). Once the transitional period is completed, this method will no longer be needed
        (along with the feature flag) and will always raise 400.

        :param source: Data center source name
        """
        if not source:
            if self.config.require_datacenter_source:
                raise common.HttpException(400, 'Missing required data center `source` parameter')
            return 'Requests without data center `source` parameter are deprecated. It will become a REQUIRED parameter. Please update your calls to pass the parameter'


def unpublish_file(output):
    # unset the flags, portal display location, and __update_publish_to
    if 'publish_to' in output['metadata']:
        del output['metadata']['publish_to']
    if 'portal' in output['metadata']:
        del output['metadata']['portal']
    if '__update_publish_to' in output:
        del output['__update_publish_to']
    output['metadata']['jat_publish_flag'] = False


def publish_file(output, dist_props, keep_defined=None, user=None):
    # Update the publish flags if both __update_publish_to and obsolete are not set
    output['metadata']['jat_publish_flag'] = True
    if '__update_publish_to' not in output and 'obsolete' not in output:
        if 'jat_label' in output['metadata']:
            if keep_defined and 'portal' in output['metadata'] and 'display_location' in output['metadata']['portal'] and str(output['_id']) in keep_defined:
                output['__update_publish_to'] = [{'user': user, "on": datetime.datetime.now(), 'display_location': {'from': '', 'to': '/'.join(output['metadata']['portal']['display_location'])}}]
            else:
                jat_label = output['metadata']['jat_label']
                publish_to = dist_props[jat_label]['publish_to']
                if len(publish_to):
                    output['metadata']['publish_to'] = publish_to
                elif 'publish_to' in output['metadata']:
                    del output['metadata']['publish_to']
                new_location = dist_props[jat_label]['display_location']
                if len(new_location):
                    if '{' in new_location:
                        new_location = eval_string(new_location, output)
                    if 'portal' not in output['metadata']:
                        output['metadata']['portal'] = {'display_location': []}
                    output['metadata']['portal']['display_location'] = new_location.split('/')
                elif 'portal' in output['metadata'] and 'display_location' in output['metadata']['portal']:
                    del output['metadata']['portal']['display_location']


def eval_string(string, template):
    '''
    string: string to evaluate - this could contain a macro construct such as {metadata.subtitle}
    template: metadata to expand macros with
    '''
    ret = u''
    inB = False
    key = ''
    for char in string:
        if char == '{' and not inB:
            inB = True
        elif char == '}' and inB:
            tVal = get_value(template, key)
            if tVal is not None and isinstance(tVal, list):
                tVal = ','.join(map(str, tVal))
            ret += str(tVal)
            key = ''
            inB = False
        elif inB:
            key += char
        else:
            ret += char
    return ret


def get_value(metadata, keys):
    on = metadata
    for key in keys.split('.'):
        if key not in on:
            return ''
        if isinstance(on[key], dict):
            on = on[key]
        else:
            return on[key]
