import argparse
from glob import glob
import json
import os
from os.path import join
import re

import yaml

from t5common.jira.connector import JiraConnector, find_asset_attribute, get_protein_metadata
from t5common.jira.assets import AssetBuilder
from t5common.jamo import JATSubmitter, MetadataBuilder
from t5common.utils import get_logger

from .utils import ISSUE_FILE


class AFJATSubmitter(JATSubmitter):

    @classmethod
    def get_outputs_and_metadata(cls, mb, directory, save_int=False):
        """Populate a MetadataBuilder with outputs and metadata from a ColabFold directory"""
        directory = os.path.abspath(directory)
        log_path = join(directory, 'log.txt')
        mb.add_output('config', join(directory, 'config.json'), file_format='json')
        mb.add_output('log', log_path, file_format='txt')
        msa = glob(join(directory, '*.a3m'))[0]
        mb.add_output('colabfold_msa', msa, file_format='a3m')

        base = os.path.basename(msa)[:-4]
        mb.add_output('coverage_figure', join(directory, f'{base}_coverage.png'), file_format='png')
        mb.add_output('pae_figure', join(directory, f'{base}_pae.png'), file_format='png')
        mb.add_output('plddt_figure', join(directory, f'{base}_plddt.png'), file_format='png')

        # Map model number to stats so we can get them down below for
        # setting the required metadata of individual PDB files
        pattern = r'rank_(?P<rank>\d+).*?_model_(?P<model_num>\d+)_seed_\d+.*?pLDDT=(?P<plddt>[\d.]+).*?pTM=(?P<ptm>[\d.]+)'
        stats_re = re.compile(pattern)
        recycle_re = re.compile(r'recycle=(\d+)')
        stats = dict()
        max_rec = 0
        with open(log_path, 'r') as f:
            for line in f:
                m = recycle_re.search(line)
                if m is not None:
                    max_rec = max(max_rec, int(m.group(1)))
                m = stats_re.search(line)
                if m is None:
                    continue
                stats[int(m.group('model_num'))] = {
                        'rank': int(m.group('rank')),
                        'plddt': float(m.group('plddt')),
                        'ptm': float(m.group('ptm')),
                    }
        mb.add_metadata(num_recycle=max_rec)

        model_re = re.compile(r'(\d+)_seed')

        for pdb in glob(join(directory, f"{base}*.pdb")):
            if re.search(r'\.r\d+\.pdb$', pdb):
                continue
            model_num = int(model_re.search(pdb).group(1))
            mb.add_output('protein_model', pdb, file_format='pdb', model_number=model_num, **stats[model_num])

        for scores in glob(join(directory, f"{base}_scores*.json")):
            model_num = int(model_re.search(scores).group(1))
            mb.add_output('scores', scores, file_format='json', model_number=model_num, rank=stats[model_num]['rank'])

        if save_int:
            for pkl in glob(join(directory, f"{base}*.pickle")):
                if re.search(r'\.r[0-9]+\.pickle$', pkl):
                    continue
                model_num = int(model_re.search(pkl).group(1))
                mb.add_output('raw_model_outputs', pkl, file_format='pkl', module_number=model_num)

    def __init__(self, jc):
        super().__init__()
        self.jc = jc

    @property
    def template_name(self):
        return "alphafold_results"

    def get_template_data(self, directory, save_int=False):
        """Get the payload for submitting to JAT

        Args:
            directory:  The directory to get results form
            save_int:   Save intermediate pickle files if True.
        """
        mb = MetadataBuilder()

        # Add output
        self.get_outputs_and_metadata(mb, join(directory, 'prediction'), save_int=save_int)

        with open(join(directory, 'issue'), 'r') as f:
            issue = f.read().strip()
        virus_id, protein_id = get_protein_metadata(self.jc, issue)
        mb.add_metadata(issue=issue,
                        virus_id=virus_id,
                        protein_id=protein_id)

        return mb.doc


def main():
    epi = """The following environment variables must be set:

    JIRA_HOST   - The Jira host URL
    JIRA_USER   - The user to connect to Jira with
    JIRA_TOKEN  - The token for connecting to Jira
    JAMO_HOST   - The JAMO host URL
    JAMO_TOKEN  - The application token for connecting to JAMO
    """
    parser = argparse.ArgumentParser(description="Submit AlphaFold results to JAMO and Jira",
                                     epilog=epi,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('directory', type=str, help='The directory submit-af-job was run from')
    parser.add_argument('--save-int', action='store_true', help='Save intermediate files to JAMO', default=False)
    parser.add_argument('-f', '--force', action='store_true', help='Force run. Do not stop if already submitted', default=False)
    args = parser.parse_args()

    logger = get_logger('publish-af-results')

    jat_key_file = join(args.directory, 'jat_key')

    jc = JiraConnector()

    js = AFJATSubmitter(jc)

    if os.path.exists(jat_key_file) and not args.force:
        with open(jat_key_file, 'r') as f:
            jat_key = f.read().strip()
        jamo_url = js.get_url(jat_key)
        logger.error(f"Found existing JAT key - {jat_key}. See {jamo_url} for results")
        exit(1)

    with open(os.path.join(args.directory, ISSUE_FILE), 'r') as f:
        issue_key = f.read().strip()

    logger.info(f"Submitting AlphaFold results from {args.directory} to JAT")
    jat_data, response = js.submit(args.directory, save_int=args.save_int)
    with open(join(args.directory, 'metadata.json'), 'w') as f:
        json.dump(jat_data, f, indent=2)

    if response.status_code != 200:
        errors = yaml.dump(response.json(), default_flow_style=False)
        logger.error(f"Unable to submit to JAT\n{errors}")
        exit(1)
    else:
        response = response.json()
        jat_key = response['jat_key']
        logger.info(f"AlphaFold results published to JAT under record {jat_key}")
        with open(jat_key_file, 'w') as f:
            f.write(jat_key)

    ab = AssetBuilder(47, {'target_asset': 770, 'jamo_url': 771})

    logger.info(f"Creating asset in Jira")
    issue = jc.get_issue(issue_key)
    af_asset = jc.create_asset(ab(target_asset=issue['fields']['customfield_10113'][0]['objectId'],
                                  jamo_url=js.get_url(response)))
    asset_url = f"{os.environ['JIRA_HOST']}/jira/servicedesk/assets/object-schema/3?mode=object&objectId={af_asset['id']}&typeId={ab.type_id}&view=list"
    logger.info(f'Asset created. Visit the URL below for more details\n{asset_url}')

    update_data = {"fields": {"customfield_10152": [{'objectId': af_asset['id'],
                                                     'workspaceId': af_asset['workspaceId'],
                                                     'id': af_asset['globalId']}]}}
    logger.info(f"Updating issue {issue_key} with asset {af_asset['id']}")
    jc.update_issue(issue_key, pdate_data)

    # Done state is 151
    # Rejected state is 171
    # In Progress state is 181
    # Cancelled state is 191
    # Open state is 201
    # In Review state is 211
    logger.info(f"Closing issue {issue_key}")
    jc.transition_issue(issue_key, 151)


if __name__ == '__main__':
    main()
