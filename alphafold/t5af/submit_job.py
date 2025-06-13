import argparse
import os

from t5common.jira.connector import JiraConnector, find_asset_attribute
from t5common.job import SlurmJob
from t5common.utils import get_logger

from .utils import ISSUE_FILE


def write_fasta(sequence, description, filename):
    """
    Writes a protein sequence to a FASTA file.

    :param sequence: The protein sequence as a string.
    :param description: The description line for the FASTA file.
    :param filename: The name of the output FASTA file.
    """
    with open(filename, 'w') as fasta_file:
        # Write the description line
        fasta_file.write(f">{description}\n")
        # Write the sequence, wrapping it at 60 characters per line
        for i in range(0, len(sequence), 60):
            fasta_file.write(sequence[i:i+60] + '\n')


def main():
    epi = """
The following environment variables must be set:
    JIRA_HOST       - The Jira host URL
    JIRA_USER       - The user to connect to Jira with
    JIRA_TOKEN      - The token for connecting to Jira
    MSA_DB_DIR      - The path to the MSA database
    AF2_WEIGHTS_DIR - The path to the AlphaFold2 weights, as downloaded by ColabFold

This command will write three files to the current working directory:
    issue   - The issue used to set up this job
    msa.sh  - The job for running the multiple sequence alignment
    fold.sh - The job for running AlphaFold2
Files will be overwritten if they exist.
    """
    parser = argparse.ArgumentParser(description="Run AlphaFold using ColabFold for a protein indicated in a Jira issue",
                                     epilog=epi,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('issue', type=str, help='The issue to process')
    parser.add_argument('--no-submit', action='store_true', help='Do not submit jobs to Slurm', default=False)
    parser.add_argument('--no-jira', action='store_true', help='Do not update Jira', default=False)
    parser.add_argument('-m', '--msa', type=str, help='The MSA to use. If not provided, run MSA job', default=None)
    args = parser.parse_args()

    logger = get_logger('submit-af-job')

    jc = JiraConnector()

    issue = jc.get_issue(args.issue)
    key = issue['key']
    with open(ISSUE_FILE, "w") as f:
        print(key, file=f)

    msa = args.msa
    msa_job_id = None
    if msa is None:
        asset = jc.get_asset(issue['fields']['customfield_10113'][0]['objectId'])

        sequence = find_asset_attribute(asset, 'Parent Protein Seq', key='name')[0]['value']
        name = find_asset_attribute(asset, 'Original Target ID', key='name')[0]['value']

        write_fasta(sequence, name, "input.fasta")

        # Set up job for MSA
        msa_job = SlurmJob(project='m4521', jobname=f"t5af_msa__{key}", output="msa.%J.log", error="msa.%J.log", time="02:00:00")
        msa_job.set_env_var('MMSEQS_PATH', "mmseqs")
        msa_job.set_env_var('MSA_OUTPUT_DIR', "msa")
        msa_job.set_env_var('NCORES', "256")
        msa_job.set_env_var('INPUT', "input.fasta")
        msa_job.add_command('colabfold_search --mmseqs ${MMSEQS_PATH} --threads=${NCORES} ${INPUT} ${MSA_DB_DIR} ${MSA_OUTPUT_DIR}')

        msa_sh = "msa.sh"
        logger.info(f"Writing MSA job script to {msa_sh}")
        with open(msa_sh, 'w') as f:
            msa_job.write(f)
        msa_job_id = '0000000' if args.no_submit else msa_job.submit_job(msa_sh)

        msg = f"ColabFold MSA job submitted to Perlmutter. Job ID {msa_job_id}"
        logger.info(msg)
        if not args.no_jira:
            jc.add_comment(args.issue, msg)

        msa = os.path.join("msa", f"{name}.a3m")

    # Set up job for AlphaFold
    fold_job = SlurmJob(project='m4521', jobname=f"t5af_fold__{key}", output="fold.%J.log", error="fold.%J.log", gpus=1, queue='shared', time="02:00:00")
    if msa_job_id is not None:
        fold_job.add_addl_jobflag(fold_job.wait_flag, msa_job_id)

    fold_job.set_env_var('MSA_FILE', msa)
    fold_job.set_env_var('PREDICTION_DIR', "prediction")

    fold_job.add_command('colabfold_batch --data ${AF2_WEIGHTS_DIR} --save-all --save-recycles --num-recycle=5 ${MSA_FILE} ${PREDICTION_DIR}')
    fold_job.add_command("if [ $? -eq 0 ]; then t5 jwf mark-job WORKFLOW_FINISHED; else t5 jwf mark-job WORKFLOW_FAILED; fi")

    fold_sh = "fold.sh"
    logger.info(f"Writing MSA job script to {fold_sh}")
    with open(fold_sh, 'w') as f:
        fold_job.write(f)
    fold_job_id = '1111111' if args.no_submit else fold_job.submit_job(fold_sh)

    msg = f"ColabFold prediction job submitted to Perlmutter. Job ID {fold_job_id}"
    logger.info(msg)
    if not args.no_jira:
        jc.add_comment(args.issue, msg)
        logger.info(f"Marking {args.issue} as In Progress")
        jc.transition_issue(args.issue, 181)   # "In Progress" status id is 181


if __name__ == '__main__':
    main()

