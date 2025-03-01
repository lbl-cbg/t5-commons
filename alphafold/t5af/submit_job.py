import argparse
import os

from t5common.jira.connector import JiraConnector, find_asset_attribute
from t5common.job import SlurmJob

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
    parser = argparse.ArgumentParser(description="Poll Jira projects and run a script for each issue.")
    parser.add_argument('issue', type=str, help='The issue to process')
    parser.add_argument('-s', '--submit', action='store_true', help='Submit jobs to Slurm', default=False)
    args = parser.parse_args()

    jc = JiraConnector()

    issue = jc.get_issue(args.issue)
    key = issue['key']

    asset = jc.get_asset(issue['fields']['customfield_10113'][0]['objectId'])

    sequence = find_asset_attribute(asset, 'Parent Protein Seq', key='name')[0]['value']
    name = find_asset_attribute(asset, 'Original Target ID', key='name')[0]['value']

    write_fasta(sequence, name, "input.fasta")

    # Set up job for MSA
    msa_job = SlurmJob(project='m4521', jobname=f"t5af_msa__{key}", output="msa.%J.log", error="msa.%J.log", time="02:00:00")
    msa_job.set_env_var('MMSEQS_PATH', "mmseqs")
    msa_job.set_env_var('MSA_DB_DIR', "$CFS/m4521/resources/colabfold/msa_db")
    msa_job.set_env_var('MSA_OUTPUT_DIR', "msa")
    msa_job.set_env_var('NCORES', "256")
    msa_job.set_env_var('INPUT', "input.fasta")
    msa_job.add_command('colabfold_search --mmseqs ${MMSEQS_PATH} --threads=${NCORES} ${INPUT} ${MSA_DB_DIR} ${MSA_OUTPUT_DIR}')

    msa_sh = "msa.sh"
    with open(msa_sh, 'w') as f:
        msa_job.write(f)
    msa_job_id = msa_job.submit_job(msa_sh) if args.submit else '0000000'

    # jc.add_comment(args.issue, f"ColabFold MSA job submitted to Perlmutter. Job ID {msa_job_id}")

    # Set up job for AlphaFold
    fold_job = SlurmJob(project='m4521', jobname=f"t5af_fold__{key}", output="fold.%J.log", error="fold.%J.log", gpus=1, queue='shared', time="02:00:00")
    fold_job.add_addl_jobflag(fold_job.wait_flag, msa_job_id)

    fold_job.set_env_var('MSA_FILE', os.path.join("msa", f"{name}.a3m"))
    fold_job.set_env_var('PREDICTION_DIR', "prediction")

    fold_job.set_env_var('CUDA_VISIBLE_DEVICES', '$(($JOB_NUMBER % 4))')
    fold_job.add_command('colabfold_batch --save-all --save-recycles --num-recycle=5 ${MSA_FILE} ${PREDICTION_DIR}')
    fold_job.add_command('mark-job finished')

    fold_sh = "fold.sh"
    with open(fold_sh, 'w') as f:
        fold_job.write(f)
    fold_job_id = fold_job.submit_job(fold_sh) if args.submit else '1111111'

    # jc.add_comment(args.issue, f"ColabFold prediction job submitted to Perlmutter. Job ID {fold_job_id}")

    # jc.transition_issue(args.issue, "10054")   # "In Progress" status id is 10054


if __name__ == '__main__':
    main()

