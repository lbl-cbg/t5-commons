import argparse

from t5common.jira import JiraConnector, find_asset_attribute

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
    args = parser.parse_args()

    jc = JiraConnector()

    issue = jc.get_issue(args.issue)

    asset = jc.get_asset(issue['fields']['customfield_10113'][0]['objectId'])

    sequence = find_asset_attribute(asset, 'Parent Protein Seq', key='name')[0]['value']
    name = find_asset_attribute(asset, 'Original Target ID', key='name')[0]['value']

    write_fasta(sequence, name, "input.fasta")

    # Set up job for MSA
    msa_job = SlurmJob(project='m4521', jobname=f"msa__{issue}", output="msa.%J.log", error="msa.%J.log")
    msa_job.set_env_var('MMSEQS_PATH', "mmseqs")
    msa_job.set_env_var('MSA_DB_DIR', "$CFS/m4521/resources/colabfold/msa_db")
    msa_job.set_env_var('MSA_OUTPUT_DIR', "msa")
    msa_job.add_command('colabfold_search --mmseqs ${MMSEQS_PATH} --threads=${NCORES} ${INPUT} ${MSA_DB_DIR} ${MSA_OUTPUT_DIR}')

    msa_job_id = msa_job.submit("msa.sh")

    jc.add_comment(args.issue, f"ColabFold MSA job submitted to Perlmutter. Job ID {msa_job_id}")

    # Set up job for AlphaFold
    fold_job = SlurmJob(project='m4521', jobname=f"fold__{issue}", output="fold.%J.log", error="fold.%J.log")
    fold_job.add_addl_jobflag(fold_job.wait_flag, msa_job_id)

    fold_job.set_env_var('MSA_FILE', os.path.join("msa", f"{name}.a3m"))
    fold_job.set_env_var('PREDICTION_DIR', "prediction")

    fold_job.set_env_var('CUDA_VISIBLE_DEVICES', '$(($JOB_NUMBER % 4))')
    fold_job.add_command('colabfold_batch --save-all --save-recycles --num-recycle=5 ${MSA_FILE} ${PREDICTION_DIR}')
    fold_job.add_command('echo done > status')

    fold_job_id = fold_job.submit("fold.sh" )

    jc.add_comment(args.issue, f"ColabFold prediction job submitted to Perlmutter. Job ID {fold_job_id}")

    jc.transition_issue(args.issue, "10054")   # "In Progress" status id is 10054


if __name__ == '__main__':
    main()

