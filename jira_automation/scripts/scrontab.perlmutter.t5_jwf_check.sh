#!/bin/bash
# t5 jwf
#SCRON -q cron
#SCRON -C cron
#SCRON -A m4521
#SCRON -c 2
#SCRON -t 00:30:00
#SCRON --job-name=t5_jwf_check
#SCRON --chdir=/global/cfs/cdirs/m4521/jira_automation
#SCRON --dependency=singleton
#SCRON --output=/global/cfs/cdirs/m4521/jira_automation/t5_jwf.perlmutter.%j.log
#SCRON --error=/global/cfs/cdirs/m4521/jira_automation/t5_jwf.perlmutter.%j.log
#SCRON --open-mode=append
if [[ ! -z "${SLURM_MEM_PER_CPU}" ]]; then
    unset SLURM_MEM_PER_CPU
    unset SLURM_OPEN_MODE
fi
*/5 * * * * bash /global/common/software/m4521/jira_automation/scripts/t5_jwf.sh check /global/common/software/m4521/t5wf
