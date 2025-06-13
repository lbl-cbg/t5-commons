# t5 jwf start
#SCRON -q cron
#SCRON -C cron
#SCRON -A m4521
#SCRON -c 2
#SCRON -t 00:30:00
#SCRON --job-name=t5_jwf_start
#SCRON --chdir=/global/cfs/cdirs/m4521/jira_automation
#SCRON --dependency=singleton
#SCRON --output=/global/cfs/cdirs/m4521/jira_automation/t5_jwf.perlmutter.%j.log
#SCRON --error=/global/cfs/cdirs/m4521/jira_automation/t5_jwf.perlmutter.%j.log
#SCRON --open-mode=append
*/5 * * * * bash /global/common/software/m4521/jira_automation/scripts/t5_jwf.sh start /global/common/software/m4521/t5wf
