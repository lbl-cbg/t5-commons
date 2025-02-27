SHELL=/bin/bash
BASH_ENV=~/.bashrc_conda
*/2 * * * * bash /global/common/software/m4521/jamo_code/dt_service/scripts/run_dt_service_nersc_prod.sh /global/common/software/m4521/jamo_env >> /global/cfs/cdirs/m4521/jamo/dt_service/dt_service.`basename \`hostname\` .nersc.gov`.log 2>&1
