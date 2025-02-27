#!/bin/bash
# dt_service (prod)
#SCRON -q workflow
#SCRON -A m4521
#SCRON -c 2
#SCRON -t 90-00:00:00
#SCRON --time-min=12:00:00
#SCRON --job-name=jamo_dt_service
#SCRON --chdir=/global/cfs/cdirs/m4521/jamo/dt_service
#SCRON --dependency=singleton
#SCRON --output=/global/cfs/cdirs/m4521/jamo/dt_service/dt_service.perlmutter.%j.log
#SCRON --open-mode=append
* * * * * bash /global/common/software/m4521/jamo_code/dt_service/scripts/run_dt_service_perlmutter_prod.sh /global/common/software/m4521/jamo_env
