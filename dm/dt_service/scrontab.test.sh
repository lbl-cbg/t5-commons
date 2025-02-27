#!/bin/bash -l
# dt_service (prod)
#SCRON -q workflow
#SCRON -A m4521
#SCRON -c 2
#SCRON -t 01:00:00
#SCRON --time-min=00:01:00
#SCRON --job-name=jamo_dt_service
#SCRON --chdir=$DTS_LOG_DIR
#SCRON --dependency=singleton
#SCRON --output=$DTS_LOG_DIR/workflow_test.%j.log
#SCRON --open-mode=append
* * * * * $DTS_SCRIPT_DIR/workflow_test.sh
