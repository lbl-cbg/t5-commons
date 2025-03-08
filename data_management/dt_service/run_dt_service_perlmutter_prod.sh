#!/bin/bash

ENV=${1?"Please provide the environment to load before running"}

module load python
conda activate $ENV

TIME_TO_RUN=2592000  # We want dt_service to run for 30 days
cd $DTS_LOG_DIR

echo "`date +"%Y-%m-%dT%H:%M:%S"` dt-service starting with environment $ENV"

dt-service -D t5 -t 1 -f perlmutter -k ingest,copy,tar,md5 -r $TIME_TO_RUN $JAMO_URL -l prod
