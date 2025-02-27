#!/bin/bash

#SCRON -q cron
#SCRON --constraint=cron

URL="https://data-dev.taskforce5.lbl.gov"

export BIN_HOME=$CFS/m4521/jamo_code/dt_service/scripts
export DT_ERROR=$CFS/m4521/jamo_code/dt_service/dt_cron_perlmutter.log

cd $HOME
jamo_status=$(curl -k -s $URL/api/tape/diskusage | grep files)
FILE_NAME=run_dt_service_perlmutter_prod.bash

function get_job_id {
    for job_id in $(squeue --me -q cron -t RUNNING -O JobID | tail -n +2); do
        if [[ ! -z "$(scontrol show jobid -dd $job_id | grep JobName | grep $FILE_NAME)" ]]; then
            return
        fi
    done
    job_id=0
}

get_job_id

if [[ "$jamo_status" == "" ]] ; then

    echo $(date) $0 JAMO down >> $DT_ERROR 2>&1

elif [[ -f "STOP" ]] ; then

    # if STOP is in the home directory
    if [[ $job_id -gt 0 ]]; then
          echo $(date) $0 STOP set, stopping any running services >> $DT_ERROR 2>&1
          scancel -f -s SIGINT $job_id >> $DT_ERROR 2>&1
    fi

elif [[ $job_id -eq 0 ]]; then

    echo $(date) $0 start dt_service Perlmutter prod >> $DT_ERROR 2>&1
    cd $BIN_HOME/LOGS >> $DT_ERROR 2>&1

    sbatch $BIN_HOME/$FILE_NAME >| $DT_ERROR 2>&1

fi
