#!/bin/bash

CMD=${1?"Please specify the t5 jwf command to run"}
ENV=${2?"Please provide the environment to load before running"}
CONFIG=${3-"config.yaml"}

module load python
conda activate $ENV

cd $JWF_RUN_DIR

echo "`date +"%Y-%m-%dT%H:%M:%S"` - $PWD - t5 jwf $CMD"

t5 jwf $CMD $CONFIG
