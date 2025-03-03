#!/bin/bash

#ls -a

#. ./.env
export $(cat .env | xargs)

python main.py