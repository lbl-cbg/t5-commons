#!/bin/bash -f
if [ $# -eq 2 ] && [ $1 = "cd" ]; then
   echo 'cd `$JAMO_DIR/bin/jat '$*'`'
   else
   echo '$JAMO_DIR/bin/jat '$*''
fi 
