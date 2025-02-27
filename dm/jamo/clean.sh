#!/bin/sh
CONF=$1

jamo-init --clean $CONF/lapinpy.config $CONF/tape.config $CONF/metadata.config
jat-init --clean $CONF/lapinpy.config $CONF/analysis.config
