#!/bin/sh
CONF=$1

lapind init $CONF/lapinpy.config
jamo-init $CONF/lapinpy.config $CONF/tape.config $CONF/metadata.config
jat-init $CONF/lapinpy.config $CONF/analysis.config

lapind run $CONF jamo.tape jamo.metadata jat.analysis
