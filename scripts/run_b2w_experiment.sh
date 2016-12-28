#!/bin/bash -x

# load the config, load the data, then run this script from the h-store root directory
# e.g.
# $ ~/h-store/scripts/load_config.sh configs/b2w-18p/ b2w
# $ ~/h-store/configs/b2w-18p/commands.sh load

clients="istc5 istc7 istc11 istc12"
config=b2w-30p

# 6000000 ms = 100 minutes
INTERVAL=6000000
offset_low=0
offset_high=$INTERVAL
for i in 0 1 2 3 4 5 6 7 8 9
do
    HR=`expr \( $offset_low / 3600000 \)`
    if [ $HR -lt 10 ]; then HR=0${HR}; fi
    MM=`expr \( $offset_low % 3600000 \) / 60000`
    if [ $MM -lt 10 ]; then MM=0${MM}; fi

    cp configs/${config}/b2w.properties_2016_07_01_${HR}${MM}00 properties/benchmarks/b2w.properties
    ./scripts/deploy-file.sh properties/benchmarks/b2w.properties
    data_path=/data/rytaft/client_ops_2016_07_01_${HR}${MM}00_4m
    ./scripts/load_client_operations.sh $data_path "$clients"
    ./configs/${config}/commands.sh run
    ./scripts/save_results.sh results/${config}_2016_07_01_${HR}${MM}00

    offset_low=$offset_high
    offset_high=`expr $offset_high + $INTERVAL`
done
