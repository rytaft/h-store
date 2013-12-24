#!/bin/bash

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    exit $exit_status
}

# ---------------------------------------------------------------------

DATA_DIR="out"
FABRIC_TYPE="ssh"
FIRST_PARAM_OFFSET=1

EXP_TYPES=( \
    "reconfig-ycsb-zipf --partitions=4"
    "reconfig-ycsb-zipf --partitions=8"
    "reconfig-ycsb-zipf --partitions=16"
)

#for b in smallbank tpcc seats; do
for b in ycsb; do
# for b in seats; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \          
        --no-json \
        --benchmark-size=4000000 \
        --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=120000 \
        --client.warmup=60000 \
        --client.output_results_csv=interval_res.csv \
        --reconfig=150000:2:0
    )
    
    i=0
    cnt=${#EXP_TYPES[@]}
    while [ "$i" -lt "$cnt" ]; do
        ./experiment-runner.py $FABRIC_TYPE ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
            --exp-type=${EXP_TYPES[$i]}
        FIRST_PARAM_OFFSET=0
        i=`expr $i + 1`
    done

done
