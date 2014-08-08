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
FIRST_PARAM_OFFSET=0

EXP_TYPES=( 
    "stopcopy-ycsb-zipf --partitions=12 --benchmark-size=20000000 --exp-suffix=xl --reconfig=20000:2:0 " 
    "reconfig-ycsb-zipf --partitions=12 --benchmark-size=20000000 --exp-suffix=xl --reconfig=20000:2:0 " 
)

#    "stopcopy-ycsb-uniform --partitions=8 --benchmark-size=20000000 --exp-suffix=xl1contract --reconfig=2565000:1:0 " \
#    "reconfig-ycsb-unirom --partitions=8 --benchmark-size=20000000 --exp-suffix=xl1contract --reconfig=2565000:1:0 " \



for b in ycsb; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
        --plot \
        --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=300000 \
        --client.warmup=30000 \
        --client.output_results_csv=interval_res.csv
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
