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
    
    "stopcopy-ycsb-uniform --partitions=8 --benchmark-size=10000000 --exp-suffix=expand --reconfig=30000:expand:0 " 
    "reconfig-ycsb-uniform --partitions=8 --benchmark-size=10000000 --exp-suffix=expand --reconfig=30000:expand:0 " 
    "reconfig-ycsb-uniform --partitions=8 --benchmark-size=10000000 --exp-suffix=expand_s10 --splitplan=10 --reconfig=30000:expand:0 " 
    "reconfig-ycsb-uniform --partitions=8 --benchmark-size=10000000 --exp-suffix=expand_s20 --splitplan=20 --reconfig=30000:expand:0 " 

)




for b in ycsb; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
        --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=360000 \
        --client.warmup=30000 \
        --plandelay=200 \
       --chunksize=10000 \
        --asyncsize=8000 \ 
        --asyncdelay=150 \ 
        --client.output_results_csv=interval_res.csv \
        --global.hasher_plan=scripts/reconfiguration/plans/ycsb-size10000000-8-expand.json
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
