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
    
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_16kb --chunksize=16 --asyncsize=16 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_64kb --chunksize=64 --asyncsize=64 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_256kb --chunksize=256 --asyncsize=256 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_512kb --chunksize=512 --asyncsize=512 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_1mb --chunksize=1000 --asyncsize=1000 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_2mb --chunksize=2000 --asyncsize=2000 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_5mb --chunksize=5000 --asyncsize=5000 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_10mb --chunksize=10000 --asyncsize=10000 --asyncdelay=150 --splitplan=5  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s5_20mb --chunksize=20000 --asyncsize=20000 --asyncdelay=150 --splitplan=5  --plandelay=100  " 

    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s3 --chunksize=10000 --asyncsize=8000 --asyncdelay=150 --splitplan=3  --plandelay=100  " 
    "reconfig-ycsb-uniform --exp-suffix=par-lc2_s4 --chunksize=10000 --asyncsize=8000 --asyncdelay=150 --splitplan=4  --plandelay=100  " 

    
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
        --client.duration=420000 \
        --client.warmup=30000 \
        --partitions=16 \
        --benchmark-size=10000000 \
         --reconfig=30000:contract2:0 \
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
