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
    "reconfig-ycsb-zipf  --exp-suffix=par-s1_64kb --chunksize=64 --asyncsize=64 --asyncdelay=150 --splitplan=1  --plandelay=100  "
#    "reconfig-ycsb-zipf  --exp-suffix=par-s1_64kb --chunksize=1000 --asyncsize=1000 --asyncdelay=150 --splitplan=1  --plandelay=100  "
      
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
        --plot \
        --partitions=2 \
        --benchmark-size=100000 \
        --reconfig=10000:3:0 \
        --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=60000 \
        --client.warmup=5000 \
       
#        --site.reconfig_adaptive=true \
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
