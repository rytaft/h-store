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
#    "reconfig-dynsplit-becca --partitions=2 --benchmark-size=2 --splitplan=4 --plandelay=100 --chunksize=4048 --asyncsize=348 --asyncdelay=100 "
    "reconfig-dynsplit-fine-grained --partitions=12 --benchmark-size=36 --splitplan=4 --plandelay=100 --chunksize=10048 --asyncsize=8048 --asyncdelay=500 --global.hasher_plan=scripts/reconfiguration/plans/tpcc-size36-12-fine.json" 
    )

for b in tpcc; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
        --plot \
	    --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=300000 \
        --client.warmup=10000 \
        --client.output_results_csv=interval_res.csv \
        --reconfig=60000:1:0 \
        --sweep-reconfiguration 
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
