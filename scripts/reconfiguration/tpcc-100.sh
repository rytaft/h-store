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
    "reconfig-dynsplit-fine-grained --partitions=18 --benchmark-size=100 --exp-suffix=hs_s20 --splitplan=20 --plandelay=200 --asyncdelay=150 --global.hasher_plan=scripts/reconfiguration/plans/tpcc-size100-18-fine.json" 
    "reconfig-dynsplit-fine-grained --partitions=18 --benchmark-size=100 --exp-suffix=s4 --splitplan=4 --plandelay=200 --asyncdelay=150 --global.hasher_plan=scripts/reconfiguration/plans/tpcc-size100-18-fine.json" 
    "reconfig-dynsplit-fine-grained --partitions=18 --benchmark-size=100  --plandelay=100 --asyncdelay=150 --global.hasher_plan=scripts/reconfiguration/plans/tpcc-size100-18-fine.json" 
    "reconfig-dynsplit --partitions=18 --benchmark-size=100 --exp-suffix=s2 --splitplan=2 --plandelay=10 --asyncdelay=10 " 
    "reconfig-dynsplit --partitions=18 --benchmark-size=100 --plandelay=100 --asyncdelay=50 " 
    "stopcopy-dynsplit --partitions=18 --benchmark-size=100  --plandelay=100 --asyncdelay=50 " 
    "reactive-dynsplit --partitions=18 --benchmark-size=100 --plandelay=100 --asyncdelay=50 " 
    "nonopt-dynsplit --partitions=18 --benchmark-size=100 --plandelay=100 --asyncdelay=50 " 
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
        --client.duration=360000 \
        --client.warmup=30000 \
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
