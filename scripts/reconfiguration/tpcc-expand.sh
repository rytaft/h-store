confi#!/bin/bash

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

EXP_TYPES=( \
    "stopcopy-2b --partitions=8 --benchmark-size=16 --exp-suffix=tpc4to8expand --reconfig=185000:1:0" \
    "reconfig-2b --partitions=8 --benchmark-size=16 --exp-suffix=tpc4to8expand --reconfig=185000:1:0" \
)

#for b in smallbank tpcc seats; do
for b in tpcc; do
# for b in seats; do
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
        --client.duration=210000 \
        --client.warmup=30000 \
        --client.output_results_csv=interval_res.csv
        --global.hasher_plan=scripts/reconfiguration/plans/tpcc-size16-8-expand.json \
        
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
