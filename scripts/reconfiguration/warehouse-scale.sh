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

EXP_TYPES=( \
    "reconfig-test --partitions=2 --client.scalefactor=0.1 --results-dir=${DATA_DIR}/scale.1" \
    "reconfig-test --partitions=2 --client.scalefactor=0.2 --results-dir=${DATA_DIR}/scale.2" \
    "reconfig-test --partitions=2 --client.scalefactor=0.5 --results-dir=${DATA_DIR}/scale.5" \
    "reconfig-test --partitions=2 --client.scalefactor=1 --results-dir=${DATA_DIR}/scale1" \
    "reconfig-test --partitions=2 --client.scalefactor=2 --results-dir=${DATA_DIR}/scale2" \
    "reconfig-test --partitions=2 --client.scalefactor=3 --results-dir=${DATA_DIR}/scale4" \
)

#for b in smallbank tpcc seats; do
for b in tpcc; do
# for b in seats; do
    PARAMS=( \
        --no-update \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
	    --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=120000 \
        --client.warmup=10000 \
        --client.output_results_csv=interval_res.csv \
        --client.txnrate=5 \
        --reconfig=95000:2:0
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
