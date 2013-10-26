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
    "reconfig-motivation  --global.hasher_plan=scripts/reconfiguration/plans/tpcc-reconfig-motivation-1.json --partitions=2  --results-dir=${DATA_DIR} --exp-suffix w2" \
    "reconfig-motivation --global.hasher_plan=scripts/reconfiguration/plans/tpcc-reconfig-motivation-2.json --partitions=2  --results-dir=${DATA_DIR} --exp-suffix w4" \
    "reconfig-motivation --global.hasher_plan=scripts/reconfiguration/plans/tpcc-reconfig-motivation-3.json --partitions=2  --results-dir=${DATA_DIR} --exp-suffix w8" \
    "reconfig-motivation --global.hasher_plan=scripts/reconfiguration/plans/tpcc-reconfig-motivation-4.json --partitions=2  --results-dir=${DATA_DIR} --exp-suffix w16" \
)

#for b in smallbank tpcc seats; do
for b in tpcc; do
# for b in seats; do
    PARAMS=( \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
        --client.interval=10000 \
        --client.output_interval=false \
        --client.duration=300000 \
        --client.warmup=30000 \
	--overwrite \
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
