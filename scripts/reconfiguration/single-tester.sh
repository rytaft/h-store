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
    "reconfig-test --client.threads_per_host=1 --client.count=4 --exp-suffix=c4t1 "  \
    "reconfig-test --client.threads_per_host=2 --client.count=4 --exp-suffix=c4t2 "  \
    "reconfig-test --client.threads_per_host=4 --client.count=4 --exp-suffix=c4t4 "  \
    "reconfig-test --client.threads_per_host=8 --client.count=4 --exp-suffix=c4t8 "  \
    "reconfig-test --client.threads_per_host=12 --client.count=4 --exp-suffix=c4t12 "  \
    "reconfig-test --client.threads_per_host=2 --client.count=2 --exp-suffix=c2t2 "  \
    "reconfig-test --client.threads_per_host=4 --client.count=2 --exp-suffix=c2t4 "  \
    "reconfig-test --client.threads_per_host=8 --client.count=2 --exp-suffix=c2t8 "  \
    "reconfig-test --client.threads_per_host=16 --client.count=2 --exp-suffix=c2t16 "  \
    "reconfig-test --client.threads_per_host=1 --client.count=6 --exp-suffix=c6t1 "  \
    "reconfig-test --client.threads_per_host=2 --client.count=6 --exp-suffix=c6t2 "  \
    "reconfig-test --client.threads_per_host=4 --client.count=6 --exp-suffix=c6t4 "  \
    "reconfig-test --client.threads_per_host=8 --client.count=6 --exp-suffix=c6t8 "  \
)

#for b in smallbank tpcc seats; do
for b in tpcc ycsb; do
# for b in seats; do
    PARAMS=( \
        --no-update \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --partitions=1 \
        --client.interval=5000 \
        --client.output_interval=true \
        --client.duration=60000 \
        --client.warmup=60000 \
        --client.output_results_csv=interval_res.csv \
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
