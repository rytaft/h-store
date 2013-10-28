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
    "reconfig-perf --client.threads_per_host=1 --client.count=4 --exp-suffix=conc-c4t1 "  \
    "reconfig-perf --client.threads_per_host=2 --client.count=4 --exp-suffix=conc-c4t2 "  \
    "reconfig-perf --client.threads_per_host=4 --client.count=4 --exp-suffix=conc-c4t4 "  \
#    "reconfig-perf --client.threads_per_host=8 --client.count=4 --exp-suffix=conc-c4t8 "  \
)
OLD_EXP_TYPES=( \
    "reconfig-perf --client.threads_per_host=8 --client.count=4 --exp-suffix=conc-c4t8 "  \
    "reconfig-perf --client.threads_per_host=12 --client.count=4 --exp-suffix=conc-c4t12 "  \
    "reconfig-perf --client.threads_per_host=2 --client.count=2 --exp-suffix=conc-c2t2 "  \
    "reconfig-perf --client.threads_per_host=4 --client.count=2 --exp-suffix=conc-c2t4 "  \
    "reconfig-perf --client.threads_per_host=8 --client.count=2 --exp-suffix=conc-c2t8 "  \
    "reconfig-perf --client.threads_per_host=16 --client.count=2 --exp-suffix=conc-c2t16 "  \
    "reconfig-perf --client.threads_per_host=1 --client.count=6 --exp-suffix=conc-c6t1 "  \
    "reconfig-perf --client.threads_per_host=2 --client.count=6 --exp-suffix=conc-c6t2 "  \
    "reconfig-perf --client.threads_per_host=4 --client.count=6 --exp-suffix=conc-c6t4 "  \
    "reconfig-perf --client.threads_per_host=8 --client.count=6 --exp-suffix=conc-c6t8 "  \

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
        --client.interval=30000 \
        --client.output_interval=true \
        --client.duration=300000 \
        --client.warmup=60000 \
        --client.output_results_csv=interval_res.csv \
	--overwrite \
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
