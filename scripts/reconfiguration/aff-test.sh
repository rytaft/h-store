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
    "affinity-dyn-b1000-t10000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b1000-t1000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b1000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b500-t10000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b500-t1000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b500 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b10-t10000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b10-t1000 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \
    "affinity-dyn-b10 --partitions=12 --benchmark-size=ss --exp-suffix=ss" \

#    "affinity-dyn --partitions=12 --benchmark-size=xs --exp-suffix=xs" \
#    "affinity-dyn --partitions=12 --benchmark-size=s --exp-suffix=s" \
#    "affinity-dyn --partitions=12 --benchmark-size=m --exp-suffix=m" \
#    "affinity-dyn --partitions=12 --benchmark-size=l --exp-suffix=l" \
#    "affinity-dyn --partitions=4 --benchmark-size=xs2 --exp-suffix=xs2" \
#    "affinity-dyn --partitions=4 --benchmark-size=s2 --exp-suffix=s2" \
#    "affinity-dyn --partitions=4 --benchmark-size=m2 --exp-suffix=m2" \
#    "affinity-dyn --partitions=4 --benchmark-size=l2 --exp-suffix=l2" \
)

for b in affinity; do
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
        --affinity=30000 \
        --sweep-reconfiguration 
    )
        #--global.hasher_plan=plan_affinity.json \
    
    i=0
    cnt=${#EXP_TYPES[@]}
    while [ "$i" -lt "$cnt" ]; do
        ./experiment-runner.py $FABRIC_TYPE ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
            --exp-type=${EXP_TYPES[$i]}
        FIRST_PARAM_OFFSET=0
        i=`expr $i + 1`
    done

done
