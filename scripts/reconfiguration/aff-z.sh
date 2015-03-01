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
    "affinity-ms --partitions=32 --benchmark-size=z --exp-suffix=32z" \
    "affinity-ms --partitions=24 --benchmark-size=z --exp-suffix=24z" \
    "affinity-ms-sk2 --partitions=32 --benchmark-size=z --exp-suffix=32zsk2" \
    "affinity-ms-sk2 --partitions=24 --benchmark-size=z --exp-suffix=24zsk2" \
    "affinity-ms-sk3 --partitions=32 --benchmark-size=z --exp-suffix=32zsk3" \
    "affinity-ms-sk3 --partitions=24 --benchmark-size=z --exp-suffix=24zsk3" \
    "affinity-ms-sk4 --partitions=32 --benchmark-size=z --exp-suffix=32zsk4" \
    "affinity-ms-sk4 --partitions=24 --benchmark-size=z --exp-suffix=24zsk4" \
    "affinity-ms-sk5 --partitions=32 --benchmark-size=z --exp-suffix=32zsk5" \
    "affinity-ms-sk5 --partitions=24 --benchmark-size=z --exp-suffix=24zsk5" \
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
