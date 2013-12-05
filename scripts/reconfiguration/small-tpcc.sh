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
    "reconfig-tpcc-small --partitions=8 --benchmark-size=32 --exp-suffix=0.1 --client.scalefactor=0.1 --results-dir=${DATA_DIR}"  \
    "reconfig-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.2  --client.scalefactor=0.2 --results-dir=${DATA_DIR}" \
    "reconfig-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.5  --client.scalefactor=0.5 --results-dir=${DATA_DIR}" \
    "reconfig-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.3  --client.scalefactor=0.3 --results-dir=${DATA_DIR}" \
    "stopcopy-tpcc-small --partitions=8 --benchmark-size=32 --exp-suffix=0.1 --client.scalefactor=0.1 --results-dir=${DATA_DIR}"  \
    "stopcopy-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.2  --client.scalefactor=0.2 --results-dir=${DATA_DIR}" \
    "stopcopy-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.5  --client.scalefactor=0.5 --results-dir=${DATA_DIR}" \
    "stopcopy-tpcc-small --partitions=8 --benchmark-size=32  --exp-suffix=0.3  --client.scalefactor=0.3 --results-dir=${DATA_DIR}" \
)

for b in tpcc; do
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
        --client.duration=280000 \
        --client.warmup=10000 \
        --client.output_results_csv=interval_res.csv \
        --client.txnrate=5 \
        --reconfig=195000:2min:0
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
