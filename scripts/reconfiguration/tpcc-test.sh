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
    "reconfig-dynsplit --partitions=4 --benchmark-size=12 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=1000 " 
#    "reconfig-2b --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=1000  --exp-suffix=base" 
    )
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=2000  --exp-suffix=split-10-size-20-delay-2-asyncdelay-2" 
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=5000  --exp-suffix=split-10-size-20-delay-2-asyncdelay-5" 
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=500  --exp-suffix=split-10-size-20-delay-2-asyncdelay-p5" 
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=20000 --asyncsize=20000 --asyncdelay=200  --exp-suffix=split-10-size-20-delay-2-asyncdelay-p2" 
#    )
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=10000 --asyncsize=10000  --exp-suffix=split-10-size-10-delay-2"\
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=5000 --asyncsize=5000  --exp-suffix=split-10-size-5-delay-2"\
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=500 --asyncsize=500  --exp-suffix=split-10-size-.5-delay-2"\
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=1000 --asyncsize=1000  --exp-suffix=split-10-size-1-delay-2"\
#    "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=2000 --chunksize=30000 --asyncsize=2000  --exp-suffix=split-10-size-30l2a-delay-2"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=5000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-10-size-20-delay-5"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=5 --plandelay=5000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-5-size-20-delay-5"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=25 --plandelay=5000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-25-size-20-delay-5"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=50 --plandelay=5000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-50-size-20-delay-5"\    
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=1000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-10-size-20-delay-1"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=10000 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-10-size-20-delay-10"\
    # "reconfig-dynsplit --partitions=2 --benchmark-size=4 --splitplan=10 --plandelay=500 --chunksize=20000 --asyncsize=20000  --exp-suffix=split-10-size-20-delay-.5"\

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
