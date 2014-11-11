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
    #"nonopt-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2 --reconfig=30000:spread2:0 " 
    #"split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2 --reconfig=30000:spread2:0 " 
    #"eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2 --reconfig=30000:spread2:0 " 
    #"reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2 --reconfig=30000:spread2:0 " 
    #"reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2_s5 --splitplan=5 --reconfig=30000:spread2:0 "

    #"nonopt-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2 --reconfig=30000:contract2:0 " 
    #"split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2 --reconfig=30000:contract2:0 " 
    #"eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2 --reconfig=30000:contract2:0 " 
    #"reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2 --reconfig=30000:contract2:0 " 
    #"reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2_s5 --splitplan=5  --reconfig=30000:contract2:0 " 

    #"nonopt-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2_s5 --splitplan=5 --reconfig=30000:spread2:0 "
    #"split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2_s5 --splitplan=5 --reconfig=30000:spread2:0 "
    #"eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=ls2_s5 --splitplan=5 --reconfig=30000:spread2:0 "
    
    "nonopt-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2_s5 --splitplan=5 --reconfig=30000:contract2:0 " 
    "split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2_s5 --splitplan=5 --reconfig=30000:contract2:0 " 
    "eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=lc2_s5 --splitplan=5 --reconfig=30000:contract2:0 " 

    "nonopt-ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf --reconfig=30000:shuffle:0 " 
    "split-merge-only--ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf --reconfig=30000:shuffle:0 " 
    "eager-pull-only-ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf --reconfig=30000:shuffle:0 " 
    "reconfig-ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf  --reconfig=30000:shuffle:0 "
    "reconfig-ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf_s5 --splitplan=5  --reconfig=30000:shuffle:0 "
    "split-merge-only--ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf_s5 --splitplan=5  --reconfig=30000:shuffle:0 " 
    "eager-pull-only-ycsb-uniform --partitions=16 --benchmark-size=10000000 --exp-suffix=shuf_s5 --splitplan=5  --reconfig=30000:shuffle:0 "  
  
    "nonopt-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "reconfig-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp_s5 --splitplan=5  --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "split-merge-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp_s5 --splitplan=5   --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json" 
    "eager-pull-only-ycsb-zipf --partitions=16 --benchmark-size=10000000 --exp-suffix=exp_s5 --splitplan=5   --reconfig=30000:expand:0 --global.hasher_plan=scripts/reconfiguration/plans/y-l-expand.json"   
    

)


for b in ycsb; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --exp-attempts=1 \        
        --no-json \
#        --plot \
        --sweep-reconfiguration \
        --client.interval=1000 \
        --client.output_interval=true \
        --client.duration=420000 \
        --client.warmup=30000 \
        --plandelay=200 \
        --chunksize=10000 \
        --asyncsize=8000 \ 
        --asyncdelay=150 \ 
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
