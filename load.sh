#!/bin/bash

if [ $# -lt 6 ]
then
        echo "Load benchmark in the cluster (EStore project)"
        echo "Parameters"
        echo "1) benchmark type (e.g. ycsb, voter)"
        echo "2) skew (uniform, high-skew, low-skew)"
        echo "3) number of tuples"
        echo "4) number of partitions per server"
        echo "2) initial server"
        echo "3) number of servers"
        echo "5) servers to skip (optional)"
        echo
        echo "Press ENTER to continue with default parameters"
        read
        exit
fi

if [ -z "$1" ]
then
        bench="ycsb"
else
        bench=$1
fi

if [ -z "$2" ]
then
        skew="high-skew"
else
        skew=$2
fi

if [[ $2 =~ '(uniform|high-skew|low-skew)' ]]
then
        echo "incorrect skew"
        exit
fi

if [ $2 = "uniform" ]
then
        sed -i".bak" '/requestdistribution/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/skew_factor/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/num_hot_spots/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/percent_accesses_to_hot_spots/d' properties/benchmarks/ycsb.properties
        echo "requestdistribution=uniform" >> properties/benchmarks/ycsb.properties
fi

if [ $2 = "low-skew" ]
then
        sed -i".bak" '/requestdistribution/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/skew_factor/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/num_hot_spots/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/percent_accesses_to_hot_spots/d' properties/benchmarks/ycsb.properties
        echo "requestdistribution=zipfian" >> properties/benchmarks/ycsb.properties
        echo "skew_factor = 0.65" >> properties/benchmarks/ycsb.properties
fi

if [ $2 = "high-skew" ]
then
        sed -i".bak" '/requestdistribution/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/skew_factor/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/num_hot_spots/d' properties/benchmarks/ycsb.properties
        sed -i".bak" '/percent_accesses_to_hot_spots/d' properties/benchmarks/ycsb.properties
        echo "requestdistribution=zipfian" >> properties/benchmarks/ycsb.properties
        echo "skew_factor = 0.65" >> properties/benchmarks/ycsb.properties
        echo "num_hot_spots=30" >> properties/benchmarks/ycsb.properties
        echo "percent_accesses_to_hot_spots=0.80" >> properties/benchmarks/ycsb.properties
fi

if [ -z "$3" ]
then
        tuples=60000000
else
        tuples=$3
fi
sed -i".bak" '/num_records/d' properties/benchmarks/ycsb.properties
echo "num_records = $tuples" >> properties/benchmarks/ycsb.properties

if [ -z "$4" ]
then
        part_per_server=6
else
        part_per_server=$4
fi

if [ -z "$5" ]
then
        init_server=5
else
        init_server=$5
fi

if [ -z "$6" ]
then
        num_servers=5
else
        num_servers=$6
fi

skip_list="${@:7}"
partitions=$(( $part_per_server * $num_servers ))

python scripts/reconfiguration/plan-generator.py -t ycsb -s $tuples -p $partitions > plan.json
./prepare.sh $bench $init_server $num_servers $part_per_server $skip_list

ant hstore-benchmark -Dproject=$bench -Dglobal.hasher_plan=plan.json -Dglobal.hasher_class=edu.brown.hashing.TwoTieredRangeHasher -Dnoshutdown=true -Dnoexecute=true -Dsite.txn_restart_limit_sysproc=100 -Dsite.jvm_asserts=false -Dsite.reconfig_live=false | tee load.log