#!/bin/bash -x

# load the config, load the data, then run this script from the h-store root directory
# e.g.
# $ ~/h-store/scripts/load_config.sh configs/wikipedia-18p/ wikipedia
# $ ~/h-store/configs/wikipedia-18p/commands.sh load
# $ ./scripts/run_wikipedia_experiment.sh

# DEFAULTS:
clients="istc1 istc5 istc6 istc7"
config=wikipedia-36p
YYYY=2016
MM=08
DD=01
data_path=wikilogs/load-08-en

offset=0
end_iter=15

# truncate file
: > agg_load_hist.csv 

for i in "$@"
do
case $i in
    -c=*|--clients=*)
    clients="${i#*=}"
    shift # past argument=value
    ;;
    -C=*|--config=*)
    config="${i#*=}"
    shift # past argument=value
    ;;
    -y=*|--YYYY=*)
    YYYY="${i#*=}"
    shift # past argument=value
    ;;
    -m=*|--MM=*)
    MM="${i#*=}"
    shift # past argument=value
    ;;
    -d=*|--DD=*)
    DD="${i#*=}"
    shift # past argument=value
    ;;
    -o=*|--offset=*)
    offset="${i#*=}"
    shift # past argument=value
    ;;
    -e=*|--end_iter=*)
    end_iter="${i#*=}"
    shift # past argument=value
    ;;
    -p=*|--path=*)
    data_path="${i#*=}"
    shift # past argument=value
    ;;
    *)
            # unknown option
    ;;
esac
done

echo "clients   = ${clients}"
echo "config    = ${config}"
echo "date      = ${YYYY}_${MM}_${DD}"
echo "offset    = ${offset}"
echo "end_iter  = ${end_iter}"
echo "data_path = ${data_path}"

# 18000 s = 5 hours
INTERVAL=18000
for i in $(seq 1 ${end_iter})
do
    DD=`expr \( $offset / 86400 \) + 1`
    if [ $DD -lt 10 ]; then DD=0${DD}; fi
    hr=`expr \( $offset % 86400 \) / 3600`
    if [ $hr -lt 10 ]; then hr=0${hr}; fi
    mm=`expr \( $offset % 3600 \) / 60`
    if [ $mm -lt 10 ]; then mm=0${mm}; fi
    ss=`expr \( $offset % 60 \)`
    if [ $ss -lt 10 ]; then ss=0${ss}; fi
    date="${YYYY}_${MM}_${DD}_${hr}${mm}${ss}"

    cp configs/${config}/wikipedia.properties properties/benchmarks/wikipedia.properties
    echo "start_offset = $offset" >> properties/benchmarks/wikipedia.properties
    ./scripts/deploy-file.sh properties/benchmarks/wikipedia.properties

    file_id=i
    if [ $file_id -lt 10 ]; then file_id=0${file_id}

    file_path=${data_path}/rates-${data_path}-${file_id}.txt
    cp ${file_path} txnrates.txt
    ./scripts/deploy-file txnrates.txt
    ./configs/${config}/commands.sh run
    ./scripts/save_results.sh results/${config}_${date}

    offset=`expr $offset + $INTERVAL`
done

./scripts/save_results_and_logs.sh results/${config}_${date}
