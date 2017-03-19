#!/bin/bash -x

# load the config, load the data, then run this script from the h-store root directory
# e.g.
# $ ~/h-store/scripts/load_config.sh configs/b2w-18p/ b2w
# $ ~/h-store/configs/b2w-18p/commands.sh load
# $ ./scripts/run_b2w_experiment.sh

# DEFAULTS:
clients="istc5 istc6 istc7 istc8"
config=b2w-30p
YYYY=2016
MM=07
DD=01
offset=0
end_iter=9

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
    *)
            # unknown option
    ;;
esac
done

echo "clients  = ${clients}"
echo "config   = ${config}"
echo "date     = ${YYYY}_${MM}_${DD}"
echo "offset   = ${offset}"
echo "end_iter = ${end_iter}"

# 3000000 ms = 50 minutes
INTERVAL=3000000
for i in $(seq 0 ${end_iter})
do
    hr=`expr \( $offset % 86400000 \) / 3600000`
    if [ $hr -lt 10 ]; then hr=0${hr}; fi
    mm=`expr \( $offset % 3600000 \) / 60000`
    if [ $mm -lt 10 ]; then mm=0${mm}; fi
    ss=`expr \( $offset % 60000 \) / 1000`
    if [ $ss -lt 10 ]; then ss=0${ss}; fi
    date="${YYYY}_${MM}_${DD}_${hr}${mm}${ss}"

    cp configs/${config}/b2w.properties properties/benchmarks/b2w.properties
    echo "start_offset = $offset" >> properties/benchmarks/b2w.properties
    ./scripts/deploy-file.sh properties/benchmarks/b2w.properties
    data_path=/data/rytaft/client_ops_${date}_4m
    ./scripts/load_client_operations.sh $data_path "$clients"
    ./configs/${config}/commands.sh run
    ./scripts/save_results.sh results/${config}_${date}

    offset=`expr $offset + $INTERVAL`
done
