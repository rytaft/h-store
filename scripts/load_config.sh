#!/bin/bash

config_path=$1
benchmark=$2

cp $config_path/*.json ~/h-store
cp $config_path/$benchmark.properties properties/benchmarks/$benchmark.properties
./$config_path/commands.sh prepare
~/h-store/scripts/deploy.sh config
