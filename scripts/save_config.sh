#!/bin/bash

mkdir $1
cp plan.json $1
cp $2.jar $1
cp properties/benchmarks/$2.properties $1
