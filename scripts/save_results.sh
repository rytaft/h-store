#!/bin/bash

mkdir $1
cp plan_out.json $1
cp *-partition*.log $1
cp obj/logs/sites/site-* $1
cp *.csv $1
cp out.log $1
