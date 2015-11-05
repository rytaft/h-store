#!/bin/bash

mkdir $1
cp *.json $1
cp *-partition*.log $1
cp -r obj/logs $1
cp *.csv $1
cp out.log $1
cp hevent.log $1
