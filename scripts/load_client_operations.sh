#!/bin/bash -x

path=$1
clients=$2

i=0
for s in $clients
do
    if [[ "$s" != "$HOSTNAME" ]]
    then
	scp $path/b2w_sample_operations_$i.txt $s:~/h-store/b2w_sample_operations.txt
    else
	cp $path/b2w_sample_operations_$i.txt ~/h-store/b2w_sample_operations.txt
    fi
    i=`expr $i + 1`
done