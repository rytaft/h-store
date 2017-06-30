#!/bin/bash

mkdir $1
cp plan_out.json $1
cp -r obj/logs $1-logs
cp results.csv $1
cp out.log $1
cp hevent.log $1

for s in istc1 istc2 istc4 istc5 istc6 istc7 istc8 istc9 istc10 istc11 istc12 istc13 
do
        echo "copying hevent.log from $s"
        if [[ "$s" != "$HOSTNAME" ]]
        then
                scp $s:~/h-store/hevent.log $1/hevent-${s}.log
                if [[ $? -ne 0 ]]
                then
                        echo "Failed to copy hevent.log from $s"
                        echo "$?"
                fi 
        else
            cp ~/h-store/hevent.log $1/hevent-${s}.log
        fi
done
