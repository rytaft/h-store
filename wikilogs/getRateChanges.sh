#!/bin/bash

if [ -z "$2" ]
then
	echo "enter file with entries"
	exit
fi

rm rates*

first=1
rate=0

cat $1 | while read line
do
	if [ "$first" -eq "1" ]
	then
		first=0
		lastVal=$line
	else
		rate=$(echo "e((1 / $2)*l($line / $lastVal))" | bc -l)
		lastVal=$line
		for i in `seq "$2"`
		do 
			echo $rate >> rates-$1
			echo $i
		done
	fi
done
