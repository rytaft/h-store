#!/bin/bash


if [ $# -lt 4 ]
then
	echo "Allocates partitions starting from server da${init_server}"
	echo "Parameters"
	echo "1) benchmark type (e.g. tpcc, ycsb)"
	echo "2) initial server"
	echo "3) number of servers"
	echo "4) number of partitions per server"
	echo "5) servers to skip (optional)"
	exit
fi

cmd="ant hstore-prepare -Dproject=$1 -Dhosts=\""
init_server=$2
num_servers=$3
part_per_server=$4
skip_list="${@:5}"

curr_host=0
curr_part=0
curr_server=$init_server
first_loop=""

for (( var=1; var<=$num_servers; var++ ))
do
	end_part=`expr $curr_part + $part_per_server - 1`
	if [[ $skip_list =~ $curr_server ]]
	then
		var=`expr $var - 1`
	else
		if [ "$curr_server" -lt "10" ]
		then
			cmd="${cmd}${first_loop}da0${curr_server}:${curr_host}:${curr_part}-${end_part}"
		else
			cmd="${cmd}${first_loop}da${curr_server}:${curr_host}:${curr_part}-${end_part}"
		fi
		curr_part=`expr $curr_part + $part_per_server`
		curr_host=`expr $curr_host + 1`
		first_loop=";"
	fi
	curr_server=`expr $curr_server + 1`
done

echo "$cmd\""
set -x
eval "$cmd\""
