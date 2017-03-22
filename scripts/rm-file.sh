#!/bin/bash

for s in istc1 istc2 istc3 istc4 istc5 istc6 istc7 istc8 istc12 istc13
do
	echo "deleting from $s"
        if [[ "$s" != "$HOSTNAME" ]]
        then
		#scp *.json $s:/localdisk/mserafini/h-store > /dev/null
		ssh $s 'rm ~/h-store/'"$1"''
		if [[ $? -ne 0 ]]
		then
			echo "Failed to delete $1 from $s"
			echo "$?"
		fi 
	else
	    rm $1
	fi
done
