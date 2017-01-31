#!/bin/bash

for s in istc1 istc5 istc7 istc11 istc12 
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
	fi
done
