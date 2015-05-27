#!/bin/bash

for s in istc2 istc4 istc5 istc6 istc7 istc8 istc9 istc10 istc11 istc12 
do
	echo "copying to $s"
        if [[ "$s" != "$HOSTNAME" ]]
        then
		#scp *.json $s:/localdisk/mserafini/h-store > /dev/null
		scp $1 $s:~/h-store/$1
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying json plans $s"
			echo "$?"
			exit 1
		fi 
	fi
done
