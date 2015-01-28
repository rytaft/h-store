#!/bin/bash

for s in da01 da02 da04 da05 da06 da07 da08 da09 da10 da11 da12 da14 da15
do
	echo "copying to $s"
        if [[ "$s" != "$HOSTNAME" ]]
        then
		scp *.json $s:/localdisk/mserafini/h-store > /dev/null
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying json plans $s"
			echo "$?"
			exit 1
		fi 
		scp *.jar $s:/localdisk/mserafini/h-store > /dev/null
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying jar files $s"
			echo "$?"
			exit 1
		fi
		if [[ "$1" != "config" ]]  
		then
			rm -rf $s:/localdisk/mserafini/h-store/obj/release/* > /dev/null
			if [[ $? -ne 0 ]]
			then
				echo "Failed removing executables from $s"
				echo "$?"
				exit 1
			fi 
			scp -r obj/release/* $s:/localdisk/mserafini/h-store/obj/release > /dev/null
			if [[ $? -ne 0 ]]
			then
				echo "Failed copying executables to $s"
				echo "$?"
				exit 1
			fi 
		fi
	fi
done
