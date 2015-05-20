#!/bin/bash

#for s in istc10 istc11 istc12
for s in istc2 istc4 istc5 istc6 istc7 istc8 istc9 istc10 istc11 istc12 
do
	echo "copying to $s"
        if [[ "$s" != "$HOSTNAME" ]]
        then
		#scp *.json $s:/localdisk/mserafini/h-store > /dev/null
		scp *.json $s:~/h-store > /dev/null
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying json plans $s"
			echo "$?"
			exit 1
		fi 
		#scp *.jar $s:/localdisk/mserafini/h-store > /dev/null
		scp *.jar $s:~/h-store > /dev/null
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying jar files $s"
			echo "$?"
			exit 1
		fi
		#scp properties/benchmarks/* $s:/localdisk/mserafini/h-store/properties/benchmarks > /dev/null
		scp properties/benchmarks/* $s:~/h-store/properties/benchmarks > /dev/null
		if [[ $? -ne 0 ]]
		then
			echo "Failed copying property files $s"
			echo "$?"
			exit 1
		fi
		if [[ "$1" != "config" ]]  
		then
			rm -rf $s:~/h-store/obj/release/* > /dev/null
			#rm -rf $s:/localdisk/mserafini/h-store/obj/release/* > /dev/null
			if [[ $? -ne 0 ]]
			then
				echo "Failed removing executables from $s"
				echo "$?"
				exit 1
			fi 
			#scp -r obj/release/* $s:/localdisk/mserafini/h-store/obj/release > /dev/null
			scp -r obj/release/* $s:~/h-store/obj/release > /dev/null
			if [[ $? -ne 0 ]]
			then
				echo "Failed copying executables to $s"
				echo "$?"
				exit 1
			fi 
		fi
	fi
done
