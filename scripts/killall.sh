#!/bin/bash

#for s in da01 da02 da04 da05 da06 da07 da08 da09 da10 da11 da12 da14 da15
for s in istc10 istc11 istc12 istc13 
do
	ssh $s "killall java"
done
