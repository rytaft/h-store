#!/bin/bash

for i in {2..13}
do
	ssh istc$i "rm ~/h-store/*.log"
done
