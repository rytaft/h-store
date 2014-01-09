#!/bin/bash

if [ -z "$3" ]
then
	echo "enter date as three parameters: DD MM YYYY"
	exit
fi

set -v

rm projectcounts-*
rm *-tmp.txt
rm output-*

wget -O month-index-tmp.txt http://dumps.wikimedia.org/other/pagecounts-raw/$3/$3-$2/
grep projectcounts-$3$2$1 month-index-tmp.txt | cut -c16-44 > list-files-tmp.txt

awk '{print "http://dumps.wikimedia.org/other/pagecounts-raw/2013/2013-10/" $0}' list-files-tmp.txt > list-urls-tmp.txt
wget -i list-urls-tmp.txt

for f in projectcounts-* 
do
	grep '^en ' $f | cut -d ' ' -f 3 >> "output-$3-$2-$1"
done

rm projectcounts-*
rm *-tmp.txt
