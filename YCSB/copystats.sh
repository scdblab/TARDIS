#!/bin/bash

source machines.sh

machines=( "${CACHE_IPS[@]}" "${CLIENT_IPS[@]}" $MONGO_IP )
dir="/home/hieun/Desktop/results"

this="localhost"
for m in ${machines[@]}
do
	echo "Copy stats "$m

	ssh $m "killall sar"
	if [ "$m" != "$this" ]
	then
		# copy
		scp $m:$dir/* $dir
		ssh $m "rm -r $dir/*"	
	fi
done

