#!/bin/bash

declare -A caches
caches=( ["/home/hieun/Desktop/EW/IQ-Twemcached/src/twemcache"]="twemcache" )
#["/home/hieun/Desktop/EW/memcached-1.4.36/memcached"]="memcached")
#caches=( ["/home/hieun/Desktop/EW/CAMPServer/src/twemcache"]="camp" ) 
#["/home/hieun/Desktop/EW/memcached-1.4.36/memcached"]="memcached" )
#caches=( ["/home/hieun/Desktop/EW/memcached-1.4.36/memcached"]="memcached" )

MONGO_IP="10.0.1.45"
TIME="10000"
DB_FAIL="0,10000"

for c in "${!caches[@]}";
do
	echo $c
done
#exit 1

for c in "${!caches[@]}";
do
	for cachesize in "260" "280" "300"
	do
		for thread in "1"
		do
			# restart cache
			killall twemcache
			killall memcached

			sleep 2

			let max_item_size=$((cachesize / 4 * 1024 * 1024));
			echo $max_item_size
			cmd="$c -m $cachesize -t 4 -c 4096 -M 0 > /dev/null 2>&1 &"
			echo $cmd
			eval $cmd

			sleep 2

			# run
			bin/ycsb run mongodb-mc-redlease -P workloads/workloada -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=localhost:11211 -p writeback=false -p numarworker=10 -p dbfail=$DB_FAIL -s -threads $thread -p maxexecutiontime=$TIME -p reconcachememory=$cachesize -p monitorspacewrite=true 2>&1 | tee output_${caches[$c]}_$cachesize

			{ sleep 2; echo "stats slabs"; sleep 2; echo "stats"; sleep 2; echo "quit"; sleep 1; } | telnet localhost 11211 > "cache_${caches[$c]}_"$cachesize
		done
	done
done
