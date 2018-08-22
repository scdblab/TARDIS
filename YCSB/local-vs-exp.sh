#!/bin/bash

CACHE_IP="10.0.0.210"
MONGO_IP="10.0.1.45"
CLIENT_IP="10.0.0.210"

ARCHIVE="/home/hieun/Desktop/archive"
RESULT="/home/hieun/Desktop/results"
TIME="40"
DB_FAIL="0,20"

for try in {1..1}
do
	for thread in 1 #2 4 8 16 32 64
	do
		# restart cache
		ssh $CACHE_IP "killall twemcache"
		ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/IQ-Twemcached/src/twemcache -t 8 -c 4096 -m 10000 > /dev/null 2>&1 &" &
	
		#sleep 2
		
		# start stats
		#bash "startstats.sh"

		# warm-up cache
		echo "Performing warm up..."
		bin/ycsb load mongodb-mc-redlease -P workloads/workloada -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$CACHE_IP:11211 -p fullwarmup=true


		echo "Running experiment..."
		# run
		#bin/ycsb run mongodb-mc-redlease -P workloads/workloada -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$CACHE_IP:11211 -p fullwarmup=true -p writeback=false -p numarworker=10 -p dbfail=$DB_FAIL -s -threads $thread -p maxexecutiontime=$TIME -p ewstats=false -p monitorspacewrite=true 2>&1 | tee $RESULT/"run" 

		# copy cache stats
		#{ sleep 2; echo "stats"; sleep 2; echo "stats slabs"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $RESULT/"cachestats.txt"

		# stop stats
		#bash "copystats.sh"

		#folder="normal-try$try-th$thread"
		#mkdir -p $ARCHIVE/$folder
		#mv $RESULT/* $ARCHIVE/$folder/
		#eval "java -jar graph.jar $ARCHIVE/$folder $CACHE_IP $MONGO_IP $CLIENT_IP"
	done
done
