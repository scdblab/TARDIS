
#!/bin/bash

source machines.sh

ARCHIVE="/home/hieun/Desktop/archive"
RESULT="/home/hieun/Desktop/results"
TIME="300"

RECORDS="100000"

declare -A modes
#modes=( ["failed"]="0,6000" ["normal"]="9000,10000" )
modes=( ["normal"]="9000,10000" )
THREADSS=( "8" )

declare -A partitions
#partitions=( [${CLIENT_IPS[0]}]="16666" [${CLIENT_IPS[1]}]="16666" [${CLIENT_IPS[2]}]="16667" [${CLIENT_IPS[3]}]="16667" [${CLIENT_IPS[4]}]="16667" [${CLIENT_IPS[5]}]="16667" )
#partitions=( [${CLIENT_IPS[0]}]="25000" [${CLIENT_IPS[1]}]="25000" [${CLIENT_IPS[2]}]="25000" [${CLIENT_IPS[3]}]="25000" )

conf="/home/hieun/Dropbox/redis.conf"
cachetype="redis"
#cachetype="camp"

function join_by { local IFS="$1"; shift; caches="$*"; }

for THREADS in "${THREADSS[@]}"
do
for m in "${!modes[@]}";
do
	for numcaches in 1
	do
		echo "Mode: "$m

		cache_ips=("${CACHE_IPS[@]:0:$numcaches}")
		echo "Cache IPs: ${cache_ips[@]}"

		caches=( "${cache_ips[@]/%/:11211}" )
		join_by , ${caches[@]}
		echo "Caches: $caches"

		#exit 1
		for CLIENT_IP in ${CLIENT_IPS[@]}
		do
			ssh $CLIENT_IP "killall java"
			sleep 1
		done

		# restart all cache (no matter how mache cache servers are used)
		for CACHE_IP in ${cache_ips[@]}
		do
			echo "Restart cache "$CACHE_IP
			ssh $CACHE_IP "killall twemcache"
			ssh $CACHE_IP "killall redis-server"

			if [ "$cachetype" != "redis" ] 
			then
				echo "Start twemcache"
				ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/CAMPServer/src/twemcache -t 8 -c 4096 -m 10000 > /dev/null 2>&1 &" &		
			else
				echo "Start redis"
				ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/redis-3.2.8/src/redis-server $conf --maxmemory 10g --port 11211 > /dev/null 2>&1 &" &
			fi
		done

		sleep 2

		#exit 1

		# warm-up cache
		if [ "$cachetype" != "redis" ] 
		then
			cmd="bin/ycsb load mongodb-mc-redlease -s -P workloads/workloada -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=$numcaches -p phase=load -p fullwarmup=true -p numarworker=0 > $RESULT/load 2>&1"
			echo $cmd
			eval $cmd
		else
			cd ~/ycsb/YCSB
			cmd="bin/ycsb load redis -s -P workloads/workloada -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=10 -p alpha=5 -p phase=load > $RESULT/load 2>&1"
			echo $cmd
			eval $cmd
			cd ~/Desktop/EW/YCSB
		fi

		#exit 1

		# start stats
		bash "startstats.sh"

		index="0"
		# run
		for CLIENT_IP in ${CLIENT_IPS[@]}
		do
			recordPerClient=${partitions[$CLIENT_IP]}
			echo "Record for this client: $recordPerClient"

			if [ "$cachetype" != "redis" ] 
			then
				cmd="bin/ycsb run mongodb-mc-redlease -P workloads/workloada -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=$numcaches -s -threads $THREADS -p maxexecutiontime=$TIME -p fullwarmup=true -p dbfail=${modes[$m]} -p insertstart=$index -p insertcount=$recordPerClient -p numarworker=2 2>&1"
				echo $cmd
				ssh $CLIENT_IP "cd ~/Desktop/EW/YCSB && $cmd > $RESULT/run_$CLIENT_IP" &
			else
				cmd="bin/ycsb run redis -s -threads $THREADS -P workloads/workloada -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=10 -p dbfail=${modes[$m]} -p alpha=5 -p phase=run -p maxexecutiontime=$TIME -p insertstart=$index -p insertcount=$recordPerClient 2>&1"
				echo $cmd
				ssh $CLIENT_IP "cd ~/ycsb/YCSB && $cmd > $RESULT/run_$CLIENT_IP" &
			fi

			index=$((index + recordPerClient))
		done

		#exit 1

		sleep $((TIME+60))

		# copy cache stats
		for CACHE_IP in ${cache_ips[@]}
		do
			{ sleep 2; echo "stats"; sleep 2; echo "stats slabs"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $RESULT/$CACHE_IP"cachestats.txt"
		done

		# stop stats
		bash "copystats.sh"

		if [ "$cachetype" != "redis" ] 
		then
			folder="hs-$m-ncache$numcaches-th$THREADS"
		else
			folder="redis-hs-$m-ncache$numcaches-th$THREADS"
		fi
		mkdir -p $ARCHIVE/$folder
		mv $RESULT/* $ARCHIVE/$folder/
		eval "java -jar graph.jar $ARCHIVE/$folder $CACHE_IP $MONGO_IP $CLIENT_IP"
	done
done
done
