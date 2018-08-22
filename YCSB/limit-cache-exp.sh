#!/bin/bash

source machines.sh

ARCHIVE="/home/hieun/Desktop/archive"
RESULT="/home/hieun/Desktop/results"
#TIME="300"

RECORDS="100000"

declare -A modes
#modes=( ["failed"]="0,6000" ["normal"]="9000,10000" )
#modes=( ["failed"]="0,6000" )
modes=( ["normal"]="9000,10000" )

conf="/home/hieun/Dropbox/redis.conf"
#cachetype="redis"
#cachetype="camp"
#cachetype="memcached"

cache_ips=$CACHE_IPS
CACHE_IP=${cache_ips[0]}
AR="0"
#workload="workloada"

#echo $CACHE_IP
#exit 1

function join_by { local IFS="$1"; shift; caches="$*"; }

caches=( "${cache_ips[@]/%/:11211}" )
join_by , ${caches[@]}

THREADSS=( "125" )
#THREADSS=( "32" )
#THREADS="1"

for memory in "50" "100" "150" "200" "250"
do
for numcachethreads in "1"
do
for TIME in "300" #"180" "300" "600" "900" "1200"
do
for workload in "workloadc"
do
#for cachetype in "camp" "memcached"
for cachetype in "redis"
do
for try in {1..1}
do
	for THREADS in "${THREADSS[@]}"
	do
		for m in "${!modes[@]}";
		do
			for numclients in 4 #8
			do
				clients=("${CLIENT_IPS[@]:0:$numclients}")
				echo "Client: ${clients[@]}"
				echo "Thread: $THREADS"
	
				for CLIENT_IP in ${clients[@]}
				do
					ssh $CLIENT_IP "killall java"
					sleep 1
				done

				# kill all existing monitorin
				ssh $CACHE_IP "sudo killall perf"
				ssh $CACHE_IP "kill -9 \$(ps aux | grep '[n]etwork.sh' | awk '{print \$2}')"
				ssh $CACHE_IP "kill -9 \$(ps aux | grep '[n]et-ban.sh' | awk '{print \$2}')"

				# Restart cache
				echo "Restart cache "$CACHE_IP
				ssh $CACHE_IP "killall twemcache"
				ssh $CACHE_IP "killall redis-server"
				ssh $CACHE_IP "killall memcached"

				if [ "$cachetype" == "camp" ] 
				then
					echo "Start CAMP"
					ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/Desktop/EW/CAMPServer/src/twemcache -t 1 -c 4096 -m $memory > /dev/null 2>&1 &" &
				elif [ "$cachetype" == "memcached" ]
				then
					echo "Start memcached"
					if [ "$numcachethreads" == "1" ]
					then
						ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/Desktop/EW/memcached-1.4.36/memcached -t 1 -c 4096 -m $memory > /dev/null 2>&1 &" &
					else
						ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/memcached-1.4.36/memcached -t $numcachethreads -c 4096 -m $memory > /dev/null 2>&1 &" &
					fi
				elif [ "$cachetype" == "redis" ]
				then
					echo "Start redis"
					ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/redis-3.2.8/src/redis-server $conf --maxmemory "$memory"mb --port 11211 > /dev/null 2>&1 &" &
				elif [ "$cachetype" == "twemcache" ]				
				then
					echo "Start twemcache"
					if [ "$numcachethreads" == "1" ]
					then
						ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/Desktop/EW/IQ-Twemcached/src/twemcache -t 1 -c 4096 -m $memory > /dev/null 2>&1 &" &
					else
						ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/IQ-Twemcached/src/twemcache -t $numcachethreads -c 4096 -m $memory > /dev/null 2>&1 &" &
					fi
				fi

				sleep 2

				# warm-up cache
				if [ "$cachetype" != "redis" ] 
				then
					cmd="bin/ycsb load mongodb-mc-redlease -s -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -p phase=load -p fullwarmup=true -p numarworker=0 > $RESULT/load 2>&1"
					echo $cmd
					eval $cmd
				else
					cd ~/ycsb/YCSB
					cmd="bin/ycsb load redis -s -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=0 -p alpha=10 -p phase=load > $RESULT/load 2>&1"
					echo $cmd
					eval $cmd
					cd ~/Desktop/EW/YCSB
				fi	

				#exit 1

				# start stats
				bash "startstats.sh"

				index="0"
				#recordPerClient=$((RECORDS / numclients))
				recordPerClient="100000"

				# run
				for CLIENT_IP in ${clients[@]}
				do
					echo "Record for this client: $recordPerClient"

					if [ "$cachetype" != "redis" ] 
					then
						cmd="bin/ycsb run mongodb-mc-redlease -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -s -threads $THREADS -p maxexecutiontime=$TIME -p fullwarmup=false -p dbfail=${modes[$m]} -p insertstart=$index -p insertcount=$recordPerClient -p numarworker=$AR"
						echo $cmd
						#exit 1
						ssh $CLIENT_IP "cd ~/Desktop/EW/YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &
					else
						cmd="bin/ycsb run redis -s -threads $THREADS -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=$AR -p dbfail=${modes[$m]} -p alpha=10 -p phase=run -p maxexecutiontime=$TIME -p insertstart=$index -p insertcount=$recordPerClient"
						echo $cmd
						#exit 1
						ssh $CLIENT_IP "cd ~/ycsb/YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &
					fi

					#index=$((index + recordPerClient))
				done

				if [ "$cachetype" == "redis" ]
				then
					pid=$(ssh $CACHE_IP pgrep redis-server)
				elif [ "$cachetype" == "camp" ]
				then
					pid=$(ssh $CACHE_IP pgrep twemcache)
				else
					pid=$(ssh $CACHE_IP pgrep memcached)
				fi

				echo $pid
		
				# start monitoring
				perfcmd="nohup sudo perf stat -e cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-stores,L1-dcache-store-misses,L1-dcache-prefetch-misses,L1-icache-load,L1-icache-load-misses,LLC-loads,LLC-stores,LLC-prefetches,dTLB-loads,dTLB-load-misses,dTLB-stores,dTLB-store-misses,iTLB-loads,iTLB-load-misses,branch-loads,branch-load-misses -p $pid > $RESULT/perf 2>&1 &"
				echo "Perf cmd: $perfcmd"
				ssh $CACHE_IP $perfcmd
				ssh $CACHE_IP "nohup bash network.sh bond0 > $RESULT/network &"
				ssh $CACHE_IP "nohup bash net-ban.sh bond0 > $RESULT/netban &"

				sleepTime=$((TIME+20))
				echo "Sleep for "$sleepTime
				sleep $sleepTime

				# stop monitoring
				echo "Stop monitoring"
				pid=$(ssh $CACHE_IP pgrep perf) # | sed -n 2p)
				echo "Perf pid: $pid"
				ssh $CACHE_IP "sudo kill -SIGINT $pid"
				ssh $CACHE_IP "kill -9 \$(ps aux | grep '[n]etwork.sh' | awk '{print \$2}')"
				ssh $CACHE_IP "kill -9 \$(ps aux | grep '[n]et-ban.sh' | awk '{print \$2}')"
	
				# Copy stats
				{ sleep 2; echo "stats"; sleep 2; echo "stats slabs"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $RESULT/$CACHE_IP"cachestats.txt"

				# stop stats
				bash "copystats.sh"

				folder="lc-$cachetype-mem$memory-$workload-th$THREADS"
				mkdir -p $ARCHIVE/$folder
				mv $RESULT/* $ARCHIVE/$folder/
				eval "java -jar graph.jar $ARCHIVE/$folder $CACHE_IP $MONGO_IP $CLIENT_IP"	
			done	
		done
	done
done
done
done
done
done
done
