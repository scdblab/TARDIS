#!/bin/bash

source machines.sh

YCSB="~/ycsb/YCSB"
ARCHIVE="/home/hieun/Desktop/archive"
RESULT="/home/hieun/Desktop/results"

RECORDS="100000"

declare -A modes
#modes=( ["failed"]="0,6000" ["normal"]="9000,10000" )
#modes=( ["failed"]="0,6000" )
modes=( ["normal"]="9000,10000" )

conf="/home/hieun/Dropbox/redis.conf"

cache_ips=$CACHE_IPS
CACHE_IP=${cache_ips[0]}
AR="0"

function join_by { local IFS="$1"; shift; caches="$*"; }

caches=( "${cache_ips[@]/%/:11211}" )
join_by , ${caches[@]}

#THREADSS=( "1" "2" "4" "6" "8" "10" "12" "14" "16" "32" "64" "128" "256" "512" )
THREADSS=( "18" "30" )
#THREADSS=( "1" )
#THREADS="1"

for TIME in "300"
do
  for workload in "workloada"
  do
    for cachetype in "memcached"
    do
      for numcachethreads in "1"
      do
        for try in {1..1}
        do
	        if [ "$cachetype" == "redis" -a "$numcachethreads" != "1" ]
	        then
		        # skip if redis multi-threads (this configuration doesn't exist)
		        continue
	        fi

          echo "Time: $TIME"
          echo "Cache Type: $cachetype"
          echo "Number of cache threads: $numcachethreads"
          echo "Try: $try"

	        for THREADS in "${THREADSS[@]}"
	        do
		        for m in "${!modes[@]}";
		        do
			        for numclients in 1
			        do
				        clients=("${CLIENT_IPS[@]:0:$numclients}")
				        echo "Client: ${clients[@]}"
				        echo "Number of Threads: $THREADS"
	
				        for CLIENT_IP in ${clients[@]}
				        do
					        ssh $CLIENT_IP "killall java"
					        sleep 1
				        done

				        # kill all existing monitoring
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
					        ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/Desktop/EW/CAMPServer/src/twemcache -t 1 -c 4096 -m 10000 > /dev/null 2>&1 &" &
				        elif [ "$cachetype" == "memcached" ]
				        then
					        echo "Start memcached"
					        if [ "$numcachethreads" == "1" ]
					        then
						        ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/Desktop/EW/memcached-1.4.36/memcached -t 1 -c 4096 -m 10000 > /dev/null 2>&1 &" &
					        else
						        ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/memcached-1.4.36/memcached -t $numcachethreads -c 4096 -m 10000 > /dev/null 2>&1 &" &
					        fi
				        elif [ "$cachetype" == "redis" ]
                then
					        echo "Start redis"
					        ssh $CACHE_IP "nohup taskset -c 0 /home/hieun/redis-3.2.8/src/redis-server $conf --maxmemory 10g --port 11211 > /dev/null 2>&1 &" &
				        fi

				        sleep 2

				        # warm-up cache
                if [ "$cachetype" == "none" ]
                then
                  cmd="bin/ycsb load mongodb -s -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -p phase=load -p fullwarmup=true -p numarworker=0 > $RESULT/load 2>&1"
                  echo $cmd
                  eval $cmd
				        elif [ "$cachetype" != "redis" ] 
				        then
					        cmd="bin/ycsb load mongodb-mc-redlease -s -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -p phase=load -p fullwarmup=true -p numarworker=0 > $RESULT/load 2>&1"
					        echo $cmd
					        eval $cmd
				        else
					        cmd="bin/ycsb load redis -s -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=0 -p alpha=10 -p phase=load > $RESULT/load 2>&1"
					        echo $cmd
					        eval $cmd
				        fi	

				        #exit 1

				        # start stats
				        bash "startstats.sh"

				        # run
				        for CLIENT_IP in ${clients[@]}
				        do
                  if [ "$cachetype" == "none" ]
                  then
                    cmd="bin/ycsb run mongodb -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -s -threads $THREADS -p maxexecutiontime=$TIME -p fullwarmup=true -p dbfail=${modes[$m]} -p numarworker=$AR"
						        echo $cmd
						        ssh $CLIENT_IP "cd $YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &
					        elif [ "$cachetype" != "redis" ] 
					        then
						        cmd="bin/ycsb run mongodb-mc-redlease -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p cacheservers=$caches -p numcacheservers=1 -s -threads $THREADS -p maxexecutiontime=$TIME -p fullwarmup=true -p dbfail=${modes[$m]} -p numarworker=$AR"
						        echo $cmd
						        ssh $CLIENT_IP "cd $YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &
					        else
						        cmd="bin/ycsb run redis -s -threads $THREADS -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p redis.hosts=$caches -p mongo.host=$MONGO_IP:27017 -p ar=$AR -p dbfail=${modes[$m]} -p alpha=10 -p phase=run -p maxexecutiontime=$TIME"
						        echo $cmd
						        ssh $CLIENT_IP "cd $YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &
					        fi

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

				        echo "Pid: $pid"
		
				        # start monitoring
				        perfcmd="nohup sudo perf stat -e cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,L1-dcache-stores,L1-dcache-store-misses,L1-dcache-prefetch-misses,L1-icache-load,L1-icache-load-misses,LLC-loads,LLC-stores,LLC-prefetches,dTLB-loads,dTLB-load-misses,dTLB-stores,dTLB-store-misses,iTLB-loads,iTLB-load-misses,branch-loads,branch-load-misses -p $pid > $RESULT/perf 2>&1 &"
				        echo "Perf cmd: $perfcmd"
				        ssh $CACHE_IP $perfcmd
				        ssh $CACHE_IP "nohup bash network.sh eth0 > $RESULT/network &"
				        ssh $CACHE_IP "nohup bash net-ban.sh eth0 > $RESULT/netban &"

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

				        folder="vs-try$try-clients$numclients-$workload-th$THREADS"
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
