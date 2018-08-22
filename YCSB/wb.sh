#!/bin/bash

source machines.sh

YCSB="~/ycsb/YCSB"
ARCHIVE="/home/hieun/Desktop/archive"
RESULT="/home/hieun/Desktop/results"

ycsb_client="mongodb-mc-redlease"
modes=( ["normal"]="9000,10000" )   # dbfail mode (no dbfail for this exp)
AR="10"
TIME="300"
writeconcern="acknowledged"

function join_by { local IFS="$1"; shift; caches="$*"; }

caches=( "${CACHE_IPS[@]/%/:11211}" )
join_by , ${caches[@]}

wlsize="1m"
workloads=( "workloada_"$wlsize "workloadb_"$wlsize )
readonly="readonly_"$wlsize

for try in {1..1}
do
  for cache_mode in "back" "through" "around" "nocache"
  do
    for workload in ${workloads[@]}
    do
      #for arsleeptime in 10000 60000
      #do
      for thread in 512
      do
        echo "Client: $ycsb_client"
        echo "Time: $TIME"
        echo "Client IPs: ${CLIENT_IPS[@]}"
        echo "Cache IPs: ${CACHE_IPS[@]}"
        echo "Cache Mode: $cache_mode"
        echo "Workload: $workload"
        echo "# Thread: $thread"
        echo "# Try: $try"

        # reset mongo
        java -jar ExpRestart.jar restartFC $MONGO_IP hieun /home/hieun/Desktop/mongo/ /home/hieun/Desktop/mongo_ycsb_1m/ back
        #java -jar ExpRestart.jar restart $MONGO_IP hieun /home/hieun/Desktop/mongo/ /home/hieun/Desktop/mongo_ycsb_1m/
        #mongo --host $MONGO_IP --port 27017 < mongoscript.js
        #ssh $MONGO_IP "sudo service mongod restart"

        sleep 2

        # restart cache
        for CACHE_IP in ${CACHE_IPS[@]}
        do
          echo "Restart cache "$CACHE_IP
          ssh $CACHE_IP "killall twemcache"
          ssh $CACHE_IP "killall redis-server"
          ssh $CACHE_IP "killall memcached"
          sleep 2
          ssh $CACHE_IP "nohup /home/hieun/Desktop/EW/memcached-1.4.36/memcached -t 8 -c 4096 -m 5540 > /dev/null 2>&1 &" &
        done

        sleep 2

        # warm up for 150 secs
        for CLIENT_IP in ${CLIENT_IPS[@]}
        do
          cmd="bin/ycsb run $ycsb_client -P workloads/$readonly -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p mongodb.writeConcern=$writeconcern -p cacheservers=$caches -p numcacheservers=1 -s -threads $thread -p maxexecutiontime=150 -p fullwarmup=false -p dbfail=${modes[$m]} -p numarworker=$AR -p cachemode=through"
          echo $cmd
          ssh $CLIENT_IP "cd $YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &    
        done

        sleepTime=170
        echo "Sleep for "$sleepTime
        sleep $sleepTime

        #exit 1

        # start stats
        bash "startstats.sh"

        # run multi-clients
        for CLIENT_IP in ${CLIENT_IPS[@]}
        do
          cmd="bin/ycsb run $ycsb_client -P workloads/$workload -jvm-args \"-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication\" -p mongodb.url=mongodb://$MONGO_IP:27017 -p mongo.database=ycsb -p mongodb.writeConcern=$writeconcern -p cacheservers=$caches -p numcacheservers=1 -s -threads $thread -p maxexecutiontime=$TIME -p fullwarmup=false -p dbfail=${modes[$m]} -p alpha=10 -p numarworker=$AR -p cachemode=$cache_mode -p arsleeptime=$arsleeptime"
          echo $cmd
          ssh $CLIENT_IP "cd $YCSB && $cmd > $RESULT/run_$CLIENT_IP 2>&1 &" &    
        done

        sleepTime=$((TIME+20))
        echo "Sleep for "$sleepTime
        sleep $sleepTime

        # get cache stats
        for CACHE_IP in ${CACHE_IPS[@]}
        do
          { sleep 2; echo "stats"; sleep 2; echo "stats slabs"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $RESULT/$CACHE_IP"cachestats.txt"
        done

        # collec stats
        bash "copystats.sh"

        folder="wb-$try-$cache_mode-$workload-th$thread"
        mkdir -p $ARCHIVE/$folder
        mv $RESULT/* $ARCHIVE/$folder/
        eval "java -jar graph.jar $ARCHIVE/$folder $CACHE_IP $MONGO_IP $CLIENT_IP"
      done
      #done
    done
  done
done
