#!/bin/bash
ycsb_dir="/tmp/YCSB"
cd $ycsb_dir
db=$1
wl=$2
numrecs=$3
dur=$4
mynumthreads=$5
redis=$6
mongo=$7
numar=$8
readBW=$9
updateBW=${10}
arBW=${11}
arSleep=${12}
metricsFile=${13}
dbfail=${14}
alpha=${15}
write_back=${16}
readAlBW=${17}
writeSetValue=${18}

killall java
./bin/ycsb -jvm-args="-XX:+UseG1GC -XX:+UseStringDeduplication -XX:+PrintGCTimeStamps -XX:+PrintGC -Xmx12G" run $db -P workloads/$wl -P workloads/db.properties -p recordcount=$numrecs -p stringkey=false -p operationcount=0 -p maxexecutiontime=$dur -s -threads $mynumthreads -p mongodb.writeConcern=journal -p redis.hosts=$redis -p mongo.host=$mongo -p ar=$numar -p alpha=$alpha -p readBW=$readBW -p updateBW=$updateBW -p arBW=$arBW -p arSleep=$arSleep -p metricsFile=$metricsFile -p dbfail=$dbfail -p slaresponsetime=100 -p writeBack=$write_back -p readAlBW=$readAlBW -p writeSetValue=$writeSetValue 2>&1