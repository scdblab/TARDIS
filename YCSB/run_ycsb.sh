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
arSleep=${9}
metricsFile=${10}
dbfail=${11}
alpha=${12}
cachemode=${13}
tardis=${14}
ewstats=${15}

killall java
./bin/ycsb -jvm-args="-XX:+UseG1GC -XX:+UseStringDeduplication -XX:+PrintGCTimeStamps -XX:+PrintGC -Xmx12G" run $db -P workloads/$wl -p recordcount=$numrecs -p stringkey=false -p operationcount=0 -p maxexecutiontime=$dur -s -threads $mynumthreads -p mongodb.writeConcern=acknowledged -p cacheservers=$redis -p mongodb.url=mongodb://$mongo:27017 -p arsleeptime=$arSleep -p metricsFile=$metricsFile -p dbfail=$dbfail -p slaresponsetime=100 -p cachemode=$cachemode -p tardismode=$tardis -p writeset=false -p numarworker=$numar -p alpha=$alpha -p ewstats=$ewstats -p fullwarmup=false -p insertorder=ordered 2>&1
