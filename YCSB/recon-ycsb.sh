#!/bin/bash

MONGO_IP=10.0.1.10
CACHE_IP=10.0.1.15

OUTPUT_DIR="/home/hieun/Desktop/results"
ARCHIVE="/home/hieun/Desktop/archive"
CACHE_DIR="/home/hieun/redis-3.2.8/src/redis-server"

#declare -A WORKLOADS=( ["lp"]="$PREFIX/workloads/ListPendingAction" ["ro"]="$PREFIX/workloads/ReadOnlyAction" ["read9"]="$PREFIX/workloads/9SymmetricWorkload" ["read99"]="$PREFIX/workloads/99SymmetricWorkload"  ["read999"]="$PREFIX/workloads/999SymmetricWorkload" ["vp"]="$PREFIX/workloads/ViewProfileAction" ["lf"]="$PREFIX/workloads/ListFriendsAction")

run_experiment () {

	# restart cache
	ssh $CACHE_IP "killall redis-server"
	ssh $CACHE_IP "nohup $CACHE_DIR ~/Dropbox/redis.conf --maxmemory 15000mb --port 11211 --maxmemory-policy noeviction > /dev/null 2>&1 &" &

	sleep 5

	# load
	echo "load data"
	./bin/ycsb load redis -s -P workloads/workloada -jvm-args "-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication" -p "redis.hosts=$CACHE_IP:11211" -p "mongo.host=$MONGO_IP:27017" -p "ar=$2" -p "alpha=$3" -p "phase=load" > $6 2>&1

	bash "startstats.sh"
	
	{ sleep 2; echo "info"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $OUTPUT_DIR/"cachestats.txt"

	# run
	echo "run"
	./bin/ycsb run redis -s -threads $1 -P workloads/workloada -jvm-args "-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication" -p "redis.hosts=$CACHE_IP:11211" -p "mongo.host=$MONGO_IP:27017" -p "ar=$2" -p "dbfail=$4" -p "alpha=$3" -p "phase=run" -p "maxexecutiontime=$5" > $7 2>&1
	
	# copy cache stats
	{ sleep 2; echo "info"; sleep 2; echo "quit"; sleep 1; } | telnet $CACHE_IP 11211 > $OUTPUT_DIR/"cachestats.txt"

	# stop stats
	bash "copystats.sh"
}

echo "Clean up results folder"
rm -r $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

ar="10"
alpha="10"
db_fail="600,1200"
maximum_time="480"
ar=10
alpha=10
db_fail="0,300"
for thread in "20" "1"
do
	for ar in "10" "100" "1"
	do
		for alpha in "1000"
		do
			load="$OUTPUT_DIR/load.txt"
			run="$OUTPUT_DIR/run.txt"
			run_experiment $thread $ar $alpha $db_fail $maximum_time $load $run
			folder="recon-ycsb-recovery-$ar-$alpha-$thread"
			mkdir -p $ARCHIVE/$folder
			mv DistrStats $ARCHIVE/$folder/
			mv $OUTPUT_DIR/* $ARCHIVE/$folder/
		done
	done
done



# for mem in {20..200..20}
# do
# 	killall redis-server
# 	sleep 5
# 	nohup $CACHE_DIR ~/Dropbox/redis.conf --maxmemory $(($mem))mb --port 11211 --maxmemory-policy noeviction > /dev/null 2>&1 &
# 	sleep 5

# 	run="$OUTPUT_DIR/run.txt"
# 	./bin/ycsb run redis -s -threads 1 -P workloads/workloada -jvm-args "-Xmx12g -XX:+UseG1GC -XX:+UseStringDeduplication" -p "redis.hosts=127.0.0.1:11211" -p "mongo.host=$MONGO_IP:27017" -p "ar=10" -p "dbfail=0,6000" -p "alpha=10" -p "phase=run" -p "maxexecutiontime=1800" -p "successWrite=true" > $run 2>&1
# 	folder="recon-ycsb-success-write-$mem"
# 	mkdir -p $ARCHIVE/$folder
# 	mv DistrStats $ARCHIVE/$folder/
# 	mv $OUTPUT_DIR/* $ARCHIVE/$folder/
# done
# maximum_time="1500"
# db_fail="600,1200"
# ar=10
# alpha=10
# for thread in "1" "2" "4" "8" "16" "32" "64"
# do
# 	load="$OUTPUT_DIR/load.txt"
# 	run="$OUTPUT_DIR/run.txt"
# 	run_experiment $thread $ar $alpha $db_fail $maximum_time $load $run
# 	folder="recon-ycsb-scale-$thread"
# 	mkdir -p $ARCHIVE/$folder
# 	mv DistrStats $ARCHIVE/$folder/
# 	mv $OUTPUT_DIR/* $ARCHIVE/$folder/
# done
