#!/bin/bash

source machines.sh

machines=( "${CACHE_IPS[@]}" "${CLIENT_IPS[@]}" $MONGO_IP )
dir="/home/hieun/Desktop/results"

for m in ${machines[@]}
do
	echo "Start stats "$m

	ssh $m "killall sar"

	ssh $m "mkdir -p $dir"

	cmd="sar -P ALL 10 > $dir/"$m"cpu_log.txt & "
	ssh $m $cmd

	cmd="sar -n DEV 10 > $dir/"$m"net_log.txt & "
	ssh $m $cmd

	cmd="sar -d 10 > $dir/"$m"disk_log.txt & "
	ssh $m $cmd

	cmd="sar -r 10 > $dir/"$m"mem_log.txt & "
	ssh $m $cmd
done

