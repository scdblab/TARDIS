#!/bin/bash

server="10.0.1.15"
client="10.0.1.20"
time="300"

for thread in 1 2 4 8 10 12 14 16
do
	echo "Thead $thread"

	echo "Kill iperf"
	ssh $server "killall iperf"
	ssh $client "killall iperf"
	sleep 2

	echo "Start iperf server"
	ssh $server "nohup iperf3 -s -p 12345 -f K > s_output_$thread 2>&1 &"

	sleep 2

	echo "Start iperf client"
	ssh $client "iperf3 -c $server -p 12345 -t $time -P $thread -N -f K -l 1024000 > c_output_$thread 2>&1"

	# copy to local
	mkdir -p ~/Desktop/iperf_res
	scp $server:~/s_output_$thread ~/Desktop/iperf_res/
	scp $client:~/c_output_$thread ~/Desktop/iperf_res/
done
