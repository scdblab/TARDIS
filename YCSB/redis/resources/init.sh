#!/bin/bash
numServers=$1

user="haoyu"
script_dir="/proj/BG/$user/tardis/emulab"
home_dir="/users/$user/tardis"

# mkdir -p $home_dir && cd $home_dir/
# git config --global credential.helper store
# git clone https://github.com/HaoyuHuang/YCSB.git

# wget http://download.redis.io/releases/redis-3.2.9.tar.gz
# tar xzf redis-3.2.9.tar.gz
# cd redis-3.2.9
# make

for ((i=0;i<numServers;i++)); 
do
	echo "*******************************************"
	echo "*******************************************"
    echo "******************* h$i ********************"
    echo "*******************************************"
    echo "*******************************************"
	ssh -oStrictHostKeyChecking=no h$i "sudo apt-get update"
    ssh -oStrictHostKeyChecking=no h$i "sudo apt-get --yes install screen"
    ssh -n -f -oStrictHostKeyChecking=no h$i screen -L -S env1 -dm "$script_dir/env0.sh"
done

sleep 10

sleepcount="0"

for ((i=0;i<numServers;i++)); 
do
	while ssh -oStrictHostKeyChecking=no h$i "screen -list | grep -q env1"
	do 
		((sleepcount++))
		sleep 60
		echo "waiting for h$i "
	done
done

echo "init env took $sleepcount minutes"

# bash $script_dir/installmongo100k.sh
# bash $script_dir/copy.sh