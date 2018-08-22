#!/bin/bash
echo "####Builing codes"
home_dir="/tmp"
# bash /proj/BG/haoyu/emulab/cachecompile.sh
# cd $home_dir/IQ-Twemcached/ && make

cd $home_dir/YCSB/ && mvn -pl com.yahoo.ycsb:redis-binding -am clean package -DskipTests