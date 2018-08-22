#!/bin/bash
user="haoyu"
for host in "h0" "h1"
do
	ssh -oStrictHostKeyChecking=no $host "sudo rm -rf /tmp/*"
	ssh -oStrictHostKeyChecking=no $host "cp -r /users/$user/tardis/* /tmp"
	ssh -oStrictHostKeyChecking=no $host "sudo cp -r /proj/BG/haoyu/settings.xml /usr/share/maven/conf/settings.xml"
	ssh -oStrictHostKeyChecking=no $host "bash /proj/BG/$user/tardis/emulab/build-all.sh"
done

# tail --pid=128277 -f /dev/null
# bash run_exp_240.sh

# sudo rm -rf /tmp/* && cp -r /users/haoyu/nvcache/* /tmp && bash /proj/BG/haoyu/emulab/build-all.sh