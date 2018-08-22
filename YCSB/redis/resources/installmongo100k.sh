#!/bin/bash
# install mongo 3.4
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.4.list
#echo "deb [ arch=amd64 ] http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.4.list
sudo apt-get update

sudo apt-get install -y --allow-unauthenticated mongodb-org

sudo service mongod stop

# copy files
sudo cp /proj/BG/haoyu/mongod-j.conf /etc/mongod.conf
sudo rm -rf /var/lib/mongodb/*

sudo su -c "/usr/testbed/bin/mkextrafs /mnt"
sudo rm -rf /mnt/mongodb
sudo mkdir -p /mnt/mongodb
sudo chown -R mongodb:mongodb /mnt/mongodb

sudo cp -av /proj/BG/yaz/ycsbcache/mongoycsb100k/* /mnt/mongodb
sudo rm -rf /mnt/log/mongodb/mongod.log
sudo mkdir -p /mnt/log/mongodb
sudo chown -R mongodb:mongodb /mnt/log/mongodb
sudo service mongod start