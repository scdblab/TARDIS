#!/bin/bash
basedir="/proj/BG/yaz"
sudo apt-get update
sudo apt-get --yes install mysql-server
sudo service mysql stop
sudo su -c "cat $basedir/my.cnf > /etc/mysql/my.cnf"
sudo cp -rva "$basedir/mysqlbackups/order1MInt/mysql/" /var/lib/
sudo su -c "chmod -R 777 /var/lib/mysql"

sudo su -c "ulimit -n 1024000;service mysql start"
sudo service mysql stop
sudo mkdir -p /lib/systemd/system/mysql.service.d/
sudo su -c "echo "[Service]" > /lib/systemd/system/mysql.service.d/limit_nofile.conf"
sudo su -c "echo "LimitNOFILE=1024000" >> /lib/systemd/system/mysql.service.d/limit_nofile.conf"
sudo systemctl daemon-reload
sudo systemctl restart mysql
#CREATE USER 'root'@'%' IDENTIFIED BY 'root';
#GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
