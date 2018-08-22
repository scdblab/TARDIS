#!/bin/bash
basedir="/proj/BG/yaz/ycsbcache"
sudo apt-get update
sudo su -c "/usr/testbed/bin/mkextrafs /mnt"
sudo apt-get --yes install mysql-server
sudo service mysql stop
sudo su -c "cat $basedir/my2.cnf > /etc/mysql/my.cnf"
sudo su -c "cat $basedir/usr.sbin.mysqld /etc/apparmor/usr.sbin.mysqld"
sudo cp -rva "/proj/BG/yaz/mysqlbackups/order10MInt/mysql/" /mnt/
sudo su -c "chmod -R 777 /mnt/mysql"

sudo su -c "ulimit -n 1024000;service mysql start"
sudo service mysql stop
sudo mkdir -p /lib/systemd/system/mysql.service.d/
sudo su -c "echo "[Service]" > /lib/systemd/system/mysql.service.d/limit_nofile.conf"
sudo su -c "echo "LimitNOFILE=1024000" >> /lib/systemd/system/mysql.service.d/limit_nofile.conf"
sudo systemctl daemon-reload
sudo systemctl restart mysql
#CREATE USER 'root'@'%' IDENTIFIED BY 'root';
#GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
