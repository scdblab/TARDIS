#!/bin/bash

#wget http://expect.nist.gov/old/expect-5.43.0.tar.gz -P ~/Downloads/
#cd ~/Downloads
#tar -zxvf expect-5.43.0.tar.gz
#cd expect-5.43.0
#./configure
#echo "golinux" | sudo -S make install

user="haoyu"

basedir="/users/$user/Downloads"

mkdir -p $basedir

echo "Unzipping libevent..."
cd $basedir
wget https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz
tar -zxvf libevent-2.0.21-stable.tar.gz

echo "Install LIBEVENT"
cd libevent-2.0.21-stable
./configure
sudo -S make install

echo "Install AUTOMAKE"
wget http://ftp.gnu.org/gnu/m4/m4-1.4.11.tar.gz -P $basedir
cd $basedir
tar -zxvf m4-1.4.11.tar.gz
cd m4-1.4.11
./configure
sudo -S make install
wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.68.tar.gz -P $basedir
cd $basedir
tar -zxvf autoconf-2.68.tar.gz
cd autoconf-2.68
./configure
sudo -S make install
wget ftp://ftp.gnu.org/gnu/automake/automake-1.11.3.tar.gz -P $basedir
cd $basedir
tar -zxvf automake-1.11.3.tar.gz
cd automake-1.11.3
./configure
sudo -S make install

sudo apt-get update
sudo apt-get install zip unzip

# cd $basedir/libevent-2.0.21-stable
# ./configure
# sudo -S make install
# cd $basedir/m4-1.4.11
# ./configure
# sudo -S make install
# cd $basedir/autoconf-2.68
# ./configure
# sudo -S make install
# cd $basedir/automake-1.11.3
# ./configure
# sudo -S make install

sudo apt-get update
sudo apt-get install zip unzip


echo "Start twemcache"
# cd ~/nvcache/IQ-Twemcached/config
# sudo rm -r *
# cp ~/Downloads/automake-1.11.3/lib/compile .
# cp ~/Downloads/automake-1.11.3/lib/config.guess .
# cp ~/Downloads/automake-1.11.3/lib/config.sub .
# cp ~/Downloads/automake-1.11.3/lib/depcomp .
# cp ~/Downloads/automake-1.11.3/lib/install-sh .
# cp ~/Downloads/automake-1.11.3/lib/missing .
cd /users/$user/migration/IQ-Twemcached/
./configure
make


