#!/bin/bash

sudo apt-get update
sudo apt-get --yes install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose

sudo apt-get --yes install python-tk
sudo apt-get --yes install screen
sudo apt-get --yes install htop
sudo apt-get --yes install maven


sudo su -c "echo 'logfile /tmp/screenlog' >> /etc/screenrc"


#sudo apt-get install -y software-properties-common python-software-properties

sleep 10



echo "2nd batch"
echo "============"
#sudo add-apt-repository -y ppa:webupd8team/java
echo "============"
echo ""
echo "============"
sudo apt-get update
echo "============"
echo ""
echo "============"
#echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
echo "============"
echo ""
echo "============"
#sudo apt-get install -y oracle-java8-installer
echo "============"
echo ""
echo "============"

# sudo apt-get install -y software-properties-common #python-software-properties debconf-utils
# sudo add-apt-repository ppa:webupd8team/java
# sudo apt-get update
# echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
# sudo apt install oracle-java8-installer

# sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get install -y openjdk-8-jdk
sudo update-alternatives --config java
sudo update-alternatives --config javac

#sudo apt-get install -y memcached
echo "============"
echo ""
echo "============"
sudo apt-get install -y sysstat
echo "============"
echo ""
