#!/usr/bin/env bash
set -e

# Exit if setup is already done (Vagrant 1.2.x backward compatibility).
if [ -e "/opt/vagrant-installed" ]; then
    exit 0
fi
mkdir /opt/vagrant-installed


export DEBIAN_FRONTEND=noninteractive

# Date
# https://help.ubuntu.com/community/UbuntuTime
TIMEZONE=$(head -n 1 "/etc/timezone")
echo "Europe/Oslo" | tee /etc/timezone
dpkg-reconfigure --frontend noninteractive tzdata


apt-get install -q -y python-software-properties git

# Docker
sh -c "curl http://get.docker.io/gpg | apt-key add -"
sh -c "echo deb https://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
apt-get update -q
apt-get install -q -y linux-image-extra-`uname -r` lxc-docker

echo "limit nofile 262144 262144" >> /etc/init/docker.conf
sed -i.bak 's/docker -d/docker -d -r/' /etc/init/docker.conf
service docker restart
sleep 3


# Go development environment
add-apt-repository -y ppa:duh/golang
apt-get update -q -y
apt-get install -q -y golang

# ~/go ~/go/src ~/go/pkg ~/go/bin
mkdir /home/vagrant/go/pkg
mkdir /home/vagrant/go/bin
chown -R vagrant.vagrant /home/vagrant/go

echo "" >> /home/vagrant/.bashrc
echo "export GOPATH=~/go" >> /home/vagrant/.bashrc
