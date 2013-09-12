#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive

# Date
# https://help.ubuntu.com/community/UbuntuTime
TIMEZONE=$(head -n 1 "/etc/timezone")
if [ $TIMEZONE != "Europe/Oslo" ]; then
    echo "Europe/Oslo" | sudo tee /etc/timezone
    sudo dpkg-reconfigure --frontend noninteractive tzdata
fi

apt-get update -q
apt-get install -q -y python-software-properties git

# Docker.io
if [ ! -e "/opt/docker" ]; then
    mkdir /opt/docker

    sh -c "curl http://get.docker.io/gpg | apt-key add -"
    sh -c "echo deb https://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
    apt-get update -q
    apt-get install -q -y linux-image-extra-`uname -r` lxc-docker
fi

# Go development environment
if [ ! -e "/opt/go-env" ]; then
    mkdir /opt/go-env

    add-apt-repository -y ppa:duh/golang
    apt-get update -q -y
    apt-get install -q -y golang

    # ~/go ~/go/src ~/go/pkg ~/go/bin
    mkdir /home/vagrant/go/pkg
    mkdir /home/vagrant/go/bin
    chown -R vagrant.vagrant /home/vagrant/go

    echo "" >> /home/vagrant/.bashrc
    echo "export GOPATH=~/go" >> /home/vagrant/.bashrc
fi
