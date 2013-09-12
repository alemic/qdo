#!/bin/bash
set -e

if [ ! -e "/var/lib/redis" ]; then
    sudo mkdir /var/lib/redis/
fi
if [ ! -e "/var/log/qdo" ]; then
    sudo mkdir /var/log/qdo
fi

sudo sh -c 'echo "" > /var/log/qdo/qdo.log'

go build
sudo docker stop $(sudo docker ps -a | grep borgenk/qdo:latest | cut -c1-12)
sudo docker build -t="borgenk/qdo" .
sudo docker run -d -p 6379:6379 -p 8080:8080 -v /var/log/qdo/:/var/log/qdo/ -v /var/lib/redis/:/var/lib/redis/ borgenk/qdo

rm qdo

sudo tail -f /var/log/qdo/qdo.log
