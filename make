#!/bin/bash
set -e

if [ ! -e "/var/lib/redis" ]; then
    sudo mkdir /var/lib/redis/
fi
if [ ! -e "/var/log/qdo" ]; then
    sudo mkdir /var/log/qdo
fi

go build
sudo docker stop $(sudo docker ps -a -q)
sudo docker build -t="borgenk/qdo" .
sudo docker run -d -p 6379:6379 -v /var/log/qdo/:/var/log/qdo/ -v /var/lib/redis/:/var/lib/redis/ borgenk/qdo

rm qdo

if [ ! -e "/var/log/qdo/qdo.log" ]; then
	sleep 4
fi
sudo tail -f /var/log/qdo/qdo.log
