FROM ubuntu
MAINTAINER Borge N. Kjelsrud <borgenk@gmail.com>

RUN apt-get update
RUN apt-get upgrade -y

RUN echo "Europe/Oslo" | tee /etc/timezone
RUN dpkg-reconfigure --frontend noninteractive tzdata

# Redis
RUN apt-get install -y python-software-properties python-setuptools
RUN add-apt-repository ppa:chris-lea/redis-server
RUN apt-get update
RUN apt-get install -y redis-server
ADD conf/docker/redis/redis.conf /etc/redis/redis.conf
#VOLUME ["/var/lib/redis"]
EXPOSE 6379

# Supervisor
RUN easy_install supervisor
ADD conf/docker/supervisord.conf /etc/supervisord.conf

CMD ["/usr/local/bin/supervisord", "-n", "-c", "/etc/supervisord.conf"]

# QDo
ADD lib/web/template/ /var/www/web/template
ADD qdo /usr/bin/qdo
EXPOSE 8080
