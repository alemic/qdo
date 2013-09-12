FROM ubuntu:12.04
MAINTAINER Borge N. Kjelsrud <borgenk@gmail.com>

RUN echo "Europe/Oslo" | tee /etc/timezone
RUN dpkg-reconfigure --frontend noninteractive tzdata

# Redis
RUN apt-get install -y -q python-software-properties python-setuptools
RUN add-apt-repository ppa:chris-lea/redis-server
RUN apt-get update -q
RUN apt-get install -y -q redis-server
VOLUME ["/var/lib/redis"]
EXPOSE 6379

# Supervisor
RUN easy_install supervisor

CMD ["/usr/local/bin/supervisord", "-n", "-c", "/etc/supervisord.conf"]

# Qdo
EXPOSE 8080

ADD docker/redis/redis.conf /etc/redis/redis.conf
ADD docker/supervisord.conf /etc/supervisord.conf
ADD lib/web/template/ /var/www/
ADD qdo /usr/bin/qdo
