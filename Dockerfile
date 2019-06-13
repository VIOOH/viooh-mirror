FROM anapsix/alpine-java:8u202b08_jdk
MAINTAINER Sathya Vittal <sathyavijayan.vittal@viooh.com>

# Switch to root user and remove packages that depend on zlib.
USER root
RUN  apk add --update \
supervisor \
&& rm -rf /var/cache/apk/*

RUN apk --no-cache --force --purge del libcurl apk-tools libssh2 curl zlib

# Switch back to service user and continue as usual
# USER service

COPY ./target/viooh-mirror*-standalone.jar /opt/viooh-mirror.jar

CMD exec $JAVA_HOME/bin/java -server -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Dfile.encoding=utf-8 $JAVA_OPTS  -jar /opt/viooh-mirror.jar
