FROM openjdk:13-jdk

MAINTAINER Data Engineers <data.engineers@viooh.com>


COPY ./target/viooh-mirror*-standalone.jar /opt/viooh-mirror.jar

USER nobody

ENV MIRROR_OPTS="-server -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Dfile.encoding=utf-8 -Dnetworkaddress.cache.ttl=30 -Dnetworkaddress.cache.negative.ttl=10"

CMD exec $JAVA_HOME/bin/java $MIRROR_OPTS $JAVA_OPTS -jar /opt/viooh-mirror.jar
