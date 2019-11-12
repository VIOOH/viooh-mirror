FROM openjdk:12-jdk-alpine

MAINTAINER Data Engineers <data.engineers@viooh.com>

# Switch to root user and remove packages that depend on zlib.
USER root

ARG glibc_version=2.29-r0

ENV GLIBC_VERSION=$glibc_version

# install compression support
RUN apk --no-cache --force add libc6-compat zlib ca-certificates wget
RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
RUN wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk
RUN apk --no-cache --force --purge add glibc-${GLIBC_VERSION}.apk

# Clean up
RUN rm glibc-${GLIBC_VERSION}.apk \
        && apk --no-cache --force --purge del libcurl apk-tools libssh2 curl ca-certificates wget \
        && rm -rf /var/cache/apk/*


COPY ./target/viooh-mirror*-standalone.jar /opt/viooh-mirror.jar

ENV MIRROR_OPTS="-server -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Dfile.encoding=utf-8 -Dnetworkaddress.cache.ttl=30 -Dnetworkaddress.cache.negative.ttl=10"

CMD exec $JAVA_HOME/bin/java $MIRROR_OPTS $JAVA_OPTS -jar /opt/viooh-mirror.jar
