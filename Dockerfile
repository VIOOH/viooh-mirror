#
# Copyright 2019-2020 VIOOH Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:13-jdk

MAINTAINER Data Engineers <data.engineers@viooh.com>

COPY ./target/viooh-mirror*-standalone.jar /opt/viooh-mirror.jar

USER nobody

ENV SERVICE_OPTS="-server -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Dfile.encoding=utf-8 -Dnetworkaddress.cache.ttl=30 -Dnetworkaddress.cache.negative.ttl=10"

CMD exec $JAVA_HOME/bin/java $SERVICE_OPTS $JAVA_OPTS -jar /opt/viooh-mirror.jar
