#!/bin/bash -xe
[ "$CONFLUENT_HOME" = "" ] && echo "Please set the CONFLUENT_HOME environment variable." && exit 1

kap="${CONFLUENT_HOME}/bin/kafka-avro-console-producer"

VERSION_1='{"type": "record",
            "name": "person",
            "fields": [{"name": "first_name", "type": "string"},
                       {"name": "last_name", "type": "string"} ]}'


$kap --broker-list localhost:9092 --topic starwars --property value.schema="${VERSION_1}"  <<-THE-END
{"first_name": "Luke", "last_name": "Skywalker"}
{"first_name": "Anakin", "last_name": "Skywalker"}
{"first_name": "Princess", "last_name": "Leila"}
THE-END
