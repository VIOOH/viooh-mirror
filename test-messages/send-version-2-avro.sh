#!/bin/bash
[ "$CONFLUENT_HOME" = "" ] && echo "Please set the CONFLUENT_HOME environment variable." && exit 1

kap="${CONFLUENT_HOME}/bin/kafka-avro-console-producer"

VERSION_2='{"type": "record",
            "name": "person",
            "fields": [{"name": "first_name", "type": "string"},
                       {"name": "last_name", "type": "string"},
                       {"name": "sex", "type": "string", "default": "person"} ]}'


$kap --broker-list localhost:9092 --topic starwars --property value.schema="${VERSION_2}" <<-THE-END
{"first_name": "Jango", "last_name": "Fett", "sex": "male"}
{"first_name": "Qui-Gon", "last_name": "Jinn", "sex": "male"}
{"first_name": "Kylo", "last_name": "Ren", "sex": "male"}
THE-END
