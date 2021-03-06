#!/bin/bash
[ "$CONFLUENT_HOME" = "" ] && echo "Please set the CONFLUENT_HOME environment variable." && exit 1

kap="${CONFLUENT_HOME}/bin/kafka-avro-console-producer"

VERSION_3='{"type": "record",
            "name": "person",
            "fields": [{"name": "first_name", "type": "string"},
                       {"name": "last_name", "type": "string"},
                       {"name": "sex", "type": "string"},
                       {"name": "affiliations", "type": ["null", {"type": "array", "items": "string"}], "default": null} ]}'


$kap --broker-list localhost:9092 --topic starwars --property value.schema="${VERSION_3}" <<-THE-END
{"first_name": "Mace", "last_name": "Windu", "sex": "male", "affiliations": {"array":["JEDI"]}}
{"first_name": "Darth", "last_name": "Maul", "sex": "male", "affiliations": {"array":["SITH","X"]}}
{"first_name": "Maz", "last_name": "Kanata", "sex": "female", "affiliations": null}
THE-END
