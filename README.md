# viooh-mirror

![kafka-mirror](./doc/kafka-mirror.gif)
_Rotating Kafka's head statue by David Cerny_


Viooh-Mirror an utility to mirror kafka topics across clusters along
with their schemas.


## Configuration
`viooh-mirror` uses 1config for configuration. For more information
see: [1config](https://github.com/BrunoBonacci/1config).

The application can be configured to run multiple `mirrors`. Each
`mirror` is a consumer loop that sends each record received from the
source topic to the destination topic. source and destination topics
can belong to different kafka clusters. When the avro serde is used
the schemas are automatically created in the destination cluster.

```clojure
{;;consumer group-id for each mirror is constructed by joining the group-id-prefix and the mirror name
 :group-id-prefix "foggy_mirror"
 :mirrors
 [{:name "mirror_copy_11"
   :source
   {:kafka {:bootstrap.servers "localhost:9092"}
    :topic {:topic-name "mirror_test"}
    :schema-registry-url "http://localhost:8081"}

   :destination
   {:kafka {:bootstrap.servers "localhost:9092"}
    :topic {:topic-name "mirror_test_copy_5"}
    :schema-registry-url "http://localhost:8081"}

   ;; [key-serde-type value-serde-type]
   :serdes [:string :avro]}

  {:name "mirror_copy_12"
   :source
   {:kafka {:bootstrap.servers "localhost:9092"}
    :topic {:topic-name "mirror_test"}
    :schema-registry-url "http://localhost:8081"}

   :destination
   {:kafka {:bootstrap.servers "localhost:9092"}
    :topic {:topic-name "mirror_test_copy_6"}
    :schema-registry-url "http://localhost:8081"}

   :serdes [:string :string]}]


   ;; to display metrics to the console use:
   ;; :metrics {:type :console, :reporting-frequency-seconds 30}

   ;; to send metrics to Prometheus use:
   :metrics
   {:type :prometheus
    ;; the url for the prometheus push gateway
    :push-gateway-url  "http://localhost:9091"
    :reporter-name     "viooh-mirror"
    :grouping-keys     {"env" "dev",
                        "version" "1_2_3",
                        "foo" "bar"}}}

```

## Usage

```
# 1config key by default is `viooh-mirror`. This can be overridden using the env variable `ONE_CONF_KEY`.
# 1config env is picked from environment variable `ENV` (`local` by default)
$ lein run
```

## Monitoring

Metrics are pushed with the prefix `vioohmirror.**`. To see the number
of messages which are being polled by topic you can look for metrics
of the form
`vioohmirror.messages.poll.<group-id-prefix>_<mirror-name>.(count|1min_rate)`

To look for the number of messages which are being sent to the mirrored kafka look for metrics in the form
`vioohmirror.messages.send.<group-id-prefix>_<mirror-name>.inner.(count|1min_rate)` and for errors look for
`vioohmirror.messages.send.<group-id-prefix>_<mirror-name>.inner_errors.(count|1min_rate)`.

Circuit breakers are set around the source and destination schema-registry for the various operations.
