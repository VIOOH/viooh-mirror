# viooh-mirror

Utility to mirror kafka topics across clusters.


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
    :reporter-name     "mirror"
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
