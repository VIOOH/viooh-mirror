(ns repl-mirror
  (:use viooh.mirror.mirror)
  (:require [jackdaw.client :as k]
            [viooh.mirror.serde :as s]
            [viooh.mirror.schema-mirror :as sm]
            [clojure.walk :refer [stringify-keys]]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))



(comment

  (in-ns 'viooh.mirror.mirror)

  (def group-id-prefix "test")
  (def serdes [:string :avro])
  (def mirror-cfg
    {:name "prv_DigitalReservation_PT"
     ;;TODO CHANGE CFG
     :mirror-mode :lenient
     ;;TODO CHANGE CFG
     :subject-naming-strategy :topic-name
     :source
     {:kafka {:bootstrap.servers "kf1.dataplatform.jcdecaux.com:9092,kf2.dataplatform.jcdecaux.com:9092,kf3.dataplatform.jcdecaux.com:9092"
              :max.partition.fetch.bytes "1000000"
              :max.poll.records "50000"
              :auto.offset.reset "earliest"}
      :topic {:topic-name "prv_DigitalReservation_PT"}
      :schema-registry-url "http://registry.dataplatform.jcdecaux.com"}

     :destination
     {:kafka {:bootstrap.servers "10.1.151.67:9092,10.1.152.252:9092,10.1.153.241:9092"}
      :topic {:topic-name "prd.datariver.prv_DigitalReservation"}
      :schema-registry-url "https://schema-registry.dev.develop.farm"}

     :serdes [:string :avro]})


  (def closed? (atom false))
  (def p (promise))
  (def group-id (str/join "_" [group-id-prefix "test"]))
  (def mirror-name group-id)
  (def source (:source mirror-cfg))
  (def destination (:destination mirror-cfg))
  (def src-schema-registry-url (:schema-registry-url source))
  (def dest-schema-registry-url (:schema-registry-url destination))
  (def src-topic-cfg (:topic source))
  (def src-topic (:topic-name src-topic-cfg))
  (def dest-topic-cfg (:topic destination))
  (def dest-topic (:topic-name dest-topic-cfg))
  (def src-serdes (s/serdes serdes src-schema-registry-url))
  (def dest-serdes (s/serdes serdes dest-schema-registry-url))

  (sm/mirror-schemas mirror-cfg)

  ;; start consumer and producers
  (def c (consumer group-id source src-serdes))
  (def p (producer destination dest-serdes))
  (def k-subs (k/subscribe c [src-topic-cfg]))

  ;; get records
  (def records (k/poll c 3000))
  (def value (-> records first :value))

  (def rec (-> records first))


  )
