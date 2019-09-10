(ns repl-schema-mirror
  (:use viooh.mirror.schema-mirror)
  (:require [clojure.set :refer [difference]]
            [clojure.tools.logging :as log]
            [viooh.mirror.schema-registry :as sr]))

(comment

  (in-ns 'viooh.mirror.schema-mirror)

  (def diff
    (compare-subjects
     "http://registry.dataplatform.jcdecaux.com"
     "prv_ProofOfPlay_V12-value"

     "https://schema-registry.dev.develop.farm"
     "prd.datariver.prv_ProofOfPlay_V12-value"))


  (def diff
    (compare-subjects
     "http://registry.dataplatform.jcdecaux.com"
     "prv_DigitalReservation_IT-value"

     "https://schema-registry.dev.develop.farm"
     "prd.datariver.prv_DigitalReservation-value"))

  (def diff
    (compare-subjects
     "http://registry.dataplatform.jcdecaux.com"
     "prv_DigitalReservation_UK-value"

     "http://registry.dataplatform.jcdecaux.com"
     "prv_DigitalReservation_SE-value"))

  (analyse-compatibility diff)
  (analyse-strict-schema-versions diff)
  (analyse-schema-versions-lenient diff)
  (analyse-schema-versions-lenient-unordered diff)


  (def cfg
    {:name "prv_DigitalReservation_PT",
     :mirror-mode :lenient
     :subject-naming-strategy :topic-name,
     :source
     {:kafka
      {:bootstrap.servers
       "kf1.dataplatform.jcdecaux.com:9092,kf2.dataplatform.jcdecaux.com:9092,kf3.dataplatform.jcdecaux.com:9092",
       :max.partition.fetch.bytes "1000000",
       :max.poll.records "50000",
       :auto.offset.reset "earliest"},
      :topic {:topic-name "prv_DigitalReservation_UK"},
      :schema-registry-url "http://registry.dataplatform.jcdecaux.com"},
     :destination
     {:kafka
      {:bootstrap.servers
       "10.1.151.67:9092,10.1.152.252:9092,10.1.153.241:9092"},
      :topic {:topic-name "prd.datariver.prv_DigitalReservation4"},
      :schema-registry-url "https://schema-registry.dev.develop.farm"},
     :serdes [:string :avro]})


  (analyse-subjetcs cfg)
  (mirror-schemas cfg)


  )
