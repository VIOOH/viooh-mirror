(ns viooh.mirror.serde
  (:require [jackdaw.serdes :refer [string-serde]]
            [safely.core :refer [safely]]
            [jackdaw.serdes.avro.schema-registry :as sr]
            [clojure.set :refer [difference union]])
  (:import [io.confluent.kafka.serializers KafkaAvroDeserializer KafkaAvroSerializer]
           [org.apache.kafka.common.serialization Serdes]))

(defn- avro-serializer
  [schema-registry]
  (KafkaAvroSerializer. schema-registry))



(defn- avro-deserializer
  [schema-registry]
  (KafkaAvroDeserializer. schema-registry))



(defmulti serde (fn [type config] type))


(defmethod serde :string
  [_ _]
  (string-serde))


(defmethod serde :avro
  [_ schema-registry]
  (Serdes/serdeFrom (avro-serializer schema-registry)
                    (avro-deserializer schema-registry)))


(defn serdes
  [[key-serde value-serde] schema-registry]
  {:key-serde (serde key-serde schema-registry)
   :value-serde (serde value-serde schema-registry)})
