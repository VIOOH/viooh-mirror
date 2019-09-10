(ns viooh.mirror.serde
  (:require [jackdaw.serdes :refer [string-serde]]
            [viooh.mirror.schema-registry :as r])
  (:import [io.confluent.kafka.serializers
            KafkaAvroDeserializer KafkaAvroSerializer]
           [org.apache.kafka.common.serialization Serdes]))

(defn- avro-serializer
  [url]
  (KafkaAvroSerializer. (r/schema-registry url)))



(defn- avro-deserializer
  [url]
  (KafkaAvroDeserializer. (r/schema-registry url)))



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
