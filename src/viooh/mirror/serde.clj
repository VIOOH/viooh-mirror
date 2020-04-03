(ns viooh.mirror.serde
  (:require [jackdaw.serdes :refer [string-serde]]
            [viooh.mirror.schema-registry :as r])
  (:import [io.confluent.kafka.serializers
            KafkaAvroDeserializer KafkaAvroSerializer]
           [org.apache.kafka.common.serialization Serdes]
           (java.util HashMap)))

(defn avro-serde-config
  [key-subject-name-strategy value-subject-name-strategy]
  (doto (HashMap.)
    (.put "key.subject.name.strategy" key-subject-name-strategy)
    (.put "value.subject.name.strategy" value-subject-name-strategy)))

(defn- avro-serializer
  [url key-subject-name-strategy value-subject-name-strategy]
  (KafkaAvroSerializer. (r/schema-registry url)
                        (avro-serde-config key-subject-name-strategy
                                           value-subject-name-strategy)))

(defn- avro-deserializer
  [url key-subject-name-strategy value-subject-name-strategy]
  (KafkaAvroDeserializer. (r/schema-registry url)
                          (avro-serde-config key-subject-name-strategy
                                             value-subject-name-strategy)))


(defmulti serde (fn [type _ _ _] type))


(defmethod serde :string
  [_ _ _ _]
  (string-serde))


(defmethod serde :avro
  [_ schema-registry key-subject-name-strategy value-subject-name-strategy]
  (Serdes/serdeFrom (avro-serializer schema-registry key-subject-name-strategy value-subject-name-strategy)
                    (avro-deserializer schema-registry key-subject-name-strategy value-subject-name-strategy)))


(defn serdes
  [[key-serde value-serde] schema-registry key-subject-name-strategy value-subject-name-strategy]
  {:key-serde (serde key-serde schema-registry)
   :value-serde (serde value-serde schema-registry)})
