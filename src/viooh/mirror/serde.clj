(ns viooh.mirror.serde
  (:require [jackdaw.serdes :refer [string-serde]]
            [jackdaw.serdes.avro.schema-registry :as sr])
  (:import [io.confluent.kafka.serializers KafkaAvroDeserializer KafkaAvroSerializer]
           org.apache.kafka.common.serialization.Serdes))



(defn- avro-serializer
  [{:keys [schema-registry-url max-capacity]}]
  (KafkaAvroSerializer.
   (sr/client schema-registry-url (or max-capacity 128))
   {"auto.register.schemas" true
    "schema.registry.url" schema-registry-url}))



(defn- avro-deserializer
  [{:keys [schema-registry-url max-capacity]}]
  (KafkaAvroDeserializer.
   (sr/client schema-registry-url (or max-capacity 128))))



(defmulti serde (fn [type config] type))



(defmethod serde :string
  [_ _]
  (string-serde))



(defmethod serde :avro
  [_ config]
  (Serdes/serdeFrom (avro-serializer config)
                    (avro-deserializer config)))



(defn serdes
  [[key-serde value-serde] config]
  {:key-serde (serde key-serde config)
   :value-serde (serde value-serde config)})
