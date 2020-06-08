;;
;; Copyright 2019-2020 VIOOH Ltd
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns viooh.mirror.serde
  (:require [jackdaw.serdes :refer [string-serde]]
            [clojure.tools.logging :as log]
            [viooh.mirror.schema-registry :as r]
            [clojure.walk :as walk])
  (:import [io.confluent.kafka.serializers
            KafkaAvroDeserializer KafkaAvroSerializer]
           [org.apache.kafka.common.serialization Serdes]))



(defn avro-serde-config
  [sr-url {:as schema-registry-configs} key-subject-name-strategy value-subject-name-strategy]
  (merge schema-registry-configs
         {"schema.registry.url"       sr-url
          "key.subject.name.strategy" key-subject-name-strategy
          "value.subject.name.strategy" value-subject-name-strategy}))



(defn- avro-serializer
  [schema-registry key-subject-name-strategy value-subject-name-strategy]
  (let [sr-url (:url schema-registry)
        sr-configs (walk/stringify-keys (:configs schema-registry))]
    (KafkaAvroSerializer. (:client (r/schema-registry sr-url sr-configs))
                          (avro-serde-config sr-url sr-configs
                                             key-subject-name-strategy
                                             value-subject-name-strategy))))



(defn- avro-deserializer
  [schema-registry key-subject-name-strategy value-subject-name-strategy]
  (let [sr-url (:url schema-registry)
        sr-configs (walk/stringify-keys (:configs schema-registry))]
    (KafkaAvroDeserializer. (:client (r/schema-registry sr-url sr-configs))
                            (avro-serde-config sr-url
                                               sr-configs
                                               key-subject-name-strategy
                                               value-subject-name-strategy))))



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
  {:key-serde (serde key-serde schema-registry key-subject-name-strategy value-subject-name-strategy)
   :value-serde (serde value-serde schema-registry key-subject-name-strategy value-subject-name-strategy)})
