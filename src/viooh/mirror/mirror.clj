(ns viooh.mirror.mirror
  (:require [jackdaw.client :as k]
            [viooh.mirror.serde :as s]
            [clojure.walk :refer [stringify-keys]]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer]
           [org.apache.kafka.clients.producer
            ProducerRecord RecordMetadata]
           [org.apache.kafka.common.header Headers]))



(defn consumer
  "returns a kafka consumer.`enable.auto.commit` is set to false.
  `auto.offset.reset` is defaulted to earliest.`serde` are configured
  as per the viooh.mirror.serde namespace."
  [group-id cfg serdes]
  (let [default-cfg {:auto.offset.reset "earliest"
                     :group.id group-id}
        fixed-cfg   {:enable.auto.commit false}]
    (-> (merge default-cfg (:kafka cfg) fixed-cfg)
        stringify-keys
        (k/consumer (s/serdes serdes cfg)))))



(defn producer
  "returns a kafka producer.`serde` are configured as per the
  viooh.mirror.serde namespace."
  [cfg serdes]
  (-> (:kafka cfg)
      stringify-keys
      (k/producer (s/serdes serdes cfg))))



(defn ->ProducerRecord
  "Returns a new producer record with the supplied attributes."
  [{:keys [topic-name]} timestamp k v headers]
  (ProducerRecord. ^String topic-name nil ^Long (long timestamp) k v ^Headers headers))



(defn send-to-destination
  "Sends the supplied records to the destination topic using the
  supplied producer. Retries the whole batch a few times before giving
  up. Returns `true` if all the records were successfully produced to
  the destination topic, `false` otherwise. "
  [p dest-topic records]
  (safely
   (doseq [{:keys [key value headers timestamp] :as r} records]
     @(k/send! p (->ProducerRecord dest-topic timestamp key value headers)))
   true
   :on-error
   :max-retry 3
   :default false
   :retry-delay [:random-exp-backoff :base 300 :+/- 0.35 :max 30000]))



(defn mirror
  "Polls records from the consumer `c`, and sends them to the
  destination topic using the producer supplied in a loop. If the
  production fails, the same records are tried again. The loop exits
  if `closed?` is set to true."
  [mirror-name ^KafkaConsumer c ^KafkaProducer p dest-topic closed?]
  (loop [records (k/poll c 3000)]
    (log/infof "[%s] Got %s records" mirror-name (count records))
    (cond
      @closed? :closed
      (send-to-destination p dest-topic records)
      (do
        (.commitSync c)
        (recur (k/poll c 3000)))
      :else
      (recur records))))



(defn start-mirror
  "Starts a `mirror`.A `mirror` is a consumer loop that sends each record
  received from the source topic to the destination topic. source and
  destination topics can belong to different kafka clusters. The
  producer and consumer are setup with the avro serdes from the
  viooh.mirror.serde namespace which automatically create the schemas
  in the destination cluster."
  [group-id-prefix serdes {:keys [name source destination] :as mirror-cfg}]
  (let [closed? (atom false)
        p (promise)
        group-id (str/join "_" [group-id-prefix name])]
    (future
      (log/infof "[%s] Starting mirror" name)
      (with-open [c (consumer group-id source serdes)
                  p (producer destination serdes)]

        (k/subscribe c [(:topic source)])

        (safely
         (mirror group-id c p (:topic destination) closed?)
         :on-error
         :max-retry :forever))

      (log/infof "[%s] Stopping mirror" name)
      (deliver p true))

    (fn [] (reset! closed? true) p)))
