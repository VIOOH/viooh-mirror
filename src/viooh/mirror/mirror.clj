(ns viooh.mirror.mirror
  (:require [jackdaw.client :as k]
            [viooh.mirror.serde :as s]
            [viooh.mirror.schema-mirror :as sm]
            [clojure.walk :refer [stringify-keys]]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [integrant.core :as ig]
            [samsara.trackit :refer [track-rate]]
            [kafka-ssl-helper.core  :as ssl-helper])
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
                     :group.id group-id
                     :max.poll.interval.ms Integer/MAX_VALUE}
        fixed-cfg   {:enable.auto.commit false}
        merged-cfg  (-> (merge default-cfg (:kafka cfg) fixed-cfg)
                       (update :max.poll.interval.ms int)
                       stringify-keys)]
    (log/info "Creating a Kafka Consumer for group-id" group-id
              "and serdes:" serdes)
    (k/consumer merged-cfg serdes)))



(defn producer
  "returns a kafka producer.`serde` are configured as per the
  viooh.mirror.serde namespace."
  [cfg serdes]
  (let [p-cfg (-> (:kafka cfg)
                 stringify-keys)]
    (log/info "Creating a Kafka Producer using serdes:" serdes)
    (k/producer p-cfg serdes)))



(defn ->ProducerRecord
  "Returns a new producer record with the supplied attributes."
  [{:keys [topic-name]} timestamp k v headers]
  (ProducerRecord. ^String topic-name nil ^Long (long timestamp) k v ^Headers headers))



(defn- yield-on-new-value
  "returns the value when it hasn't been observed before.
  NOTE: (not suitable for high cardinality properties)
  "
  []
  (let [observed (volatile! #{})]
    (fn [value]
      (when-not (@observed value)
        (vswap! observed conj value)
        value))))



(defn mirror
  "Polls records from the consumer `c`, and sends them to the
  destination topic using the producer supplied in a loop. If the
  production fails, the same records are tried again. The loop exits
  if `closed?` is set to true."
  [{:keys [poll-interval] mirror-name :name
    {dest-topic :topic} :destination :as mirror-cfg}
   ^KafkaConsumer c ^KafkaProducer p closed?]
  (let [is-new-schema? (yield-on-new-value)]
    (loop [records (k/poll c poll-interval)]
      (log/infof "[%s] Got %s records" mirror-name (count records))
      (track-rate (format "vioohmirror.messages.poll.%s" mirror-name) (count records))

      ;; send each record to destination kafka/topic
      (doseq [{:keys [key value headers timestamp] :as r} records]
        (safely
         (when-not @closed?
           ;; When a new schema is detected, the mirror-schema
           ;; will repair all missing schemas.
           (when (is-new-schema? (sm/avro-schema value))
             (sm/mirror-schemas mirror-cfg))

           ;; TODO: use kafka trx to batch send requests
           @(k/send! p (->ProducerRecord dest-topic timestamp key value headers)))
         :on-error
         :max-retries :forever
         :track-as (format "vioohmirror.messages.send.%s" mirror-name)))

      ;; commit checkpoint
      (when-not @closed?

        (when (seq records)
          (.commitSync c))
        ;; and continue
        (recur (k/poll c poll-interval))))))



(defn- with-ssl-options
  "Takes a consumer/producer config and wraps ssl options (keystores, etc...)"
  [{:keys [private-key ca-cert-pem cert-pem] :as config}]
  (if (and private-key ca-cert-pem cert-pem)
    (merge (dissoc config :private-key :cert-pem :ca-cert-pem)
           (ssl-helper/ssl-opts config))
    config))



(defn start-mirror
  "Starts a `mirror`.A `mirror` is a consumer loop that sends each record
  received from the source topic to the destination topic. source and
  destination topics can belong to different kafka clusters. The
  producer and consumer are setup with the avro serdes from the
  viooh.mirror.serde namespace which automatically create the schemas
  in the destination cluster."
  [{:keys [consumer-group-id name source destination serdes] :as mirror-cfg}]
  (let [closed? (atom false)
        p (promise)
        src-schema-registry-url (:schema-registry-url source)
        dest-schema-registry-url (:schema-registry-url destination)
        src-topic-cfg (:topic source)
        src-topic (:topic-name src-topic-cfg)
        dest-topic-cfg (:topic destination)
        dest-topic (:topic-name dest-topic-cfg)
        src-serdes (s/serdes serdes src-schema-registry-url)
        dest-serdes (s/serdes serdes dest-schema-registry-url)
        destination (update destination :kafka with-ssl-options)]
    (future
      (log/infof "[%s] Starting mirror" name)
      (safely
       (when-not @closed?
         ;; TODO: mirror subjects even before start consuming
         ;; TODO: check if you need to create topic
         (with-open [c (consumer consumer-group-id source src-serdes)
                     p (producer destination dest-serdes)]

           (k/subscribe c [src-topic-cfg])
           (log/info "Subscribed to source using topic config:" src-topic-cfg)
           (mirror mirror-cfg c p closed?)))
       :on-error
       :max-retries :forever
       :track-as (format "vioohmirror.init.%s" name))

      (log/infof "[%s] Stopping mirror" name)
      (deliver p true))

    (fn [] (reset! closed? true) p)))



(defmethod ig/init-key ::mirrors [_ {:keys [groups] :as cfg}]
  (let [stop-fns (doall (map start-mirror (mapcat :mirrors groups)))]
    (log/info "Started all mirrors")
    stop-fns))



(defmethod ig/halt-key! ::mirrors [_ stop-fns]
  (let [p (doall (map #(%) stop-fns))]
    (run! deref p))
  (log/info "Stopped all mirrors"))
