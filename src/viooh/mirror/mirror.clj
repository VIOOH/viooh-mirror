(ns viooh.mirror.mirror
  (:require [jackdaw.client :as k]
            [viooh.mirror.serde :as s]
            [viooh.mirror.schema-mirror :as sm]
            [clojure.walk :refer [stringify-keys]]
            [safely.core :refer [safely]]
            [taoensso.timbre :as log]
            [jackdaw.serdes.avro.schema-registry :as sr]
            [clojure.string :as str]
            [integrant.core :as ig])
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
        fixed-cfg   {:enable.auto.commit false}
        merged-cfg  (-> (merge default-cfg (:kafka cfg) fixed-cfg)
                        stringify-keys)]
    (log/info "Creating a Kafka Consumer using config:" merged-cfg
              "and serdes:" serdes)
    (k/consumer merged-cfg serdes)))



(defn producer
  "returns a kafka producer.`serde` are configured as per the
  viooh.mirror.serde namespace."
  [cfg serdes]
  (let [p-cfg (-> (:kafka cfg)
                  stringify-keys)]
    (log/info "Creating a Kafka Producer using config:" p-cfg
              "and serdes:" serdes)
    (k/producer p-cfg serdes)))



(defn ->ProducerRecord
  "Returns a new producer record with the supplied attributes."
  [{:keys [topic-name]} timestamp k v headers]
  (ProducerRecord. ^String topic-name nil ^Long (long timestamp) k v ^Headers headers))



(defn send-to-destination
  "Sends the supplied records to the destination topic using the
  supplied producer. Retries the whole batch a few times before giving
  up. Returns `true` if all the records were successfully produced to
  the destination topic, `false` otherwise. "
  [p dest-topic value-schema-mirror records]
  (safely
   (doseq [{:keys [key value headers timestamp] :as r} records]
     (value-schema-mirror value)
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
  [mirror-name ^KafkaConsumer c ^KafkaProducer p dest-topic value-schema-mirror closed?]
  (loop [records (k/poll c 3000)]
    (log/infof "[%s] Got %s records" mirror-name (count records))
    (cond
      @closed? :closed
      (send-to-destination p dest-topic value-schema-mirror records)
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
        group-id (str/join "_" [group-id-prefix name])
        src-schema-registry-url (:schema-registry-url source)
        dest-schema-registry-url (:schema-registry-url destination)
        src-topic-cfg (:topic source)
        src-topic (:topic-name src-topic-cfg)
        dest-topic-cfg (:topic destination)
        dest-topic (:topic-name dest-topic-cfg)
        src-registry (sr/client src-schema-registry-url (or (:max-capacity source) 128))
        dest-registry (sr/client dest-schema-registry-url (or (:max-capacity destination) 128))
        src-serdes (s/serdes serdes src-registry)
        dest-serdes (s/serdes serdes dest-registry)
        value-schema-mirror (sm/create-schema-mirror src-registry dest-registry
                                                     src-topic dest-topic false)]
    (future
      (log/infof "[%s] Starting mirror" name)
      (with-open [c (consumer group-id source src-serdes)
                  p (producer destination dest-serdes)]

        (safely
         (k/subscribe c [src-topic-cfg])
         (log/info "Subscribed to source using topic config:" src-topic-cfg)
         (mirror group-id c p dest-topic-cfg value-schema-mirror closed?)
         :on-error
         :max-retry :forever))

      (log/infof "[%s] Stopping mirror" name)
      (deliver p true))

    (fn [] (reset! closed? true) p)))



(defmethod ig/init-key ::mirrors [_ {:keys [group-id-prefix mirrors] :as cfg}]
  (let [stop-fns (doall
                  (map (fn [{:keys [serdes] :as mirror}]
                         (start-mirror group-id-prefix serdes mirror))
                       mirrors))]
    (log/info "Started all mirrors")
    stop-fns))

(defmethod ig/halt-key! ::mirrors [_ stop-fns]
  (let [p (doall (map #(%) stop-fns))]
    (run! deref p))
  (log/info "Stopped all mirrors"))
