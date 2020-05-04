(ns viooh.mirror.mirror
  (:require [jackdaw.client :as k]
            [jackdaw.admin :as ka]
            [viooh.mirror.serde :as s]
            [viooh.mirror.schema-mirror :as sm]
            [clojure.walk :refer [stringify-keys]]
            [safely.core :refer [safely]]
            [safely.thread-pool :refer [thread]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [integrant.core :as ig]
            [samsara.trackit :refer [track-rate track-count]]
            [com.brunobonacci.mulog :as u])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer]
           [org.apache.kafka.clients.producer
            ProducerRecord RecordMetadata]
           [org.apache.kafka.clients.admin AdminClient]
           [org.apache.kafka.common.header Headers]
           [org.apache.avro Schema]))



(defn- wait-for-topic-ready?
  [^AdminClient admin-client {:keys [topic-name] :as topic}
   & {:keys [max-retries] :or {max-retries :forever}}]
  (safely
   (->>
    (ka/describe-topics admin-client [topic])
    (every? (fn [[topic-name {:keys [partition-info]}]]
              (every? (fn [part-info]
                        (and (boolean (:leader part-info))
                           (seq (:isr part-info))))
                      partition-info))))
   :on-error
   :circuit-breaker :kafka-admin-request
   :timeout 60000
   :max-retries max-retries
   :retry-delay [:random 3000 :+/- 0.30]
   :failed? #(not %)
   :log-stacktrace false
   :message (format "Waiting for the topic %s to become ready." topic-name)))



(defn- describe-topic
  [^AdminClient admin-client {:keys [topic-name] :as topic}]
  (safely
   (-> (ka/describe-topics admin-client [topic]) vals first)
   :on-error
   :circuit-breaker :kafka-admin-request
   :timeout 60000
   :default nil
   :log-stacktrace false
   :message (str "reading status of topic:" topic-name)))



(defn ensure-topic!
  "It ensures that the given topic exists in the given kafka cluster.
  If not present it will create the topic and wait until the topic is
  ready."
  [kafka-cfg {:keys [topic-name partition-count replication-factor] :as topic}]
  {:pre [(:bootstrap.servers kafka-cfg) (:topic-name topic)
         (:partition-count topic) (:replication-factor topic)]}

  ;; TODO: this should be done in the general conf.
  (safely
   (log/infof "Ensuring topic %s exists and it is ready to use." topic-name)
   (with-open [^java.lang.AutoCloseable admin (ka/->AdminClient (stringify-keys kafka-cfg))]
     ;; create if not exists
     (when-not (describe-topic admin topic)
       (log/infof "Creating topic '%s' in destination cluster" topic-name)
       ;; create topic
       (ka/create-topics! admin [topic])
       (u/log ::topic-created :topic topic-name
              :partition-count partition-count :replication-factor replication-factor))

     ;; wait for the topic to become ready
     (wait-for-topic-ready? admin topic :max-retries 10))
   :on-error
   :max-retries :forever
   :message (format "Ensuring topic %s exists and it is ready." topic-name)))


;;
;; Utility function to decide best values for mirroring
;; out of a number of strategies:
;; examples are: {:type :fix :val 3}, or {:type :mirror-source :min 3 :max 12}
;;
(defmulti config-value-mirror
  (fn [{:keys [type]} _] type))



(defmethod config-value-mirror :fix
  [{:keys [value]} proposed-value]
  value)



(defmethod config-value-mirror :mirror-source
  [{low :min high :max} proposed-value]
  (min high (max low proposed-value)))



(defn mirror-kafka-topic-configuration!
  [{:keys [source destination topics-mirroring] :as mirror-cfg}]
  (let [src-info (with-open [^java.lang.AutoCloseable admin
                             (ka/->AdminClient (stringify-keys (:kafka source)))]
                   (describe-topic admin (:topic source)))
        partitions (-> src-info :partition-info count)
        replicas   (-> src-info :partition-info first :replicas count)]
    (when-not src-info
      (throw (ex-info "Source topic doesn't exists" (:topic source))))

    (let [partitions (config-value-mirror
                      (:partition-count topics-mirroring) (or partitions 0))
          replicas   (config-value-mirror
                      (:replication-factor topics-mirroring) (or replicas 0))
          dest-topic (merge {:partition-count partitions
                             :replication-factor replicas} (:topic destination))]

      ;; ensure topic exists in destination cluster
      (ensure-topic! (:kafka destination) dest-topic))))



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
  (let [is-new-schema? (yield-on-new-value)
        sleeps         (atom [900000 0 1400000 0])]
    (loop [records (k/poll c poll-interval)]
      (log/debugf "[%s] Got %s records" mirror-name (count records))
      (u/log ::messages-polled :num-records (count records))
      (track-rate (format "vioohmirror.messages.poll.%s" mirror-name) (count records))
      (track-count (format "vioohmirror.kafka.polls.%s" mirror-name))

      (when (some #(clojure.string/includes? mirror-name %) ["DigitalReservation_MO" "prv_DigitalReservation_IE"])
        (let [s (or (first @sleeps) 0)
              _ (swap! sleeps rest)]
          (Thread/sleep s)))

      ;; send each record to destination kafka/topic
      (->> records
         (map (fn [{:keys [key value headers timestamp] :as r}]
                (safely
                 (when-not @closed?
                   ;; When a new schema is detected, the mirror-schema
                   ;; will repair all missing schemas.
                   (let [^Schema src-schema (sm/avro-schema value)
                         schema-name (.getFullName src-schema)]
                     (when (is-new-schema? src-schema)
                       (sm/mirror-schemas mirror-cfg schema-name)))

                   ;; returns a java future
                   (k/send! p (->ProducerRecord dest-topic timestamp key value headers)))
                 :on-error
                 :max-retries :forever
                 :track-as (format "vioohmirror.messages.send.%s" mirror-name))))
         ;; realize the entire sequence to avoid the small batching effect
         ;; of the chunked sequences as batches are too small.
         (doall)
         ;; wait for all the send to be acknowledged before moving forward
         (run! deref))

      ;; track event when the messages have been sent and acknowledged
      (u/log ::messages-sent :num-records (count records))

      ;; commit checkpoint
      (when-not @closed?

        (when (seq records)
          (.commitSync c))
        ;; and continue
        (recur (k/poll c poll-interval))))))



(defn start-mirror
  "Starts a `mirror`.A `mirror` is a consumer loop that sends each record
  received from the source topic to the destination topic. source and
  destination topics can belong to different kafka clusters. The
  producer and consumer are setup with the avro serdes from the
  viooh.mirror.serde namespace which automatically create the schemas
  in the destination cluster."
  [{:keys [consumer-group-id name source destination serdes key-subject-name-strategy value-subject-name-strategy] :as mirror-cfg}]
  (let [closed? (atom false)
        p (promise)
        src-schema-registry-url (:schema-registry-url source)
        dest-schema-registry-url (:schema-registry-url destination)
        src-topic-cfg (:topic source)
        src-topic (:topic-name src-topic-cfg)
        dest-topic-cfg (:topic destination)
        dest-topic (:topic-name dest-topic-cfg)
        src-serdes (s/serdes serdes src-schema-registry-url key-subject-name-strategy value-subject-name-strategy)
        dest-serdes (s/serdes serdes dest-schema-registry-url key-subject-name-strategy value-subject-name-strategy)]
    (thread {:name name}
      ;; It sleeps for a bounded random amount of time.  It is used to
      ;; spread the mirror starts uniformly and avoid polling storms.
      (safely.core/sleep 5000 :+/- 0.5)

      (u/with-context {:mirror-name name :src-schema-registry-url src-schema-registry-url
                       :dest-schema-registry-url dest-schema-registry-url
                       :src-topic src-topic :dest-topic dest-topic}
        (log/infof "[%s] Starting mirror" name)
        (u/log ::mirror-started)
        (safely
            (when-not @closed?
              ;; TODO: mirror subjects even before start consuming

              ;; ensuring that the destination topic exists
              (when (get-in mirror-cfg [:topics-mirroring :auto-create-topics] true)
                (mirror-kafka-topic-configuration! mirror-cfg))

              (with-open [^java.lang.AutoCloseable c (consumer consumer-group-id source src-serdes)
                          ^java.lang.AutoCloseable p (producer destination dest-serdes)]

                (k/subscribe c [src-topic-cfg])
                (log/info "Subscribed to source using topic config:" src-topic-cfg)
                (mirror mirror-cfg c p closed?)))
          :on-error
          :max-retries :forever
          :track-as (format "vioohmirror.init.%s" name))

        (log/infof "[%s] Stopping mirror" name)
        (u/log ::mirror-stopped)
        (deliver p true)))

    (fn [] (reset! closed? true) p)))



(defmethod ig/init-key ::mirrors [_ {:keys [groups] :as cfg}]
  (let [mirrors  (->> groups (mapcat :mirrors) (filter :enabled))
        stop-fns (->> mirrors (map start-mirror) doall)]
    (log/info "Started all mirrors")
    (u/log ::mirrors-initiated :mirrors (count mirrors))
    stop-fns))



(defmethod ig/halt-key! ::mirrors [_ stop-fns]
  (let [p (doall (map #(%) stop-fns))]
    (run! deref p))
  (log/info "Stopped all mirrors"))
