(ns viooh.mirror.main
  (:gen-class)
  (:require [viooh.mirror.mirror :as mirror]
            [viooh.mirror.http-server :as http-server]
            [com.brunobonacci.oneconfig :refer [configure deep-merge]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [integrant.core :as ig]
            [samsara.trackit :as trackit]))



(def DEFAULT-CONFIG
  {;; The configuration is designed around a number of groups A group
   ;; is contains the configuration for a number individual stream
   ;; mirrors. A mirror is a configuration which specifies a source
   ;; and a destination topic.  Mirrors that have in common the same
   ;; source and destination clusters can use groups to simplify the
   ;; configuration otherwise redundant. All the properties defined at
   ;; the group level will be merged in the individual mirror
   ;; definitions.
   ;;
   ;; The mirror's name and the consumer group will be automatically
   ;; generated using the :group-id-prefix. The consumer-group-id can
   ;; be overridden.
   ;;
   :groups
   [;; All the properties defined at the group level will be
    ;; available also at the single mirror configuration level
    ;; {
    ;;  ;; This is prefixed on all mirror `:name`
    ;;  :group-id-prefix "prod"
    ;;
    ;;  ;; the mirroring strategy used for the whole group. Currently we
    ;;  ;; support two modes `:strict` (default) and `:lenient`. The
    ;;  ;; Strict mode will attempt to have an exact copy of the subjects
    ;;  ;; across the two schema-registry, if this is not possible then it
    ;;  ;; will abort the mirroring. Lenient will try to ensure that all
    ;;  ;; the schemas which are present in the source subject are also
    ;;  ;; replicated in the destination cluster *in the same relative order*
    ;;  ;; however it will allow the destination to have additional schemas
    ;;  ;; which are not present in the source. Useful to merge a number
    ;;  ;; of similar topics into one or to use the destination as development
    ;;  ;; version of the source and thus allow to submit newer versions.
    ;;  ;; :mirror-mode :strict
    ;;
    ;;  ;; The Consumer poll-interval, the maximum amount of time a consumer
    ;;  ;; will wait when no records are present before returning a empty batch.
    ;;  ;; :poll-interval 10000
    ;;
    ;;  ;; the kafka and schema-registry information for the source
    ;;  ;; clusters (the ones you want to take the data from)
    ;;  :source
    ;;  {;; the kafka properties to use to connect the the source clusters
    ;;   :kafka {;; the comma-separated list of brokers to connect the consumer
    ;;           :bootstrap.servers "source:9092"
    ;;           ;; From which point you want to start consuming: "earliest"
    ;;           ;; or "latest", (default: earliest).
    ;;           :auto.offset.reset "earliest"
    ;;           ;; here you might want to add additional consumer properties
    ;;           ;; for example:
    ;;           :max.partition.fetch.bytes "1000000"
    ;;           :max.poll.records "50000"
    ;;           }
    ;;   ;; the url of the schema registry associated with the source kafka cluster
    ;;   :schema-registry-url "http://source-sr:8081"}
    ;;
    ;;  ;; The information for the destination (where to copy the data).
    ;;  :destination
    ;;  {:kafka {;; the comma-separated list of brokers to connect the producer
    ;;           :bootstrap.servers "destination:9092"
    ;;           ;; here you might want to add additional producer properties
    ;;           }
    ;;   :schema-registry-url "http://destinaiton-sr:8081"}
    ;;
    ;;  ;; Now in `:mirrors` you can list all the topics (source/dest) to mirror
    ;;  ;; from the above clusters. You can specify additional properties which
    ;;  ;; are specific only to a particular mirror or override previously defined
    ;;  ;; values (defined at group level).
    ;;  :mirrors
    ;;  [{;; the name of the particular mirror. If not defined it will be automatically
    ;;    ;; generated to be the name of the source and destination topics.
    ;;    ;;:name "<prefix>__<source-topic>__<dest-topic>"
    ;;
    ;;    ;; The group-id for the consumer. This will be used for checkpointing and load
    ;;    ;; and load-balancing. It is important that it is consistent over time.
    ;;    ;; IF CHANGED IT WILL CAUSE THE CONSUMER TO RESET TO THE EARLIEST (OR LATEST) RECORD.
    ;;    ;; if not provided it will be automatically generated as follow.
    ;;    ;; :consumer-group-id "<env>.viooh-mirror.<source-topic>__<dest-topic>"
    ;;
    ;;    ;; in this case it will override the group-level configuration just
    ;;    ;; for this specific mirror.
    ;;    ;; :mirror-mode :lenient
    ;;
    ;;    ;; which strategy to use for the subject naming `:topic-name` is the default
    ;;    ;; and the only supported option at the moment
    ;;    ;; :subject-naming-strategy :topic-name
    ;;
    ;;    ;; The source topic to mirror
    ;;    :source      {:topic {:topic-name "source-topic"}}
    ;;    ;; The name of the topic to mirror to.
    ;;    :destination {:topic {:topic-name "mirror_test_copy_4"}}
    ;;
    ;;    ;; the SerDes to use for the key and the value respectively
    ;;    :serdes [:string :avro]}]
    ;;  }
    ;; more groups can be added
    ]


   :metrics
   {;; type can be `:console` or `:prometheus`, or any if the TrackIt
    ;; supported reporters, or use `nil` to disable it
    ;; see Prometheus configuration here: https://github.com/samsara/trackit#prometheus
    ;;:type :console

    ;; Intentionally leaving out FileDescriptorRatioGauge as it does
    ;; not work with JDK12.See:
    ;; https://github.com/metrics-clojure/metrics-clojure/issues/133
    ;; and
    ;; https://github.com/metrics-clojure/metrics-clojure/pull/142.
    ;; Once the aboce issue is fixed, trackit needs to be released
    ;; with the upgraded metrics-clojure dependencies before we can
    ;; report this gauge again.
    :jvm-metrics [:memory :gc :threads :attributes]
    ;; how often the stats will be displayed
    :reporting-frequency-seconds 60}
   })



(def DEFAULT-MIRROR-CONFIG
  {:mirror-mode             :strict
   :subject-naming-strategy :topic-name
   :poll-interval           10000})



(defn env
  "returns the current environmet the system is running in.
   This has to be provided by the infrastructure"
  []
  (or (System/getenv "ENV") "local"))



(defn version
  "returns the version of the current version of the project
   from the resource bundle."
  []
  (some->> (io/resource "viooh-mirror.version")
           slurp
           str/trim))



(defn- apply-single-mirror-default
  "given the configuration of a single mirror, and the configuration of a group
  it merges the two configurations and applies the defautls"
  [group-config mirror-config]
  (as-> mirror-config $

    ;; applied the mirror level defaults and the group level defaults
    (deep-merge DEFAULT-MIRROR-CONFIG (dissoc group-config :mirrors :name) $)

    ;; if a name is not provided it generates based on the source and destination topics
    (update $ :name (fn [name]
                      (or name
                         (format "%s__%s"
                                 (get-in $ [:source :topic :topic-name])
                                 (get-in $ [:destination :topic :topic-name])))))

    ;; it add the groups prefix to the name (when defined)
    (update $ :name (fn [name]
                      (if (:group-id-prefix group-config)
                        (format "%s__%s" (:group-id-prefix group-config) name)
                        name)))

    ;; if not set, it generates a standard consumer-group-id
    (update $ :consumer-group-id (fn [cgid]
                                   (or cgid
                                      (format "%s.viooh-mirror.%s"
                                              (env) (:name $)))))))




(defn- apply-config-defaults
  "Applied the default configuration"
  [cfg]
  (as-> cfg $
     (deep-merge DEFAULT-CONFIG $)
     (update $ :groups
             (partial mapv
                (fn [group]
                  (update group :mirrors
                          (partial mapv (partial apply-single-mirror-default group))))))))




(defn config-key
  "returns the current environmet the system is running in.
   This has to be provided by the infrastructure"
  []
  (or (System/getenv "ONE_CONF_KEY") "viooh-mirror"))



(defn print-vanity-title
  "Prints a cool vanity title from the title.txt file in project
  resources"
  []
  (-> "title.txt"
     io/resource slurp
     (format (version))
     println))



(defn start-metrics!
  "Start the metrics reporter"
  [{:keys [metrics] :as config}]
  (when (:type metrics)
    (trackit/start-reporting! metrics)))



(defn -main
  [& args]
  (print-vanity-title)
  (log/infof "Starting viooh-mirror v%s in env: '%s'" (version) (env))
  (let [cfg (:value (configure {:key (config-key) :env (env) :version (version)}))
        cfg (apply-config-defaults cfg)]

    (start-metrics! cfg)
    (ig/init {::http-server/server {}
              ::mirror/mirrors cfg})))



(comment

  (def system (-main))

  (ig/halt! system)

  )
