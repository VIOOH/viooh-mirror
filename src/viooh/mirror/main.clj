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
  {
   ;;:groups
   ;;[
   ;; {:group-id-prefix "foggy"
   ;;
   ;;  :source
   ;;  {:kafka {:bootstrap.servers "localhost:9092"
   ;;           :auto.offset.reset "earliest"}
   ;;   :schema-registry-url "http://localhost:8081"}
   ;;
   ;;  :destination
   ;;  {:kafka {:bootstrap.servers "localhost:9092"}
   ;;   :schema-registry-url "http://localhost:8081"}
   ;;
   ;;  :mirrors
   ;;  [{:source      {:topic {:topic-name "mirror_test"}}
   ;;    :destination {:topic {:topic-name "mirror_test_copy_4"}}}]
   ;;
   ;;  }
   ;;
   ;; ]


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
