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
  {:group-id-prefix "foggy"
   ;; :mirrors
   ;; [{:name "mirror"
   ;;   :source
   ;;   {:kafka {:bootstrap.servers "localhost:9092"
   ;;            :auto.offset.reset "earliest"}
   ;;    :topic {:topic-name "mirror_test"}
   ;;    :schema-registry-url "http://localhost:8081"}

   ;;   :destination
   ;;   {:kafka {:bootstrap.servers "localhost:9092"}
   ;;    :topic {:topic-name "mirror_test_copy_4"}
   ;;    :schema-registry-url "http://localhost:8081"}}]

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
  (let [cfg (:value (configure {:key (config-key) :env (env) :version (version)}))
        cfg (deep-merge DEFAULT-CONFIG cfg)]

    (start-metrics! cfg)
    (ig/init {::http-server/server {}
              ::mirror/mirrors cfg})))



(comment

  (def system (-main))

  (ig/halt! system)

  )
