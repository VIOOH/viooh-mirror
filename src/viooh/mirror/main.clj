(ns viooh.mirror.main
  (:gen-class)
  (:require [viooh.mirror.mirror :refer [start-mirror]]
            [com.brunobonacci.oneconfig :refer [configure]]
            [taoensso.timbre :as log]
            [timbre-ns-pattern-level :as timbre-ns-pattern-level]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))



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
   })



(defn env
  "returns the current environmet the system is running in.
   This has to be provided by the infrastructure"
  []
  (or (System/getenv "ENV") "dev"))



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



(defn deep-merge
  "Like merge, but merges maps recursively."
  [& maps]
  (let [maps (filter (comp not nil?) maps)]
    (if (every? map? maps)
      (apply merge-with deep-merge maps)
      (last maps))))



(defn stop-all
  "Takes a list of stop-fns returned by the
  `viooh.mirror.mirror/start-mirror` function. The stop-fns return a
  promise which can be used to block until the mirrors are fully
  stopped. This function calls all the stop-fns first to signal the
  mirrors to stop and then derefs the promises returned to block until
  all the mirrors have stopped."
  [stop-fns]
  (let [p (doall (map #(%) stop-fns))]
    (run! deref p)))



(defn start
  [{:keys [group-id-prefix mirrors]}]
  (doall
   (map (fn [{:keys [serdes] :as mirror}]
          (start-mirror group-id-prefix serdes mirror))
        mirrors)))

(defn setup-logging
  []
  (let [conf (-> (io/resource "logging-config.edn")
                 slurp
                 (edn/read-string)
                 (merge (when (= "dev" (env))
                          {"viooh.*" :debug
                           "org.apache.kafka.*" :info})))]
    (log/info "config:" conf)
    (log/merge-config! {:middleware [(timbre-ns-pattern-level/middleware conf)]})))

(defn -main
  [& args]

  (print-vanity-title)
  (setup-logging)
  (let [cfg (:value (configure {:key (config-key) :env (env) :version (version)}))]
    (start cfg)))



(comment


  (def mirrors
    (-main))

  (stop-all mirrors)

  ;;
  )
