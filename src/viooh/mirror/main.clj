(ns viooh.mirror.main
  (:gen-class)
  (:require [viooh.mirror.mirror :as mirror]
            [viooh.mirror.http-server :as http-server]
            [com.brunobonacci.oneconfig :refer [configure]]
            [taoensso.timbre :as log]
            [timbre-ns-pattern-level :as timbre-ns-pattern-level]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [integrant.core :as ig]))



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


(defn setup-logging
  []
  (let [conf (-> (io/resource "logging-config.edn")
                 slurp
                 (edn/read-string))]
    (log/info "config:" conf)
    (log/merge-config! {:middleware [(timbre-ns-pattern-level/middleware conf)]})))

(defn -main
  [& args]

  (print-vanity-title)
  (setup-logging)
  (let [cfg (:value (configure {:key (config-key) :env (env) :version (version)}))]

    (ig/init {::http-server/server {}
              ::mirror/mirrors cfg})))



(comment

  (def system (-main))

  (ig/halt! system)

  )
