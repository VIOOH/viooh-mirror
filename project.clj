(defproject viooh-mirror (-> "resources/viooh-mirror.version" slurp .trim)
  :description "Utility to mirror kafka topics across clusters"
  :url "https://github.com/VIOOH/viooh-mirror"
  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]
  :dependencies [[org.clojure/clojure "1.10.1"]

                 [com.taoensso/encore "2.112.0"]
                 [samsara/trackit-core "0.9.2"]
                 [samsara/trackit-prometheus "0.9.2"]
                 [integrant "0.7.0"]
                 [http-kit "2.3.0"]
                 [metosin/compojure-api "1.1.11"]
                 [org.flatland/ordered "1.5.7"]   ;;Version pulled by compojure-api has a bug on jdk 11

                 [com.brunobonacci/safely "0.5.0-alpha7"]
                 [com.brunobonacci/oneconfig "0.10.2"
                  :exclusions [samsara/trackit-core]]

                 [fundingcircle/jackdaw "0.6.4"]
                 [io.confluent/kafka-schema-registry-client "5.1.2"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 ;;logging madness
                 [org.clojure/tools.logging "0.5.0"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.codehaus.janino/janino "3.0.15"] ;; logback configuration conditionals :(
                 [com.internetitem/logback-elasticsearch-appender "1.6"]]

  :main viooh.mirror.main

  :profiles {:uberjar {:aot :all}
             :dev {:jvm-opts ["-D1config.default.backend=fs"]}})
