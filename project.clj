(defproject com.viooh/viooh-mirror (-> "resources/viooh-mirror.version" slurp .trim)
  :description "Utility to mirror selected Kafka topics and their schemas across clusters."
  :url "https://github.com/VIOOH/viooh-mirror"


  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]

  :dependencies [[org.clojure/clojure "1.10.3"]

                 [samsara/trackit-core "0.9.3"]
                 [samsara/trackit-prometheus "0.9.3"]
                 [integrant "0.8.0"]
                 [http-kit "2.5.3"]
                 [cheshire "5.10.0"]
                 [metosin/compojure-api "1.1.13"]

                 [com.brunobonacci/safely "0.5.0"]
                 [com.brunobonacci/oneconfig "0.16.0"
                  :exclusions [samsara/trackit-core com.fasterxml.jackson.core/jackson-databind]]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]

                 [com.viooh/kafka-ssl-helper "0.9.0"]

                 [fundingcircle/jackdaw "0.7.10"]
                 [io.confluent/kafka-schema-registry-client "5.4.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]

                 ;;logging
                 [org.clojure/tools.logging "1.1.0"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.codehaus.janino/janino "3.1.3"] ;; logback configuration conditionals :(
                 [com.internetitem/logback-elasticsearch-appender "1.6"]

                 ;; observability
                 [com.brunobonacci/mulog               "0.7.1"]
                 [com.brunobonacci/mulog-elasticsearch "0.7.1"]
                 [com.brunobonacci/mulog-kafka         "0.7.1"]
                 [com.brunobonacci/mulog-cloudwatch    "0.7.1"]]

  :main viooh.mirror.main

  :global-vars {*warn-on-reflection* true}

  :profiles {:uberjar {:aot :all}
             :dev {:jvm-opts ["-D1config.default.backend=fs"]
                   :dependencies [[midje "1.9.10"]
                                  [org.clojure/test.check "1.1.0"]
                                  [criterium "0.4.6"]]
                   :plugins      [[lein-midje "3.2.2"]]}})
