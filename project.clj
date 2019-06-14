(defproject viooh-mirror (-> "resources/viooh-mirror.version" slurp .trim)
  :description "Utility to mirror kafka topics across clusters"
  :url "https://github.com/VIOOH/viooh-mirror"
  :dependencies [[org.clojure/clojure "1.10.0"]

                 [com.taoensso/encore "2.112.0"]
                 [samsara/trackit-core "0.9.0"]

                 [com.brunobonacci/safely "0.5.0-alpha6"]
                 [com.brunobonacci/oneconfig "0.10.1"
                  :exclusions [samsara/trackit-core]]

                 [fundingcircle/jackdaw "0.6.4"]
                 ;;logging madness
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.fzakaria/slf4j-timbre "0.3.6"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]]

  :main viooh.mirror.main

  :profiles {:uberjar {:aot :all}
             :dev {:jvm-opts ["-D1config.default.backend=fs"]}})
