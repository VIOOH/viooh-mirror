(defproject viooh-mirror "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]

                 [com.taoensso/encore "2.112.0"]
                 [samsara/trackit-core "0.9.0"]

                 [com.brunobonacci/safely "0.5.0-alpha6"]
                 [com.brunobonacci/oneconfig "0.9.2"
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
