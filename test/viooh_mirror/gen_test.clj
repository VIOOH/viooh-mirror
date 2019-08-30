(ns viooh-mirror.gen-test
  (:require [viooh.mirror.schema-mirror :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.string :as str]))

;;
;; Property based testing. Custom generators for subjects diffs
;;

(def num-tests
  (or
   (println "TC_NUM_TESTS=" (or (System/getenv "TC_NUM_TESTS") 100))
   (Integer/getInteger "test-check.num-tests")
   (some-> (System/getenv "TC_NUM_TESTS") Integer/parseInt)
   100))



(def compatibility-levels-gen
  (gen/elements ["NONE" "FORWARD" "BACKWARD" "FULL"
                 "FORWARD_TRANSITIVE" "BACKWARD_TRANSITIVE"
                 "FULL_TRANSITIVE"]))



(def schema-gen
  (gen/hash-map
   :id gen/nat
   :version gen/nat
   :schema (gen/fmap #(keyword (str "schema-" %)) gen/nat)))



(def subject-gen
  (gen/hash-map
   :schema-registry (gen/such-that not-empty gen/string-alphanumeric)
   :subject         (gen/such-that not-empty gen/string-alphanumeric)
   :compatibility   (gen/frequency [[8 compatibility-levels-gen] [2 (gen/return nil)]]),
   :global-compatibility compatibility-levels-gen,
   :versions
   (gen/fmap (comp vec (partial sort-by :version)) (gen/vector schema-gen))))



(def diff-gen
  (gen/hash-map
   :source subject-gen,
   :destination subject-gen))



(comment
  (gen/sample diff-gen 10)
  )
