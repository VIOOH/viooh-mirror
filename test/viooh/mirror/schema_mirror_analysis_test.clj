;;
;; Copyright 2019-2020 VIOOH Ltd
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns viooh.mirror.schema-mirror-analysis-test
  (:require [viooh.mirror.schema-mirror :refer :all]
            [midje.sweet :refer :all]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;         ----==| C O M P A R E - C O M P A T I B I L I T Y |==----          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def source-schema-registry ::source-schema-registry)
(def destination-schema-registry ::destination-schema-registry)



(fact "analyse-compatibility: if subject compatibility level is
 specified and it used"

  (->>
      {:source
       {:schema-registry source-schema-registry,
        :subject "source-subject-value",
        :compatibility "FORWARD",
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))
  => (contains {:src-level "FORWARD" :dst-level "NONE"} )
  )



(fact "analyse-compatibility: when the subject compatibility level
   isn't specified the global registry compatibility is used instead."

  (->>
      {:source
       {:schema-registry source-schema-registry,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility nil
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))
  => (contains {:src-level "NONE" :dst-level "FORWARD_TRANSITIVE"} )
  )



(fact "analyse-compatibility: destination schema registry information
 are passed down to the output"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility nil
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))

  => (contains {:dst-schema-registry-client destination-schema-registry,
                :dst-subject "destination-subject-value"}))



(fact "analyse-compatibility: test is `true` if and only if both src
 and dst compatibility levels are the same"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility nil
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))

  => (contains {:test false})


  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))

  => (contains {:test true})



  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "BACKWARD",
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "BACKWARD",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))

  => (contains {:test false})


  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "BACKWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-compatibility))

  => (contains {:test true})
  )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;----==| C O M P A R E - S T R I C T - S C H E M A - V E R S I O N S |==---- ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact "analyse-strict-schema-versions: If schemas match in number
 order and content test should be true"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-strict-schema-versions))

  => (contains {:test true})

  )



(fact "analyse-strict-schema-versions: If schemas mismatch: 1 missing
 in destination (in this case it could be repaired by adding the
 missing schema)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}]}}
    (analyse-strict-schema-versions))

  => (contains {:test false :src 2 :dst 1
                :missing [:schema2] :matches? [true false]})

  )



(fact "analyse-strict-schema-versions: If schemas mismatch: schema in
 different orders (in this case it cannot be repaired)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 2, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}]}}
    (analyse-strict-schema-versions))

  => (contains {:test false :src 2 :dst 2
                :missing [] :matches? [false false]})

  )



(fact "analyse-strict-schema-versions: If schemas mismatch: earlier
 version is missing (this cannot be repaired)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 2, :schema :schema2}]}}
    (analyse-strict-schema-versions))

  => (contains {:test false :src 2 :dst 1
                :missing [:schema1] :matches? [false false]})

  )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;        ----==| C O M P A R E - S C H E M A - L E N I E N T |==----         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(fact "analyse-schema-versions-lenient: If source and destination
 match exactly, then the test should be true"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-schema-versions-lenient))

  => (contains {:test true})
  )



(fact "analyse-schema-versions-lenient: If all the source schemas are in
  the destination as well (even if destination has more schemas) AND
  IN THE SAME RELATIVE ORDER, then the test should be true"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         {:id 2, :version 3, :schema :schema2}]}}
      (analyse-schema-versions-lenient))

  => (contains {:test true})
  )



(fact "analyse-schema-versions-lenient: If all the source schemas are
  in the destination as well (even if destination has more schemas)
  BUT NOT IN THE SAME ORDER, then the test should be false (this
  cannot be repaired if compatibility level is different than NONE)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 3, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient))

  => (contains {:test false})
  )



(fact "analyse-schema-versions-lenient: If one of more schemas are
 missing from the destination then test should be false and the
 missing schema should be listed. (this can be repaired by adding the
 new schema in the destination)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 634, :version 4, :schema :schema3}
         {:id 774, :version 5, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema2}
         {:id 14,:version 3, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient))

  => (contains {:test false
                :matches? [true true false false]
                :missing '(:schema3 :schema4)})
  )



(fact "analyse-schema-versions-lenient: If not all the source schemas
  are in the destination (even if destination has more schemas) BUT
  NOT IN THE SAME ORDER, then the test should be false (this cannot be
  repaired if compatibility level is different than NONE)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 634, :version 4, :schema :schema3}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 3, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient))

  => (contains {:test false})
  )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;      ----==| C O M P A R E - S C H E M A - U N O R D E R E D |==----       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact "analyse-schema-versions-lenient-unordered: If source and
 destination match exactly, then the test should be true"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test true})
  )



(fact "analyse-schema-versions-lenient-unordered: If all the source
  schemas are in the destination as well (even if destination has more
  schemas) AND IN THE SAME RELATIVE ORDER, then the test should be
  true"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         {:id 2, :version 3, :schema :schema2}]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test true})
  )



(fact "analyse-schema-versions-lenient-unordered: If all the source
  schemas are in the destination as well (even if destination has more
  schemas) BUT NOT IN THE SAME ORDER, then the test should be
  true (this fine if the compatibility level is NONE)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 3, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test true})
  )



(fact "analyse-schema-versions-lenient-unordered: If one of more
 schemas are missing from the destination then test should be false
 and the missing schema should be listed. (this can be repaired by
 adding the new schema in the destination)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 634, :version 4, :schema :schema3}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test false
                :src 3 :dst 2
                :missing #{:schema2 :schema3}})
  )



(fact "analyse-schema-versions-lenient-unordered: If not all the
  source schemas are in the destination (even if destination has more
  schemas) BUT NOT IN THE SAME ORDER, then the test should be
  false (this can be repaired by adding the missing schema)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 634, :version 4, :schema :schema3}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 3, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test false :missing #{:schema3}})
  )



(fact "analyse-schema-versions-lenient-unordered: If not all the
  source schemas are in the destination (even if destination has more
  schemas) BUT NOT IN THE SAME ORDER, then the test should be
  false (this can be repaired by adding the missing schema)"

  (->>
      {:source
       {:schema-registry-client source-schema-registry,
        :subject "source-subject-value",
        :compatibility "NONE",
        :global-compatibility "BACKWARD",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 634, :version 4, :schema :schema3}]},

       :destination
       {:schema-registry-client destination-schema-registry,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 2, :version 3, :schema :schema2}
         {:id 1, :version 1, :schema :schema1}
         {:id 5, :version 2, :schema :schema9}
         ]}}
    (analyse-schema-versions-lenient-unordered))

  => (contains {:test false :missing #{:schema3}})
  )
