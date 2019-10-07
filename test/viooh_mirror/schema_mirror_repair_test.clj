(ns viooh-mirror.schema-mirror-repair-test
  (:require [viooh.mirror.schema-mirror :refer :all]
            [midje.sweet :refer :all]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;          ----==| R E P A I R   C O M P A T I B I L I T Y |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact
 "repair->analyse-compatibility: if src and dst compatibility are the
  same no repair action is required."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-compatibility)
  (repair-actions))

 => nil

 )



(fact
 "repair->analyse-compatibility: if src and dst compatibility differs
 then the appropriate repair action should be created"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility nil
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-compatibility)
  (repair-actions))

 =>
 [{:action :change-subject-compatibility,
   :schema-registry "destination-schema-registry",
   :subject "destination-subject-value",
   :level "NONE"}]

 )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;          ----==| R E P A I R   S C H E M A   S T R I C T |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact
 "repair->analyse-strict-schema-versions: if src and dst subjects have
 exactly the same schemas then no repair action is required "


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-strict-schema-versions)
  (repair-actions))

 => nil

 )



(fact
 "repair->analyse-strict-schema-versions: if src and dst subjects have
 exactly the same schemas then no repair action is required "


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-strict-schema-versions)
  (repair-actions))

 => nil

 )



(fact
 "repair->analyse-strict-schema-versions: if the src has some new
 schemas which are not present in the destination then a register new
 schema action should be issued for every new schema"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-strict-schema-versions)
  (repair-actions))


 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-strict-schema-versions: if the src has some new
 schemas which are not present in the destination then a register new
 schema action should be issued for every new schema (empty
 destination case)"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions []}}
  (analyse-strict-schema-versions)
  (repair-actions))


 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema1}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema2}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-strict-schema-versions: if the existing schemas in
 the destination subject don't match exactly the schemas in the source
 then it cannot be repaired and an exception should be raised"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema3}]}}
  (analyse-strict-schema-versions)
  (repair-actions))


 => [{:action :raise-error,
     :message "Strict mirror not possible as source and destination subjects have different schemas",
     :data
     {:type :analyse-strict-schema-versions,
      :dst-schema-registry "destination-schema-registry",
      :dst-subject "destination-subject-value",
      :test false,
      :src 4,
      :dst 2,
      :matches? [true false false false]}}]

 )



(fact
 "repair->analyse-strict-schema-versions: if the existing schemas in
 the destination subject don't match exactly the schemas in the source
 then it cannot be repaired and an exception should be
 raised (different order case)"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 2, :version 2, :schema :schema2}
     {:id 1, :version 1, :schema :schema1}]}}
  (analyse-strict-schema-versions)
  (repair-actions))

 => [{:action :raise-error,
     :message "Strict mirror not possible as source and destination subjects have different schemas",
     :data
     {:type :analyse-strict-schema-versions,
      :dst-schema-registry "destination-schema-registry",
      :dst-subject "destination-subject-value",
      :test false,
      :src 2,
      :dst 2,
      :matches? [false false]}}]

 )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;       ----==| R E P A I R   S C H E M A   U N O R D E R E D |==----        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact
 "repair->analyse-schema-versions-lenient-unordered: if src and dst
 subjects have exactly the same schemas then no repair action is
 required "


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-schema-versions-lenient-unordered)
  (repair-actions))

 => nil

 )



(fact
 "repair->analyse-schema-versions-lenient-unordered: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-schema-versions-lenient-unordered)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-schema-versions-lenient-unordered: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued (even if the common schema
 in the destination are in a different order)"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema2}
     {:id 2, :version 2, :schema :schema1}]}}
  (analyse-schema-versions-lenient-unordered)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-schema-versions-lenient-unordered: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued (even if in the destination
 subject there are schemas which are not present in the source)."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema2}
     {:id 2, :version 2, :schema :schema1}
     {:id 52, :version 3, :schema :schema8}
     {:id 80, :version 4, :schema :schema9}]}}
  (analyse-schema-versions-lenient-unordered)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;         ----==| R E P A I R   S C H E M A   L E N I E N T |==----          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(fact
 "repair->analyse-schema-versions-lenient: if src and dst subjects
 have exactly the same schemas then no repair action is required "


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => nil

 )



(fact
 "repair->analyse-schema-versions-lenient: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}]}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-schema-versions-lenient: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued. This should work even
 if the destination has no schemas at all"


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    []}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema1}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema2}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-schema-versions-lenient: if the destination subject
  has a common subset of schemas with the source but in a different
  relative ordering, then it should fail as it could be a breach of
  the compatibility."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility "FORWARD_TRANSITIVE",
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "FORWARD_TRANSITIVE",
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema2}
     {:id 2, :version 2, :schema :schema1}]}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => [{:action :raise-error,
     :message "Lenient mirror not possible as source and destination subjects don't share a common root set of schemas.",
     :data
     {:type :analyse-schema-versions-lenient,
      :dst-schema-registry "destination-schema-registry",
      :dst-subject "destination-subject-value",
      :test false,
      :src 4,
      :dst 2,
      :matches? [false false false false]}}]

 )



(fact
 "repair->analyse-schema-versions-lenient: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued (even if in the destination
 subject there are schemas which are not present in the source) as long
 as the relative order of the common schemas is maintained."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}
     {:id 52, :version 3, :schema :schema8}
     {:id 80, :version 4, :schema :schema9}]}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => [{:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema3}
    {:action :register-schema,
     :schema-registry "destination-schema-registry",
     :subject "destination-subject-value",
     :schema :schema4}]

 )



(fact
 "repair->analyse-schema-versions-lenient: if all the source schema
  are present in the destination as well, even though the destination
  has additional schemas, and the relative ordering is respected,
  then no repair should be required."


 (->>
  {:source
   {:schema-registry "source-schema-registry",
    :subject "source-subject-value",
    :compatibility nil,
    :global-compatibility "NONE",
    :versions
    [{:id 181, :version 1, :schema :schema1}
     {:id 482, :version 2, :schema :schema2}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]},

   :destination
   {:schema-registry "destination-schema-registry",
    :subject "destination-subject-value",
    :compatibility "NONE"
    :global-compatibility "FORWARD_TRANSITIVE",
    :versions
    [{:id 1, :version 1, :schema :schema1}
     {:id 2, :version 2, :schema :schema2}
     {:id 52, :version 3, :schema :schema8}
     {:id 80, :version 4, :schema :schema9}
     {:id 652, :version 3, :schema :schema3}
     {:id 680, :version 4, :schema :schema4}]}}
  (analyse-schema-versions-lenient)
  (repair-actions))

 => nil

 )
