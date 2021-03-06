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
(ns viooh.mirror.schema-mirror-repair-test
  (:require [viooh.mirror.schema-mirror :refer :all]
            [midje.sweet :refer :all]
            [viooh.mirror.schema-registry :as sr]))



(def source-schema-registry-client ::fake-src-client)
(def destination-schema-registry-client ::fake-dst-client)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;          ----==| R E P A I R   C O M P A T I B I L I T Y |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact "repair->analyse-compatibility: if src and dst compatibility are the
  same no repair action is required."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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



(fact "repair->analyse-compatibility: if src and dst compatibility differs
  then the appropriate repair action should be created"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
    :schema-registry-client destination-schema-registry-client,
    :subject "destination-subject-value",
    :level "NONE"}]

  )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;          ----==| R E P A I R   S C H E M A   S T R I C T |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(fact "repair->analyse-strict-schema-versions: if src and dst subjects
  have exactly the same schemas then no repair action is required "


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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



(fact "repair->analyse-strict-schema-versions: if src and dst subjects
  schemas which are not present in the destination then a register new
  schema action should be issued for every new schema"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-strict-schema-versions)
    (repair-actions))


  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-strict-schema-versions: if the src has some new
 schemas which are not present in the destination then a register new
 schema action should be issued for every new schema (empty
 destination case)"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions []}}
    (analyse-strict-schema-versions)
    (repair-actions))


  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema1}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema2}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-strict-schema-versions: if the existing schemas
 in the destination subject don't match exactly the schemas in the
 source then it cannot be repaired and an exception should be raised"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
        :dst-schema-registry-client destination-schema-registry-client,
        :dst-subject "destination-subject-value",
        :test false,
        :src 4,
        :dst 2,
        :matches? [true false false false]}}]

  )



(fact "repair->analyse-strict-schema-versions: if the existing schemas
  in the destination subject don't match exactly the schemas in the
  source then it cannot be repaired and an exception should be
  raised (different order case)"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
        :dst-schema-registry-client destination-schema-registry-client,
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


(fact "repair->analyse-schema-versions-lenient-unordered: if src and
  dst subjects have exactly the same schemas then no repair action is
  required "


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-schema-versions-lenient-unordered)
    (repair-actions))

  => nil)



(fact "repair->analyse-schema-versions-lenient-unordered: if the
  source has additional schemas which are not in the destination, then
  a register action for each new schema must be issued."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-schema-versions-lenient-unordered)
    (repair-actions))

  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}])



(fact "repair->analyse-schema-versions-lenient-unordered: if the
  source has additional schemas which are not in the destination, then
  a register action for each new schema must be issued (even if the
  common schema in the destination are in a different order)"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema2}
         {:id 2, :version 2, :schema :schema1}]}}
    (analyse-schema-versions-lenient-unordered)
    (repair-actions))

  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-schema-versions-lenient-unordered: if the
  source has additional schemas which are not in the destination, then
  a register action for each new schema must be issued (even if in the
  destination subject there are schemas which are not present in the
  source)."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;         ----==| R E P A I R   S C H E M A   L E N I E N T |==----          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(fact "repair->analyse-schema-versions-lenient: if src and dst
 subjects have exactly the same schemas then no repair action is
 required "


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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



(fact "repair->analyse-schema-versions-lenient: if the source has
  additional schemas which are not in the destination, then a register
  action for each new schema must be issued."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        [{:id 1, :version 1, :schema :schema1}
         {:id 2, :version 2, :schema :schema2}]}}
    (analyse-schema-versions-lenient)
    (repair-actions))

  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-schema-versions-lenient: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued. This should work even if
 the destination has no schemas at all"


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
        :subject "destination-subject-value",
        :compatibility "NONE"
        :global-compatibility "FORWARD_TRANSITIVE",
        :versions
        []}}
    (analyse-schema-versions-lenient)
    (repair-actions))

  => [{:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema1}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema2}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-schema-versions-lenient: if the destination
  subject has a common subset of schemas with the source but in a
  different relative ordering, then it should fail as it could be a
  breach of the compatibility."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility "FORWARD_TRANSITIVE",
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
        :dst-schema-registry-client destination-schema-registry-client,
        :dst-subject "destination-subject-value",
        :test false,
        :src 4,
        :dst 2,
        :matches? [false false false false]}}]

  )



(fact "repair->analyse-schema-versions-lenient: if the source has
 additional schemas which are not in the destination, then a register
 action for each new schema must be issued (even if in the destination
 subject there are schemas which are not present in the source) as
 long as the relative order of the common schemas is maintained."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema3}
      {:action :register-schema,
       :schema-registry-client destination-schema-registry-client,
       :subject "destination-subject-value",
       :schema :schema4}]

  )



(fact "repair->analyse-schema-versions-lenient: if all the source
  schema are present in the destination as well, even though the
  destination has additional schemas, and the relative ordering is
  respected, then no repair should be required."


  (->>
      {:source
       {:schema-registry-client source-schema-registry-client,
        :subject "source-subject-value",
        :compatibility nil,
        :global-compatibility "NONE",
        :versions
        [{:id 181, :version 1, :schema :schema1}
         {:id 482, :version 2, :schema :schema2}
         {:id 652, :version 3, :schema :schema3}
         {:id 680, :version 4, :schema :schema4}]},

       :destination
       {:schema-registry-client destination-schema-registry-client,
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



(fact " test mirror schema version repair for subject topic name
 strategy.  this test validate that the mirror will mirror the missing
 schema in the source registry to the target registry in the source
 ORDER to avoid having incompatible ascending schemas versions"

  (against-background
      (sr/schema-registry "src-reg" anything) => ::src-reg
      (sr/schema-registry "dst-reg" anything) => ::dst-reg
      (sr/subject-compatibility ::src-reg "aaa-value") => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::src-reg) => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::dst-reg) => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::dst-reg "bbb-value") => "FORWARD_TRANSITIVE"
      (sr/versions ::dst-reg "bbb-value") => [1 2]
      (sr/versions ::src-reg "aaa-value") => [1 2 3 4]
      (sr/schema-metadata ::dst-reg "bbb-value" 1)
      => {:version 1 :id 1 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }]}"}
      (sr/schema-metadata ::dst-reg "bbb-value" 2)
      => {:version 2 :id 2 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg "aaa-value" 1)
      => {:version 1 :id 1 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }]}"}
      (sr/schema-metadata ::src-reg "aaa-value" 2)
      => {:version 2 :id 2 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg "aaa-value" 3)
      => {:version 3 :id 3 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg "aaa-value" 4)
      => {:version 4 :id 4 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"z\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      )
  (->
      {:name "my-mirror-schema-cfg",
       :mirror-mode :strict
       :value-subject-name-strategy "io.confluent.kafka.serializers.subject.TopicNameStrategy",
       :source
       {:topic {:topic-name "aaa"},
        :schema-registry-url "src-reg"},

       :destination
       {:topic {:topic-name "bbb"},
        :schema-registry-url "dst-reg"},
       :serdes [:string :avro]}
    (compare-subjects "aaa.bbb")
    analyse-strict-schema-versions)
  => {:dst 2
      :dst-schema-registry-client ::dst-reg
      :dst-subject "bbb-value"
      :matches? [true true false false]
      :missing [(sr/parse-schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}]}")
                (sr/parse-schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"z\", \"type\":[\"long\", \"null\"], \"default\": 0}]}")]

      :src 4
      :test false
      :type :analyse-strict-schema-versions})



(fact "test mirror schema version repair for subject record name
   strategy.  this test validate that the mirror will mirror the
   missing schema in the source registry to the target registry in the
   source ORDER to avoid having incompatible ascending schemas
   versions"

  (against-background
      (sr/schema-registry "src-reg" anything) => ::src-reg
      (sr/schema-registry "dst-reg" anything) => ::dst-reg
      (sr/subject-compatibility ::src-reg "aaa.bbb") => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::src-reg) => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::dst-reg) => "FORWARD_TRANSITIVE"
      (sr/subject-compatibility ::dst-reg "aaa.bbb") => "FORWARD_TRANSITIVE"
      (sr/versions ::dst-reg "aaa.bbb") => [1 2]
      (sr/versions ::src-reg "aaa.bbb") => [1 2 3 4]
      (sr/schema-metadata ::dst-reg "aaa.bbb" 1)
      => {:version 1 :id 1 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }]}"}
      (sr/schema-metadata ::dst-reg "aaa.bbb" 2)
      => {:version 2 :id 2 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg  "aaa.bbb" 1)
      => {:version 1 :id 1 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }]}"}
      (sr/schema-metadata ::src-reg "aaa.bbb" 2)
      => {:version 2 :id 2 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg "aaa.bbb" 3)
      => {:version 3 :id 3 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      (sr/schema-metadata ::src-reg "aaa.bbb" 4)
      => {:version 4 :id 4 :schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"z\", \"type\":[\"long\", \"null\"], \"default\": 0}]}"}
      )
  (->
      {:name "my-mirror-schema-cfg",
       :mirror-mode :strict
       :value-subject-name-strategy "io.confluent.kafka.serializers.subject.RecordNameStrategy",
       :source
       {:topic {:topic-name "eee"},
        :schema-registry-url "src-reg"},

       :destination
       {:topic {:topic-name "eee"},
        :schema-registry-url "dst-reg"},
       :serdes [:string :avro]}
    (compare-subjects "aaa.bbb")
    analyse-strict-schema-versions)
  => {:dst 2
      :dst-schema-registry-client ::dst-reg
      :dst-subject "aaa.bbb"
      :matches? [true true false false]
      :missing [(sr/parse-schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}]}")
                (sr/parse-schema "{\"type\" : \"record\", \"namespace\" : \"aaa\", \"name\" : \"bbb\", \"fields\" : [{ \"name\" : \"Name\" , \"type\" : \"string\" }, {\"name\":\"x\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"y\", \"type\":[\"long\", \"null\"], \"default\": 0}, {\"name\":\"z\", \"type\":[\"long\", \"null\"], \"default\": 0}]}")]

      :src 4
      :test false
      :type :analyse-strict-schema-versions})
