(ns viooh.mirror.schema-mirror
  (:require [clojure.set :refer [difference]]
            [clojure.tools.logging :as log]
            [viooh.mirror.schema-registry :as sr])
  (:import [io.confluent.kafka.serializers AvroSchemaUtils]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                      ----==| S T R A T E G Y |==----                       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;
;; This namespace contains the functions to analyse and mirror
;; schemas.  While mirroring topics there a number of important things
;; to keep in mind in regards to schemas:
;;
;;  - Order in which schemas are registered matters (unless
;;    comatibility is NONE).
;;
;;  - Messages can arrive with any of the registered schemas for a
;;    given subject
;;
;;  - Messages can arrive with schemas versions which are no-longer
;;    in the subject (deleted version, but records still present).
;;
;;  - Schemas in their object form are comparable objects (just values).
;;
;;  - Destination subjects might have more schemas than the source;
;;    for example local testing or when multiple source topics
;;    are sent to the same destination topic.
;;
;; The strategy used here is to compare the source and destination
;; subjects generating a comparative structure which then is used for
;; analysing which `repair-actions` needs to be taken in order to
;; converge to a mirrored state.
;;
;; The function `compare-subjects` will take a source registry and
;; subject and a destination registry and subject and generate collect
;; the information that could influence the mirroring, such as the
;; list of schema versions and the compatibility on each side.
;;
;; The output of `compare-subjecs` will be fed into a number of
;; `analyse-*` functions which will compare the two subjects
;; based on several criteria.
;; The output of each analysis will be then processed to produce
;; repair actions to make the destination converge to an acceptable
;; state in comparison to the source.
;;
;; The processing can be summarised as follow:
;;
;; ```
;;        ;; logical flow
;;        (->> (compare-subjects src dest)  ;
;;             (analyse-subjects)           ;
;;             (map repair-actions)         ;
;;             (run! perform-repairs))      ;
;; ```
;;
;; This is only an approximation as the repair of the compatibility
;; level must be done first as it influences the logic of how schemas
;; are compared.
;;



(defn pad
  "Given a size and a collection it returns a sequence with coll
   and `nil` for any item up to `s`. If coll contains more elements
  than `s` then `coll` is returned with all the elements

       (pad 5 [1 2 3]) => (1 2 3 nil nil)
       (pad 3 [1 2 3 4 5]) => (1 2 3 4 5)

  "
  [s coll]
  (let [s (max s (count coll))]
    (-> (partition s s (repeat nil) coll)
       first
       (or (repeat s nil)))))



(defn subject-name
  [strategy topic key-or-val]
  {:pre [(#{:topic-name} strategy) (#{:key :value} key-or-val)]}
  (case strategy
    :topic-name
    (str topic "-" (name key-or-val))))



(defn avro-schema
  "Returns the avro schema of the record passed."
  [record]
  (AvroSchemaUtils/getSchema record))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ----==| C O M P A R E - S U B J E C T S |==----               ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(defn compare-subjects
  "It compares the two subjects and returns information about the compatibility,
  the versions and the schemas"
  [{{src-registry :schema-registry-url {src-topic :topic-name} :topic} :source
    {dst-registry :schema-registry-url {dst-topic :topic-name} :topic} :destination
    :keys [mirror-mode subject-naming-strategy]}]
  (let [src-subject (subject-name subject-naming-strategy src-topic :value)
        dst-subject (subject-name subject-naming-strategy dst-topic :value)]
    {:source
     {:schema-registry src-registry
      :subject src-subject
      :compatibility        (sr/subject-compatibility src-registry src-subject)
      :global-compatibility (sr/subject-compatibility src-registry)
      :versions (->> (sr/versions src-registry src-subject)
                   (map (partial sr/schema-metadata src-registry src-subject))
                   (map #(update % :schema sr/parse-schema)))}
     :destination
     {:schema-registry dst-registry
      :subject dst-subject
      :compatibility        (sr/subject-compatibility dst-registry dst-subject)
      :global-compatibility (sr/subject-compatibility dst-registry)
      :versions (->> (sr/versions dst-registry dst-subject)
                   (map (partial sr/schema-metadata dst-registry dst-subject))
                   (map #(update % :schema sr/parse-schema)))}}))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ----==| A N A L Y S E - S U B J E C T S |==----               ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn analyse-compatibility
  "It compares the effective subject compatibility level"
  [{{src-subject :subject src-compt :compatibility
     src-sr-compt :global-compatibility} :source
    {dst-subject :subject dst-compt :compatibility
     dst-sr-compt :global-compatibility
     dst-schema-registry :schema-registry} :destination}]
  (let [src (or src-compt src-sr-compt)
        dst (or dst-compt dst-sr-compt)]
    {:type :analyse-compatibility :test (= src dst)
     :src-level src
     :dst-schema-registry dst-schema-registry
     :dst-subject dst-subject
     :dst-level dst}))



(defn analyse-strict-schema-versions
  "It compares the schemas from both and expects to be exactly the same
  and in the same order."
  [{{src-subject :subject src-versions :versions} :source
    {dst-subject :subject dst-versions :versions
     dst-schema-registry :schema-registry} :destination}]
  (let [src (mapv :schema src-versions)
        dst (mapv :schema dst-versions)
        s   (max (count src-versions) (count src-versions))]
    {:type  :analyse-strict-schema-versions
     :dst-schema-registry dst-schema-registry
     :dst-subject dst-subject
     :test (= src dst) :src (count src-versions) :dst (count dst-versions)
     :matches? (mapv #(= %1 %2) (pad s src) (pad s dst))
     :missing (remove (into #{} dst) src)}))



(defn analyse-schema-versions-lenient
  "It compares that all the schemas from the source are in the
  destination as well and in the same order but it doesn't care if
  there are other schemas"
  [{{src-subject :subject src-versions :versions} :source
    {dst-subject :subject dst-versions :versions
     dst-schema-registry :schema-registry} :destination}]
  (let [src (map :schema src-versions)
        dst (map :schema dst-versions)
        dst-match-src (filter (into #{} src) dst)
        missing (remove (into #{} dst) src)
        s   (max (count src) (count dst-match-src))]
    {:type :analyse-schema-versions-lenient
     :dst-schema-registry dst-schema-registry
     :dst-subject dst-subject
     :test (= src dst-match-src) :src (count src-versions)
     :dst (count dst-versions) :matches? (mapv #(= %1 %2) (pad s src) (pad s dst-match-src))
     :missing missing}))



(defn analyse-schema-versions-lenient-unordered
  "It compares that all the schemas from the source are in the
  destination as well but it doesn't care if there are other schemas
  or whether are in a different order"
  [{{src-subject :subject src-versions :versions} :source
    {dst-subject :subject dst-versions :versions
     dst-schema-registry :schema-registry} :destination}]
  (let [src (into #{} (map :schema src-versions))
        dst (into #{} (map :schema dst-versions))
        missing (difference src dst)]
    {:type :analyse-schema-versions-lenient-unordered
     :dst-schema-registry dst-schema-registry
     :dst-subject dst-subject
     :test (empty? missing) :src (count src-versions)
     :dst (count dst-versions) :missing missing}))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                ----==| R E P A I R - A C T I O N S |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmulti repair-actions :type)



;; -------------------------------------------------------------------
;; It attempts to repair the consistency level of the destination
;; subject such that it will match the effective source compatibility
;; level.
;; -------------------------------------------------------------------
(defmethod repair-actions :analyse-compatibility
  [{:keys [test dst-schema-registry dst-subject src-level]}]
  (when-not test
    [{:action :change-subject-compatibility
      :schema-registry dst-schema-registry
      :subject dst-subject
      :level src-level}]))



(defn- is-prefix-matching?
  "Returns true if the matches has at least `n` starting `true` values."
  [n matches]
  (= (take n matches)
     (repeat n true)))



;; -------------------------------------------------------------------
;; In this case it is required that source and destinations are a
;; exact mirror one of each other. Therefore it is required that the
;; schemas versions are the same and in the same order.  The only
;; repair possible is that one or more schemas are missing in the
;; destination given that they have a common prefix (which it could be
;; empty, in case destination is new).
;;
;; Here we check whether the source contains more schemas than the
;; destination and if the existing schemas in the destination match
;; exactly the source. In any other case it is not possible to repair
;; the destination, therefore we raise an error
;; -------------------------------------------------------------------
(defmethod repair-actions :analyse-strict-schema-versions
  [{:keys [test dst-schema-registry dst-subject src dst matches? missing]
    :as data}]
  (when-not test
    ;; It can only be repaired if the source has some newer (missing)
    ;; schema which are not present in the tail of the destination
    ;; subject.
    (if (and (> src dst) (is-prefix-matching? dst matches?))
      ;; generate an action for every schema to add
      (map (fn [s]
             {:action :register-schema
              :schema-registry dst-schema-registry
              :subject dst-subject
              :schema s}) missing)
      ;;
      ;; If the one missing is in the middle or the beginning, of if the
      ;; destination has more schemas than the source this cannot be
      ;; repaired.
      [{:action :raise-error
        :message "Strict mirror not possible as source and destination subjects have different schemas"
        :data (dissoc data :missing)}])))



;; -------------------------------------------------------------------
;; This is probably the easiest case, we don't case about schema
;; ordering we want only to make sure that all the schemas which are
;; present in the source are registered in the destination subject as
;; well and we don't even care if the destination has additional
;; subjects. For any missing schema we issue a schema registration
;; request.
;; -------------------------------------------------------------------
(defmethod repair-actions :analyse-schema-versions-lenient-unordered
  [{:keys [test dst-schema-registry dst-subject src dst missing]
    :as data}]
  (when (and (not test) (seq missing))
    ;; generate an action for every schema to add
    (map (fn [s]
           {:action :register-schema
            :schema-registry dst-schema-registry
            :subject dst-subject
            :schema s}) missing)))



;; -------------------------------------------------------------------
;; In this case we allow the target to have additional schemas in the
;; subject but we want to ensure that the relative order of the
;; schemas is the same as the source subject. The use case for this
;; one could be for example to mirror a production stream into a test
;; environment and allow the test environment to register a newer
;; version of the schema with additional changes. At this point the
;; source will have a set of common schemas, plus one or more schemas
;; which are present only in the target side, and when one of the
;; schema version becomes official and promoted to production we want
;; to make sure that the relative order (as well as the compatibility
;; rules) are respected for the given subject.
;; -------------------------------------------------------------------
(defmethod repair-actions :analyse-schema-versions-lenient
  [{:keys [test dst-schema-registry dst-subject dst missing matches?]
    :as data}]
  (when-not test
    ;; It can only be repaired if the source has some newer (missing)
    ;; schema which are not present in the destination subject.
    ;; Even if the destination subject has schemas which are not
    ;; present in the source.
    (if (and ;; check if there are missing schemas to be added
         (seq missing)
         ;; check that both have a common set of schemas
         (or (= (dedupe matches?) [true false])
            ;; OR the destination has no schema registered
            (= dst 0)))
      ;; generate an action for every schema to add
      (map (fn [s]
             {:action :register-schema
              :schema-registry dst-schema-registry
              :subject dst-subject
              :schema s}) missing)
      ;;
      ;; If the one missing is in the middle or the beginning, of if the
      ;; destination has more schemas than the source this cannot be
      ;; repaired.
      [{:action :raise-error
        :message "Lenient mirror not possible as source and destination subjects don't share a common root set of schemas."
        :data (dissoc data :missing)}])))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;               ----==| P E R F O R M - R E P A I R S |==----                ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmulti perform-repair :action)



(defmethod perform-repair :change-subject-compatibility
  [{:keys [schema-registry subject level action] :as a}]
  (log/info "Performing repair action: " action
            "on registry:" schema-registry
            "and subject:" subject
            "setting to level:" level)
  (sr/update-subject-compatibility schema-registry subject level))



(defmethod perform-repair :register-schema
  [{:keys [schema-registry subject schema action] :as a}]
  (log/info "Performing repair action: " action
            "on registry:" schema-registry
            "and subject:" subject
            "schema:" schema)
  (sr/register-schema schema-registry subject schema))



(defmethod perform-repair :raise-error
  [{:keys [message] :as a
    {:keys [dst-schema-registry dst-subject] :as data} :data}]
  (log/warn "Repair not possible"
            "on registry:" dst-schema-registry
            "and subject:" dst-subject
            "reason:" message)
  (throw (ex-info message data)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                ----==| M I R R O R - S C H E M A S |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn analyse-subjetcs
  "Returns a list of repair actions which are required. The analysis and
   repair must be repeated until the analysis comes back clear (no
   repair actions required). This is due to the fact that some analysis
   are only executed if the previous problem has been fixed.
  "
  [{:keys [mirror-mode] :as mirror-cfg}]

  ;; TODO: should we handle avro schemas for keys as well?
  (let [diff          (compare-subjects mirror-cfg)
        compatibility (analyse-compatibility diff)]

    (or
     ;;
     ;; If the compatibility need repaired, then run this one first
     ;;
     (repair-actions compatibility)

     ;;
     ;; if compatibility doesn't need repairs, then analyse schemas
     ;;
     (->
      (cond
        (= mirror-mode :strict)
        (analyse-strict-schema-versions diff)

        (and (= mirror-mode :lenient) (= "NONE" (:dst-level compatibility)))
        (analyse-schema-versions-lenient-unordered diff)

        (and (= mirror-mode :lenient) (not= "NONE" (:dst-level compatibility)))
        (analyse-schema-versions-lenient diff)

        :else
        (throw (ex-info "Unhandled mirroring mode."
                        {:mirror-mode mirror-mode :dst-level compatibility})))
      repair-actions))))



;; -------------------------------------------------------------------
;;
;; `mirror-schemas` it takes a mirroring configuration and it attempts
;; to mirror the schemas. If repairs actions are required these will
;; be performed (when possible);
;;
;; The first step is to detect whether the destination subject has a
;; different compatibility level, if so, then we will attempt repair
;; it before attempting any other analysis. The reason is that the
;; compatibility level also determine which whether the analysis on
;; schema ordering is relavant or not.
;;
;; After the compatibility has been fixed, we will attempt to analyse
;; the schemas and repair them.
;; -------------------------------------------------------------------
(defn mirror-schemas
  [{{src-registry :schema-registry-url {src-topic :topic-name} :topic} :source
    {dst-registry :schema-registry-url {dst-topic :topic-name} :topic} :destination
    :keys [mirror-mode subject-naming-strategy] :as mirror}]

  (loop [current-repairs  (analyse-subjetcs mirror)
         previous-repairs #{}]

    (when (seq current-repairs)

      (let [failed-repairs (filter previous-repairs current-repairs)]
        (when (seq failed-repairs)
          (throw (ex-info "Failed to repair subject"
                          {:mirror mirror
                           :failed-repairs failed-repairs}))))

      ;; attempting repairs
      (run! perform-repair current-repairs)

      ;; next round
      (recur (analyse-subjetcs mirror) (into previous-repairs current-repairs)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ----==| R E P L   S E S S I O N |==----                   ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(comment


  ;;
  ;; single mirror configuration
  ;;
  (def mirror-cfg
    {:name "my-mirror",
     :mirror-mode :strict
     :subject-naming-strategy :topic-name,
     :source
     {:kafka
      {:bootstrap.servers "broker1:9092",
       :max.partition.fetch.bytes "1000000",
       :max.poll.records "50000",
       :auto.offset.reset "earliest"},
      :topic {:topic-name "source-topic"},
      :schema-registry-url "http://source.registry.com/"},

     :destination
     {:kafka
      {:bootstrap.servers "destbroker1:9092"},
      :topic {:topic-name "destination-topic"},
      :schema-registry-url "http://destination.registry.com/"},
     :serdes [:string :avro]})


  ;; create comparison structure
  (def diff
    (compare-subjects mirror-cfg))

  ;; compare subjects on several criteria
  (analyse-compatibility diff)
  (analyse-strict-schema-versions diff)
  (analyse-schema-versions-lenient diff)
  (analyse-schema-versions-lenient-unordered diff)




  ;; analyse differences and propose required changes to the
  ;; destination subject in order to mirror the source this only
  ;; computes the repair actions, but doesn't perform any change
  (analyse-subjetcs mirror-cfg)

  ;;
  ;; `mirror-schema` will analyse the subjects and make any necessary
  ;; change to the destination subject to match the source subject.
  ;; THIS PERFORMS CHANGES THE DESTINATION SCHEMA REGISTRY
  (mirror-schemas mirror-cfg)


  )
