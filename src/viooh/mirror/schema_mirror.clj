(ns viooh.mirror.schema-mirror
  (:require [safely.core :refer [safely]]
            [clojure.tools.logging :as log]
            [clojure.set :refer [difference union]]
            [clojure.string :as str]
            [viooh.mirror.schema-registry :as sr])
  (:import [io.confluent.kafka.schemaregistry.client
            SchemaRegistryClient
            CachedSchemaRegistryClient
            SchemaMetadata]
           [io.confluent.kafka.schemaregistry.client.rest.exceptions RestClientException]
           [io.confluent.kafka.serializers AvroSchemaUtils]
           [io.confluent.kafka.serializers.subject TopicNameStrategy]
           [org.apache.avro Schema]
           ))



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
;;    comatibility is NONE)
;;
;;  - Messages can arrive with any of the registered schemas for a
;;    given subject
;;
;;  - Messages can arrive with schemas versions which are no-longer
;;    in the subject (deleted version, but records still present)
;;
;;  - Schemas in their object form are comparable objects (just values)
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
;;        (->> (compare-subjects src dest)
;;             (analyse-subjects)
;;             (map repair-actions)
;;             (run! perform-repairs))
;; ```
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ----==| C O M P A R E - S U B J E C T S |==----               ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(defn compare-subjects
  "It compares the two subjects and returns information about the compatibility,
  the versions and the schemas"
  [src-registry src-subject dst-registry dst-subject]
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
                 (map #(update % :schema sr/parse-schema)))}})







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
    {:type :analyse-schema-versions
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
    {:type :analyse-schema-versions
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



(defmethod repair-actions :analyse-compatibility
  [{:keys [test dst-schema-registry dst-subject src-level]}]
  (when-not test
    [{:action :change-subject-compatibility
      :schema-registry dst-schema-registry
      :subject dst-subject
      :level src-level}]))



(defmethod repair-actions :analyse-compatibility
  [{:keys [test dst-schema-registry dst-subject src-level]}]
  (when-not test
    [{:action :change-subject-compatibility
      :schema-registry dst-schema-registry
      :subject dst-subject
      :level src-level}]))



(defn mirror-schemas
  [{{src-registry :schema-registry-url {src-topic :topic-name} :topic} :source
    {dst-registry :schema-registry-url {dst-topic :topic-name} :topic} :destination
    :keys [mirror-mode subject-naming-strategy]}]

  ;; TODO: should we handle avro schemas for keys as well?
  (let [src-subject   (subject-name subject-naming-strategy src-topic :value)
        dst-subject   (subject-name subject-naming-strategy dst-topic :value)
        diff          (compare-subjects src-registry src-subject dst-registry dst-subject)
        compatibility (analyse-compatibility diff)]


    (cond
      (= mirror-mode :strict)
      (->> [compatibility (analyse-strict-schema-versions diff)])

      (and (= mirror-mode :lenient) (= "NONE" (:dst compatibility)))
      (->> [compatibility (analyse-schema-versions-lenient-unordered diff)])

      (and (= mirror-mode :lenient) (= "NONE" (:dst compatibility)))
      (->> [compatibility (analyse-schema-versions-lenient diff)]))
    ))




(defonce DEFAULT-NAME-STRATEGY (TopicNameStrategy. )) ;;Look to make this configurable per serde

(defn empty-schemas-subjects-atom
  [src-topic dest-topic]
  (atom {:observed-schema-ids          #{}
         :source-topic                 src-topic
         :destination-topic            dest-topic
         :mirrored-subjects-versions   {}}))



(defn observe-schema-id!
  [known-schemas-subjects schema-id]
  (swap! known-schemas-subjects update :observed-schema-ids #(conj (or %1 #{}) %2) schema-id))



(defn observed-schema-id?
  [known-schemas-subjects schema-id]
  (-> @known-schemas-subjects
     :observed-schema-ids
     (contains? schema-id)))



(defn observe-subject-version!
  [known-schemas-subjects src-subject dest-subject src-schema-id dest-schema-id version]
  (swap! known-schemas-subjects update-in
         [:mirrored-subjects-versions {:source-subject src-subject :destination-subject dest-subject}]
         (fn [mirrored-subject-versions]
           (-> (or mirrored-subject-versions {})
              (assoc version {:source-schema-id src-schema-id :destination-schema-id dest-schema-id})))))



(defn ensure-subject-compatibility
  [^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry src-subject dest-subject]
  (let [src-compatibility    (safely
                              (try
                                (.getCompatibility src-registry src-subject)
                                (catch RestClientException rce
                                  (if (and (= (.getStatus rce) 404)
                                         (= (.getErrorCode rce) 40401))
                                    (do
                                      (log/info "Source registry does not have any compatiblity level for subject:"
                                                src-subject ", falling back to source registry's global"
                                                "compatibility.")
                                      (.getCompatibility src-registry nil))
                                    (throw rce))))
                              :on-error
                              :circuit-breaker :schema-registry-src
                              :track-as "vioohmirror.schemas.src.ensure_compat.read"
                              :max-retries :forevever
                              :message (str "Unable to get compatibility level from source registry"
                                            "for subject:" src-subject))
        dest-compatibility   (safely
                              (try
                                (.getCompatibility dest-registry dest-subject)
                                (catch RestClientException rce
                                  (if (and (= (.getStatus rce) 404)
                                         (= (.getErrorCode rce) 40401))
                                    (do
                                      (log/info "Destination registry does not have any compatiblity level for subject:"
                                                dest-subject)
                                      nil)
                                    (throw rce))))
                              :on-error
                              :circuit-breaker :schema-registry-dst
                              :track-as "vioohmirror.schemas.dst.ensure_compat.read"
                              :max-retries :forevever
                              :message (str "Unable to get compatibility level from destination registry"
                                            "for subject:" dest-subject))]
    (if (= src-compatibility dest-compatibility)
      (log/info "Source subject:" src-subject " has the same compatibility:" src-compatibility
                "as destination subject:" dest-subject)
      (do
        (log/info "Source subject:" src-subject " and Destination subject:" dest-subject
                  "have DIFFERENT compatibilites source compatibility:" src-compatibility
                  "destination compatibility:" dest-compatibility)
        (safely
         (.updateCompatibility dest-registry dest-subject src-compatibility)
         :on-error
         :circuit-breaker :schema-registry-dst
         :track-as "vioohmirror.schemas.dst.ensure_compat.upd"
         :max-retries :forever
         :message (str "Unable to update the compatibility level:" src-compatibility
                       " at destination registry for subject:" dest-subject))
        (log/info "Updated destination compatiblity to :" src-compatibility " at destination registry for subject:" dest-subject)))))



(defn register-missing-versions
  [known-schemas-subjects ^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry
   src-subject dest-subject missing-versions]
  (doseq [missing-version missing-versions]
    (let [src-metadata      (safely
                             (.getSchemaMetadata src-registry src-subject missing-version)
                             :on-error
                             :circuit-breaker :schema-registry-src
                             :track-as "vioohmirror.schemas.src.register_missing.read"
                             :max-retries :forever
                             :message (str "Unable to getSchemaMetadata from source registry for subject:" src-subject
                                           " missing-version:" missing-version " for source registry"))
          _                (log/debug "SchemaMetadata Request. src-registry"
                                      "[Request subject:" src-subject ", version:" missing-version "]"
                                      "[Response  schemametadata:" (bean src-metadata) "]")
          src-schema       (Schema/parse (.getSchema src-metadata))
          src-schema-id    (.getId src-metadata)
          dest-schema-id   (safely
                            (.register dest-registry dest-subject src-schema)
                            :on-error
                            :circuit-breaker :schema-registry-dst
                            :track-as "vioohmirror.schemas.dst.register_missing.write"
                            :max-retries :forever
                            :message (str "Unable to regsiter schema at destination registry for subject:" dest-subject
                                          " missing-schema:" src-schema))
          _                (log/debug "Register schema Request. dest-registry"
                                      "[Request subject:" dest-subject ", schema:" src-schema "]"
                                      "[Response schema-id:" dest-schema-id "]")]
      (observe-subject-version! known-schemas-subjects src-subject dest-subject
                                src-schema-id dest-schema-id missing-version)
      (log/infof "Registration of schema at dest-registry for missing-version: %s of subject %s resulted in dest schema-id: %s"
                 missing-version dest-subject dest-schema-id))))



(defn mirror-schema-versions
  [known-schemas-subjects ^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry
   src-topic dest-topic is-schema-key avro-obj]
  (when avro-obj
    (let [schema (AvroSchemaUtils/getSchema avro-obj)
          src-subject (.subjectName DEFAULT-NAME-STRATEGY src-topic is-schema-key schema)
          dest-subject (.subjectName DEFAULT-NAME-STRATEGY dest-topic is-schema-key schema)
          schema-id (safely
                     (.getId src-registry src-subject schema)
                     :on-error
                     :circuit-breaker :schema-registry-src
                     :track-as "vioohmirror.schemas.src.versions.read"
                     :max-retries :forever
                     :message (str "Unable to getId from source registry for subject:"
                                   src-subject " for schema:" schema))
          _         (log/debug "Get SchemaId Request. src-registry"
                               "[Request subject:" src-subject ", schema:" schema "]"
                               "[Response schema-id:" schema-id "]")]
      (when-not (observed-schema-id? known-schemas-subjects schema-id)
        (log/infof "[%s] first time observing schema-id: %s for source subject: %s and destination subject: %s"
                   (str src-topic "-" dest-topic) schema-id src-subject dest-subject)
        (let [src-versions (safely
                            (set (.getAllVersions src-registry src-subject))
                            :on-error
                            :circuit-breaker :schema-registry-src
                            :track-as "vioohmirror.schemas.src.versions.read_all"
                            :max-retries :forever
                            :message (str "Unable to getAllVersions from source registry"
                                          " for subject:" src-subject))
              _             (log/debug "Get All Versions Request. src-registry"
                                       "[Request subject:" src-subject "]"
                                       "[Resposne versions:" src-versions "]")
              dest-versions (safely
                             (try
                               (set (.getAllVersions dest-registry dest-subject))
                               (catch RestClientException rce
                                 (if (and (= (.getStatus rce) 404)
                                        (= (.getErrorCode rce) 40401))
                                   (do
                                     (log/info "Destination registry does not have a registered subject:"
                                               dest-subject " and thus no versions. Using a #{} for versions")
                                     #{})
                                   (throw rce))))
                             :on-error
                             :circuit-breaker :schema-registry-dst
                             :track-as "vioohmirror.schemas.dst.versions.read_all"
                             :retryable-error? #(not (and (= (type %) RestClientException)
                                                        (= (.getStatus %) 404)
                                                        (= (.getErrorCode %) 40401)))
                             :max-retries :forever
                             :message (str "Unable to getAllVersions from destination registry"
                                           " for subject:" dest-subject))
              _              (log/debug "Get All Versions Request. dest-registry"
                                        "[Request subject:" dest-subject "]"
                                        "[Response versions:" dest-versions "]")
              missing-versions (difference src-versions dest-versions)]
          (log/infof (str "[%s] Comparison of subject versions for source subject: %s and destination subject: %s"
                          " resulted in src-versions: %s  dest-versions: %s dest-missing-versions: %s")
                     (str src-topic "-" dest-topic) src-subject dest-subject src-versions dest-versions
                     missing-versions)
          (when-not (empty? missing-versions)
            (ensure-subject-compatibility src-registry dest-registry src-subject dest-subject)
            (register-missing-versions known-schemas-subjects src-registry dest-registry src-subject
                                       dest-subject missing-versions))
          (observe-schema-id! known-schemas-subjects schema-id)))))
  avro-obj)



(defn create-schema-mirror
  [^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry
   src-topic dest-topic is-schema-key]
  (let [known-schemas-subjects (empty-schemas-subjects-atom src-topic dest-topic)]
    (partial mirror-schema-versions
       known-schemas-subjects src-registry dest-registry
       src-topic dest-topic is-schema-key)))
