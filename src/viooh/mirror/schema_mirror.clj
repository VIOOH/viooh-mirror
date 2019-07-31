(ns viooh.mirror.schema-mirror
  (:require [safely.core :refer [safely]]
            [taoensso.timbre :as log]
            [clojure.set :refer [difference union]])
  (:import [io.confluent.kafka.schemaregistry.client
            SchemaRegistryClient
            CachedSchemaRegistryClient]
           [io.confluent.kafka.schemaregistry.client.rest.exceptions RestClientException]
           [io.confluent.kafka.serializers AvroSchemaUtils]
           [io.confluent.kafka.serializers.subject TopicNameStrategy]
           [org.apache.avro Schema]
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
