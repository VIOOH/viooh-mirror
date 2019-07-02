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

(defn register-missing-versions
  [known-schemas-subjects ^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry
   src-subject dest-subject missing-versions]
  (doseq [missing-version missing-versions]
    (let [src-metadata      (safely
                             (.getSchemaMetadata src-registry src-subject missing-version)
                             :on-error
                             :max-retry :forever
                             :retry-delay [:random-exp-backoff :base 200 :+/- 0.50 :max 30000]
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
                            :max-retry :forever
                            :retry-delay [:random-exp-backoff :base 200 :+/- 0.50 :max 30000]
                            :message (str "Unable to regsiter schema at destination registry for subject:" dest-subject
                                          " missing-schema:" src-schema))
          _                (log/debug "Register schema Request. dest-registry"
                                      "[Request subject:" dest-subject ", schema:" src-schema "]"
                                      "[Response schema-id:" dest-schema-id "]")]
      (observe-subject-version! known-schemas-subjects src-subject dest-subject
                                src-schema-id dest-schema-id missing-version))))



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
                     :max-retry :forever
                     :retry-delay [:random-exp-backoff :base 200 :+/- 0.50 :max 30000]
                     :message (str "Unable to getId from source registry for subject:"
                                   src-subject " for schema:" schema))
          _         (log/debug "Get SchemaId Request. src-registry"
                               "[Request subject:" src-subject ", schema:" schema "]"
                               "[Response schema-id:" schema-id "]")]
      (when-not (observed-schema-id? known-schemas-subjects schema-id)
        (let [src-versions (safely
                            (set (.getAllVersions src-registry src-subject))
                            :on-error
                            :max-retry :forever
                            :retry-delay [:random-exp-backoff :base 200 :+/- 0.50 :max 30000]
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
                             :retryable-error? #(not (and (= (type %) RestClientException)
                                                          (= (.getStatus %) 404)
                                                          (= (.getErrorCode %) 40401)))
                             :max-retry :forever
                             :retry-delay [:random-exp-backoff :base 200 :+/- 0.50 :max 30000]
                             :message (str "Unable to getAllVersions from destination registry"
                                           " for subject:" dest-subject))
              _              (log/debug "Get All Versions Request. dest-registry"
                                        "[Request subject:" dest-subject "]"
                                        "[Response versions:" dest-versions "]")
              missing-versions (difference src-versions dest-versions)]
          (register-missing-versions known-schemas-subjects src-registry dest-registry src-subject
                                     dest-subject missing-versions)
          (observe-schema-id! known-schemas-subjects schema-id)))))
  avro-obj)




(defn create-schema-mirror
  [^CachedSchemaRegistryClient src-registry ^CachedSchemaRegistryClient dest-registry
   src-topic dest-topic is-schema-key]
  (let [known-schemas-subjects (empty-schemas-subjects-atom src-topic dest-topic)]
    (partial mirror-schema-versions
             known-schemas-subjects src-registry dest-registry
             src-topic dest-topic is-schema-key)))
