(ns viooh.mirror.schema-registry
  (:require [clojure.string :as str]
            [safely.core :refer [safely]])
  (:import
   [io.confluent.kafka.schemaregistry.client.rest.exceptions RestClientException]
   [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]
   [org.apache.avro Schema]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;        ----==| S C H E M A   R E G I S T R Y   C L I E N T |==----         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol ToClojure
  (->clj [x] "Convert x to Clojure data types"))



(extend-protocol ToClojure

  io.confluent.kafka.schemaregistry.client.SchemaMetadata
  (->clj [^SchemaMetadata sm]
    {:id      (.getId sm)
     :version (.getVersion sm)
     :schema  (.getSchema sm)})

  nil
  (->clj [_] nil))



(def ^{:arglists '([url] [url capacity])
       :doc "It creates a CachedSchemaRegistry client and it caches the instance."
       :tag CachedSchemaRegistryClient}
  schema-registry
  (memoize
   (fn schema-registry-direct
     ([^String url]
      (schema-registry-direct url 256))
     ([^String url ^long capacity]
      (CachedSchemaRegistryClient. url capacity)))))



(defmacro return-nil-when-not-found
  {:indent/style [0]}
  [& body]
  `(try
     ~@body
     (catch RestClientException x#
       (when-not (or (= 40401 (.getErrorCode x#))
                    (= 40402 (.getErrorCode x#))
                    (= 40403 (.getErrorCode x#)))
         (throw x#)))))



(def ^:private endpoint-key
  (memoize
   (fn
     [^String url]
     (when url
       (-> url
          (str/replace-first #"^https?://" "")
          (str/replace #"[^\w\d]" "_")
          (str/replace #"_+$" "")
          (keyword))))))



(defmacro with-circuit-breaker
  "Wraps the call wit a different circuit breaker for every endpoint"
  {:indent/style [0]}
  [^String url & body]
  `(let [url# ~url]
     (safely
      ~@body
      :on-error
      :max-retries 5
      :message (str "Error while processing request for schema-registry: " url#)
      :circuit-breaker (endpoint-key url#)
      :timeout 5000)))



(defn subjects
  "Returns all the subjects registered int he schema registry"
  [^String url]
  (with-circuit-breaker url
    (.. (schema-registry url)
        (getAllSubjects))))



(defn versions
  "Returns all the versions of a given subject"
  [^String url ^String subject]
  (with-circuit-breaker url
    (return-nil-when-not-found
     (.. (schema-registry url)
         (getAllVersions subject)))))



(defn schema-version
  "Given a subject and a schema it returns the version of the schema"
  [^String url ^String subject ^Schema schema]
  (with-circuit-breaker url
    (return-nil-when-not-found
     (.. (schema-registry url)
         (getVersion subject schema)))))



(defn schema-metadata
  "Returns the schema metadata for the given subject and version.
  If the version is not provided then it returns the latest version"
  ([^String url ^String subject]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (->clj
       (.. (schema-registry url)
           (getLatestSchemaMetadata subject))))))
  ([^String url ^String subject ^long version]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (->clj
       (.. (schema-registry url)
           (getSchemaMetadata subject version)))))))



(defn retrieve-schema
  "Given a schema id, and optionally a subject, it returns the Avro schema"
  ([^String url ^long id]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (.. (schema-registry url)
          (getById id)))))
  ([^String url ^long id ^String subject]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (.. (schema-registry url)
          (getBySubjectAndId subject id))))))



(defn subject-compatibility
  "Given a subject it returns the subject compatibility level or nil if not found.
   Without a subject it returns the default compatibility level"
  ([^String url]
   (subject-compatibility url nil))
  ([^String url ^String subject]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (.. (schema-registry url)
          (getCompatibility subject))))))



(defn update-subject-compatibility
  "Given a subject it updates the subject compatibility level to the given value
   Without a subject it updates the default compatibility level"
  ([^String url ^String level]
   (update-subject-compatibility url nil level))
  ([^String url ^String subject ^String level]
   (with-circuit-breaker url
     (return-nil-when-not-found
      (.. (schema-registry url)
          (updateCompatibility subject level))))))



(defn register-schema
  "Given a subject and a schema it register the schema if new and returns the id"
  [^String url ^String subject ^Schema schema]
  (with-circuit-breaker url
    (.. (schema-registry url)
        (register subject schema))))



(defn delete-subject
  "Given a subject it removes it if found and returns the list of
  deleted versions"
  [^String url ^String subject]
  (with-circuit-breaker url
    (return-nil-when-not-found
     (.. (schema-registry url)
         (deleteSubject subject)))))



(defn delete-version
  "Given a subject and a schema version it removes it if found and
  returns the list of deleted versions"
  [^String url ^String subject ^long version]
  (with-circuit-breaker url
    (return-nil-when-not-found
     (.. (schema-registry url)
         (deleteSchemaVersion subject (str version))))))



(defn parse-schema
  "Given a Avro schema as a string returns a Avro RecordSchema object"
  [^String schema]
  (Schema/parse schema))
