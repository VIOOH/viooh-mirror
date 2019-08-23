(ns viooh.mirror.schema-registry
  (:import
   [io.confluent.kafka.schemaregistry.client.rest.exceptions RestClientException]
   [org.apache.avro Schema]
   [io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient]))


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
   (fn
     ([url]
      (schema-registry url 256))
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



(defn subjects
  "Returns all the subjects registered int he schema registry"
  [url]
  (.. (schema-registry url)
      (getAllSubjects)))



(defn versions
  "Returns all the versions of a given subject"
  [url ^String subject]
  (return-nil-when-not-found
   (.. (schema-registry url)
       (getAllVersions subject))))



(defn schema-version
  "Given a subject and a schema it returns the version of the schema"
  [url ^String subject ^Schema schema]
  (return-nil-when-not-found
   (.. (schema-registry url)
       (getVersion subject schema))))



(defn schema-metadata
  "Returns the schema metadata for the given subject and version.
  If the version is not provided then it returns the latest version"
  ([url ^String subject]
   (return-nil-when-not-found
    (->clj
     (.. (schema-registry url)
         (getLatestSchemaMetadata subject)))))
  ([url ^String subject ^long version]
   (return-nil-when-not-found
    (->clj
     (.. (schema-registry url)
         (getSchemaMetadata subject version))))))



(defn retrieve-schema
  "Given a schema id, or a , it returns the Avro schema"
  ([url ^long id]
   (return-nil-when-not-found
    (.. (schema-registry url)
        (getById id))))
  ([url ^long id ^String subject]
   (return-nil-when-not-found
    (.. (schema-registry url)
        (getBySubjectAndId subject id)))))



(defn subject-compatibility
  "Given a schema id, or a , it returns the Avro schema"
  [url ^String subject]
  (return-nil-when-not-found
   (.. (schema-registry url)
       (getCompatibility subject))))



(defn register-schema
  "Given a subject and a schema it register the schema if new and returns the id"
  [url ^String subject ^Schema schema]
  (.. (schema-registry url)
      (register subject schema)))



(defn delete-subject
  "Given a subject it removes it if found and returns the list of deleted versions"
  [url ^String subject]
  (return-nil-when-not-found
   (.. (schema-registry url)
       (deleteSubject subject))))



(comment

  (def url "https://schema-registry.dev.develop.farm")

  (subjects url)

  (versions url "prd.datariver.prv_DigitalReservation-value")

  (schema-metadata url "prd.datariver.prv_DigitalReservation-value" 3)

  (retrieve-schema url 5)
  (retrieve-schema url 5 "prd.datariver.prv_DigitalReservation-value")

  (schema-version url "prd.datariver.prv_DigitalReservation-value" (retrieve-schema url 5))

  (subject-compatibility url "prd.datariver.prv_DigitalReservation-value" )

  (register-schema url "test" (retrieve-schema url 5))
  (delete-subject url "test")

  )
