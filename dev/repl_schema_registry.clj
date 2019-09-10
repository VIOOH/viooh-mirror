(ns repl-schema-registry
  (:use viooh.mirror.schema-registry)
  (:require [clojure.string :as str]
            [safely.core :refer [safely]]))


(comment

  (def url "https://schema-registry.dev.develop.farm")

  (subjects url)

  (->> (subjects url)
     sort)

  (versions url "prd.datariver.prv_DigitalReservation-value")

  (schema-metadata url "prd.datariver.prv_DigitalReservation-value" 1)

  (->> (schema-metadata url "prd.datariver.prv_DigitalReservation-value" 1)
     :schema
     parse-schema)

  (retrieve-schema url 5)
  (retrieve-schema url 451 "prd.datariver.prv_DigitalReservation-value")

  (schema-version url "prd.datariver.prv_DigitalReservation-value" (retrieve-schema url 5))

  (subject-compatibility url "prd.datariver.prv_DigitalReservation-value" )

  (register-schema url "test" sc1)
  (delete-subject url "test")

  (delete-version url "test" 5)

  (update-subject-compatibility url "test" "FORWARD_TRANSITIVE")
  )
