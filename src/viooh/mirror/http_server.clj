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
(ns viooh.mirror.http-server
  (:require [org.httpkit.server :as httpkit]
            [clojure.tools.logging :as log]
            [compojure.api.sweet :refer [api GET]]
            [ring.util.response :as resp]
            [integrant.core :as ig]))



(defmethod ig/init-key ::server [_ {:keys [server]}]
  (let [handler        (api
                        (GET (:healthcheck-uri server "/healthcheck") []
                             (resp/response "Spectrum is green" )))
        stop-server-fn (httpkit/run-server handler server)]
    (log/info "http server started")
    stop-server-fn))



(defmethod ig/halt-key! ::server [_ stop-server-fn]
  (stop-server-fn)
  (log/info "http server stopped"))
