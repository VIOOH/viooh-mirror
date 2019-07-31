(ns viooh.mirror.http-server
  (:require [org.httpkit.server :as httpkit]
            [clojure.tools.logging :as log]
            [compojure.api.sweet :refer [api GET]]
            [ring.util.response :as resp]
            [integrant.core :as ig]))







(defmethod ig/init-key ::server [_ _]
  (let [handler        (api
                        (GET "/healthcheck" []
                             (resp/response "Spectrum is green" )))
        stop-server-fn (httpkit/run-server handler {:port 9090})]
    (log/info "http server started")
    stop-server-fn))

(defmethod ig/halt-key! ::server [_ stop-server-fn]
  (stop-server-fn)
  (log/info "http server stopped"))
