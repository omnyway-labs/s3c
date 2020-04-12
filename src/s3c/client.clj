(ns s3c.client
  (:require
   [saw.core :as saw])
  (:import
   [com.amazonaws.services.s3
    AmazonS3ClientBuilder]))

(defonce client (atom nil))

(defn lookup []
  @client)

(defn make []
  (-> (AmazonS3ClientBuilder/standard)
      (.withCredentials (saw/creds))
      (.withRegion (saw/region))
      .build))

(defn init! [auth]
  (saw/login auth)
  (reset! client (make)))
