(ns s3c.object
  (:refer-clojure :exclude [get])
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [saw.core :as saw]
   [saw.util :refer [error-as-value]]
   [s3c.client :as client]
   [s3c.acl :as acl])
  (:import
   (com.amazonaws.services.s3.model
    ListObjectsV2Request
    GetObjectRequest
    CopyObjectRequest
    PutObjectRequest
    DeleteObjectRequest
    ObjectMetadata
    AccessControlList
    GroupGrantee
    Permission)))

(defn delete [bucket key]
  (->> (doto (DeleteObjectRequest. bucket key)
         (.withBucketName bucket)
         (.withKey key))
       (.deleteObject (client/lookup))))

(defn- as-keys [res]
  {:token (.getNextContinuationToken res)
   :keys  (map #(.getKey %) (.getObjectSummaries res))})

(defn- list-keys*
  ([bucket prefix]
   (->> (doto (ListObjectsV2Request.)
         (.withBucketName bucket)
         (.withPrefix prefix))
       (.listObjectsV2 (client/lookup))
       (as-keys)))
  ([bucket prefix token]
   (->> (doto (ListObjectsV2Request.)
         (.withBucketName bucket)
         (.withPrefix prefix)
         (.withContinuationToken token))
       (.listObjectsV2 (client/lookup))
       (as-keys))))

(defn list-keys [bucket prefix]
  (loop [{:keys [token keys]} (list-keys* bucket prefix)
         acc  []]
    (if-not token
      (flatten (conj acc keys))
      (recur (list-keys* bucket prefix token)
             (conj acc keys)))))

(defn copy [bucket src-key dst-key]
  (->> (doto (CopyObjectRequest. bucket
                                 src-key
                                 bucket
                                 dst-key))
       (.copyObject (client/lookup))))

(defn s3-url [bucket key]
  (.getResourceUrl (client/lookup) bucket key))

(defn make-metadata [{:keys [content-type content-length]}]
  (doto (ObjectMetadata.)
    (.setContentType content-type)))

(defn put [bucket key input-stream metadata]
  (->> (make-metadata metadata)
       (PutObjectRequest. bucket key input-stream)
       (.putObject (client/lookup))))

(defn get [bucket key]
  (->> (GetObjectRequest. bucket key)
       (.getObject (client/lookup))
       (.getObjectContent)))

(defn delete [bucket key]
  (.delete-object (client/lookup) bucket key))

(defn bucket-exists? [bucket]
  (.doesBucketExist (client/lookup) bucket))

(defn create-bucket [bucket]
  (.createBucket (client/lookup) bucket))

(defn list-buckets []
  (.listBuckets (client/lookup)))
