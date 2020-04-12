(ns s3c.object
  (:refer-clojure :exclude [get])
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [saw.core :as saw]
   [s3c.client :as client]
   [s3c.acl :as acl])
  (:import
   (com.amazonaws HttpMethod)
   (com.amazonaws.services.s3.model
    ListObjectsV2Request
    GetObjectRequest
    CopyObjectRequest
    PutObjectRequest
    DeleteObjectRequest
    DeleteObjectsRequest
    ObjectMetadata
    AccessControlList
    GroupGrantee
    Permission
    GeneratePresignedUrlRequest)))

(defn delete [bucket key]
  (->> (doto (DeleteObjectRequest. bucket key)
         (.withBucketName bucket)
         (.withKey key))
       (.deleteObject (client/lookup))))

(defn delete-keys [bucket keys]
  (->> (doto (DeleteObjectsRequest. bucket)
         (.withBucketName bucket)
         (.withKeys keys))
       (.deleteObjects (client/lookup))))

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

(defn copy [src-bucket src-key dst-bucket dst-key]
  (->> (doto (CopyObjectRequest. src-bucket
                                 src-key
                                 dst-bucket
                                 dst-key))
       (.copyObject (client/lookup))))

(defn make-dst-key [src-prefix dst-prefix key]
  (if (empty? src-prefix)
    key
    (let [actual (->> (re-pattern (str src-prefix "/"))
                      (str/split key)
                      (second))]
      (if (empty? dst-prefix)
        actual
        (str dst-prefix "/" actual)))))

(defn copy-prefix [src-bucket src-prefix dst-bucket dst-prefix]
  (doseq [k (list-keys src-bucket src-prefix)]
    (let [dst-key (make-dst-key src-prefix dst-prefix k)]
      (prn k dst-key)
      (copy src-bucket k dst-bucket dst-key))))

(defn s3-url [bucket key]
  (.getResourceUrl (client/lookup) bucket key))

(defn make-metadata [{:keys [content-type content-length]}]
  (doto (ObjectMetadata.)
    (.setContentType content-type)))

(defn put [bucket key input-stream metadata]
  (->> (make-metadata metadata)
       (PutObjectRequest. bucket key input-stream)
       (.putObject (client/lookup))))

(defn put-file [bucket key file]
  (let [f (io/as-file file)]
    (when (.exists f)
      (->> (PutObjectRequest. bucket key f)
           (.putObject (client/lookup))))))

(defn get [bucket key]
  (->> (GetObjectRequest. bucket key)
       (.getObject (client/lookup))
       (.getObjectContent)))

(defn get-file [bucket key file]
  (let [req (GetObjectRequest. bucket key)
        f   (io/as-file file)]
    (->> (.getObject (client/lookup) req f)
         (.getObjectContent))))

(defn get-as-str [bucket key]
  (->> (GetObjectRequest. bucket key)
       (.getObject (client/lookup))
       (.getObjectAsString)))

(defn delete [bucket key]
  (.delete-object (client/lookup) bucket key))

(defn bucket-exists? [bucket]
  (.doesBucketExist (client/lookup) bucket))

(defn create-bucket [bucket]
  (.createBucket (client/lookup) bucket))

(defn list-buckets []
  (->> (.listBuckets (client/lookup))
       (map #(.getName %))))

(defn list-common-prefixes [bucket prefix]
  (->> (doto (ListObjectsV2Request.)
         (.withBucketName bucket)
         (.withDelimiter "/")
         (.withPrefix prefix))
       (.listObjectsV2 (client/lookup))
       (.getCommonPrefixes)))

(defn generate-presigned-url
  ([bucket key]
   (generate-presigned-url bucket key nil))
  ([bucket key expiration-date]
   (generate-presigned-url bucket key expiration-date :get))
  ([bucket key expiration-date http-method]
   (let [method (HttpMethod/valueOf (-> http-method
                                        name
                                        str/upper-case))
         request (cond-> (GeneratePresignedUrlRequest. bucket key)
                   expiration-date (.withExpiration expiration-date)
                   method          (.withMethod method))]
     (.generatePresignedUrl (client/lookup) request))))
