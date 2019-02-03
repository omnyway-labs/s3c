(ns s3c.core
  (:refer-clojure :exclude [get select])
  (:require
   [saw.util :refer [error-as-value]]
   [s3c.client :as client]
   [s3c.acl :as acl]
   [s3c.object :as ob]
   [s3c.stream :as stream]
   [s3c.select :as select]))

(defn list-buckets []
  (error-as-value
   (ob/list-buckets)))

(defn bucket-exists? [bucket]
  (error-as-value
   (ob/bucket-exists? bucket)))

(defn create-bucket [bucket]
  (error-as-value
   (ob/create-bucket bucket)))

(defn list-keys [bucket prefix]
  (try
    (ob/list-keys bucket prefix)
    (catch Exception e
      nil)))

(defn copy [bucket src-key dst-bucket dst-key]
  (error-as-value
   (ob/copy bucket src-key dst-bucket dst-key)))

(defn copy-prefix [src-bucket src-prefix dst-bucket dst-prefix]
  (error-as-value
   (ob/copy-prefix src-bucket src-prefix dst-bucket dst-prefix)))

(defn delete [bucket key]
  (error-as-value
   (ob/delete bucket key)))

(defn move [bucket src-key dst-key]
  (error-as-value
   (copy bucket src-key dst-key)
   (delete bucket src-key)))

(defn url [bucket key]
  (ob/s3-url bucket key))

(defn put-str [bucket key text]
  (let [stream   (stream/str->stream text)
        metadata {:content-type "text"}]
    (error-as-value
     (ob/put bucket key stream metadata))))

(defn get-str [bucket key]
  (error-as-value
   (->> (ob/get bucket key)
        (stream/stream->str))))

(defn put
  ([bucket key filename]
   (ob/put-file bucket key filename))
  ([bucket key input-stream metadata]
   (error-as-value
    (ob/put bucket key input-stream metadata)
    (url bucket key)))
  ([bucket key input-stream metadata acl]
   (error-as-value
    (ob/put bucket key input-stream metadata)
    (if (or (= acl :public) (nil? acl))
      (->> (acl/grant-world-readable)
           (acl/update bucket key))
      (acl/update bucket key))
    (url bucket key))))

(defn list-common-prefixes [bucket]
  (ob/list-common-prefixes bucket))

(defn parse-path [path]
  (let [[_ bucket key _]  (re-matches #"^s3://([^/]+)/(.*?([^/]+)/?)$" path)]
    {:bucket bucket
     :key    key}))

(defn select
  ([bucket prefix filters]
   (when-let [keys (list-keys bucket prefix)]
     (-> (select/query bucket keys filters)
         (stream/read-seq))))
  ([bucket prefix filters out-file]
   (when-let [keys (list-keys bucket prefix)]
     (-> (select/query bucket keys filters)
         (stream/write-seq out-file)))))


(defn init! [auth]
  (client/init! auth))
