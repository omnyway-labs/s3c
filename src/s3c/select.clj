(ns s3c.select
  (:require
   [clojure.string :as str]
   [s3c.client :as client])
  (:import
   [com.amazonaws.services.s3.model
    JSONInput
    JSONOutput
    CompressionType
    ExpressionType
    InputSerialization
    OutputSerialization
    SelectObjectContentEvent
    SelectObjectContentEventVisitor
    SelectObjectContentRequest
    SelectObjectContentResult
    ListObjectsV2Request]))

(defn- make-json-input []
  (doto (JSONInput.)
    (.withType "LINES")))

(defn- make-json-output []
  (doto (JSONOutput.)
    (.withRecordDelimiter "\n")))

(defn- make-input-serialization []
  (doto (InputSerialization.)
    (.withJson (make-json-input))
    (.withCompressionType "GZIP")))

(defn- make-output-serialization []
  (doto (OutputSerialization.)
    (.withJson (make-json-output))))

(defn- make-object-content-request [bucket key expression]
  (doto (SelectObjectContentRequest.)
    (.withBucketName bucket)
    (.withKey key)
    (.withExpression expression)
    (.withExpressionType "SQL")
    (.withInputSerialization (make-input-serialization))
    (.withOutputSerialization (make-output-serialization))))

(defn- as-result [result]
  (->> (.getPayload result)
       (.getRecordsInputStream)))

(defn- as-key-path [path]
  (->> (str/split (name path) #"\.")
       (map pr-str)
       (str/join #".")))

(defn- map->where-clause [filters]
  (if (map? filters)
    (letfn [(as-form [[k v]]
              (format "s.%s = '%s'" (as-key-path k) v))]
      (->> (map as-form filters)
           (interpose " and ")
           (apply str)
           (format "%s")))
    filters))

(defn make-expression [m]
  (if (empty? m)
    "select * from S3Object s"
    (->> (map->where-clause m)
         (str "select * from S3Object s where "))))

(defn- query-key* [bucket key filters]
  (->> (make-expression filters)
       (make-object-content-request bucket key)
       (.selectObjectContent (client/lookup))
       (as-result)))

(defn query [bucket keys filters]
  (map #(query-key* bucket % filters) keys))
