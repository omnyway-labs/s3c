(ns s3c.core-test
  (:require
   [clojure.test :refer :all]
   [saw.core :as saw]
   [s3c.core :as s3]))


(def test-bucket (System/getenv "TEST_BUCKET"))

(defn setup []
  (s3/init! (saw/session)))

(deftest parse-path-test
  (is (= {:bucket "test-bucket" :key "foo.edn"}
         (s3/parse-path "s3://test-bucket/foo.edn" ))))

(deftest ^:integration put-get-test
  (setup)
  (is (true? (s3/bucket-exists? test-bucket)))

  (testing "Put and Get Strings"
    (is (= "foo"
           (do
             (s3/put-str test-bucket "test.txt" "foo")
             (s3/get-str test-bucket "test.txt")))))

  (is (= ["test.txt"]
         (s3/list-keys test-bucket "test.txt")))

  (is (= {:error-id :access-denied, :msg "Access Denied"}
         (s3/put-str "fake-bucket" "test.txt" "foo"))))

(deftest ^:integration select-test
  (setup)

  (testing "Shallow query"
    (is (= 1
           (-> (s3/select test-bucket "test/" {:name "foo.bar"})
               (count)))))

  (testing "Nested query"
    (is (= "{\"id\":4,\"name\":\"foo\",\"address\":\"Seattle\",\"car\":\"Tesla\",\"context\":{\"request-id\":\"Root-1=xyz\"}}"
           (-> (s3/select test-bucket "test/"
                         {:context.request-id "Root-1=xyz"})
                 (first))))))
