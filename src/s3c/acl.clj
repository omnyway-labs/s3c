(ns s3c.acl
  (:refer-clojure :exclude [get update])
  (:require
   [clojure.string :as str]
   [s3c.client :as client])
  (:import
   [com.amazonaws.services.s3.model
    AccessControlList
    GetObjectAclRequest
    GroupGrantee
    Permission]))

(defn grant-world-readable []
  {:user :all-users :op :read})

(defn get [bucket key]
  (->> (GetObjectAclRequest. bucket key)
       (.getObjectAcl (client/lookup))))

(defn str->kw [s]
  (-> (str/replace s "_" "-")
      (str/lower-case)
      (keyword)))

(defn kw->acl-property-name [kw]
  (->> (str/split (name kw) #"-")
       (map str/capitalize)
       (str/join)))

(defn make-acl [{:keys [user op]} ^AccessControlList acl]
  (let [grant      (-> user kw->acl-property-name GroupGrantee/valueOf)
        permission (-> op kw->acl-property-name Permission/valueOf)]
    (doto acl
      (.grantPermission grant permission))))

(defn update [bucket key acl]
  (->> (get bucket key)
       (make-acl acl)
       (.setObjectAcl (client/lookup) bucket key)))
