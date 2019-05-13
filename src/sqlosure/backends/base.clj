(ns sqlosure.backends.base
  "This namespace defines the default implementation for all supported SQLosure
  types. Backends will want to implement their specific types be reifying the
  type implementations provided here."
  (:require [active.clojure.lens :as lens]
            [sqlosure.time :as time]
            [sqlosure.type :as type]
            [sqlosure.type-implementation :as ti])
  (:import [java.sql PreparedStatement ResultSet]))

(defn null
  "Takes a type-implementation and returns an implementation with it's
  atomic-type set to nullable."
  [impl]
  (lens/shove impl (lens/>> ti/type-implementation-base-type-lens
                            type/atomic-type-nullable?-lens)
              true))

(def string%
  (ti/implement type/string%
                (fn [^PreparedStatement stmt ix val] (.setString stmt ix val))
                (fn [^ResultSet rs ix] (.getString rs ix))))

(def integer%
  (ti/implement type/integer%
                (fn [^PreparedStatement stmt ix val] (.setInt stmt ix val))
                (fn [^ResultSet rs ix] (.getInt rs ix))))

(def double%
  (ti/implement type/double%
                (fn [^PreparedStatement stmt ix val] (.setDouble stmt ix val))
                (fn [^ResultSet rs ix] (.getDouble rs ix)) ))

(def boolean%
  (ti/implement type/boolean%
                (fn [^PreparedStatement stmt ix val] (.setBoolean stmt ix val))
                (fn [^ResultSet rs ix] (.getBoolean rs ix))))
(def blob%
  (ti/implement type/blob%
                (fn [^PreparedStatement stmt ix val] (.setBlob stmt ix val))
                (fn [^ResultSet rs ix] (.getBlob rs ix))))

(def bytea%
  (ti/implement type/bytea%
                (fn [^PreparedStatement stmt ix val] (.setBytes stmt ix val))
                (fn [^ResultSet rs ix] (.getBytes rs ix))))

(def clob%
  (ti/implement type/clob%
                (fn [^PreparedStatement stmt ix val] (.setClob stmt ix val))
                (fn [^ResultSet rs ix] (.getClob rs ix))))

(def date%
  ;; NOTE `val` here is a `java.time.LocalDate` which has to be coerced to and
  ;;      from `java.sql.Date` first.
  (ti/implement type/date%
                (fn [^PreparedStatement stmt ix val]
                  (.setDate stmt ix (time/to-sql-date val)))
                (fn [^ResultSet rs ix]
                  (time/from-sql-date (.getDate rs ix)))))

(def timestamp%
  (ti/implement type/timestamp%
                (fn [^PreparedStatement stmt ix val]
                  (.setTimestamp stmt ix (time/to-sql-timestamp val)))
                (fn [^ResultSet rs ix]
                  (time/from-sql-timestamp (.getTimestamp rs ix)))))

(def types {type/string%             string%
            type/string%-nullable    (null string%)
            type/integer%            integer%
            type/integer%-nullable   (null integer%)
            type/double%             double%
            type/double%-nullable    (null double%)
            type/boolean%            boolean%
            type/boolean%-nullable   (null boolean%)
            type/blob%               blob%
            type/blob%-nullable      (null blob%)
            type/bytea%              bytea%
            type/bytea%-nullable     (null bytea%)
            type/clob%               clob%
            type/clob%-nullable      (null clob%)
            type/date%               date%
            type/date%-nullable      (null date%)
            type/timestamp%          timestamp%
            type/timestamp%-nullable (null timestamp%)})
