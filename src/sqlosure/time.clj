(ns sqlosure.time
  (:require [sqlosure.type :refer [date? timestamp?]]
            [active.clojure.condition :refer [assertion-violation]])
  (:import [java.time Instant LocalDate LocalDateTime ZoneId]
           [java.sql Date Timestamp]
           [java.text SimpleDateFormat]))

(defn make-date
  "Wrapper around some common 'constructor' calls of LocalDate."
  ([] (LocalDate/now))
  ([year month day] (LocalDate/of year month day)))

(defn make-timestamp
  "Wrapper around some common 'constructor' calls of LocalDateTime."
  ([] (LocalDateTime/now))
  ([year month day hour minute]
   (LocalDateTime/of year month day hour minute))
  ([year month day hour minute second]
   (LocalDateTime/of year month day hour minute second))
  ([year month day hour minute second nano-of-second]
   (LocalDateTime/of year month day hour minute second nano-of-second)))

(defn to-sql-date
  "Takes a date object produced by java.time.LocalDate and returns a
  java.sql.Date."
  [^java.time.LocalDate date]
  (java.sql.Date/valueOf date))

(defn from-sql-date
  "Takes a date object of type java.sql.Date and returns a java.time.LocalDate."
  [^java.sql.Date date]
  (.toLocalDate date))

(defn to-sql-timestamp
  "Takes a date object produced by java.time.LocalDateTime and returns a
  java.sql.Timestamp."
  [t]
  (cond
    (instance? Timestamp t) t

    (instance? LocalDateTime t)
    (Timestamp/valueOf t)

    :else
    (assertion-violation `to-sql-timestamp "value of invalid timestamp type" t)))

(defn from-sql-timestamp
  "Takes a date object of type java.sql.Timestamp and returns a java.time.LocalDateTime."
  [sql-timestamp]
  (if (instance? Timestamp sql-timestamp)
    (.toLocalDateTime sql-timestamp)
    (assertion-violation `from-sql-timestamp "value of invalid timestamp type" sql-timestamp)))

(defn coerce-time-values
  "Takes the list of arguments and returns the list with all time values coerced
  to their sql counterparts."
  [vs]
  (letfn [(coerce [v]
            (cond
              (date? v) (to-sql-date v)
              (timestamp? v) (to-sql-timestamp v)
              :else v))]
    (map coerce vs)))


;; NOTE: since sqlite3 does not support real dates, those must be represented as
;; a string. Use these functions for coercion.
(defn to-sql-time-string
  "Takes a java.time.LocalDate(Time) and returns it's string representation."
  [d]
  (.toString d))

(defn from-sql-time-string
  "Takes the string representation of a date (yyyy-MM-dd) and returns a
  `java.time.LocalDate` object."
  [ts]
  (as-> ts t
       (.parse (SimpleDateFormat. "yyyy-MM-dd") t)
       (.getTime t)
       (Instant/ofEpochMilli t)
       (LocalDateTime/ofInstant t (ZoneId/systemDefault))
       (.toLocalDate t)))

(defn from-sql-timestamp-string
  "Takes the string representation of a date (yyyy-MM-dd'T'HH:mm:ss.SSS) and
  returns a `java.time.LocalDateTime` object."
  [tss]
  (as-> tss t
    (.parse (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS") t)
    (.getTime t)
    (Instant/ofEpochMilli t)
    (LocalDateTime/ofInstant t (ZoneId/systemDefault))))
