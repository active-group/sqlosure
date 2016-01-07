(ns sqlosure.time
  (:require [sqlosure.type :refer [date? timestamp?]]
            [active.clojure.condition :refer [assertion-violation]])
  (:import [java.time LocalDate LocalDateTime]))

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
  [d]
  (if (instance? java.time.LocalDate d)
    (java.sql.Date/valueOf d)
    (assertion-violation 'to-sql-date "value of invalid date type" d)))

(defn from-sql-date
  "Takes a date object of type java.sql.Date and returns a java.time.LocalDate."
  [sql-date]
  (if (instance? java.sql.Date sql-date)
    (.toLocalDate sql-date)
    (assertion-violation 'from-sql-date "value of invalid date type" sql-date)))

(defn to-sql-timestamp
  "Takes a date object produced by java.time.LocalDateTime and returns a
  java.sql.Timestamp."
  [t]
  (if (instance? java.time.LocalDateTime t)
    (java.sql.Timestamp/valueOf t)
    (assertion-violation 'to-sql-timestamp "value of invalid timestamp type" t)))

(defn from-sql-timestamp
  "Takes a date object of type java.sql.Timestamp and returns a java.time.LocalDateTime."
  [sql-timestamp]
  (if (instance? java.sql.Timestamp sql-timestamp)
    (.toLocalDateTime sql-timestamp)
    (assertion-violation 'from-sql-timestamp "value of invalid timestamp type" sql-timestamp)))

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
