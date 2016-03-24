(ns sqlosure.db-connection
  (:require [active.clojure.record :refer [define-record-type]]
            [sqlosure
             [sql-put :as put]
             [time :as time]
             [type :as t]]))

(define-record-type type-converter
  ^{:doc "`type-converter` serves as a container for the conversion functions
          between Clojure and DB types."}
  (make-type-converter value->db-value db-value->value) type-converter?
  [^{:doc "A function that takes a `sqlosure.type` type and a value and returns
           a corresponding value of a type the DB understands."}
   value->db-value type-converter-value->db-value
   ^{:doc "Inverse function to `value->db-value."}
   db-value->value type-converter-db-value->value])

(def sqlite3-type-converter
  (make-type-converter
   (fn [typ value]
     (cond
       (= typ t/boolean%) (if value 1 0)
       (= typ t/date%) (time/to-sql-time-string value)
       (= typ t/timestamp%) (time/to-sql-time-string value)
       :else value))
   (fn [typ value]
     (cond
       (= typ t/boolean%) (not= value 0)
       (= typ t/date%) (time/from-sql-time-string value)
       (= typ t/timestamp%) (time/from-sql-timestamp-string value)
       :else value))))

(def postgresql-type-converter
  (make-type-converter
   (fn [typ value]
      (case typ
        t/date% (time/to-sql-date value)
        t/timestamp% (time/to-sql-timestamp value)
        value))
   (fn [typ value]
     (case typ
       t/date% (time/from-sql-date value)
       t/timestamp% (time/from-sql-timestamp value)
       value))))

(define-record-type
  ^{:doc "`db-connection` serves as a container for storing the current
          db-connection as well as backend specific conversion and printer
          functions."}
  db-connection
  (make-db-connection conn parameterization type-converter) db-connection?
  [^{:doc "The database connection map as used by jdbc."}
   conn db-connection-conn
   ^{:doc "A function to print values in a way the dbms understands."}
   parameterization db-connection-paramaterization
   ^{:doc "A `db-connection/type-converter` record for conversion between
           Clojure and db types."}
   type-converter db-connection-type-converter])

(defn- sqlite3-put-combine
  "sqlite3 specific printer for combine queries."
  [param op left right]
  (print "SELECT * FROM (")
  (put/put-sql-select param left)
  (print (case op
           :union " UNION "
           :intersection " INTERSECT "
           :difference " EXCEPT "))
  (put/put-sql-select param right)
  (print ")"))

(defn- sqlite3-put-literal
  "sqlite3 specific printer for literals."
  [type val]
  (if (or (= true val) (= false val))
    (do (if val (print 1) (print 0))
        [])
    (put/default-put-literal type val)))

(def sqlite3-sql-put-parameterization
  "Printer for sqliter3."
  (put/make-sql-put-parameterization put/default-put-alias sqlite3-put-combine sqlite3-put-literal))

(def postgresql-sql-put-parameterization
  (put/make-sql-put-parameterization put/put-dummy-alias put/default-put-combine put/default-put-literal))
