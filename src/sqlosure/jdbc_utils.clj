(ns sqlosure.jdbc-utils
  "Utilities for using clojure.java.jdbc as a backend."
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.time :as time]
            [active.clojure.condition :as c]
            [clojure.java.jdbc :as jdbc]))

(defn query-row-fn
  "Takes the types of a  relational scheme and a row returned by a query, then
  returns a vector of values converted by convert-base-value, in the
  order defined by the given scheme."
  [convert-base-value types row]
  ;; Note: not much more than 'map' currently
  (let [ ;; vals are/must be in the order of the scheme
        conv-vals (mapv convert-base-value ;; assert (base-type? tt) ?
                        types
                        row)]
    (assert (= (count types) (count row)))
    conv-vals))

(defn result-set-fn
  [as-arrays? row-fn result-set-fn scheme-cols results]
  ;; Note: cols is mangeled by jdbc (lowercase symbols, made unique with a suffix)
  (let [[cols & rows] results
        rows (if as-arrays?
               ;; the cols are not needed, as the row always complies to
               ;; the rel-scheme used (no 'select *')
               rows
               ;; Use the cols from the scheme; it cannot contain duplicates for example; but
               ;; it will be strings (other than jdbc)
               (map #(zipmap scheme-cols %) rows))]
    ((or result-set-fn doall)
     (if row-fn (map row-fn rows) rows))))

(defn- put-select [parameterization select]
  (put/sql-select->string parameterization select))

(defn query
  "Takes a jdbc db-spec a sql-select statement and a relational scheme
  and runs the query against the database, applying
  `from-db-value` to the selected values, and `to-db-value` to the parameters."
  [db-spec select scheme from-db-value to-db-value parameterization opts]
  (let [[select-query & select-types+args] (put-select parameterization select)]
    (jdbc/query db-spec
                (cons select-query (map (fn [[t v]] (to-db-value t v)) select-types+args))
                :result-set-fn #(result-set-fn (:as-arrays? opts) (:row-fn opts)
                                               (:result-set-fn opts)
                                               (rel/rel-scheme-columns scheme) %)
                :as-arrays? true
                :row-fn #(query-row-fn from-db-value (rel/rel-scheme-types scheme) %))))
