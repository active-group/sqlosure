(ns sqlosure.jdbc-utils
  "Utilities for using clojure.java.jdbc as a backend."
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.time :as time]))

(defn query-row-fn
  "Takes a relational scheme and a row returned by a query, then returns a map
  of the key-value pairs with values converted via convert-base-value."
  [convert-base-value scheme row]
  (let [alist (rel/rel-scheme-alist scheme)]
    (into {} (map (fn [[k v] tt]
                    ;; assert (base-type? tt) ?
                    [k (convert-base-value tt v)])
                  row
                  (vals alist)))))

(defn- put-select [parameterization select]
  (put/sql-select->string parameterization select))

(defn query
  "Takes a jdbc db-spec a sql-select statement and a relational scheme
  and runs the query against the connected database, applying
  `convert-base-value` to the selected values."
  [db-spec select scheme convert-base-value parameterization]
  (let [[select-query & select-args] (put-select parameterization select)]
    (query db-spec
           (cons select-query (time/coerce-time-values select-args))
           :row-fn #(query-row-fn convert-base-value scheme %))))
