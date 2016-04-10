(ns sqlosure.jdbc-utils
  "Utilities for using clojure.java.jdbc as a backend."
  (:import [java.sql PreparedStatement ResultSet])
  (:require [clojure.java.jdbc :as jdbc]
            [sqlosure
             [relational-algebra :as rel]
             [sql-put :as put]]))

(defn result-set-seq
  "Creates and returns a lazy sequence of maps corresponding to the rows in the
   java.sql.ResultSet rs."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)
        idxs (range 1 (inc (.getColumnCount rsmeta)))
        row-values (fn [] (map (fn [^Integer i] (jdbc/result-set-read-column (.getObject rs i) rsmeta i)) idxs)) ;; FIXME, inline
        rows ((fn thisfn []
                (if (.next rs)
                  (cons (vec (row-values))
                        (lazy-seq (thisfn)))
                  (.close rs))))]
    rows))

(defn- set-parameters
  "Add the parameters to the given statement."
  [stmt params]
  (dorun (map-indexed (fn [ix value]
                        (.setObject stmt (inc ix) (jdbc/sql-value value))) ;; FIXME: type-specific
                      params)))

;; top-level API for actual SQL operations

(defn query
  "Given a database connection and a vector containing SQL and optional parameters,
  perform a simple database query."
  [db sql params & prepare-options]
  (let [run-query-with-params
        (^{:once true} fn* [con]
         (let [^PreparedStatement stmt (apply jdbc/prepare-statement con sql prepare-options)]
           (set-parameters stmt params)
           (.closeOnCompletion stmt)
           (result-set-seq (.executeQuery stmt))))]
    (if-let [con (jdbc/db-find-connection db)]
      (run-query-with-params con)
      (with-open [con (jdbc/get-connection db)]
        (doall ; sorry
         (run-query-with-params con))))))
