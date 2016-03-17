(ns sqlosure.database
  (:require [sqlosure.optimization :as o]
            [sqlosure.db-connection :as c]
            [sqlosure.db-sqlite3 :as sqlite]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.type :as t]
            [sqlosure.sql :as sql]))

(defn run-query
  "Takes a database connection and a query and runs it against the database.
  The optional keyword arguments specify how to
  construct the result set:
    :as-arrays? - return each row as a vector of the field values, default false, in which
      a row is represented as a hash-map of the columns of the query scheme to the corresponding
      field values.
    :row-fn - applied to each row (vector or map) as the result set is constructed, defaults
      to identity.
    :result-set-fn - applied to a lazy sequence of all rows, defaults doall. Note that the
      function must realize the sequence, as the connection to the database may be closed after
      run-query returns.
    "
  [conn q & opts]
  (let [opts-map (apply hash-map opts)
        qq (if (or (not (contains? opts-map :optimize?))
                   (:optimize? opts-map)) ;; optimize? on by default
             (o/optimize-query q)
             q)]
    (c/db-query conn (rsql/query->sql qq) (rel/query-scheme qq)
                (dissoc opts-map :optimize?))))

(defn insert!
  "Takes a database connection"
  [conn sql-table & args]
  (let [[scheme vals] (if (and (t/pair? args) (rel/rel-scheme? (first args)))
                        [(first args) (rest args)]
                        [(rel/query-scheme sql-table) args])]
    (c/db-insert conn (sql/sql-table-name (rel/base-relation-handle sql-table))
                 scheme
                 vals)))

(defn delete!
  [conn sql-table criterion-proc]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))]
    (c/db-delete
     conn name
     (rsql/expression->sql
      (apply criterion-proc
             (map rel/make-attribute-ref
                  (rel/rel-scheme-columns (rel/query-scheme sql-table))))))))

(defn update!
  [conn sql-table criterion-proc alist-first & args]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme (rel/query-scheme sql-table)
        attr-exprs (map rel/make-attribute-ref
                        (rel/rel-scheme-columns scheme))]
    (c/db-update conn name scheme
                 (rsql/expression->sql (apply criterion-proc attr-exprs))
                 (into {}
                       (map (fn [[k v]]
                              [k (rsql/expression->sql v)])
                            (if (fn? alist-first)
                              (apply alist-first attr-exprs)
                              (cons alist-first args)))))))

(def close-database c/close-db-connection)
(def run-sql c/db-run-sql)

(defn call-with-database
  [conn thunk]
  (do
    (when-not conn
      (throw (Exception. (str 'call-with-database
                              " re-entered call-with-database"))))
    thunk
    (close-database conn)))
