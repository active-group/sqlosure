(ns sqlosure.database
  (:require [sqlosure.optimization :as o]
            [sqlosure.db-connection :as c]
            [sqlosure.db-sqlite3 :as sqlite]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.type :as t]
            [sqlosure.sql :as sql]))

(defn run-query
  [conn q]
  (let [qq (o/optimize-query q)]
    (c/db-query conn (rsql/query->sql qq) (rel/query-scheme qq))))

(defn insert
  [conn sql-table & rest]
  ())

(defn delete
  [conn sql-table criterion-proc]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))]
    (c/db-delete
     conn name
     (rsql/expression->sql
      (apply criterion-proc
             (map (fn [[k v]] (rel/make-attribute-ref k))
                  (rel/rel-scheme-alist (rel/query-scheme sql-table))))))))

(defn update
  [conn sql-table criterion-proc alist-first & rest]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme (rel/query-scheme sql-table)
        attr-exprs (map (fn [[k v]] (rel/make-attribute-ref k))
                        (rel/rel-scheme-alist scheme))]
    (c/db-update conn name scheme
                 (rsql/expression->sql (apply criterion-proc attr-exprs))
                 (into {}
                       (map (fn [[k v]]
                              [k (rsql/expression->sql v)])
                            (if (fn? alist-first)
                              (apply alist-first attr-exprs)
                              (cons alist-first rest)))))))

(def close-database c/close-db-connection)
(def run-sql c/db-run-sql)

(defn call-with-database
  [conn thunk]
  (do
    (when (not conn)
      (throw (Exception. (str 'call-with-database
                              " re-entered call-with-database"))))
    thunk
    (close-database conn)))
