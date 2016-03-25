(ns sqlosure.database
  (:require [clojure.java.jdbc :as jdbc]
            [sqlosure
             [db-connection :as db]
             [jdbc-utils :as jdbc-utils]
             [optimization :as o]
             [relational-algebra :as rel]
             [relational-algebra-sql :as rsql]
             [sql :as sql]
             [sql-put :as put]
             [type :as t]]))

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
  [conn q & {:keys [optimize?] :or {optimize? true} :as opts-map}]
  (let [qq (if optimize? (o/optimize-query q) q)
        c (db/db-connection-type-converter conn)]
    (jdbc-utils/query (db/db-connection-conn conn) (rsql/query->sql qq)
                      (rel/query-scheme qq)
                      (db/type-converter-db-value->value c)
                      (db/type-converter-value->db-value c)
                      (db/db-connection-paramaterization conn)
                      (dissoc opts-map :optimize?))))

(defn insert!
  [conn sql-table & args]
  ;; FIXME: doesn't work as expected when called with explicit rel-scheme.
  (let [[scheme vals] (if (and (t/pair? args) (rel/rel-scheme? (first args)))
                        [(first args) (rest args)]
                        [(rel/query-scheme sql-table) args])
        c (db/db-connection-type-converter conn)]
    (jdbc/insert!
     (db/db-connection-conn conn)
     (sql/sql-table-name (rel/base-relation-handle sql-table))
     (into {}
           (map (fn [[k t] v]
                  [k ((db/type-converter-value->db-value c) t v)])
                (rel/rel-scheme-map scheme)
                vals)))))

(defn delete!
  [conn sql-table criterion-proc]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))]
    (jdbc/delete! (db/db-connection-conn conn)
                  name
                  (put/sql-expression->string
                   (db/db-connection-paramaterization conn)
                   (apply criterion-proc
                          (map rel/make-attribute-ref
                               (rel/rel-scheme-columns (rel/query-scheme sql-table))))))))

(defn update!
  [conn sql-table criterion-proc alist-first & args]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme (rel/query-scheme sql-table)
        attr-exprs (map rel/make-attribute-ref
                        (rel/rel-scheme-columns scheme))
        alist (into {}
                    (map (fn [[k v]]
                           [k (rsql/expression->sql v)])
                         (if (fn? alist-first)
                           (apply alist-first attr-exprs)
                           (cons alist-first args))))]
    (jdbc/update! (db/db-connection-conn conn)
                  name
                  (into {} (map (fn [[k v]] [k (:val v)]) alist))
                  (put/sql-expression->string
                   (db/db-connection-paramaterization conn)
                   (rsql/expression->sql (apply criterion-proc attr-exprs))))))

(defn run-sql
  [conn sql]
  (jdbc/execute! (db/db-connection-conn conn) sql))
