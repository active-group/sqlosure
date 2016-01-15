(ns ^{:doc "postgresql specific implementation of db-connection.
See also: [HaskellDB.SQl.PostgreSQL](https://hackage.haskell.org/package/haskelldb-2.2.4/docs/src/Database-HaskellDB-Sql-PostgreSQL.html#generator)."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.db-postgresql
  (:require [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
            [sqlosure.time :as time]
            [sqlosure.jdbc-utils :as jdbc-utils]
            [clojure.java.jdbc :refer :all]
            [clojure.string :as s]))

(defn- postgresql-db
  "Takes a postgresql db-connection and returns the corresponding
  jdbc-connection map."
  [conn]
  (:data conn))

(defn- put-expr [conn select]
  (put/sql-expression->string (db/db-connection-sql-put-parameterization conn) select))

(defn- postgresql-value->value
  "No conversion necessary."
  [tt val]
  (cond
    (= tt t/date%) (time/from-sql-date val)
    (= tt t/timestamp%) (time/from-sql-timestamp val)
    :else val))

(defn- value->postgresql-value
  "No conversion necessary."
  [tt val]
  val)

;; Parameterization as in put/default.
(def postgresql-sql-put-parameterization
  (put/make-sql-put-parameterization put/put-dummy-alias put/default-put-combine put/default-put-literal))

(defn- postgresql-query
  "Takes a db-connection, a sql-select statement and a relational scheme and
  runs the query against the connected database."
  [conn select scheme]
  (jdbc-utils/query (postgresql-db conn) query select scheme
                    postgresql-value->value
                    (db/db-connection-sql-put-parameterization conn)))

(defn- postgresql-insert
  "Takes a db-connection, a table name (string), a relational scheme and a
  vector of vals to be inserted and inserts a new record into the connected
  database's table."
  [conn table scheme vals]
  (let [alist (rel/rel-scheme-alist scheme)]
    (insert! (postgresql-db conn)
             table
             (into
              {}
              (map (fn [[k t] v]
                     [k v]) alist (time/coerce-time-values vals))))))

(defn- postgresql-delete
  "Takes a db-connection, a table name (string) and a sql-expr criterion and
  deleted the matching records from the connected database's table."
  [conn table criterion]
  (delete! (postgresql-db conn)
           table
           (put-expr conn criterion)))

(defn- postgresql-update
  "Takes a db-connection, a table-name (string), a relational scheme, a sql-expr
  criterion and a map of column-name->new-value and applies the update to the
  connected database's table."
  [conn table scheme criterion alist]
  (let [clauses (into {} (map (fn [[k v]] [k (:val v)]) alist))]
    (update! (postgresql-db conn)
             table
             clauses
             (put-expr conn criterion))))

(defn- postgresql-run-sql
  [conn sql]
  (execute! (postgresql-db conn) [sql]))

(defn- postgresql-db-spec
  "Returns a usual jdbc connection spec for the connection to a postgresql server."
  [db-host db-port db-name db-user db-password]
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname (str "//"  db-host ":" db-port "/" db-name)
   :user db-user
   :password db-password})

(defn postgresql->db-connection
  "Returns a db-connection for a db name and (jdbc) connection specification, with specializations for postgresql."
  [db-name db-spec]
  (db/make-db-connection "postgresql"  ;; type
                         db-name       ;; name
                         db-spec       ;; data
                         
                         db-name  ;; handle
                         postgresql-sql-put-parameterization
                         nil
                         (fn [conn query scheme]
                           (postgresql-query conn query scheme))
                         (fn [conn table scheme vals]
                           (postgresql-insert conn table scheme vals))
                         (fn [conn table criterion]
                           (postgresql-delete conn table criterion))
                         (fn [conn table scheme criterion alist]
                           (postgresql-update conn table scheme criterion alist))
                         (fn [conn sql]
                           (query conn sql))))

(defn open-db-connection-postgresql
  [db-host db-port db-name db-user db-password]
  (postgresql->db-connection db-host db-port db-name db-user db-password))
