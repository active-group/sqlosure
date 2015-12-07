(ns ^{:doc "postgresql specific implementation of db-connection.
See also: [HaskellDB.SQl.PostgreSQL](https://hackage.haskell.org/package/haskelldb-2.2.4/docs/src/Database-HaskellDB-Sql-PostgreSQL.html#generator)."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.db-postgresql
  (:require [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.relational-algebra :as rel]
            [clojure.java.jdbc :refer :all]
            [clojure.string :as s]))

(defn- postgresql-db
  "Takes a postgresql db-connection and returns the corresponding
  jdbc-connection map."
  [conn]
  (let [conn-data (:data conn)]
    {:classname "org.postgresql.Driver"
     :subprotocol "postgresql"
     :subname (str "//"  (:db-host conn-data) ":" (:db-port conn-data) "/" (:db-name conn-data))
     :user (:db-user conn-data)
     :password (:db-password conn-data)}))

(defn- put-expr [conn select]
  (put/sql-expression->string (db/db-connection-sql-put-parameterization conn) select))

(defn- postgresql-value->value
  "No conversion necessary."
  [tt val]
  val)

(defn- value->postgresql-value
  "No conversion necessary."
  [tt val]
  val)

(defn- query-row-fn
  "Takes a relational scheme and a row returned by a query, then returns a map
  of the key-value pairs with values converted from sqlite3 to Clojure values."
  [scheme row]
  (let [alist (rel/rel-scheme-alist scheme)]
    (into {} (map (fn [[k v] tt] [k (postgresql-value->value tt v)])
                  row
                  (vals alist)))))

;; Parameterization as in put/default.
(def postgresql-sql-put-parameterization put/default-sql-put-parameterization)

(defn- put-select [conn select]
  (put/sql-select->string (db/db-connection-sql-put-parameterization conn) select))

(defn- postgresql-query
  "Takes a db-connection, a sql-select statement and a relational scheme and
  runs the query against the connected database."
  [conn select scheme]
  (query (postgresql-db conn)
         [(put-select conn select)]
         :row-fn #(query-row-fn scheme %)))

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
                     [k (value->postgresql-value t v)]) alist vals)))))

(defn- postgresql-delete
  "Takes a db-connection, a table name (string) and a sql-expr criterion and
  deleted the matching records from the connected database's table."
  [conn table criterion]
  (execute! (postgresql-db conn)
            [(str "DELETE FROM " table " WHERE "
                  (put-expr conn criterion))]))

(defn- postgresql-update
  "Takes a db-connection, a table-name (string), a relational scheme, a sql-expr
  criterion and a map of column-name->new-value and applies the update to the
  connected database's table."
  [conn table scheme criterion alist]
  (let [clauses (map (fn [[k v]] (str k "=" (put-expr conn v))) alist)]
    (execute! (postgresql-db conn)
              [(str "UPDATE " table " SET "
                    (s/join ", " clauses)
                    " WHERE "
                    (put-expr conn criterion))])))

(defn- postgresql-run-sql
  [conn sql]
  (execute! (postgresql-db conn) [sql]))

(defn- postgresql->db-connection
  [db-host db-port db-name db-user db-password]
  (db/make-db-connection "postgresql"  ;; type
                         db-name       ;; name
                         {:db-host db-host  ;; data
                          :db-port db-port
                          :db-name db-name
                          :db-user db-user
                          :db-password db-password}
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
