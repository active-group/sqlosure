(ns sqlosure.db-connection
  (:require [active.clojure.record :refer [define-record-type]]))

(define-record-type db-connection
  (make-db-connection type name data handle sql-put-parameterization
                      closer querier inserter deleter updater sql-runner)
  db-connection?
  [type db-connection-type  ;; What kind of database we're attached to.
   name db-connection-name  ;; Name of the actual DB; for humans.
   data db-connection-data  ;; Internal connection data; for the driver.
   handle db-connection-handle  ;; DB-specific connection handle.
   sql-put-parameterization db-connection-sql-put-parameterization
   closer db-connection-closer
   ;; Proc to run query.
   querier db-connection-querier  ;; :db-connection sql-select scheme -> records
   ;; Proc to insert.
   inserter db-connection-inserter  ;; :db-connection string scheme (one-of (list value) & values)
                                    ;; -> inserted-record
   ;; Proc to delete.
   deleter db-connection-deleter  ;; :db-connection string scheme sql-expr -> int
   ;; Proc to update.
   updater db-connection-updater  ;; :db-connection string scheme sql-expr (map-of field -> new-value) -> int
   sql-runner db-connection-sql-runner])

(defn set-db-connection-handle
  [conn v]
  (assoc conn :handle v))

(defn close-db-connection
  [conn]
  (do
    ((db-connection-closer conn) conn)
    (set-db-connection-handle conn nil)))

(defn db-query
  "Takes a db-connection, a sql-select and a relational scheme and runs the
  select query against the connected database."
  [conn select scheme]
  ((db-connection-querier conn) conn select scheme))

(defn db-insert
  "Takes a db-connection, a table name (string), a relational scheme and a
  vector of values and inserts it into the connected database."
  [conn table scheme vals]
  ((db-connection-inserter conn) conn table scheme vals))

(defn db-delete
  "Takes a db-connection, a table name (string) and a sql expression
  representing the criterion for deleten and deletes the matching records from
  the connected database."
  [conn table criterion]
  ((db-connection-deleter conn) conn table criterion))

(defn db-update
  "Takes a db-connection, a table name (string), a relational scheme, a sql
  expression representing the criterion for records to update and a map
  of column-name->new-value and applies the update to the connected database's
  table."
  [conn table scheme criterion alist]
  ((db-connection-updater conn) conn table scheme criterion alist))

(defn db-run-sql
  "Run a plain sql expression against the connected database."
  [conn sql]
  ((db-connection-sql-runner conn) conn sql))
