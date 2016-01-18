(ns sqlosure.db-sqlite3
  (:require [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.sql :as sql]
            [sqlosure.type :as t]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.jdbc-utils :as jdbc-utils]
            [clojure.java.jdbc :refer (execute! insert! get-connection query)]
            [clojure.string :as s]))

(defn- sqlite3-db [conn]
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname (db/db-connection-handle conn)})

(defn- put-expr [conn select]
  (put/sql-expression->string (db/db-connection-sql-put-parameterization conn) select))

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

(def ^{:private true} sqlite3-sql-put-parameterization
  "Printer for sqliter3."
  (put/make-sql-put-parameterization put/default-put-alias sqlite3-put-combine sqlite3-put-literal))

(defn- sqlite3-value->value
  "Takes a sqlosure.type and a value returned from sqlite3 and converts it back
  to the corresponding Clojure value."
  [tt val]
  (cond
    (or (= tt t/string%) (= tt t/integer%) (= tt t/double%) (= tt t/blob%)) val
    (= tt t/boolean%) (not= val 0)
    :else (throw (Exception. (str 'sqlite3-value->value ": unkown type " tt val)))))

(defn- value->sqlite3-value
  "Takes a sqlosure.type and a value and converts it to it's corresponding
  sqlite3 value."
  [tt val]
  (cond
    (or (= tt t/string%) (= tt t/integer%) (= tt t/double%) (= tt t/blob%)) val
    (= tt t/boolean%) (if val 1 0)
    :else (throw (Exception.
                  (str 'value->sqlite3-value ": unknown type " tt val)))))

(defn- sqlite3-close
  "Takes the sqlite3 connection and closes it."
  [handle]
  (.close (get-connection handle)))

(defn- sqlite3-query
  "Takes a db-connection, a sql-select statement and a relational scheme and
  runs the query against the connected database."
  [conn select scheme opts]
  (jdbc-utils/query (sqlite3-db conn) select scheme
                    sqlite3-value->value
                    value->sqlite3-value
                    (db/db-connection-sql-put-parameterization conn)
                    opts))

(defn- sqlite3-insert
  "Takes a db-connection, a table name (string), a relational scheme and a
  vector of vals to be inserted and inserts a new record into the connected
  database's table."
  [conn table scheme vals]
  (let [alist (rel/rel-scheme-alist scheme)]
    (insert! (sqlite3-db conn)
             table
             (into
              {}
              (map (fn [[k t] v]
                     [k (value->sqlite3-value t v)]) alist vals)))))

(defn- sqlite3-delete
  "Takes a db-connection, a table name (string) and a sql-expr criterion and
  deleted the matching records from the connected database's table."
  [conn table criterion]
  (execute! (sqlite3-db conn)
            [(str "DELETE FROM " table " WHERE "
                  (put-expr conn criterion))]))

(defn- sqlite3-update
  "Takes a db-connection, a table-name (string), a relational scheme, a sql-expr
  criterion and a map of column-name->new-value and applies the update to the
  connected database's table."
  [conn table scheme criterion alist]
  (let [clauses (map (fn [[k v]]
                       (str k "="
                            (put-expr conn v)))
                     alist)]
    (execute! (sqlite3-db conn)
              [(str "UPDATE " table " SET "
                    (s/join ", " clauses)
                    " WHERE "
                    (put-expr conn criterion))])))

(defn- sqlite3-run-sql
  [conn sql]
  (execute! (sqlite3-db conn) [sql]))

(defn- sqlite3->db-connection
  "Takes the filename of a sqlite3 database-file and returns a new sqlite3
  db-connection."
  [filename]
  (db/make-db-connection "sqlite3"  ;; type
                         filename  ;; name
                         filename  ;; data
                         filename   ;; handle
                         sqlite3-sql-put-parameterization
                         nil ;; As long as we're not explicitly keeping the
                         ;; connection alive, we don't have to close. Solution?
                         (fn [conn query scheme opts]
                           (sqlite3-query conn query scheme opts))
                         (fn [conn table scheme vals]
                           (sqlite3-insert conn table scheme vals))
                         (fn [conn table criterion]
                           (sqlite3-delete conn table criterion))
                         (fn [conn table scheme criterion alist]
                           (sqlite3-update conn table scheme criterion alist))
                         (fn [conn sql]
                           (query conn sql))))

(defn open-db-connection-sqlite3
  "Takes the filename of a sqlite3 database-file and returns a new sqlite3
  db-connection."
  [filename]
  (sqlite3->db-connection filename))
