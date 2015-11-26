(ns sqlosure.db-sqlite3
  (:require [sqlosure.db-connection :as db]
            [sqlosure.sql-put :as put]
            [sqlosure.sql :as sql]
            [sqlosure.type :as t]
            [sqlosure.relational-algebra :as rel]
            [clojure.java.jdbc :refer :all]
            [clojure.string :as s]))

(defn- sqlite3-db
  [subname]
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname subname})

(defn sqlite3-put-combine
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

(defn sqlite3-put-literal
  "sqlite3 specific printer for literals."
  [val]
  (if (or (= true val) (= false val))
    (if val (print 1) (print 0))
    (put/default-put-literal val)))

(def sqlite3-sql-put-parameterization
  "Printer for sqliter3."
  (put/make-sql-put-parameterization sqlite3-put-combine sqlite3-put-literal))

(defn sqlite3-value->value
  [tt val]
  (cond
    (or (= tt t/string%) (= tt t/integer%) (= tt t/double%) (= tt t/blob%)) val
    (= tt t/boolean%) (not= val 0)
    :else (throw (Exception. (str 'sqlite3-value->value ": unkown type " tt val)))))

(defn value->sqlite3-value
  [tt val]
  (cond
    (or (= tt t/string%) (= tt t/integer%) (= tt t/double%) (= tt t/blob%)) val
    (= tt t/boolean%) (if val 1 0)
    :else (throw (Exception. (str 'value->sqlite3-value ": unknown type " tt val)))))

(defn sqlite3-close
  "Takes the sqlite3 connection and closes it."
  [handle]
  (.close (get-connection handle)))

(defn sqlite3-query
  [conn q]
  (query (sqlite3-db (db/db-connection-handle conn))
         [q]))

(defn sqlite3-insert
  [conn table scheme vals]
  (let [alist (rel/rel-scheme-alist scheme)]
    (insert! conn
             table
             (into
              {}
              (map (fn [[k t] v]
                     [k (value->sqlite3-value t v)]) alist vals)))))

(defn sqlite3-delete
  [conn table criterion]
  (execute! conn
            [(str "DELETE FROM " table " WHERE "
                   (put/sql-expression->string sqlite3-sql-put-parameterization
                                               criterion))]))

(defn sqlite3-update
  [conn table criterion alist]
  (let [clauses (map (fn [[k v]]
                       (str k "="
                            (put/sql-expression->string
                             sqlite3-sql-put-parameterization v)))
                     alist)]
    (execute! conn [(str "UPDATE " table " SET "
                          (s/join ", " clauses)
                          " WHERE "
                          (put/sql-expression->string sqlite3-sql-put-parameterization
                                                      criterion))])))

(defn sqlite3->db-connection
  [filename]
  (db/make-db-connection "sqlite3"  ;; type
                         filename  ;; name
                         filename  ;; data
                         filename   ;; handle
                         sqlite3-sql-put-parameterization
                         nil ;; As long as we're not explicitly keeping the
                         ;; connection alive, we don't have to close.
                         (fn [conn query]
                           (sqlite3-query conn query))
                         (fn [conn table scheme vals]
                           (sqlite3-insert (sqlite3-db filename) table scheme vals))
                         (fn [conn table criterion]
                           (sqlite3-delete (sqlite3-db filename) table criterion))
                         (fn [conn table criterion alist]
                           (sqlite3-update (sqlite3-db filename) table criterion alist))
                         (fn [conn sql]
                           (query (sqlite3-db filename) sql))))
