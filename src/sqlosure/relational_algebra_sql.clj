(ns sqlosure.relational-algebra-sql
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]))

(defn x->sql-select
  "Takes a sql expression and turns it into a sql-select."
  [sql]
  (cond
    (sql/sql-select-empty? sql) (sql/new-sql-select)
    (and (sql/sql-select? sql)
         (empty? (sql/sql-select-attributes sql))) sql
    :else (let [new (sql/new-sql-select)]
            (sql/set-sql-select-tables new {false sql}))))

(defn query->sql
  [q]
  (cond
    (rel/base-relation? q)
    (do (when-not (sql/sql-table? (rel/base-relation-handle q))
          (throw (Exception.
                  (str 'query->sql ": base-relation not an SQL table " q))))
        (sql/make-sql-select-table (rel/base-relation-handle q)))
    (rel/project? q) (let [sql (x->sql-select (query->sql (rel/project-query q)))])))

