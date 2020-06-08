(ns sqlosure.backends.postgresql
  (:require [sqlosure.backend :as backend]
            [sqlosure.sql-put :as sql-put]))

;; TODO This is only a stub implementation.

(defn postgresql-put-alias
  [alias]
  (if alias
    (print " AS " alias)
    ;; Sub-Selections in PostgreSQL must always be aliased (we don't really care
    ;; about the table's name since we construct the projected attribute's names
    ;; ourselves.
    (print " AS " (gensym))))

(def postgresql-put-parameterization
  (sql-put/make-sql-put-parameterization postgresql-put-alias
                                         sql-put/default-put-combine))

(def implementation
  (-> backend/backend
      (backend/reify-put-parameterization postgresql-put-parameterization)))
