(ns sqlosure.backends.oracledb
  (:require [sqlosure.backend :as backend]
            [sqlosure.sql-put :as sql-put]))

;; TODO This is only a stub implementation.

(defn oracledb-put-alias
  [alias]
  (if alias
    (print " " alias)
    (print " " (gensym))))

(def oracledb-put-parameterization
  (sql-put/make-sql-put-parameterization oracledb-put-alias
                                         sql-put/default-put-combine))

(def implementation
  (-> backend/backend
      (backend/reify-put-parameterization oracledb-put-parameterization)))
