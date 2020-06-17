(ns sqlosure.backends.oracledb
  (:require [active.clojure.monad :as m]
            [sqlosure.backend :as backend]
            [sqlosure.sql-put :as sql-put]))

;; TODO This is only a stub implementation.

(defn oracledb-put-alias
  [alias]
  (if alias
    (sql-put/write! alias)
    (m/return nil)))

(def oracledb-put-parameterization
  (sql-put/make-sql-put-parameterization oracledb-put-alias
                                         sql-put/default-put-combine))

(def implementation
  (-> backend/backend
      (backend/reify-put-parameterization oracledb-put-parameterization)))
