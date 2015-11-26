(ns sqlosure.db-connection
  (:require [active.clojure.record :refer [define-record-type]]))

(define-record-type db-connection
  (make-db-connection type name data handle sql-put-parameterization
                      closer querier inserter deleter updater sql-runner)
  db-connection?
  [type db-connection-type  ;; symbol
   name db-connection-name  ;; name of actual db
   data db-connection-data  ;; internal conenction data
   handle db-connection-handle
   sql-put-parameterization db-connection-sql-put-parameterization
   closer db-connection-closer
   querier db-connection-querier
   inserter db-connection-inserter
   deleter db-connection-deleter
   updater db-connection-updater
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
  ;; In the original implementation, there also was a 'scheme' argument which
  ;; never got used.. Can't we just get rid of this?
  [conn select]
  ((db-connection-querier conn) conn select scheme))

(defn db-insert
  [conn table scheme vals]
  ((db-connection-inserter conn) conn table scheme vals))

(defn db-delete
  [conn table criterion]
  ((db-connection-deleter conn) conn table criterion))

(defn db-update
  [conn table scheme criterion alist]
  ((db-connection-updater conn) conn table scheme criterion alist))

(defn db-run-sql
  [conn sql]
  ((db-connection-sql-runner conn) conn sql))
