(ns sqlosure.lang
  (:require [active.clojure.condition :as c]
            [active.clojure.lens :as lens]
            [active.clojure.monad :as monad]
            [active.clojure.record :refer [define-record-type]]

            [sqlosure.core :as s]
            [sqlosure.db-connection :as db]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.query-comprehension :as q]))

;; Action Types
(define-record-type Create
  (make-create table record) create?
  [table create-table
   record create-record])

(define-record-type Read
  (make-read query) read?
  [query read-query])

(define-record-type Update
  (make-update table predicate-fn update-fn) update?
  [table update-table
   predicate-fn update-predicate-fn
   update-fn update-update-fn])

(define-record-type Delete
  (make-delete table predicate-fn) delete?
  [table delete-table
   predicate-fn delete-predicate-fn])

;; Constructors
(defn create!
  [table record]
  (when-not (rel/base-relation? table)
    (c/assertion-violation `create! "not a valid table" table))
  (make-create table record))

(defn read!
  [query]
  (when-not (rel/query? query)
    (c/assertion-violation `read! "not a valid query" query))
  (make-read query))

(defn update!
  [table predicate-fn update-fn]
  (when-not (rel/base-relation? table)
    (c/assertion-violation `update! "not a valid table" table))
  (make-update table predicate-fn update-fn))

(defn delete!
  [table predicate-fn]
  (when-not (rel/base-relation? table)
    (c/assertion-violation `delete! "not a valid table" table))
  (make-delete table predicate-fn))

(define-record-type NotFound (make-not-found) not-found? [])

(def not-found (monad/return (make-not-found)))

(define-record-type WriteResult
  (make-write-result affected-row-count) write-result?
  [affected-row-count write-result-affected-row-count])

(defn write-success?
  "Returns true if `write-result` affected more than zero rows."
  [write-result]
  (and (write-result? write-result)
       (pos-int? (write-result-affected-row-count write-result))))
