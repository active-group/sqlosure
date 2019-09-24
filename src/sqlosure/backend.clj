(ns sqlosure.backend
  (:require [active.clojure.record :refer [define-record-type]]
            [active.clojure.lens :as lens]
            [sqlosure.backends.base :as base]
            [active.clojure.condition :as c]))

;;   In the following lines, backend will refer to a specific RDBMS (i.e. postgresql, mysql, sqlite, ...)

;;   The following details differ between the possible backends (and are of
;;   interest to us):
;; - Supported types and the way they are treated
;;   The types supported differ between backends in two ways: 1. the types that
;;   actually exists for the backend and 2. the way they are treated. A prime
;;   example for this are booleans (OracleDB treats 0, 1 as false, true,
;;   Postgresql has a primitive boolean type). Therefore, each backend
;;   implementation needs to know how to convert types from and to db values.
;; - Supported functions and aggregation functions
;;   Each RDBMS has a slightly different set of (aggregation) functions (and an
;;   associated syntax). To support this, each backend needs to know which
;;   functions it supports and must reject a query if it references an unknown
;;   function. Further, is has to know how to "render" these functions into an
;;   actual SQL query-string.

(define-record-type Backend
  (make-backend type-implementations functions) backend?
  [type-implementations backend-type-implementations
   functions backend-functions])

(def backend (make-backend base/types {}))

(defn add-type-implementation
  "Add a type `t` to a backend `backend`."
  [backend t]
  (lens/overhaul backend backend-type-implementations conj t))

(defn reify-type-implementation
  "Reify a specific aspect of a type-implementation of type `t`. Applies `f` at
  `lens` of the implementation of type t."
  [backend t lens f]
  (lens/shove backend (lens/>> backend-type-implementations (lens/member t) lens) f))

(defn get-type-implementation
  "Get the implementation for a type `ty` from a `backend`-implementation."
  [backend ty]
  (if-let [res (lens/yank backend (lens/>> backend-type-implementations
                                           (lens/member ty)))]
    res
    (c/assertion-violation `get-type-implementation "no implementation for type" ty)))

(defn add-function
  "Add a function `f` to a backend `backend`."
  [backend f]
  (lens/overhaul backend backend-functions conj f))

(defn get-function
  "Get the implementation for a function `f` from a `backend`-implementation."
  [backend f]
  (if-let [res (lens/yank backend (lens/>> backend-functions
                                           (lens/member f)))]
    res
    (c/assertion-violation `get-function "no implementation for function" f)))
