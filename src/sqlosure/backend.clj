(ns sqlosure.backend
  (:require [active.clojure.condition :as c]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer [define-record-type]]

            [sqlosure.backends.base :as base]
            [sqlosure.sql-put :as sql-put]))

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
;; - The way certain things are laid out in the actual SQL. That includes:
;;   - The way an "alias" is printed
;;   - The way "combines" are printed (i.e. UNION, INTERSECTION, ...)
(define-record-type Backend
  (really-make-backend type-implementations functions put-parameterization) backend?
  [type-implementations backend-type-implementations
   functions backend-functions
   put-parameterization backend-put-parameterization])

(defn make-backend
  ([type-implementations functions]
   (make-backend type-implementations functions sql-put/default-sql-put-parameterization))
  ([type-implementations functions put-parameterization]
   (when-not (sql-put/sql-put-parameterization? put-parameterization)
     (c/assertion-violation `make-backend "not a valid put-parameterization" put-parameterization))
   (really-make-backend type-implementations functions put-parameterization)))

(def backend (make-backend base/types {}))

(defn add-type-implementation
  "Add a type `t` to a backend `backend`."
  [backend t]
  (lens/overhaul backend backend-type-implementations conj t))

(defn reify-type-implementation

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

(defn reify-put-parameterization
  "Reify the implementation of certain SQL specific printing operations for a backend."
  [backend put-parameterization]
  (when-not (sql-put/sql-put-parameterization? put-parameterization)
    (c/assertion-violation `reify-put-parameterization "not a valid put-parameterization" put-parameterization))
  (lens/shove backend backend-put-parameterization put-parameterization))
