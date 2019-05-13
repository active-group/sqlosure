(ns sqlosure.type-implementation
  (:require [active.clojure.condition :as c]
            [active.clojure.record :refer [define-record-type]]
            [sqlosure.type :as t]))

;;   A type implementation is a backend specific definition of a SQLosure type.
;;   Types differ from backend to backend in the way they are 1. rendered and
;;   2. how their values are converted from and to Clojure and backend-
;;   representation. A TypeImplementation conjoins a SQLosure base type and the
;;   backend specific implementation.
(define-record-type TypeImplementation
  (make-type-implementation base-type to-sql from-sql) type-implementation?
  [(base-type type-implementation-base-type type-implementation-base-type-lens)
   (to-sql type-implementation-to-sql type-implementation-to-sql-lens)
   (from-sql type-implementation-from-sql type-implementation-from-sql-lens)])

(defn implement
  [ty to-sql from-sql]
  (when-not (t/atomic-type? ty)
    (c/assertion-violation `implement "not an atomic-type" ty))
  (make-type-implementation ty to-sql from-sql))

;; Example
;; (def $string (make-type-implementation t/string%
;;                                         (fn [^PreparedStatement stmt ix val] (.setString stmt ix val))
;;                                         (fn [^ResultSet rs ix] (.getString rs ix))))
