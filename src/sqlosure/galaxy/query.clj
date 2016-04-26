(ns sqlosure.galaxy.query
  (:require [active.clojure.record :refer [define-record-type]]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.galaxy.galaxy :as glxy]
            [sqlosure.db-connection :as db]
            [active.clojure.condition :as c]
            [sqlosure.type :as t]))

(defn one-factor-scheme?
  "Takes a `sqlosure.relational-algebra/rel-scheme` and checks if it has only
  one column."
  [sch]
  (= (count (rel/rel-scheme-columns sch)) 1))

(defn sole-type-label
  "Takes a `sqlosure.relational-algebra/rel-scheme` and returns it's single
  column type."
  [sch]
  (assert (one-factor-scheme? sch))
  (get (rel/rel-scheme-map sch) (first (rel/rel-scheme-columns sch))))

(defn scheme-factor
  "Takes a `sqlosure.relational-algebra/rel-scheme` and returns it's single
  column name."
  [sch]
  (assert (one-factor-scheme? sch))
  (first (rel/rel-scheme-columns sch)))

(define-record-type galaxy
  (really-make-galaxy name scheme reference? enumerator) galaxy?
  [name galaxy-name
   scheme galaxy-scheme
   reference? galaxy-reference?
   ^{:doc "Function that returns a list of all inhabitants."}
   enumerator galaxy-enumerator])

(defn make-galaxy
  [name scheme enumerator]
  (rel/make-base-relation name scheme
                          sql/sql-universe
                          (really-make-galaxy name scheme nil enumerator)))

(defn enumerate-galaxy
  [glxy]
  ((galaxy-enumerator (rel/base-relation-handle glxy))))

(declare db-expression?)

(every? even? (range 0 10 2))

(defn db-query?
  [q]
  (cond
    (rel/empty-query? q) true
    (rel/base-relation? q) (glxy/db-galaxy? (rel/base-relation-handle q))
    (rel/combine? q) (and (glxy/db-galaxy? (rel/combine-query-1 q))
                          (glxy/db-galaxy? (rel/combine-query-2 q)))
    (rel/restrict? q) (let [underlying (rel/restrict-query q)]
                        (and (db-expression? underlying (rel/restrict-exp q))
                             (db-query? underlying)))
    (rel/project? q) (let [underlying (rel/project-query q)]
                       (and (every? (fn [[_ v]] (db-expression? underlying v))
                                    (rel/project-alist q))
                            (db-query? underlying)))
    (rel/order? q) (let [underlying (rel/order-query q)]
                     (and (every? (fn [[k _]] (db-expression? underlying k))
                                  (rel/order-alist q))
                          (db-query? underlying)))
    (rel/top? q) (db-query? (rel/top-query q))
    :else (c/assertion-violation `db-query? "unknown query" q)))

(defn db-type?
  [t]
  (cond
    (satisfies? t/base-type-protocol t)
    (or (not (t/-data t))  ;; shared type
        (glxy/db-type-data? (t/-data t)))  ;; ours
    (t/nullable-type? t) (db-type? (t/-non-nullable t))
    (t/bounded-string-type? t) true
    (t/product-type? t) (every? db-type? (t/product-type-components t))
    (t/set-type? t) (db-type? (t/set-type-member-type t))
    :else (c/assertion-violation `db-type? "unknown type" t)))

(defn db-subquery?
  [underlying query]
  (and (db-query? query)
       (or (empty? (rel/query-attribute-names query)
                   (db-query? underlying)))))

(defn db-expression?
  [underlying expr]
  (let [underlying-scheme (rel/query-scheme underlying)]
    (letfn
        [(worker [expr]
           (cond
             (rel/attribute-ref? expr)
             (db-type? (rel/expression-type
                        (rel/rel-scheme->environment underlying-scheme)
                        expr))
             (rel/const? expr) (db-type? (rel/const-type expr))
             (rel/application? expr)
             (and (rel/rator-data
                   (rel/application-rands expr))
                  (every? worker (rel/application-rands expr)))
             (glxy/tuple? expr) (every? worker (glxy/tuple-expressions expr))
             (rel/aggregation? expr) (worker (rel/aggregation-expr expr))
             (rel/case-expr? expr)
             (and (every? (fn [[k v]] (and (worker k)
                                           (worker v)))
                          (rel/case-expr-alist expr))
                  (worker (rel/case-expr-default expr)))
             (rel/scalar-subquery? expr)
             (db-subquery? underlying (rel/scalar-subquery-query expr))
             (rel/set-subquery? expr)
             (db-subquery?  underlying (rel/set-subquery-query expr))
             :else (c/assertion-violation `db-expression?
                                          "invalid expression" expr)))]
      (worker expr))))

(declare query-results)

(defn query-objects
  [q]
  (assert (one-factor-scheme? (rel/query-scheme q)))
  (map first (query-results q)))

(defn query-object
  [q]
  (let [res (query-objects q)]
    (when (seq res)
      (first res))))

(def ^:dynamic *reference-object* (atom nil))

(defn make-reference-object-galaxy
  [typ]
  (let [scheme (rel/alist->rel-scheme {"reference object" type})]
    (rel/make-base-relation (str "reference-object/" (t/base-type-name typ))
                            scheme
                            sql/sql-universe
                            (really-make-galaxy
                             "reference object"
                             scheme
                             true
                             #(list (list *reference-object*))))))

(defn make-predicate-expression-query
  [typ expr]
  (let [arefs (rel/expression-attribute-names expr)]
    (assert (= (count arefs) 1))
    (let [name (first arefs)
          glxy (make-reference-object-galaxy typ)]
      (rel/make-restrict
       expr
       (rel/make-project
        {name (rel/make-attribute-ref (galaxy-name
                                       (rel/base-relation-handle glxy)))}
        glxy)))))

(defn make-predicate-query
  [typ proc]
  (let [glxy (make-reference-object-galaxy typ)]
    (proc glxy (rel/make-attribute-ref
                (galaxy-name (rel/base-relation-handle glxy))))))

(defn make-constant-predicate-query
  [typ v]
  (rel/make-restrict (rel/make-const t/boolean% v)
                     (make-reference-object-galaxy typ)))

(defn make-true-predicate-query
  [typ]
  (make-constant-predicate-query typ true))

(defn make-false-predicate-query
  [typ]
  (make-constant-predicate-query typ false))

(defn apply-predicate-query
  [q obj]
  (assert (one-factor-scheme? (rel/query-scheme q)))
  ())

;; FIXME Get db connection in there!
(defn- query-results-1
  [env q]
  (if (db-query? q)
    (glxy/db-query-reified-results nil q)
    (compute-query-results env q)))

(defn query-results
  [q]
  (query-results-1 rel/the-empty-environment q))

