(ns ^{:doc "Implementation of relational algebra based on Mike Sperbers
relational-algebra.scm.
Replaced alist with hash-map."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.relational-algebra
  (:require [sqlosure.universe :as u]
            [sqlosure.type :as t]
            [sqlosure.utils :refer [third]]
            [clojure.set :refer [difference union]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]))

(define-record-type rel-scheme
  (make-rel-scheme alist) rel-scheme?
  [alist rel-scheme-alist]  ;; (rel-scheme -> hash-map)
  )

(def the-empty-rel-scheme (make-rel-scheme nil))
(def the-empty-environment nil)

(defn rel-scheme=?
  "Returns true if t1 and t2 are the same."
  [t1 t2]
  ;; TODO: Couldn't this just be (= t1 t2)?
  (= (rel-scheme-alist t1) (rel-scheme-alist t2)))

(defn rel-scheme-concat
  [s1 s2]
  (make-rel-scheme (into {} ; FIXME
                         (concat (rel-scheme-alist s1)
                                 (rel-scheme-alist s2)))))

(defn rel-scheme-difference
  "Return a new rel-scheme resulting of the (set-)difference of s1's and s2's
  alist."
  [s1 s2]
  (let [res (reduce (fn [m [k2 _]]
                      (if (get m k2)
                        (dissoc m k2)
                        m))
                    (rel-scheme-alist s1)
                    (rel-scheme-alist s2))]
    (if-not (empty? res)
      (make-rel-scheme res)
      (assertion-violation 'rel-scheme-difference "difference must not be empty"))))

(defn rel-scheme-unary?
  "Returns true if the rel-scheme's alist consist of only one pair."
  [scheme]
  (= 1 (count (rel-scheme-alist scheme))))

(defn rel-scheme-nullable
  "Makes all columns in a scheme nullable."
  [scheme]
  (make-rel-scheme
   (into {}
         (map (fn [[name type]]
                [name (t/make-nullable-type type)])
              (rel-scheme-alist scheme)))))

(defn rel-scheme->environment
  "Returns the relation table of a rel-scheme."
  [s]
  (rel-scheme-alist s))

(defn compose-environments
  "Combine two environments. e1 takes precedence over e2."
  [e1 e2]
  (cond
    (empty? e1) e2
    (empty? e2) e1
    :else (merge e2 e1)))

(defn lookup-env
  "Lookup a name in an environment.
  TODO: Should this return `false` as in the original?"
  [name env]
  (get env name))

;;; ----------------------------------------------------------------------------
;;; --- Primitive relations, depending on the domain universe
;;; ----------------------------------------------------------------------------

(define-record-type base-relation
  (really-make-base-relation name scheme handle) base-relation?
  [name base-relation-name
   scheme base-relation-scheme
   handle base-relation-handle])

(defn make-base-relation
  "Returns a new base relation.
  If :handle is supplied, use is as base-relation-handle, defaults to nil.
  If :universe is supplied, return a vector of [relation universe]"
  [name scheme & {:keys [universe handle]}]
  (let [rel (really-make-base-relation name scheme handle)]
    (when universe
      (u/register-base-relation! universe name rel))
    rel))

;;; ----------------------------------------------------------------------------
;;; --- EXPRESSIONS
;;; ----------------------------------------------------------------------------
(define-record-type attribute-ref
  (make-attribute-ref name) attribute-ref?
  [name attribute-ref-name])

(define-record-type const
  (make-const type val) const?
  [type const-type
   val const-val])

(define-record-type null
  (make-null type) const-null?
  [type null-type])

(define-record-type application
  (really-make-application rator rands) application?
  [rator application-rator
   rands application-rands])

(defn make-application
  [rator & rands]
  (really-make-application rator rands))

(define-record-type rator
  (really-make-rator name range-type-proc proc data) rator?
  [name rator-name
   range-type-proc rator-range-type-proc
   proc rator-proc
   data rator-data])

(defn make-rator
  [name range-type-proc proc & {:keys [universe data]}]
  (let [r (really-make-rator name range-type-proc proc data)]
    (when universe
      (u/register-rator! universe name r))
    r))

;;; ----------------------------------------------------------------------------
;;; --- QUERYS
;;; ----------------------------------------------------------------------------

(define-record-type project
  (really-make-project alist query) project?
  [alist project-alist  ;; (project -> hash-map)
   query project-query])

(defn make-project
  [alist query]
  (if (map? alist)
    (if (empty? alist)
      (if (project? query)
        (make-project alist (project-query query))
        (really-make-project alist query))
      (really-make-project alist query))
    (assertion-violation 'make-project "requires hash-map, got" alist)))

(declare query-scheme)

(defn make-extend
  "Creates a projection of some attributes while keeping all other atrtibutes in
  the relation visible too."
  [alist query]
  (make-project
   (into alist (map (fn [p]
                      [(first p)
                       (make-attribute-ref (first p))])
                    (rel-scheme-alist (query-scheme query))))
   query))

(define-record-type restrict
  (make-restrict exp query) restrict?
  [exp restrict-exp
   query restrict-query])

(define-record-type ^{:doc "Restrict a left outer product.
  This will restrict all the right-hand sides of left outer products.
  If it doesn't hold, these right-hand sides will have all-null
  columns."}
  restrict-outer
  (make-restrict-outer exp query) restrict-outer?
  [exp restrict-outer-exp
   query restrict-outer-query])

(define-record-type grouping-project
  (make-grouping-project alist query) grouping-project?
  [alist grouping-project-alist
   query grouping-project-query])

(define-record-type combine
  (really-make-combine rel-op query-1 query-2) combine?
  [rel-op combine-rel-op
   query-1 combine-query-1
   query-2 combine-query-2])

(def ^{:private true} relational-ops #{:product :left-outer-product :union
:intersection :quotient :difference})

(defn relational-op? [k] (contains? relational-ops k))

(defn make-combine [rel-op query-1 query-2]
  (if-not (relational-op? rel-op)
    (assertion-violation 'make-combine "not a relational operator" rel-op)
    (case rel-op
      :product (cond
                 (empty? query-1) query-2
                 (empty? query-2) query-1
                 :else (really-make-combine rel-op query-1 query-2))
          (really-make-combine rel-op query-1 query-2))))

(defn make-left-outer-product [query-1 query-2] (make-combine
  :left-outer-product query-1 query-2))

(defn make-product [query-1 query-2] (make-combine :product query-1 query-2))

(defn make-union [query-1 query-2] (make-combine :union query-1 query-2))

(defn make-intersection [query-1 query-2] (make-combine :intersection query-1
  query-2))

(defn make-quotient [query-1 query-2] (make-combine :quotient query-1 query-2))

(defn make-difference [query-1 query-2] (make-combine :difference query-1
  query-2))

(define-record-type empty-val (make-empty-val) empty-val? [])

(def the-empty (make-empty-val))

(def ^{:private true} order-op #{:ascending :descending})

(defn order-op? [k]
  (contains? order-op k))

(define-record-type order
  (make-order alist query) order?
  [alist order-alist  ;; (order -> hash-map)
   query order-query])

;; Top n entries.
(define-record-type top
  (make-top count query) top?
  [count top-count
   query top-query])

(define-record-type tuple
  (make-tuple expressions) tuple?
  [expressions tuple-expressions])

(def ^{:private true} aggregations-op #{:count :count-all :sum :avg :min :max :std-dev
                                        :std-dev-p :var :var-p})

(defn aggregations-op? [k]
  (contains? aggregations-op k))

(define-record-type aggregation
  (really-make-aggregation op expr) aggregation?
  [op aggregation-operator
   expr aggregation-expr])

(define-record-type aggregation*
  (really-make-aggregation* op) aggregation*?
  [op aggregation*-operator])

(defn make-aggregation
  [op & expr]
  (cond
    (empty? expr) (really-make-aggregation* op)
    (= 1 (count expr)) (apply really-make-aggregation op expr)
    :else (assertion-violation 'make-aggregation "invalid number of expressions (must be 0 or 1)" expr)))

(define-record-type case-expr
  (make-case-expr alist default) case-expr?
  [alist case-expr-alist
   default case-expr-default])

(define-record-type scalar-subquery
  (make-scalar-subquery query) scalar-subquery?
  [query scalar-subquery-query])

(define-record-type set-subquery
  (make-set-subquery query) set-subquery?
  [query set-subquery-query])

(defn fold-expression
  [on-attribute-ref on-const on-null on-application on-tuple on-aggregation on-aggregation*
   on-case on-scalar-subquery on-set-subquery expr]
  (let [next-step #(fold-expression on-attribute-ref on-const on-null on-application
                               on-tuple on-aggregation on-aggregation* on-case
                               on-scalar-subquery on-set-subquery %)]
    (cond
      (attribute-ref? expr) (on-attribute-ref (attribute-ref-name expr))
      (const? expr) (on-const (const-type expr) (const-val expr))
      (const-null? expr) (on-null (null-type expr))
      (application? expr) (on-application (application-rator expr)
                                          (map next-step (application-rands expr)))
      (tuple? expr) (on-tuple (map next-step (tuple-expressions expr)))
      (aggregation? expr) (on-aggregation (aggregation-operator expr)
                                          (next-step (aggregation-expr expr)))
      (aggregation*? expr) (on-aggregation* (aggregation*-operator expr))
      (case-expr? expr) (on-case (into {} (map (fn [[k v]] [(next-step k)
                                                            (next-step v)])
                                               (case-expr-alist expr)))
                                 (next-step (case-expr-default expr)))
      (scalar-subquery? expr) (on-scalar-subquery (scalar-subquery-query expr))
      (set-subquery? expr) (on-set-subquery (set-subquery-query expr))
      :else (assertion-violation 'fold-expression "invalid expression" expr))))

(declare query-scheme*)

(defn- expression-type*
  [env expr fail]
  (fold-expression
   (fn [name] (or (lookup-env name env)
                  (assertion-violation 'expression-type "unknown attribute" name env)))
   (fn [ty val] ty)
   identity
   (fn [rator rands] (apply (rator-range-type-proc rator) fail rands))
   t/make-product-type
   (fn [op t] (let [op (aggregation-operator expr)]
                (if (= :count op)
                  t/integer%
                  (do
                    (when fail
                      (cond
                        (contains? #{:sum :avg :std-dev :std-dev-p :var :var-p} op)
                        (when-not (t/numeric-type? t) (fail 'numeric-type t))
                        (contains? #{:min :max} op)
                        (when-not (t/ordered-type? t) (fail 'ordered-type t))))
                    t))))
   (fn [op] (let [op (aggregation*-operator expr)]
              (if (= :count-all op)
                t/integer%
                (assertion-violation 'expression-type* "unknown aggregation" op))))
   (fn [alist t] (if fail
                   (for [[p r] alist]
                     (do
                       (when-not (t/type=? t/boolean% p) (fail 'boolean p))
                       (when-not (t/type=? t r) (fail t r))))
                   t))
   (fn [subquery] (let [scheme (query-scheme* subquery env fail)
                        alist (rel-scheme-alist scheme)]
                    (when (and fail (or (empty? alist) (t/pair? (rest alist))))
                      (fail 'unary-relation subquery))
                    (key (first alist)) ; FIXME: this looks super wonky and i have no idea what this should do anyway
                    ))
   (fn [subquery] (let [scheme (query-scheme* subquery env fail)
                        alist (rel-scheme-alist scheme)]
                    (when (and fail (or (empty? alist) (t/pair? (rest alist))))
                      (fail 'unary-relation subquery))
                    (t/make-set-type (key (first alist)))))
   expr))

(defn expression-type
  "`expression-type` takes an environment map and an expression and tries to
  find the expressions type (either based on expr itself or on the
  mappings of the env)."
  [env expr & {:keys [typecheck?]}]
  (expression-type* env expr
                    (and typecheck?
                         (fn [expected thing]
                           (assertion-violation 'expression-type
                                                "type-violation (expected/found)"
                                                expected thing)))))

(defn aggregate?
  "Returns true if `expr` is or contains an aggregation."
  [expr]
  (cond
    (attribute-ref? expr) false
    (const? expr) false
    (const-null? expr) false
    (application? expr) (some aggregate? (application-rands expr))
    (tuple? expr) (some aggregate? (tuple-expressions expr))
    (aggregation? expr) true
    (case-expr? expr) (or (some (fn [[k v]] (or (aggregate? k)
                                                (aggregate? v)))
                                (case-expr-alist expr))
                          (aggregate? (case-expr-default expr)))
    (scalar-subquery? expr) false
    (set-subquery? expr) false
    :else (assertion-violation 'aggregate? "invalid expression" expr)))

(defn- query-scheme* [q env fail]
  (letfn [(to-env [scheme]
            (compose-environments (rel-scheme->environment scheme) env))
          (next-step [q*]
            (query-scheme* q* env fail))]
    (cond
      (empty-val? q) the-empty-rel-scheme
      (base-relation? q) (base-relation-scheme q)
      (project? q) (let [base-scheme (next-step (project-query q))
                         m (project-alist q)]
                     (when fail
                       (for [[_ v] m]
                         (when (aggregate? v) (fail ": non-aggregate " v))))
                     (make-rel-scheme
                      (into {}
                            (map (fn [[k v]]
                                   (let [typ (expression-type* (to-env base-scheme)
                                                               v fail)]
                                     (when (and fail (t/product-type? typ))
                                       (fail ": non-product type " typ))
                                     [k typ]))
                                 m))))
      (restrict? q) (let [scheme (next-step (restrict-query q))]
                      (when (and fail
                                 (not= t/boolean% (expression-type*
                                                   (to-env scheme)
                                                   (restrict-exp q) fail)))
                        (fail t/boolean% (restrict-exp q)))
                      scheme)

      (restrict-outer? q) (let [scheme (next-step (restrict-outer-query q))]
                            (when (and fail
                                       (not= t/boolean% (expression-type*
                                                         (to-env scheme)
                                                         (restrict-outer-exp q) fail)))
                              (fail t/boolean% (restrict-outer-exp q)))
                            scheme)
      
      (combine? q) (case (combine-rel-op q)
                     :product (let [a1 (rel-scheme-alist (next-step (combine-query-1 q)))
                                    a2 (rel-scheme-alist (next-step (combine-query-2 q)))]
                                (when fail
                                  (for [[k _] a1]
                                    (when (assoc k a2)
                                      (fail (list 'not a1) a2))))
                                (make-rel-scheme (merge a1 a2)))

                     :left-outer-product
                     (let [a1 (rel-scheme-alist (next-step (combine-query-1 q)))
                           a2 (rel-scheme-alist (rel-scheme-nullable (next-step (combine-query-2 q))))]
                       (when fail
                         (for [[k _] a1]
                           (when (assoc k a2)
                             (fail (list 'not a1) a2))))
                       (make-rel-scheme (merge a1 a2)))

                     :quotient (let [s1 (next-step (combine-query-1 q))
                                     s2 (next-step (combine-query-2 q))]
                                 (when fail

                                   (let [a1 (rel-scheme-alist s1)
                                         a2 (rel-scheme-alist s2)]

                                     (for [[k v] a2]
                                       (when-let [p2 (get v a1)]
                                         (when-not (t/type=? v p2)
                                           (fail v p2))))))
                                 (rel-scheme-difference s1 s2))
                     (:union :intersection :difference)
                     (let [s1 (next-step (combine-query-1 q))]
                       (when (and fail
                                  (not (rel-scheme=? s1
                                                     (next-step (combine-query-2 q)))))
                         (fail s1 q))
                       s1))
      (grouping-project? q) (let [base-scheme (next-step (grouping-project-query q))]
                              (make-rel-scheme
                               (into {}
                                     (map (fn [[k v]]
                                            (let [typ (expression-type* (to-env base-scheme) v fail)]
                                              (when (and fail (t/product-type? typ))
                                                (fail ": non-product type " typ))
                                              [k typ]))
                                          (grouping-project-alist q)))))
      (order? q) (let [scheme (next-step (order-query q))
                       env (to-env scheme)]
                   (when fail
                     (for [p (order-alist q)]
                       (let [exp (first p)
                             t (expression-type* env exp fail)]
                         (when-not (t/ordered-type? t)
                           (fail ": not an ordered type " t)))))
                   scheme)
      (top? q) (next-step (top-query q))
      :else (assertion-violation 'query-scheme "unknown query" q))))

(defn query-scheme
  "Return the query scheme of query `q` as a `rel-scheme`.
  If :typecheck is provided, perform basic validation of types."
  [q & {:keys [typecheck?]}]
  (query-scheme* q the-empty-environment
                 (and typecheck?
                      (fn [expected thing]
                        (assertion-violation 'query-scheme "type violation"
                                             expected thing)))))

(defn query?
  "Returns true if the `obj` is a query."
  [obj]
  (or (empty? obj) (base-relation? obj) (project? obj) (restrict? obj) (restrict-outer? obj)
      (combine? obj) (grouping-project? obj) (order? obj) (top? obj)))

(declare query->datum)

(defn expression->datum
  "Takes an expression and returns a data representation of it.
  Example:
  * `(expression->datum (make-attribute-ref \"foo\")) => (attribute-ref \"foo\")'"
  [e]
  (fold-expression
   (fn [name] (list 'attribute-ref name))
   (fn [ty val] (list 'const (t/type->datum ty) (t/const->datum ty val)))
   (fn [ty] (list 'null-type (t/type->datum ty)))
   (fn [rator rands] (list 'application (rator-name rator) rands))
   (fn [exprs] (cons 'tuple exprs))
   (fn [op expr] (list 'aggregation op expr))
   (fn [op] (list 'aggregation* op))
   (fn [alist default] (list 'case-expr alist default))
   (fn [subquery] (list 'scalar-subquery (query->datum subquery)))
   (fn [subquery] (list 'set-subquery-query (query->datum subquery)))
   e))

(defn query->datum
  [q]
  (cond
    (empty? q) (list 'empty-val)
    (base-relation? q) (list 'base-relation (base-relation-name q))
    (project? q) (list 'project (map (fn [[k v]]
                                       (cons k (expression->datum v)))
                                     (project-alist q))
                       (query->datum (project-query q)))
    (restrict? q) (list 'restrict (expression->datum (restrict-exp q))
                        (query->datum (restrict-query q)))
    (restrict-outer? q) (list 'restrict-outer (expression->datum (restrict-outer-exp q))
                              (query->datum (restrict-outer-query q)))
    (combine? q) (list (combine-rel-op q)
                       (query->datum (combine-query-1 q))
                       (query->datum (combine-query-2 q)))
    (grouping-project? q) (list 'grouping-project
                                (map (fn [[k v]]
                                       (cons k (expression->datum v)))
                                     (grouping-project-alist q))
                                (query->datum (grouping-project-query q)))
    (order? q) (list 'order (map (fn [[k v]]
                                   (list (expression->datum k) v))
                                    (order-alist q))
                     (query->datum (order-query q)))
    (top? q) (list 'top (top-count q) (query->datum (top-query q)))
    :else (assertion-violation 'query->datum "unknown query" q)))

(declare datum->expression)

(defn datum->query
  [d universe]
  (letfn [(next-step [d*] (datum->query d* universe))]
    (case (first d)
      empty-val the-empty
      base-relation (or (u/universe-lookup-base-relation universe (second d))
                        (assertion-violation 'datum->query
                                             "unknown base relation"
                                             (second d)))
      project (make-project (into {}
                                  (map (fn [p]
                                         [(first p)
                                          (datum->expression (rest p) universe)])
                                       (second d)))
                            (next-step (third d)))
      restrict (make-restrict (datum->expression (second d) universe)
                              (next-step (third d)))
      restrict-outer (make-restrict-outer (datum->expression (second d) universe)
                                          (next-step (third d)))
      (:product :left-outer-product :union :intersection :quotient :difference)
      (make-combine (first d) (next-step (second d))
                    (next-step (third d)))
      grouping-project (make-grouping-project
                        (into {}
                              (map (fn [p]
                                     [(first p)
                                      (datum->expression (rest p) universe)])
                                   (second d)))
                        (next-step (third d)))
      order (make-order (into {}
                              (map (fn [p]
                                     [(datum->expression (first p) universe) (second p)])
                                   (second d)))
                        (next-step (third d)))
      top (make-top (second d) (next-step (third d)))
      :else (assertion-violation 'datum->query "invalid datum" d))))

(defn datum->expression
  "Takes a datum and returns the corresponding expression. This is the inverse
  function of expression->datum, so
  `(= d (datum->expression (expression->datum d) u))`
  should hold."
  [d universe]
  (letfn [(next-step [d*]
            (datum->expression d* universe))]
    (case (first d)
      attribute-ref (make-attribute-ref (second d))
      const (let [ty (t/datum->type (second d) universe)]
                  (make-const ty (t/datum->const ty (third d))))
      null-type (make-null (t/datum->type (second d) universe))
      application (apply make-application
                         (or (u/universe-lookup-rator universe (second d))
                             (assertion-violation 'datum->expression
                                                  "unknown rator"
                                                  (second d)))
                         (map next-step (third d)))
      tuple (make-tuple (map next-step (rest d)))
      aggregation (make-aggregation (second d)
                                    (next-step (third d)))
      aggregation* (make-aggregation (second d))
      case-expr (make-case-expr (into {}
                                          (map (fn [[k v]] [(next-step k)
                                                            (next-step v)])
                                               (second d)))
                                (next-step (third d)))
      scalar-subquery (make-scalar-subquery (datum->query (second d) universe))
      set-subquery (make-set-subquery (datum->query (second d) universe))
      (assertion-violation 'datum->expression "invalid datum" d))))

(defn make-monomorphic-rator
  [name domain-types range-type proc & {:keys [universe data]}]
  (make-rator name
              (fn [fail & arg-types]
                (when fail
                  (do
                    (when-not (= (count domain-types) (count arg-types))
                      (fail domain-types arg-types))
                    (for [dd domain-types ad arg-types]
                      (when-not (t/type=? dd ad)
                        (fail dd ad)))))
                range-type)
              proc
              :universe universe
              :data data))

(defn null-lift-binary-predicate [pred]
  (fn [v1 v2]
    (when-not (or (empty? v1) (empty? v2))
      (pred v1 v2))))

(defn make-monomorphic-combinator
  [name domains range proc & {:keys [universe data]}]
  (let [rator (make-monomorphic-rator name domains range proc
                                      :universe universe :data data)]
    (fn [& exprs]
      (apply make-application rator exprs))))

(declare query-attribute-names)

(defn- filter-and-non-nil [s]
  (let [non-nils (filter some? s)]
    (when-not (empty? non-nils) (set non-nils))))

(defn expression-attribute-names
  "Takes an expression and returns a seq all attribute-ref's names."
  [expr]
  (filter-and-non-nil
   (fold-expression
    list
    (constantly nil)
    (constantly nil)
    (fn [rator rands] (flatten (map vec rands)))
    (fn [exprs] (flatten (map vec exprs))) ;; tuple
    (fn [_ expr] expr)
    (fn [_] "*")
    (fn [alist default]
      (vec (concat default
                   (distinct (flatten (into [] alist))))))  ;; case
    query-attribute-names
    query-attribute-names
    expr)))

(defn query-attribute-names
  "Takes a query and returns a set of all attribute-ref's names."
  [q]
  (cond
    (empty-val? q) nil
    (base-relation? q) nil
    (project? q)
    (let [subq (project-query q)
          alist (project-alist q)]
      (apply union
             (set (keys (rel-scheme-alist (query-scheme subq))))
             (query-attribute-names subq)
             (map expression-attribute-names (vals alist))))
    (restrict? q)
    (let [sub (restrict-query q)]
      (union
       (set (keys (rel-scheme-alist (query-scheme sub))))
       (query-attribute-names sub)
       (expression-attribute-names (restrict-exp q))))
    (combine? q) (union
                  (query-attribute-names (combine-query-1 q))
                  (query-attribute-names (combine-query-2 q)))
    (restrict-outer? q)
    (let [sub (restrict-outer-query q)]
      (apply union
             (set (keys (rel-scheme-alist (query-scheme sub))))
             (query-attribute-names sub)
             (expression-attribute-names (restrict-outer-exp q))))
    (grouping-project? q)
    (let [subq (grouping-project-query q)
          alist (grouping-project-alist q)]
      (apply union
             (set (map first (rel-scheme-alist (query-scheme subq))))
             (query-attribute-names subq)
             (map expression-attribute-names (vals alist))))
    (order? q)
    (let [subq (order-query q)
          alist (order-alist q)]
      (apply union
             (set (map first (rel-scheme-alist (query-scheme subq))))
             (query-attribute-names subq)
             (map expression-attribute-names (keys alist))))
    (top? q) (query-attribute-names (top-query q))
    :else (assertion-violation 'query-attribute-names "unknown query" q)))

(declare query-substitute-attribute-refs)

(defn substitute-attribute-refs [alist expr]
  (fold-expression
   (fn [name] (if-let [r (get alist name)]
                r
                (make-attribute-ref name)))
   make-const
   make-null
   (fn [rator rands] (apply make-application rator rands))
   make-tuple
   make-aggregation
   make-aggregation
   make-case-expr
   (fn [subquery] (make-scalar-subquery (query-substitute-attribute-refs alist subquery)))
   (fn [subquery] (make-set-subquery (query-substitute-attribute-refs alist subquery)))
   expr))

(defn cull-substitution-alist
  "Takes an map and a 'underlying' map of substitutions and returns a map of
  substitutions not already featured in `underlying`."
  [alist underlying]
  (let [underlying-alist (rel-scheme-alist (query-scheme underlying))]
    (into {}
          (filter (fn [[k v]]
                    (not (contains? underlying-alist k)))
                  alist))))

(defn query-substitute-attribute-refs
  [alist q]
  (letfn [(next-step [qq] (query-substitute-attribute-refs alist qq))]
    (cond
      (empty-val? q) q
      (base-relation? q) q
      (project? q) (let [sub (project-query q)
                         culled (cull-substitution-alist alist sub)]
                     (make-project
                      (into {}
                            (map (fn [[k v]] [k (substitute-attribute-refs culled v)])
                                 (project-alist q)))
                      (next-step sub)))
      (restrict? q) (let [sub (restrict-query q)
                          culled (cull-substitution-alist alist sub)]
                      (make-restrict (substitute-attribute-refs culled (restrict-exp q))
                                     (next-step sub)))
      (restrict-outer? q) (let [sub (restrict-outer-query q)
                                culled (cull-substitution-alist alist sub)]
                            (make-restrict-outer (substitute-attribute-refs culled (restrict-outer-exp q))
                                                 (next-step sub)))
      
      (combine? q) (make-combine (combine-rel-op q)
                                 (next-step (combine-query-1 q))
                                 (next-step (combine-query-2 q)))
      (grouping-project? q) (let [sub (grouping-project-query q)
                                  culled (cull-substitution-alist alist sub)]
                              (make-grouping-project
                               (into {} (map (fn [[k v]] [k (substitute-attribute-refs culled v)])
                                             (grouping-project-alist q)))
                               (next-step sub)))
      (order? q) (let [sub (order-query q)
                       culled (cull-substitution-alist alist sub)]
                   (make-order (into {} (map (fn [[k v]]
                                               [(substitute-attribute-refs culled k) v])
                                             (order-alist q)))
                               (next-step sub)))
      (top? q) (make-top (top-count q) (next-step (top-query q)))
      :else (assertion-violation 'query-substitute-attribute-refs "unknown query" q))))

(defn count-aggregations
  "Count all aggregations in an expression."
  [e]
  (fold-expression
   (fn [_] 0)
   (fn [_ _] 0)
   (fn [_] 0)
   (fn [_ rands] (apply + rands))
   (fn [exprs] (apply + exprs))
   (fn [_ expr] (inc expr))
   (fn [_] 1)
   (fn [alist default] (apply + default (map (fn [[k v]]
                                               (+ k v))
                                             alist)))
   (fn [_] 0)
   (fn [_] 0)
   e))
