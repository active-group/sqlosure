(ns ^{:author "Marco Schneider"}
  sqlosure.relational-algebra
  "Implementation of relational algebra based on Mike Sperbers relational-algebra.scm."
  (:require [active.clojure.condition :as c :refer [assertion-violation]]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer [define-record-type]]
            [clojure.set :as set :refer [difference union]]
            [sqlosure.type :as t]
            [sqlosure.universe :as u]
            [sqlosure.utils :refer [fourth third]]))

(define-record-type rel-scheme
  (^:private really-make-rel-scheme columns map grouped) rel-scheme?
  [^{:doc "Ordered sequence of the columns."}
   columns rel-scheme-columns
   ^{:doc "Map of labels to types."}
   map rel-scheme-map
   ^{:doc "`nil` or set of grouped column labels"}
   grouped rel-scheme-grouped])

(defn make-rel-scheme [columns map grouped]
  (c/assert (not (set? columns)))
  (c/assert (map? map) map)
  (c/assert (or (nil? grouped) (set? grouped)) grouped)
  (really-make-rel-scheme columns map grouped))

(defn rel-scheme-types
  "Returns the types of a rel-scheme, in the order they were created."
  [rs]
  (let [mp (rel-scheme-map rs)]
    (map #(get mp %)
         (rel-scheme-columns rs))))

(defn alist->rel-scheme
  "Construct a relational scheme from an alist of `[column-name type]`."
  [alist]
  (let [cols (map first alist)]
    (c/assert (count (set cols)) (count cols))
    (make-rel-scheme cols (into {} alist) nil)))

(def the-empty-rel-scheme (alist->rel-scheme []))
(def the-empty-environment {})

(defn rel-scheme=?
  "Returns true if t1 and t2 are the same."
  [t1 t2]
  (= t1 t2))

(defn rel-scheme-unary=?
  "Does rel scheme have only 1 column?"
  [rs]
  (= 1 (count (rel-scheme-columns rs))))

(defn rel-scheme-concat
  "Takes two rel-schemes and concatenates them. This means:
  - concatenate columns
  - merge alists
  - union grouped-sets"
  [s1 s2]
  (cond
    (nil? s1) s2
    (nil? s2) s1
    :else
    (make-rel-scheme (concat (rel-scheme-columns s1)
                             (rel-scheme-columns s2))
                     (merge (rel-scheme-map s1)
                            (rel-scheme-map s2))
                     (cond
                       (nil? (rel-scheme-grouped s1))
                       (rel-scheme-grouped s2)

                       (nil? (rel-scheme-grouped s2))
                       (rel-scheme-grouped s1)

                       :else (union (rel-scheme-grouped s1)
                                    (rel-scheme-grouped s2))))))

(defn rel-scheme-difference
  "Return a new rel-scheme resulting of the (set-)difference of s1's and s2's
  alist."
  [s1 s2]
  (let [cols2 (set (rel-scheme-columns s2))
        cols (remove cols2 (rel-scheme-columns s1))]
    (c/assert (not-empty cols))
    (make-rel-scheme cols
                     (select-keys (rel-scheme-map s1) cols)
                     (difference (rel-scheme-grouped s1) cols2))))


(defn rel-scheme-unary?
  "Returns true if the rel-scheme's alist consist of only one pair."
  [scheme]
  (= 1 (count (rel-scheme-columns scheme))))

(defn rel-scheme-nullable
  "Makes all columns in a scheme nullable."
  [scheme]
  (make-rel-scheme (rel-scheme-columns scheme)
                   (into {}
                         (map (fn [[name type]]
                                [name (t/make-nullable-type type)])
                              (rel-scheme-map scheme)))
                   (rel-scheme-grouped scheme)))

(defn rel-scheme->environment
  "Returns the relation table of a rel-scheme."
  [s]
  (rel-scheme-map s))

(define-record-type ^{:doc "Cache for the relational scheme of a query."}
  RelSchemeCache
  (really-make-rel-scheme-cache fun map-atom)
  rel-scheme-cache?
  [^{:doc "Function accepting an environment, producing a rel scheme."}
   fun rel-scheme-cache-fun

   ^{:doc "Atom containing map `environment |-> scheme`"}
   map-atom rel-scheme-cache-map-atom])

(defn make-rel-scheme-cache
  "Make a rel-scheme cache.

  - `fun` is a function accepting an environment, producing the scheme"
  [fun]
  (really-make-rel-scheme-cache fun (atom {})))

(defn rel-scheme-cache-scheme
  "Compute a relational scheme from a cache, using the cached version if possible."
  [cache env]
  (let [atom (rel-scheme-cache-map-atom cache)]
    (or (get @atom env)
        (let [scheme ((rel-scheme-cache-fun cache) env)]
          (swap! atom assoc env scheme)
          scheme))))

(defn attach-rel-scheme-cache
  "Attach a rel-scheme cache to query.

  - `query` is the query
  - `fun` is a function accepting an environment, yielding the scheme of the query"
  [query fun]
  (with-meta query {::rel-scheme-cache (make-rel-scheme-cache fun)}))

(defn query-scheme
  "Return the query scheme of query `q` as a `rel-scheme`."
  ([q]
   (query-scheme q the-empty-environment))
  ([q env]
   (rel-scheme-cache-scheme (get (meta q) ::rel-scheme-cache) env)))

(defn compose-environments
  "Combine two environments. e1 takes precedence over e2."
  [e1 e2]
  (merge e2 e1))

(defn lookup-env
  "Lookup a name in an environment."
  [name env]
  (get env name))

;;; ----------------------------------------------------------------------------
;;; --- Primitive relations, depending on the domain universe
;;; ----------------------------------------------------------------------------

(define-record-type base-relation
  ^{:doc "Primitive relations, depending on the domain universe."}
  (really-make-base-relation name scheme handle) base-relation?
  [name base-relation-name
   scheme base-relation-scheme
   handle base-relation-handle  ;; Domain specific handle.
   ])

(defn make-base-relation
  "Returns a new base relation.
  If :handle is supplied, use is as base-relation-handle, defaults to nil.
  If :universe is supplied, return a vector of [relation universe]"
  [name scheme & {:keys [universe handle]
                  :or {universe nil
                       handle nil}}]
  (let [rel (attach-rel-scheme-cache
             (really-make-base-relation name scheme handle)
             (fn [_] scheme))]
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
   range-type-proc rator-range-type-proc  ;; Gets applied to fail, arg types, yields range type.
   proc rator-proc  ;; Procedure with a Clojure implementation of the operator.
   data rator-data  ;; Domain-specific data, for outside use.
   ])

(defn make-rator
  [name range-type-proc proc & {:keys [universe data]}]
  (let [r (really-make-rator name range-type-proc proc data)]
    (when universe
      (u/register-rator! universe name r))
    r))

;;; ----------------------------------------------------------------------------
;;; --- QUERIES
;;; ----------------------------------------------------------------------------

(define-record-type empty-query
  ^{:doc "Represents an empty relational algebra value."}
  (make-empty-query) empty-query? [])

(def the-empty
  (attach-rel-scheme-cache
   (make-empty-query)
   (fn [_] the-empty-rel-scheme)))

(define-record-type project
  ^{:doc "List of pairs."}
  (really-really-make-project alist query)
  project?
  [^{:doc "Maps newly bound attribute names to expressions."}
   alist project-alist
   query project-query])

;; FIXME: temporary default for the query-scheme fail function
(defn query-scheme-fail
  [expected thing]
  (assertion-violation `query-scheme "type violation"
                       expected thing))

(declare aggregate? check-grouped expression-type)

(defn really-make-project
  [alist query]
  (let [base-scheme (query-scheme query)
        grouped (rel-scheme-grouped base-scheme)]
    
    (when (or (some (comp aggregate? second) alist)
              (set? grouped))
      ;; we're doing aggregation
      (doseq [[_ e] alist]
        (check-grouped grouped e)))

    (attach-rel-scheme-cache
     (really-really-make-project alist query)
     (fn [env]
       (alist->rel-scheme (map (fn [[k v]]
                                 (let [typ (expression-type (compose-environments (rel-scheme->environment base-scheme) env)
                                                             v)]
                                   (when (t/product-type? typ)
                                     (assertion-violation `really-make-project "non-product type" k v typ))
                                   [k typ]))
                               alist))))))

(defn make-project
  [alist query]
  (let [alist (if (map? alist)
                (vec alist)
                alist)]
    ;; If the alist is empty and the underlying query is a projection, push down
    ;; the empty alist to avoid unnecessary projections.
    (if (empty? alist)
      (loop [query query]
        (if (project? query)
          (recur (project-query query))
          (really-make-project alist query)))
      (really-make-project alist query))))

(declare project-alist-aggregate?)

(defn make-extend
  "Creates a projection of some attributes while keeping all other attributes in
  the relation visible too."
  [alist query]
  (let [scheme (query-scheme query)]
    (make-project
     (concat alist
             (map (fn [k]
                    [k (make-attribute-ref k)])
                  (if-let [grouped (rel-scheme-grouped scheme)]
                    (filter (fn [col]
                              (contains? grouped col))
                            (rel-scheme-columns scheme))
                    (if (project-alist-aggregate? alist)
                      []
                      (rel-scheme-columns scheme)))))
     query)))

(define-record-type restrict
  (really-make-restrict exp query) restrict?
  [exp restrict-exp  ;; :expression[boolean%]
   query restrict-query])

(defn make-restrict
  "Create a restriction:

  - `exp` is a boolean expression, acting as a filter
  - `query` is the underlying query"
  [exp query]
  (attach-rel-scheme-cache
    (really-make-restrict exp query)
    (fn [env]
      (let [scheme (query-scheme query env)]
        (when (not= t/boolean% (expression-type (compose-environments (rel-scheme->environment scheme) env)
                                                 exp))
          (assertion-violation `make-restrict "not a boolean condition" exp query env))
        scheme))))

(define-record-type ^{:doc "Restrict a left outer product.
  This will restrict all the right-hand sides of left outer products.
  If it doesn't hold, these right-hand sides will have all-null
  columns."}
  restrict-outer
  (really-make-restrict-outer exp query) restrict-outer?
  [exp restrict-outer-exp
   query restrict-outer-query])

(defn make-restrict-outer
  "Restrict a right-hand side of a left-outer product."
  [exp query]
  (attach-rel-scheme-cache
   (really-make-restrict-outer exp query)
   (fn [env]
     (let [scheme (query-scheme query env)]
       (when (not= t/boolean% (expression-type
                               (compose-environments (rel-scheme->environment scheme) env)
                               exp))
            (assertion-violation `make-restrict-outer "not a boolean condition" exp query env))
       scheme))))

(define-record-type combine
  (really-make-combine rel-op query-1 query-2) combine?
  [rel-op combine-rel-op  ;; Relational algebra. See below.
   query-1 combine-query-1
   query-2 combine-query-2])

(def ^{:private true} relational-ops
  #{:product :left-outer-product :union :intersection :quotient :difference})

(defn relational-op? [k] (contains? relational-ops k))

(defn make-combine [rel-op query-1 query-2]
  (when-not (relational-op? rel-op)
    (assertion-violation `make-combine "not a relational operator" rel-op))

  (attach-rel-scheme-cache
   (case rel-op
     :product (cond
                (empty-query? query-1) query-2
                (empty-query? query-2) query-1
                :else (really-make-combine rel-op query-1 query-2))
     (really-make-combine rel-op query-1 query-2))
   (fn [env]
     (case rel-op
       :product (let [r1 (query-scheme query-1 env)
                      r2 (query-scheme query-2 env)
                      a1 (rel-scheme-map r1)
                      a2 (rel-scheme-map r2)]
                  (doseq [[k _] a1]
                    (when (contains? a2 k)
                      (assertion-violation `make-combine "duplicate column name" rel-op query-1 query-2)))
                  (rel-scheme-concat r1 r2))
       
       :left-outer-product
       (let [r1 (query-scheme query-1 env)
             r2 (rel-scheme-nullable (query-scheme query-2 env))
             a1 (rel-scheme-map r1)
             a2 (rel-scheme-map r2)]
         (doseq [[k _] a1]
           (when (contains? a2 k)
             (assertion-violation `make-combine "duplicate column name" rel-op query-1 query-2)))
         (rel-scheme-concat r1 r2))

       :quotient (let [s1 (query-scheme query-1 env)
                       s2 (query-scheme query-2 env)
                       a1 (rel-scheme-map s1)
                       a2 (rel-scheme-map s2)]

                   (doseq [[k v] a2]
                     (when-let [p2 (get a1 k)]
                       (when-not (= v p2)
                         (assertion-violation `make-combine "types don't match" rel-op k v p2 query-1 query-2))))
                   (rel-scheme-difference s1 s2))

       (:union :intersection :difference)
       (let [s1 (query-scheme query-1 env)]
         (when-not (rel-scheme=? s1
                                 (query-scheme query-2 env))
           (assertion-violation `make-combine "scheme mismatch" rel-op s1 query-2))
         s1)))))

(defn make-left-outer-product [query-1 query-2]
  (make-combine :left-outer-product query-1 query-2))

(defn make-product [query-1 query-2] (make-combine :product query-1 query-2))

(defn make-union [query-1 query-2] (make-combine :union query-1 query-2))

(defn make-intersection [query-1 query-2] (make-combine :intersection query-1
  query-2))

(defn make-quotient [query-1 query-2] (make-combine :quotient query-1 query-2))

(defn make-difference [query-1 query-2] (make-combine :difference query-1
  query-2))

(def ^{:private true} order-op #{:ascending :descending})

(defn order-op? [k]
  (contains? order-op k))

(define-record-type order
  (really-make-order alist query) order?
  [alist order-alist  ;; list of pairs ((make-attribute-ref "foo") direction),
                      ;; where direction = :ascending | :descending
   query order-query])

(defn make-order
  [alist query]
  (attach-rel-scheme-cache
   (really-make-order alist query)
   (fn [env]
     (let [scheme (query-scheme query env)
           env (compose-environments (rel-scheme->environment scheme) env)]
       (doseq [p alist]
         (let [exp (first p)
               t (expression-type env exp)]
           ;; FIXME: works only for base types
           (when-not (t/ordered-type? t)
             (assertion-violation `make-order "not an ordered type " t exp))))
       scheme))))

;; Top n entries.
(define-record-type top
  ^{:doc "The top `count` entries, optionally starting at `offset`, defaulting to 0."}
  (really-make-top offset count query) top?
  [offset top-offset
   count top-count
   query top-query])

(defn make-top
  "The top `count` entries, optionally starting at `offset`, defaulting to 0."
  [offset count query]
  (attach-rel-scheme-cache
   (really-make-top offset count query)
   (fn [env]
     (query-scheme query env))))

(define-record-type group
  (really-make-group columns query)
  group?
  [^{:doc "set of columns to group by"}
   columns group-columns
   ^{:doc "underlying query"}
   query group-query])

(defn make-group
  "Make a grouped query from a basic query.

  - `columns` is a seq of columns to be grouped by
  - `query` is the underlying query"
  [columns query]
  (let [columns (set columns)]
    (attach-rel-scheme-cache
     (really-make-group columns query)
     (fn [env]
       (lens/overhaul (query-scheme query env)
                      rel-scheme-grouped
                      union columns)))))

(define-record-type DistinctQ
  (really-make-distinct-q query) distinct-q?
  [query distinct-q-query])

(defn make-distinct
  [query]
  (attach-rel-scheme-cache
   (really-make-distinct-q query)
   (fn [env]
     (query-scheme query env))))

;; END QUERIES

(define-record-type tuple
  (make-tuple expressions) tuple?
  [expressions tuple-expressions])

; FIXME should be separate for aggregation and aggregation-all
(def ^{:private true} aggregations-op
  #{:count :count-all :sum :avg :min :max :std-dev :std-dev-p :var :var-p})

(defn aggregations-op?
  "Is a thing an aggregation operation?"
  [k]
  (contains? aggregations-op k))

(defn ensure-aggregations-op!
  "Takes a `k` and runs function `f` if `k` is an [[aggregations-op?]].
  Otherwise, throws an assertion violation."
  [k f]
  (if-not (aggregations-op? k)
    (c/assertion-violation `ensure-aggregations-op! "now an aggregation operation" k)
    (f)))

(define-record-type aggregation
  (really-make-aggregation op expr) aggregation?
  [op aggregation-operator  ;; Aggregation-op or string.
   expr aggregation-expr])

(define-record-type aggregation-all
  (really-make-aggregation-all op) aggregation-all?
  [op aggregation-all-operator])

(defn make-aggregation
  "Returns an aggregation. If supplied with only the `op`, returns aggregation
  that operates on the whole query. If supplied with an expression, returns
  aggregation that operates on the result of this expression only."
  ([op]
   (ensure-aggregations-op! op #(really-make-aggregation-all op)))
  ([op expr]
   (ensure-aggregations-op! op #(really-make-aggregation op expr))))

(define-record-type case-expr
  (make-case-expr alist default) case-expr?
  [alist case-expr-alist  ;; (list (pair expression[boolean] expression)).
   default case-expr-default])

(define-record-type scalar-subquery
  (make-scalar-subquery query) scalar-subquery?
  [query scalar-subquery-query])

(define-record-type set-subquery
  (make-set-subquery query) set-subquery?
  [query set-subquery-query])

(defn fold-expression
  [on-attribute-ref on-const on-null on-application on-tuple on-aggregation on-aggregation-all
   on-case on-scalar-subquery on-set-subquery expr]
  (let [next-step #(fold-expression on-attribute-ref on-const on-null on-application
                               on-tuple on-aggregation on-aggregation-all on-case
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
      (aggregation-all? expr) (on-aggregation-all (aggregation-all-operator expr))
      (case-expr? expr) (on-case (into {} (map (fn [[k v]] [(next-step k)
                                                            (next-step v)])
                                               (case-expr-alist expr)))
                                 (next-step (case-expr-default expr)))
      (scalar-subquery? expr) (on-scalar-subquery (scalar-subquery-query expr))
      (set-subquery? expr) (on-set-subquery (set-subquery-query expr))
      :else (assertion-violation `fold-expression "invalid expression" expr))))

(defn expression-type
  "`expression-type` takes an environment map and an expression and tries to
  find the expressions type (either based on expr itself or on the
  mappings of the env)."
  [env expr]
  (fold-expression
   (fn [name] (or (lookup-env name env)
                  (assertion-violation `expression-type "unknown attribute" name env)))
   (fn [ty val] ty)
   identity
   (fn [rator rands] (apply (rator-range-type-proc rator) query-scheme-fail rands))
   t/make-product-type
   ;; aggregation
   (fn [op t] (if (= :count op)
                t/integer%
                (do
                  (cond
                    (contains? #{:sum :avg :std-dev :std-dev-p :var :var-p} op)
                    (when-not (t/numeric-type? t) (assertion-violation `expression-type "not a numeric type" op t))
                    
                    (contains? #{:min :max} op)
                    (when-not (t/ordered-type? t) (assertion-violation `expression-type "not an ordered type" op t)))
                  t)))
   ;; aggregation-all
   (fn [op] (if (= :count-all op)
              t/integer%
              (assertion-violation `expression-type "unknown aggregation" op)))
   ;; case-expr
   (fn [alist t]
     (doseq [[p r] alist]
       (when-not (= t/boolean% p) (assertion-violation `expression-type "non-boolean test in case" p r))
       (when-not (= t r) (assertion-violation `expression-type "type mismatch in case" p r)))
     t)
   (fn [subquery] (let [scheme (query-scheme subquery env)]
                    (when-not (rel-scheme-unary? scheme)
                      (assertion-violation `expression-type "must be a unary relation" subquery))
                    (get (rel-scheme-map scheme)
                         (first (rel-scheme-columns scheme)))))
   ;; FIXME what should the result here really be?
   (fn [subquery] (let [scheme (query-scheme subquery env)]
                    (when-not (rel-scheme-unary? scheme)
                      (assertion-violation `expression-type "must be a unary relation" subquery))
                    (t/make-set-type (get (rel-scheme-map scheme)
                                          (first (rel-scheme-columns scheme))))))
   expr))

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
    (aggregation-all? expr) true
    (case-expr? expr) (or (some (fn [[k v]] (or (aggregate? k)
                                                (aggregate? v)))
                                (case-expr-alist expr))
                          (aggregate? (case-expr-default expr)))
    (scalar-subquery? expr) false
    (set-subquery? expr) false
    :else (assertion-violation `aggregate? "invalid expression" expr)))

(defn project-alist-aggregate?
  "Test whether a project alist contains aggregate right-hand sides."
  [alist]
  (boolean
   (some (fn [[col expr]] (aggregate? expr))
         alist)))

(defn project-aggregate?
  "Test whether a project contains aggregate right-hand sides."
  [pr]
  (project-alist-aggregate? (project-alist pr)))

(defn- check-grouped
  "Check whether all attribute refs in an expression 
  that are not inside an application of an aggregate occur in `grouped`."
  [grouped expr]
  (cond
    (attribute-ref? expr) (when-not (contains? grouped (attribute-ref-name expr))
                            (assertion-violation `check-grouped "non-aggregate expression" expr))
    (const? expr) nil
    (const-null? expr) nil
    (application? expr) (doseq [r (application-rands expr)]
                          (check-grouped grouped r))
    (tuple? expr) (doseq [e (tuple-expressions expr)]
                    (check-grouped grouped e))
    (aggregation? expr) nil
    (aggregation-all? expr) nil
    (case-expr? expr) (do
                        (doseq [[k v] (case-expr-alist expr)]
                          (check-grouped grouped k)
                          (check-grouped grouped v))
                        (check-grouped grouped (case-expr-default expr)))
    (scalar-subquery? expr) nil
    (set-subquery? expr) nil
    :else (assertion-violation `check-grouped "invalid expression" expr)))

(defn query?
  "Returns true if the `obj` is a query."
  [obj]
  (or (empty-query? obj) (base-relation? obj) (project? obj) (restrict? obj) (restrict-outer? obj)
      (combine? obj) (order? obj) (group? obj) (top? obj) (distinct-q? obj)))

#_(declare query->datum)

#_(defn expression->datum
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
   (fn [op] (list 'aggregation-all op))
   (fn [alist default] (list 'case-expr alist default))
   (fn [subquery] (list 'scalar-subquery (query->datum subquery)))
   (fn [subquery] (list 'set-subquery-query (query->datum subquery)))
   e))

#_(defn query->datum
  [q]
  (cond
    (empty-query? q) (list 'empty-query)
    (base-relation? q) (list 'base-relation (symbol (base-relation-name q)))
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
    (order? q) (list 'order (map (fn [[k v]]
                                   (list (expression->datum k) v))
                                    (order-alist q))
                     (query->datum (order-query q)))
    (group? q) (list 'group
                     (group-columns q)
                     (query->datum (group-query q)))
    (top? q) (list 'top (top-offset q) (top-count q) (query->datum (top-query q)))
    (distinct-q? q) (list 'distinct (query->datum (distinct-q-query q)))
    :else (assertion-violation `query->datum "unknown query" q)))

#_(declare datum->expression)

#_(defn datum->query
  [d universe]
  (letfn [(next-step [d*] (datum->query d* universe))]
    (case (first d)
      empty-query the-empty
      base-relation (or (u/universe-lookup-base-relation universe (second d))
                        (assertion-violation `datum->query
                                             "unknown base relation"
                                             (second d)))
      project (make-project (map (fn [p]
                                   [(first p)
                                    (datum->expression (rest p) universe)])
                                 (second d))
                            (next-step (third d)))
      restrict (make-restrict (datum->expression (second d) universe)
                              (next-step (third d)))
      restrict-outer (make-restrict-outer (datum->expression (second d) universe)
                                          (next-step (third d)))
      (:product :left-outer-product :union :intersection :quotient :difference)
      (make-combine (first d) (next-step (second d))
                    (next-step (third d)))
      order (make-order (map (fn [p]
                               [(datum->expression (first p) universe) (second p)])
                             (second d))
                        (next-step (third d)))
      top (make-top (second d) (third d) (next-step (fourth d)))
      (assertion-violation `datum->query "invalid datum" d))))

#_(defn datum->expression
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
                             (assertion-violation `datum->expression
                                                  "unknown rator"
                                                  (second d)))
                         (map next-step (third d)))
      tuple (make-tuple (map next-step (rest d)))
      aggregation (make-aggregation (second d)
                                    (next-step (third d)))
      aggregation-all (make-aggregation (second d))
      case-expr (make-case-expr (into {}
                                      (map (fn [[k v]] [(next-step k)
                                                        (next-step v)])
                                           (second d)))
                                (next-step (third d)))
      scalar-subquery (make-scalar-subquery (datum->query (second d) universe))
      set-subquery (make-set-subquery (datum->query (second d) universe))
      (assertion-violation `datum->expression "invalid datum" d))))

(defn make-monomorphic-rator
  [name domain-types range-type proc & {:keys [universe data]}]
  (make-rator name
              (fn [fail & arg-types]
                (when fail
                  (when-not (= (count domain-types) (count arg-types))
                    (fail domain-types arg-types))
                  (doseq [dd domain-types ad arg-types]
                    (when-not (= dd ad)
                      (fail dd ad))))
                range-type)
              proc
              :universe universe
              :data data))

(defn null-lift-binary-predicate
  "Helper for defining rators.
  Takes a predicate and returns a function that takes two values to apply this
  predicate to."
  [pred]
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

(defn expression-attribute-names
  "Takes an expression and returns a set of all attribute-ref's names."
  [expr]
  (fold-expression
   (fn [attr-name] #{attr-name})
   (constantly #{})
   (constantly #{})
   (fn [rator rands] (apply set/union rands))
   (fn [exprs] (apply set/union exprs)) ;; tuple
   (fn [_ expr] expr)                   ;; on-aggregate
   (fn [_] #{"*"})                      ;; on-aggregate-all
   (fn [alist default]                  ;; on-case
     (let [flattened-alist (->> alist (apply concat) (apply set/union))]
       (set/union default flattened-alist)))
   query-attribute-names
   query-attribute-names
   expr))

(defn query-scheme-attribute-names
  [q]
  (into #{} (keys (rel-scheme->environment (query-scheme q)))))

(defn query-attribute-names
  "Assuming this is a sub-query of an expression, determine what attribute names
  of the underlying query it uses."
  [q]
  (let [compute-names
        (fn [exp-names scheme-names query-names]
          (set/union (set/difference exp-names scheme-names) query-names))]
    (cond
      (or (empty-query? q)
          (base-relation? q))
      #{}

      (project? q)
      (let [alist        (project-alist q)
            query        (project-query q)
            exp-names    (apply set/union (map (fn [[_ exp]] (expression-attribute-names exp)) alist))
            scheme-names (query-scheme-attribute-names query)
            query-names  (query-attribute-names query)]
        (compute-names exp-names scheme-names query-names))

      (restrict? q)
      (let [exp          (restrict-exp q)
            query        (restrict-query q)
            exp-names    (expression-attribute-names exp)
            scheme-names (query-scheme-attribute-names query)
            query-names  (query-attribute-names query)]
        (compute-names exp-names scheme-names query-names))

      (restrict-outer? q)
      (let [exp          (restrict-outer-exp q)
            query        (restrict-outer-query q)
            exp-names    (expression-attribute-names exp)
            scheme-names (query-scheme-attribute-names query)
            query-names  (query-attribute-names query)]
        (compute-names exp-names scheme-names query-names))

      (combine? q)
      (let [q1 (combine-query-1 q)
            q2 (combine-query-2 q)]
        (set/union (query-attribute-names q1) (query-attribute-names q2)))

      (order? q)
      (let [alist        (order-alist q)
            query        (order-query q)
            exp-names    (apply set/union (map (fn [[exp _]] (expression-attribute-names exp)) alist))
            scheme-names (query-scheme-attribute-names query)
            query-names  (query-attribute-names query)]
        (compute-names exp-names scheme-names query-names))

      (group? q)
      (let [cols         (into #{} (group-columns q))
            query        (group-query q)
            scheme-names (query-scheme-attribute-names query)
            query-names  (query-attribute-names query)]
        (compute-names cols scheme-names query-attribute-names))

      (top? q)
      (query-attribute-names (top-query q))

      (distinct-q? q)
      (query-attribute-names (distinct-q-query q))

      :else
      (assertion-violation `query-attribute-names "unknown query" q))))

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
  "Takes an map and a 'underlying' query and returns a map of
  substitutions not already featured in `underlying`."
  [alist underlying]
  (let [underlying-map (rel-scheme-map (query-scheme underlying))]
    (into {}
          (filter (fn [[k v]]
                    (not (contains? underlying-map k)))
                  alist))))

(defn query-substitute-attribute-refs
  [alist q]
  (letfn [(next-step [qq] (query-substitute-attribute-refs alist qq))]
    (cond
      (empty-query? q)    q
      (base-relation? q)  q
      (project? q)        (let [sub    (project-query q)
                                culled (cull-substitution-alist alist sub)]
                            (make-project
                             (map (fn [[k v]] [k (substitute-attribute-refs culled v)])
                                  (project-alist q))
                             (next-step sub)))
      (restrict? q)       (let [sub    (restrict-query q)
                                culled (cull-substitution-alist alist sub)]
                            (make-restrict (substitute-attribute-refs culled (restrict-exp q))
                                           (next-step sub)))
      (restrict-outer? q) (let [sub    (restrict-outer-query q)
                                culled (cull-substitution-alist alist sub)]
                            (make-restrict-outer (substitute-attribute-refs culled (restrict-outer-exp q))
                                                 (next-step sub)))
      
      (combine? q) (make-combine (combine-rel-op q)
                                 (next-step (combine-query-1 q))
                                 (next-step (combine-query-2 q)))
      (order? q)   (let [sub    (order-query q)
                         culled (cull-substitution-alist alist sub)]
                     (make-order (map (fn [[k v]]
                                        [(substitute-attribute-refs culled k) v])
                                      (order-alist q))
                                 (next-step sub)))

      (group? q) (make-group (group-columns q)
                             (next-step (group-query q)))
      
      (top? q)      (make-top (top-offset q) (top-count q) (next-step (top-query q)))
      (distinct-q? q) (make-distinct (next-step (distinct-q-query q)))
      :else         (assertion-violation `query-substitute-attribute-refs "unknown query" q))))

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
