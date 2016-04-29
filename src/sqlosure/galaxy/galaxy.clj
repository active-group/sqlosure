(ns sqlosure.galaxy.galaxy
  (:require [active.clojure
             [condition :as c]
             [record :refer [define-record-type]]]
            [sqlosure
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [type :as t]]))

(define-record-type tuple
  ^{:doc "A tuple holds a (sorted) vector of values."}
  (make-tuple expressions) tuple?
  [^{:doc "A (sorted) vector of values."}
   expressions tuple-expressions])

;; DONE
(define-record-type db-galaxy
  ^{:doc "A galaxy is the Clojure representation of a record-type in a
(relational) database. Galaxies serve as the interface which can be queried just
as a SQL-table as created by `sqlosure.core/table`."}
  (really-make-db-galaxy name type setup-fn query) db-galaxy?
  [^{:doc "The name of the galaxy."} name db-galaxy-name
   ^{:doc "The type that this galaxy represents."} type db-galaxy-type
   ^{:doc "Takes a db-connection, sets up virgin DB tables."}
   setup-fn db-galaxy-setup-fn
   query db-galaxy-query])

;; DONE
(def ^:dynamic *db-galaxies*
  "`*db-galaxies*` is a map wrapped in an atom that contains all known
  galaxies as a mapping of galaxy-name ->
  `sqlosure.realional-algebra/base-relation`."
  (atom {}))

;; TODO which universe?
(defn make&install-db-galaxy
  "`make&install-db-galaxy` takes a `name` for a new galaxy, a `type` that
  this galaxy represents, the `setup-fn` function to create the corresponding
  db tables and a `query` (?).
  It returns a `sqlosure.relational-algebra/base-relation` for this new table
  and registers the galaxy to `*db-galaxies*`."
  [name type setup-fn query]
  (let [dg (really-make-db-galaxy name type setup-fn query)
        rel (rel/make-base-relation name
                                    (rel/alist->rel-scheme {name type})
                                    :universe sql/sql-universe
                                    :handle dg)]
    (swap! *db-galaxies* assoc name rel)
    rel))

;; DONE + TESTS
(defn initialize-db-galaxies!
  "Takes a connection and installs all galaxies currently stored in
  `*db-galaxies*` to the database."
  [conn]
  (doall (map (fn [[name glxy]]
                   ((db-galaxy-setup-fn (rel/base-relation-handle glxy)) conn))
              @*db-galaxies*)))

;; DONE
(define-record-type db-type-data
  (make-db-type-data scheme reifier value->db-expression-fn)
  db-type-data?
  [scheme db-type-data-scheme
   reifier db-type-data-reifier
   value->db-expression-fn db-type-data-value->db-expression-fn])

;; TODO Which universe?
(defn make-db-type
  "`make-db-type` creates a new `sqlosure.type/base-type` for a db-type in a to
  be used in a galaxy. The newly created data type will be registered to the
  sql-universe.

  * `name`: the name of the type
  * `pred`:  a predicate function for values of that type
  * `const->datum-fn`
  * `datum->const-fn`
  * `scheme`: a `sqlosure.relational-algebra/rel-scheme` for this new type
  * `reifier`: a function that knows how to reconstruct a value of this type
               from a query result
  * `value->db-expression->fn`: a function that knows how to create a db-entry
                                from a value of this type"
  [name pred const->datum-fn datum->const-fn scheme reifier
   value->db-expression-fn
   & {:keys [ordered? numeric?]
      :or [ordered? false numeric? false]}]
  (t/make-base-type name pred const->datum-fn datum->const-fn
                    :universe sql/sql-universe
                    :data (make-db-type-data scheme reifier value->db-expression-fn)))

;; DONE
(define-record-type db-operator-data
  (make-db-operator-data base-query transformer-fn) db-operator-data?
  [base-query db-operator-data-base-query
   transformer-fn db-operator-data-transformer-fn])

;; DONE + TESTS
(defn- make-name-generator
  "Takes a prefix (String) and returns a function that returns the prefix with
  a \"_n\"-suffix, where n is an integer starting with 0 that gets incremented
  upon each subsequent call."
  [prefix]
  (let [count (atom 0)]
    (fn []
      (let [c @count]
        (swap! count inc)
        (str prefix "_" c)))))

;; DONE
(defn- list->product
  "Takes a list and returns a `sqlosure.relational-algebra/product` for this
  list."
  [ql]
  (if (empty? ql)
    rel/the-empty
    (rel/make-product (first ql)
                      (list->product (rest ql)))))

;; DONE
(defn- apply-restrictions
  "Takes a list of restrictions `rl` and a query and applies the restrictions to
  the query."
  [rl q]
  (cond
    (nil? q) rel/the-empty
    (not (rel/query? q)) (c/assertion-violation `apply-restrictions
                                                "not a query" q)
    :else
    (if (empty? rl)
      q
      (apply-restrictions (rest rl)
                          (rel/make-restrict (first rl) q)))))

;; DONE
(defn restrict-to-scheme
  "Takes a rel-scheme `scheme` and a query `q` and returns a new
  `sqlosure.relational-algebra/project` which wraps the old query in a
  projection with the mappings of `scheme`."
  [scheme q]
  (when (empty? scheme)
    (c/assertion-violation `restrict-to-scheme "empty scheme"))
  (when-not (rel/query? q)
    (c/assertion-violation `restrict-to-scheme "unknown query" q))
  (rel/make-project
   (map (fn [k]
          [k (rel/make-attribute-ref (get (rel/rel-scheme-map scheme) k))])
        (rel/rel-scheme-columns scheme))
   q))

;; DONE
(defn make-new-names
  "Takes a string `base` and a list `lis` and returns a list of
  '(\"base_0\", ..., \"base_n\") where `n` = `(count lis)`."
  [base lis]
  (when-not (empty? lis)
    (let [gen (make-name-generator base)]
      (take (count lis) (repeatedly gen)))))

(declare dbize-project dbize-expression)

;; TODO restrict-outer
(defn dbize-query*
  "Returns new query, environment mapping names to tuples."
  [q generate-name]
  (letfn
      [(worker [q generate-name]
         (cond
           (rel/empty-query? q) [rel/the-empty-rel-scheme '()]
           (rel/base-relation? q)
           (let [handle (rel/base-relation-handle q)]
             ;; If the query is a galaxy, we need to extract the underlying
             ;; query and relation.
             (if (db-galaxy? handle)
               (let [db-query (db-galaxy-query handle)
                     name (db-galaxy-name handle)
                     cols (rel/rel-scheme-columns (rel/query-scheme db-query))
                     new-names (make-new-names name cols)]
                 [(rel/make-project (map (fn [k new-name]
                                           [new-name (rel/make-attribute-ref k)])
                                         cols new-names)
                                    db-query)
                  [name
                   (make-tuple (map rel/make-attribute-ref new-names))]])
               [q '()]))
           (rel/project? q)
           (let [[alist underlying env]
                 (dbize-project (rel/project-alist q) (rel/project-query q)
                                generate-name)]
             [(rel/make-project alist underlying) env])
           (rel/restrict? q)
           (let [[underlying env] (worker (rel/restrict-query q) generate-name)
                 [exp queries restrictions]
                 (dbize-expression (rel/restrict-exp q) env
                                   (rel/restrict-query q)
                                   generate-name)]
             [(restrict-to-scheme (rel/query-scheme underlying)
                                  (apply-restrictions
                                   (cons exp restrictions)
                                   (list->product (cons underlying queries))))
              env])
           ;; (rel/restrict-outer? q) ...
           (rel/combine? q)
           (let [[dq1 env1] (worker (rel/combine-query-1 q) generate-name)
                 [dq2 env2] (worker (rel/combine-query-2 q) generate-name)]
             [(rel/make-combine (rel/combine-rel-op q) dq1 dq2)
              (concat env1 env2)])
           (rel/order? q)
           (let [[underlying env] (worker (rel/order-query q) generate-name)]
             [(rel/make-order
               (map (fn [[k v]]
                      (let [[exp queries restrictions]
                            (dbize-expression k env (rel/order-query q)
                                              generate-name)]
                        (if (or (seq queries)
                                (seq restrictions))
                          (c/assertion-violation
                           `dbize-query "object values used in order query")
                          (cons exp v))))
                    (rel/order-alist q)))
              env])
           (rel/top? q)
           (let [[underlying env] (worker (rel/top-query q) generate-name)]
             [(rel/make-top (rel/top-offset q) (rel/top-count q) underlying)
              env])
           :else (c/assertion-violation `dbize-query "unknown query" q)))]
    (worker q generate-name)))

;; DONE
(defn dbize-query
  [q]
  (dbize-query* q (make-name-generator "dbize")))

;; DONE may very well contain errors
(defn dbize-project
  [alist q-underlying generate-name]
  (let [[underlying env] (dbize-query* q-underlying generate-name)]
    ;; NOTE is it wise to loop through a map (may be unsorted)?
    (loop [alist alist
           rev '()
           bindings '()
           queries '()
           restrictions '()]
      (if (empty? alist)
        [(reverse rev)
         (apply-restrictions restrictions
                             (list->product (cons underlying queries)))
         bindings]
        (let [[name value] (first alist)
              [exp more-queries more-restrictions]
              (dbize-expression value env q-underlying generate-name)]
          (if (tuple? exp)
            (let [exprs (tuple-expressions exp)
                  new-names (make-new-names name exprs)]
              (recur (rest alist)
                     (concat (reverse (map (fn [cexp new-name]
                                             [new-name cexp]))
                                      exprs new-names)
                             rev)
                     (cons
                      (cons name
                            (make-tuple (map rel/make-attribute-ref new-names)))
                      bindings)
                     (concat more-queries queries)
                     (concat more-restrictions restrictions)))
            (recur (rest alist)
                   (cons (cons name exp) rev)
                   bindings
                   (concat more-queries queries)
                   (concat more-restrictions restrictions))))))))

(declare reify-query-result)

;; with a high possibility of errors.
(defn db-query-reified-results
  [db q]
  ;; FIXME what happened to env?
  (let [[db-q _] (dbize-query q)
        db-res (db/run-query db db-q)]
    (reify-query-result db-res (rel/query-scheme q))))

;; TODO
(defn db-query-reified-result
  [db q]
  (let [results (db-query-reified-results db q)]
    (and (seq? results)
         (ffirst results))))

;; DONE
(defn take+drop [n lis]
  [(take n lis) (drop n lis)])

;;  DONE
(defn reify-query-result
  [res scheme]
  (loop [cols (rel/rel-scheme-columns scheme)
         res res
         rev '()]
    (if (empty? cols)
      (reverse rev)
      (let [typ (get (rel/rel-scheme-map scheme) (first cols))]
        (if (and (satisfies? t/base-type-protocol typ)
                 (db-type-data? (t/-data typ)))
          (let [scheme (db-type-data-scheme (t/-data typ))
                reifier (db-type-data-reifier (t/-data typ))
                [prefix suffix] (take+drop (count (rel/rel-scheme-columns scheme)) res)]
            (recur (rest cols)
                   suffix
                   (cons (apply reifier prefix) rev)))
          (recur (rest cols)
                 (rest res)
                 (cons (rest res) rev)))))))

;; DONE
(defn rename-query
  "Takes a query `q` and a name-generator function `generate-name` and returns
  the query wrapped in a `sqlosure.relational-algebra/project` with mappings
  from a newly generated name to the old reference."
  [q generate-name]
  (when-not (fn? generate-name)
    (c/assertion-violation `rename-query
                           "generate-name is not a function"
                           generate-name))
  (when-not (rel/query? q)
    (c/assertion-violation `rename-query
                           "unknown query" q))
  (let [cols (rel/rel-scheme-columns (rel/query-scheme q))
        names (map (fn [_] (generate-name)) cols)]
    [(map rel/make-attribute-ref names)
     (rel/make-project (map (fn [name k]
                              [name (rel/make-attribute-ref k)])
                            names cols)
                       q)]))

;; TODO
(defn dbize-expression
  "Returns dbized expression, list of renamed additional queries, and list of
  restrictions on the resulting product."
  [e env underlying generate-name]
  (let [base-queries (atom '())
        restrictions (atom '())
        underlying-scheme (rel/query-scheme underlying)]
    (letfn
        [(worker [e]
           (cond
             (rel/attribute-ref? e)
             (or (get env (rel/attribute-ref-name e)) e)
             (rel/const? e)
             (let [typ (rel/const-type e)]
               (if (and (satisfies? t/base-type-protocol typ)
                        (db-type-data? (t/-data typ)))
                 ((db-type-data-value->db-expression-fn (t/-data typ))
                  (rel/const-val e))
                 e))
             (rel/const-null? e)
             (let [typ (rel/null-type e)]
               (if (and (satisfies? t/base-type-protocol typ)
                        (db-type-data? (t/-data typ)))
                 ((db-type-data-value->db-expression-fn (t/-data typ))
                  (rel/const-val e))
                 e))
             (rel/application? e)
             (let [rator (rel/application-rator e)
                   data (rel/rator-data rator)
                   base-query (when (db-operator-data? data)
                                (db-operator-data-base-query data))
                   rands (rel/application-rands e)
                   initiate
                   (fn []
                     (apply (db-operator-data-transformer-fn data)
                            (concat
                             (map worker rands)
                             (map
                              #(rel/expression-type underlying-scheme %)
                              rands))))]
               (if (db-operator-data? data)
                 ;; FIXME this case is not clear (example necessary?).
                 (if base-query
                   (let [[restriction-fn transform] (initiate)
                         base-query-refs
                         (if base-query
                           (let [[base-query-refs renamed-base-query]
                                 (rename-query base-query generate-name)]
                             (reset! base-queries (cons renamed-base-query
                                                        @base-queries))
                             base-query-refs)
                           '())]
                     (when restriction-fn
                       (reset! restrictions
                               (cons (apply restriction-fn base-query-refs)
                                     restrictions)))
                     (apply transform base-query-refs))
                   (initiate))
                 ;; Base case, nothing special here.
                 (apply rel/make-application
                        rator
                        (map worker rands))))
             (tuple? e)
             (make-tuple (map worker (tuple-expressions e)))
             (rel/aggregation? e)
             (let [expr (worker (rel/aggregation-expr e))]
               (if (= :count (rel/aggregation-operator e))
                 (rel/make-aggregation
                  :count
                  (loop [expr expr]
                    (if (tuple? expr)
                      ;; doesn't matter
                      (recur (first (tuple-expressions expr)))
                      expr)))
                 (rel/make-aggregation (rel/aggregation-operator e)
                                       expr)))
             ;; TODO aggregation*
             (rel/case-expr? e)
             ;; FIXME: this is incomplete: we should commute this
             ;; with tuples inside the case if the result type is a
             ;; DB type
             ;; FIXME Tests!
             (rel/make-case-expr (map (fn [[k v]]
                                        [(worker k) (worker v)])
                                      (rel/case-expr-alist ))
                                 (worker (rel/case-expr-default e)))
             :else (c/assertion-violation `dbize-expression
                                          "unknown expression" e)))]
      (let [dbized (worker e)]
        [dbized @base-queries @restrictions]))))
