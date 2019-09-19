(ns sqlosure.query-comprehension
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
            [sqlosure.utils :refer [zip]]
            [active.clojure.monad :refer :all]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.pprint :refer [pprint]]
            [active.clojure.condition :as c]))

(define-record-type relation
  ^{:doc "`relation` is used to track the current state and later rebuild the resulting,
correct references when running the query monad."}
  (make-relation alias scheme) relation?
  [^{:doc "The current alias."}
   alias relation-alias
   ^{:doc "The current relation-scheme."}
   scheme relation-scheme])

(defn- fresh-name
  [name alias]
  (str name "_" alias))

(defn- set-alias!
  "Takes a new alias and puts it in the current state."
  [a]
  (put-state-component! ::alias a))

(def ^{:private true} current-alias
  (get-state-component ::alias))

(def ^{:private true} new-alias
  (monadic
   [a current-alias]
   (set-alias! (inc a))
   (return a)))

(def ^{:private true} current-query
  (get-state-component ::query))

(defn- set-query!
  "Takes a query and puts it in the current state."
  [new]
  (put-state-component! ::query new))

(defn- add-to-product
  [make-product transform-scheme q]
  (monadic
   [alias new-alias]
   [query current-query]
   (let [scheme (transform-scheme (rel/query-scheme q))
         columns (rel/rel-scheme-columns scheme)
         fresh (map (fn [k] (fresh-name k alias)) columns)
         project-alist (map (fn [k fresh]
                              [fresh (rel/make-attribute-ref k)])
                            columns fresh)
         qq (rel/make-project project-alist q)])
   (set-query! (make-product query qq))
   (return (make-relation alias scheme))))

(defn embed
  "Embed a RA query into the current query."
  [q]
  (add-to-product rel/make-product identity q))

(defn outer
  "Embed a RA query as an outer query into the current query."
  [q]
  (add-to-product rel/make-left-outer-product rel/rel-scheme-nullable q))

(defn assert-alist
  [caller coll]
  (if (or (list? coll) (vector? coll))
    coll
    (c/assertion-violation caller "alist must be an ordered collection" coll)))

(defn project
  "Project some columns of the current query. Returns the resulting state.

  Example: (monadic [t (<- embed t-table)]
                    (project [[\"foo\" (! t \"foo\")]
                              [\"bar\" (! t \"bar\")]])

  The corresponding SQL statemant would be \"SELECT foo, bar FROM t\"."
  [alist]
  (assert-alist `project alist)
  (monadic
   [alias new-alias
    query current-query]
   (let [query' ((if (rel/empty-query? query) rel/make-project rel/make-extend )
                 (mapv (fn [[k v]] [(fresh-name k alias) v])
                       alist)
                 query)])
   (set-query! query')
   (return (make-relation
            alias
            (let [scheme (rel/query-scheme query)
                  env    (rel/rel-scheme->environment scheme)]
              (rel/alist->rel-scheme
               (map (fn [[k v]]
                      [k (rel/expression-type env v)])
                    alist)))))))
(defn restrict
  "Restrict the current query by a condition.

  expr -> query(nil)

  Note this doesn't return anything."
  [expr]
  (monadic
   [old current-query]
   (set-query! (rel/make-restrict expr old))))

(defn restrict-outer
  "Restrict outer part of the current query by a condition.

  expr -> query(nil)

  Note: this is a monadic action that doesn't return anything."
  [expr]
  (monadic
   [old current-query]
   (set-query! (rel/make-restrict-outer expr old))))

(defn restricted
  "Restrict the current query by a condition. Returns the resulting state.

  Example: (monadic [t (<- embed t-table)]
                    (restricted t ($<= (! t \"id\")
                                       ($integer 1))

  The corresponding SQL statement would be \"SELECT <all cols of t> FROM t WHERE id <= 1\"

  relation expr -> query(relation)."
  [rel expr]
  (monadic
   (restrict expr)
   (return rel)))

(defn group
  "Group by specified seq of column references `[rel name]`."
  [& colrefs]
  (doseq [[rel name] colrefs]
    (when-not (relation? rel)
      (assertion-violation `group-by "not a relation" rel))
    (when-not (string? name)
      (assertion-violation `group-by "not a column name" name))
    (when-not (contains? (rel/rel-scheme-map (relation-scheme rel)) name)
      (assertion-violation `group-by "unknown attribute" rel name)))
  (monadic
   [old current-query]
   (set-query! (rel/make-group (map (fn [[rel name]]
                                      (fresh-name name (relation-alias rel)))
                                    colrefs)
                               old))))

(defn group-refs
  "Group by specified seq of attribute references `(qc/! rel name)`."
  [& refs]
  (doseq [ref refs]
    (when-not (rel/attribute-ref? ref)
      (assertion-violation `group-by "not an attribute ref" ref)))
  (monadic
   [old current-query]
   (set-query! (rel/make-group (map rel/attribute-ref-name
                                    refs)
                               old))))

(defn !
  "`!` selects an attribute from a relation.

  Example: (! t \"id\") corresponds to SQL \"t.id\"."
  [rel name]
  ;; check user args
  (when-not (relation? rel)
    (assertion-violation `! (str "not a relation: " rel)))
  (when-not (contains? (rel/rel-scheme-map (relation-scheme rel)) name)
    (assertion-violation `! "unkown attribute" rel name))
  (rel/make-attribute-ref (fresh-name name (relation-alias rel))))

;; A map representing the empty state for building up the query.
(defn- make-state
  [query alias]
  {:pre [(integer? alias)]}
  {::query query
   ::alias alias})

(def ^{:private true} the-empty-state (make-state rel/the-empty 0))

(def ^{:private true} query-comprehension-monad-command-config
  (null-monad-command-config nil the-empty-state))

(defn- run-query-comprehension*
  "Returns `[retval state]`."
  [prod state]
  (run-free-reader-state-exception
   query-comprehension-monad-command-config
   prod
   state))

(defn- run-query-comprehension
  "Run the query comprehension against an empty state, and return the
  result. To actually create an executable query use [[build-query!]]
  or [[get-query]]."
  [prod]
  (let [[res state] (run-query-comprehension* prod nil)]
    res))

(defn- build-query+scheme!
  "Monadic command to create the final query from the given relation and the current monad
  state, and return it and the scheme. Also resets the state."
  [rel]
  (monadic
   [state (get-state)]
   (let [alias (relation-alias rel)
         scheme (relation-scheme rel)
         alist (map (fn [k]
                      [k (rel/make-attribute-ref (fresh-name k alias))])
                    (rel/rel-scheme-columns scheme))
         query (::query state)])
   (return [(rel/make-project alist query) scheme])))

(defn- build-query!
  "Monadic command to create the final query from the given relation and the current monad
  state, and return it and the scheme."
  [rel]
  (monadic
   [[query scheme] (build-query+scheme! rel)]
   (return query)))

(defn- get-query+scheme
  [prod]
  (let [query+scheme (run-query-comprehension
                      (monadic
                       [rel prod]
                       (build-query+scheme! rel)))]
    query+scheme))

(defn get-query
  [prod]
  (let [[query _] (get-query+scheme prod)]
    query))

(defn- combination
  [rel-op compute-scheme prod1 prod2]
  (monadic
   [query0 current-query
    alias0 current-alias]
    (let [[rel1 state1] (run-query-comprehension* prod1 (make-state query0 alias0))
          [rel2 state2] (run-query-comprehension* prod2 (make-state query0 (::alias state1)))
          a1 (relation-alias rel1)
          a2 (relation-alias rel2)
          scheme1 (relation-scheme rel1)
          scheme2 (relation-scheme rel2)
          q1 (::query state1)
          q2 (::query state2)
          p1 (rel/make-project (map (fn [k]
                                      [(fresh-name k alias0)
                                       (rel/make-attribute-ref (fresh-name k a1))])
                                    (rel/rel-scheme-columns scheme1))
                               q1)
          p2 (rel/make-project (map (fn [k]
                                      [(fresh-name k alias0)
                                       (rel/make-attribute-ref (fresh-name k a2))])
                                    (rel/rel-scheme-columns scheme2))
                               q2)])
    (monadic
     (set-alias! (inc alias0))
     (set-query! (rel/make-product (rel/make-combine rel-op p1 p2)
                                   query0))
     (return (make-relation alias0 (compute-scheme scheme1 scheme2))))))

(defn- first-scheme
  "Takes two schemes and returns the first one."
  [s1 s2]
  s1)

;; Why is only this one n-ary?
(defn union
  [p1 & prods]
  (if (empty? prods)
    p1
    (combination :union first-scheme p1
                 (apply union prods))))

(defn intersect
  [prod1 prod2]
  (combination :intersection first-scheme prod1 prod2))

(defn divide
  [prod1 prod2]
  (combination :quotient rel/rel-scheme-difference prod1 prod2))

(defn subtract
  [prod1 prod2]
  (combination :difference first-scheme prod1 prod2))

(defn order
  "`order` takes an alist of [[attribute-ref :descending/:ascending] to order
  the result by this attribute.

  Example: (monadic [t (embed t-table)]
                    (order [[(! t \"foo\") :ascending]])
                    (project {\"foo\" (! t \"foo\")}))

  The corresponding SQL statemant would be \"SELECT foo FROM t ORDER BY foo ASC\"."
  [alist]
  (monadic
   [old current-query]
   (set-query! (rel/make-order alist old))))

(defn top
  "`top` is used to define queries that return a cerain number of entries.
  When called with one argument `n`, top constructs a query that only returns
  the first `n` elements.
  Whan called with two arguments `offset` and `n`, top constructs a query that
  returns the first `n` elements with an offset of `offset`."
  ([n]
   (top nil n))
  ([offset n]
   (monadic
    [old current-query]
    (set-query! (rel/make-top offset n old)))))

(def distinct!
  (monadic [old current-query]
           (set-query! (rel/make-distinct old))))
