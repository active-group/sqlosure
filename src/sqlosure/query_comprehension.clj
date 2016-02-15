(ns sqlosure.query-comprehension
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
            [sqlosure.utils :refer [zip]]
            [active.clojure.monad :refer :all]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.pprint :refer [pprint]]))

(define-record-type relation
  (make-relation alias scheme) relation?
  [alias relation-alias
   scheme relation-scheme])

(defn fresh-name
  [name alias]
  (str name "_" alias))

(defn set-alias!
  [a]
  (put-state-component! ::alias a))

(def current-alias
  (get-state-component ::alias))

(def new-alias
  (monadic
   [a current-alias]
   (set-alias! (inc a))
   (return a)))

(def current-query
  (get-state-component ::query))

(defn set-query!
  [new]
  (put-state-component! ::query new))

(defn- add-to-product
  [make-product transform-scheme q]
  (monadic
   [alias new-alias]
   [query current-query]
   (let [scheme (transform-scheme (rel/query-scheme q))
         alist (rel/rel-scheme-alist scheme)
         fresh (map (fn [[k _]] (fresh-name k alias)) alist)
         project-alist (map (fn [[k _] fresh]
                              [fresh (rel/make-attribute-ref k)])
                            alist fresh)
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

(defn project
(defn- project0
  "Project the some columns of the current query."
  [alist extend?]
  (monadic
   [alias new-alias]
   [query current-query]
   (let [query' ((if extend? rel/make-extend rel/make-project)
                 (map (fn [[k v]] [(fresh-name k alias) v])
                      alist)
                 query)])
   (set-query! query')
   (return (make-relation
            alias
            (let [scheme (rel/query-scheme query)
                  env (rel/rel-scheme->environment scheme)]
              (rel/alist->rel-scheme
               (map (fn [[k v]]
                      [k (rel/expression-type env v)])
                    alist)))))))

(defn project
  "Project the some columns of the current query."
  [alist]
  (project0 alist true))

(defn project-only ;; FIXME: temporary solution? Can't detect automatically what's needed?
  "Project the some columns of the current query."
  [alist]
  (project0 alist false))

;; FIXME: add !
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

  Note this doesn't return anything."
  [expr]
  (monadic
   [old current-query]
   (set-query! (rel/make-restrict-outer expr old))))

(defn restricted
  "Convenienc: Return a restricted version of a relation.

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
    (when-not (contains? (rel/rel-scheme-alist (relation-scheme rel)) name)
      (assertion-violation `group-by "unknown attribute" rel name)))
  (monadic
   [old current-query]
   (set-query! (rel/make-group (map (fn [[rel name]]
                                      (fresh-name name (relation-alias rel)))
                                    colrefs)
                               old))))

(defn !
  [rel name]
  ;; check user args
  (if (not (relation? rel))
    (assertion-violation '! (str "not a relation: " rel)))
  (let [alist (rel/rel-scheme-alist (relation-scheme rel))]
    (if (contains? alist name)
      (rel/make-attribute-ref (fresh-name name (relation-alias rel)))
      (assertion-violation '! "unkown attribute" rel name))))

;; A map representing the empty state for building up the query.

(defn make-state
  [query alias]
  {:pre [(integer? alias)]}
  {::query query
   ::alias alias})

(def the-empty-state (make-state rel/the-empty 0))

(def query-comprehension-monad-command-config
  (null-monad-command-config nil the-empty-state))

(defn run-query-comprehension*
  "Returns `[retval state]`."
  [prod state]
  (run-free-reader-state-exception
   query-comprehension-monad-command-config
   prod
   state))

(defn run-query-comprehension
  "Run the query comprehension against an empty state, and return the
  result. To actually create an executable query use [[build-query!]]
  or [[get-query]]."
  [prod]
  (let [[res state] (run-query-comprehension* prod nil)]
    res))

(defn generate-query
  "Returns `[retval state]`."
  [prod state]
  (run-query-comprehension* prod state))

(defn build-query+scheme!
  "Monadic command to create the final query from the given relation and the current monad
  state, and return it and the scheme. Also resets the state."
  [rel]
  (monadic
   [state (get-state)]
   (let [alias (relation-alias rel)
         scheme (relation-scheme rel)
         alist (map (fn [[k _]]
                      [k (rel/make-attribute-ref (fresh-name k alias))])
                    (rel/rel-scheme-alist scheme))
         query (::query state)])
   (put-state! (merge state the-empty-state))
   (return [(rel/make-project alist query) scheme])))

(defn build-query!
  "Monadic command to create the final query from the given relation and the current monad
  state, and return it and the scheme."
  [rel]
  (monadic
   [[query scheme] (build-query+scheme! rel)]
   (return query)))

(defn get-query+scheme
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

(defn combination*
  [op old-query rel1 q1 rel2 q2 compute-scheme alias]
  (let [a1 (relation-alias rel1)
        a2 (relation-alias rel2)
        scheme1 (relation-scheme rel1)
        scheme2 (relation-scheme rel2)
        p1 (rel/make-project (map (fn [[k _]]
                                    [(fresh-name k alias)
                                     (rel/make-attribute-ref (fresh-name k a1))])
                                  (rel/rel-scheme-alist scheme1))
                             q1)
        p2 (rel/make-project (map (fn [[k _]]
                                    [(fresh-name k alias)
                                     (rel/make-attribute-ref (fresh-name k a2))])
                                  (rel/rel-scheme-alist scheme2))
                             q2)]
    (monadic
     (set-alias! (inc alias))
     (set-query! (rel/make-product (rel/make-combine op p1 p2)
                                   old-query))
     (return (make-relation alias (compute-scheme scheme1 scheme2))))))

(defn combination
  [rel-op compute-scheme prod1 prod2]
  (monadic
   [query0 current-query
    alias0 current-alias]
    (let [[rel1 state1] (generate-query prod1 (make-state query0 alias0))
          [rel2 state2] (generate-query prod2 (make-state query0 (::alias state1)))])
    (combination* rel-op query0
                  rel1 (::query state1) rel2 (::query state2)
                  compute-scheme
                  alias0)))

 (defn first-scheme
   [s1 s2]
   s1)

(defn union
  [prod1 prod2]
  (combination :union first-scheme prod1 prod2))

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
  [alist]
  (monadic
   [old current-query]
   (set-query! (rel/make-order alist old))))

(defn top
  ([n]
   (top nil n))
  ([offset n]
   (monadic
    [old current-query]
    (set-query! (rel/make-top offset n old)))))
