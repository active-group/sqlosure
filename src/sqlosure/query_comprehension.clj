(ns sqlosure.query-comprehension
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
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

(defn embed
  [q]
  (monadic
   [alias new-alias]
   [query current-query]
   (let [scheme (rel/query-scheme q)
         alist (rel/rel-scheme-alist scheme)
         fresh (map (fn [[k _]] (fresh-name k alias)) alist)
         project-alist (into {} (map (fn [[k _] fresh]
                                       [fresh (rel/make-attribute-ref k)])
                                     alist fresh))
         qq (rel/make-project project-alist q)])
   (set-query! (rel/make-product qq query))
   (return (make-relation alias scheme))))

(defn project
  [alist]
  (monadic
   [alias new-alias]
   [query current-query]
   (set-query! (rel/make-extend
                (into {}
                      (map (fn [[k v]] [(fresh-name k alias) v])
                           alist))
                query))
   (return (make-relation
            alias
            (let [scheme (rel/query-scheme query)]
              (rel/make-rel-scheme
               (into {}
                     (map (fn [[k v]]
                            [k (rel/expression-type (rel/rel-scheme->environment scheme) v)])
                          alist))))))))

(defn restrict
  [expr]
  (monadic
   [old current-query]
   (set-query! (rel/make-restrict expr old))
   #_([alias (get-state-component ::alias)]
    [query current-query]
    (return (make-relation
             alias
             (rel/query-scheme query))))))

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

(defn generate-query
  "Returns `[retval state]`."
  [prod state]
  (run-free-reader-state-exception
   (null-monad-command-config nil nil)
   prod
   state))

(defn get-query+scheme
  [prod]
  (let [[rel state] (run-free-reader-state-exception
                     (null-monad-command-config nil nil)
                     prod
                     the-empty-state)
        alias (relation-alias rel)
        scheme (relation-scheme rel)
        alist (into {}
                    (map (fn [[k _]]
                           [k (rel/make-attribute-ref (fresh-name k alias))])
                         (rel/rel-scheme-alist scheme)))
        query (get state ::query)]
    [(rel/make-project alist query) scheme]))
   
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
        p1 (rel/make-project (into {}
                                   (map (fn [[k _]]
                                          [(fresh-name k alias)
                                           (rel/make-attribute-ref (fresh-name k a1))])
                                        (rel/rel-scheme-alist scheme1)))
                             q1)
        p2 (rel/make-project (into {}
                                   (map (fn [[k _]]
                                          [(fresh-name k alias)
                                           (rel/make-attribute-ref (fresh-name k a2))])
                                        (rel/rel-scheme-alist scheme2)))
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
  [n]
  (monadic
   [old current-query]
   (set-query! (rel/make-top n old))))
