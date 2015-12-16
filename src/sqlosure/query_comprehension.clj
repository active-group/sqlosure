(ns sqlosure.query-comprehension
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
            [active.clojure.monad :refer :all]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.pprint :refer [pprint]]))

(defn mapmap
  "Apply f to all key-values pairs in m and put it back in a map. f must return
  a vector containing two elements."
  [f m]
  (into {} (map f m)))

(define-record-type relation
  (make-relation alias scheme) relation?
  [alias relation-alias
   scheme relation-scheme])

(defn fresh-name
  [name alias]
  (str name "-" alias))

(def new-alias
  (monadic
   [a (get-state-component ::alias)]
   (put-state-component! ::alias (inc a))
   (free-return a)))

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
                (mapmap (fn [[k v]] [(fresh-name k alias) v])
                        alist)
                query))
   (return (make-relation
            alias
            (let [scheme (rel/query-scheme query)]
              (rel/make-rel-scheme
               (mapmap (fn [[k v]]
                         [k (rel/expression-type (rel/rel-scheme->environment scheme) v)])
                       alist)))))))

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
  (let [alist (rel/rel-scheme-alist (relation-scheme rel))]
    (if (contains? alist name)
      (rel/make-attribute-ref (fresh-name name (relation-alias rel)))
      (assertion-violation '! "unkown attribute" rel name))))

(defn get-query+scheme*
  [thunk]
  (let [[rel state] (thunk)]
    (let [alias (relation-alias rel)
          scheme (relation-scheme rel)
          alist (mapmap (fn [[k _]]
                          [k (rel/make-attribute-ref (fresh-name k alias))])
                        (rel/rel-scheme-alist scheme))
          query (get state ::query)]
      [(rel/make-project alist query) scheme])))

;; A map representing the empty state for building up the query.
(def the-empty-state {::query rel/the-empty
                      ::alias 0})

(defn get-query+scheme
  [prod]
  (get-query+scheme*
   #(run-free-reader-state-exception
     (null-monad-command-config nil nil)
     prod
     the-empty-state)))

(defn get-query
  [prod]
  (let [[query _] (get-query+scheme prod)]
    query))















#_((defn combination*
   [op old-query rel1 q1 rel2 q2 compute-scheme alias]
   (let [a1 (relation-alias rel1)
         a2 (relation-alias rel2)
         scheme1 (relation-scheme rel1)
         scheme2 (relation-scheme rel2)
         p1 (rel/make-project (mapmap (fn [[k _]]
                                        [(fresh-name k alias)
                                         (rel/make-attribute-ref (fresh-name k a1))])
                                      (rel/rel-scheme-alist scheme1))
                              q1)
         p2 (rel/make-project (mapmap (fn [[k _]]
                                        [(fresh-name k alias)
                                         (rel/make-attribute-ref (fresh-name k a2))])
                                      (rel/rel-scheme-alist scheme2))
                              q2)]
     [(make-relation alias (compute-scheme scheme1 scheme2))
      (inc alias)
      (rel/make-product (rel/make-combine op p1 p2)
                        old-query)]))

 (defn first-scheme
   [s1 s2]
   s1)

 (defn order
   [alist]
   (monadic
    [old current-query]
    (set-query! (rel/make-order alist old))))

 )
