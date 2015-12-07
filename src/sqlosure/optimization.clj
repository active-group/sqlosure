(ns sqlosure.optimization
  (:require [sqlosure.relational-algebra :as r]
            [sqlosure.relational-algebra-sql :as rs]
            [clojure.set :as set]))

(defn project-alist-substitute-attribute-refs
  "Takes an alist and a project query's alist and substitutes all of the
  latter's refs."
  [alist palist]
  (into {} (map (fn [[k v]] [k (r/substitute-attribute-refs alist v)])
                palist)))

(defn order-alist-attribute-names
  "Takes an order query's alist and returns it's referenced attributes."
  [alist]
  (flatten (distinct (map r/expression-attribute-names (keys alist)))))

(defn query->alist
  "Return the rel-scheme-alist of a query's query-scheme."
  [q]
  (-> q r/query-scheme r/rel-scheme-alist))

(defn intersect-live
  "Takes a sequence of 'live' values and a query and returns the intersection of
  all refs in both the live-list and the rel-scheme-alist of the query."
  [live q]
  (into []
        (set/intersection
         (into #{} live)
         (into #{} (keys (query->alist q))))))

(defn elem?
  "Does a collection contain e?"
  [coll e]
  (some #(= % e) coll))

(defn remove-dead
  [q]
  (letfn
      [(worker [live q]
         (cond
           (nil? q) q
           (r/base-relation? q) q
           (r/project? q)
           (let [new-alist (into {} (filter (fn [[k _]] (elem? live k))
                                            (r/project-alist q)))]
             (r/make-project new-alist
                             (worker (map (fn [[k v]]
                                            (r/expression-attribute-names v))
                                          new-alist)
                                     (r/project-query q))))
           (r/restrict? q)
           (let [e (r/restrict-exp q)]
             (r/make-restrict
              e (worker (concat (r/expression-attribute-names e)
                                live)
                        (r/restrict-query q))))
           (r/order? q)
           (let [alist (r/order-alist q)]
             (r/make-order
              alist
              (worker (concat (order-alist-attribute-names alist)
                              live)
                      (r/order-query q))))
           (r/top? q) (r/make-top (r/top-count q) (worker live (r/top-query q)))
           (r/combine? q)
           (let [q1 (r/combine-query-1 q)
                 q2 (r/combine-query-2 q)]
             (let [live1 (intersect-live live q1)]
               (case (r/combine-rel-op q)
                 :product
                 (let [live2 (intersect-live live q2)]
                   (r/make-combine :product
                                   (worker live1 q1)
                                   (worker live2 q2)))
                 :quotient
                 (r/make-combine
                  :quotient
                  (worker (keys (r/rel-scheme-alist (r/query-scheme q1))) q1)
                  (worker (keys (r/rel-scheme-alist (r/query-scheme q2))) q2))
                 (r/make-combine (r/combine-rel-op q)
                                 (worker live1 q1)
                                 (worker live1 q2)))))
           (r/grouping-project? q)
           (let [new-alist (filter (fn [[k v]]
                                     (or (not (r/aggregate? v))
                                         (contains? live k)))
                                   (r/project-alist q))]
             (r/make-grouping-project
              new-alist (worker (apply concat
                                       (map (fn [[k v]]
                                              (r/expression-attribute-names v))
                                            new-alist))
                                (r/project-query q))))
           :else (throw (Exception. (str 'remove-dead ": unknown query " q)))))]
    (worker (keys (query->alist q)) q)))

(defn merge-project
  [q]
  (cond
    (nil? q) q
    (r/base-relation? q) q
    (r/project? q)
    (let [pq (merge-project (r/project-query q))
          pa (r/project-alist q)]
      (cond
        (r/project? pq)
        (r/make-project (project-alist-substitute-attribute-refs
                         (r/project-alist pq) pa)
                        (r/project-query pq))
        (r/combine? pq)
        (let [op (r/combine-rel-op pq)]
          (if (= :product op)
            (r/make-project pa pq)
            (let [q1 (r/combine-query-1 pq)
                  q2 (r/combine-query-2 pq)
                  subst #(project-alist-substitute-attribute-refs
                          (r/project-alist %1) %2)]
              (if (and (r/project? q1)
                       (r/project? q2))
                (r/make-combine op
                                (merge-project
                                 (r/make-project (subst q1 pa)
                                                 (r/project-query q1)))
                                (merge-project
                                 (r/make-project (subst q2 pa)
                                                 (r/project-query q2))))
                (r/make-project pa pq)))))
        :else (r/make-project pa pq)))
    (r/restrict? q) (r/make-restrict (r/restrict-exp q)
                                     (merge-project (r/restrict-query q)))
    (r/order? q) (r/make-order (r/order-alist q)
                               (merge-project (r/order-query q)))
    (r/top? q) (r/make-top (r/top-count q) (merge-project (r/top-query q)))
    (r/combine? q) (r/make-combine (r/combine-rel-op q)
                                   (merge-project (r/combine-query-1 q))
                                   (merge-project (r/combine-query-2 q)))
    (r/grouping-project? q)
    (let [pq (merge-project (r/grouping-project-query q))
          pa (r/grouping-project-alist q)]
      (if (r/project? pq)
        (r/make-grouping-project
         (project-alist-substitute-attribute-refs (r/project-alist pq) pa)
         (r/project-query pq))
        (r/make-grouping-project pa pq)))
    :else (throw (Exception. (str 'merge-project ": unknown query " q)))))

(defn push-restrict
  [q]
  (cond
    (nil? q) q
    (r/base-relation? q) q
    (r/project? q) (r/make-project (r/project-alist q)
                                   (push-restrict (r/project-query q)))
    (r/restrict? q)
    (let [rq (r/restrict-query q)
          re (r/restrict-exp q)]
      (cond
        (and (r/project? rq)
             (not (r/aggregate? re)))
        (let [alist (r/project-alist rq)]
          (r/make-project
           alist
           (push-restrict
            (r/make-restrict (r/substitute-attribute-refs alist re)
                             (r/project-query rq)))))
        (r/combine? rq)
        (let [op (r/combine-rel-op rq)
              q1 (r/combine-query-1 rq)
              q2 (r/combine-query-2 rq)
              attrs (r/expression-attribute-names re)]
          (cond
            (and (not (= :difference op)
                      (= :quotient op)
                      (not-empty
                       (filter (fn [[k v]]
                                 (contains? attrs k)) (query->alist q1)))))
            (r/make-combine op q1 (push-restrict (r/make-restrict re q2)))
            (not-empty (filter (fn [[k v]] (contains? attrs k))
                               (query->alist q2)))
            (r/make-combine op (push-restrict (r/make-restrict re q1) q2))
            :else (r/make-restrict re (push-restrict rq))))
        (r/restrict? rq) (let [pushed (push-restrict rq)]
                           (if (r/restrict? pushed)
                             (r/make-restrict re pushed)
                             (push-restrict (r/make-restrict re pushed))))
        (r/order? rq) (r/make-order (r/order-alist rq)
                                    (push-restrict
                                     (r/make-restrict re (r/order-query rq))))))
    (r/order? q)
    (let [oq (r/order-query q)
          alist (r/order-alist q)]
      (cond
        (r/project? oq) (let [palist (r/project-alist oq)
                              new-alist
                              (into
                               {}
                               (map
                                (fn [[k v]]
                                  [(r/substitute-attribute-refs palist v) v])))]
                          (if (not-empty (filter (fn [[k v]] (r/aggregate? k))
                                                 new-alist))
                            (r/make-order alist (push-restrict oq))
                            (r/make-project
                             palist
                             (push-restrict (r/make-order new-alist oq)))))
        (r/order? oq) (let [pushed (push-restrict oq)
                            new (r/make-order alist pushed)]
                        (if (r/order? pushed)
                          new
                          (push-restrict new)))
        (r/top? oq) (let [pushed (push-restrict oq)
                          new (r/make-order alist pushed)]
                      (if (r/top? pushed)
                        new
                        (push-restrict new)))
        :else (r/make-order alist (push-restrict oq))))
    (r/top? q)
    (let [tq (r/top-query q)
          count (r/top-count q)]
      (cond
        (r/project? tq)
        (let [passoc (r/project-alist tq)]
          (if (not-empty (filter (fn [[k v]] (r/aggregate? v)) passoc))
            (r/make-top count (push-restrict tq))
            (r/make-project
             passoc
             (push-restrict (r/make-top count (r/project-query tq))))))
        (r/order? tq) (let [pushed (push-restrict tq)
                            new (r/make-top count pushed)]
                        (if (r/order? pushed)
                          new
                          (push-restrict new)))
        (r/top? tq) (let [pushed (push-restrict tq)
                          new (r/make-top count pushed)]
                      (if (r/top? pushed)
                        new
                        (push-restrict new)))
        :else (r/make-top count (push-restrict tq))))
    (r/combine? q) (r/make-combine (r/combine-rel-op q)
                                   (push-restrict (r/combine-query-1 q))
                                   (push-restrict (r/combine-query-2 q)))
    (r/grouping-project? q)
    (r/make-grouping-project (r/grouping-project-alist q)
                             (push-restrict (r/grouping-project-query q)))
    :else (throw (Exception. (str 'push-restrict ": unknown query " q)))))

(defn optimize-query
  "Takes a query and performs some optimizations."
  [q]
  (-> q
      push-restrict
      merge-project))
