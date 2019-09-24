(ns sqlosure.optimization
  (:require [sqlosure.relational-algebra :as r]
            [sqlosure.relational-algebra-sql :as rs]
            [active.clojure.condition :as c]
            [active.clojure.lens :as lens]
            [clojure.set :as set]))

(defn project-alist-substitute-attribute-refs
  "Takes an alist and a project query's alist and substitutes all of the
  latter's refs."
  [alist palist]
  (map (fn [[k v]] [k (r/substitute-attribute-refs (into {} alist) v)]) palist))

(defn query->alist
  "Return the rel-scheme-map of a query's query-scheme."
  [q]
  (-> q r/query-scheme r/rel-scheme-map))

(defn remove-dead
  "Takes a query and removes all references to variables in underlying queries
  that are not used/unnecessary further up the query."
  [q]
  (letfn
      [(worker [live q]
         (assert (set? live))
         (cond
           (or (r/empty-query? q) (r/base-relation? q))
           q

           (r/project? q)
           (let [new-alist (filter (fn [[k _]] (contains? live k)) (r/project-alist q))]
             (r/make-project new-alist
                             ;; live variables === values of this project's alist.
                             (worker (apply set/union (map (fn [[_ v]] (r/expression-attribute-names v)) new-alist))
                                     (r/project-query q))))

           (r/restrict? q)
           (let [e (r/restrict-exp q)]
             (r/make-restrict
              e
              (worker (set/union (r/expression-attribute-names e) live)
                      (r/restrict-query q))))

           (r/restrict-outer? q)
           (let [e (r/restrict-outer-exp q)]
             (r/make-restrict-outer
              e
              (worker (set/union (r/expression-attribute-names e) live)
                      (r/restrict-outer-query q))))

           (r/order? q)
           (let [alist (r/order-alist q)]
             (r/make-order
              alist
              (worker (set/union (apply set/union (map (fn [[k _]] (r/expression-attribute-names k)) alist))
                                 live)
                      (r/order-query q))))

           (r/group? q)
           (r/make-group
            (set/intersection live (r/group-columns q))
            (worker live (r/group-query q)))

           (r/top? q)
           (r/make-top (r/top-offset q)
                       (r/top-count q)
                       (worker live (r/top-query q)))

           (r/distinct-q? q)
           (r/make-distinct (worker live (r/distinct-q-query q)))

           (r/combine? q)
           (let [r  (r/combine-rel-op q)
                 q1 (r/combine-query-1 q)
                 q2 (r/combine-query-2 q)]
             (let [live1 (set/intersection live (into #{} (r/rel-scheme-columns (r/query-scheme q1))))]
               (case r
                 (:product :left-outer-product)
                 (let [live2 (set/intersection live (into #{} (r/rel-scheme-columns (r/query-scheme q2))))]
                   (r/make-combine r
                                   (worker live1 q1)
                                   (worker live2 q2)))

                 :quotient
                 (r/make-combine r
                                 (worker (into #{} (r/rel-scheme-columns (r/query-scheme q1))) q1)
                                 (worker (into #{} (r/rel-scheme-columns (r/query-scheme q2))) q2))

                 (r/make-combine r
                                 (worker live1 q1)
                                 (worker live1 q2)))))))]
    (if-not (r/query? q)
      (c/assertion-violation `remove-dead "unknown query" q)
      (worker (into #{} (r/rel-scheme-columns (r/query-scheme q))) q))))

(defn merge-project
  [q]
  (cond
    (or (r/empty-query? q) (r/base-relation? q))
    q

    (r/project? q)
    (let [pq (merge-project (r/project-query q))
          pa (r/project-alist q)]
      (cond
        (r/project? pq)
        (if (or
             ;; (project [... agg ....] (project [...] (group [...] ...)))
             ;; ... is not the same as (project [... agg ...] (group [...] ...))
             (r/project-aggregate? q)
             (r/project-aggregate? pq)) 
          (r/make-project pa pq)
          (r/make-project (project-alist-substitute-attribute-refs (r/project-alist pq) pa)
                          (r/project-query pq)))

        (r/combine? pq)
        (let [op (r/combine-rel-op pq)]
          (if (or (= op :product)
                  (= op :left-outer-product))
            (r/make-project pa pq)
            (let [q1    (r/combine-query-1 pq)
                  q2    (r/combine-query-2 pq)
                  subst #(project-alist-substitute-attribute-refs
                          (into {} (r/project-alist %1)) %2)]
              (if (and (r/project? q1)
                       (r/project? q2))
                (r/make-combine op
                                (merge-project
                                 (r/make-project (subst q1 pa)
                                                 (r/project-query q1)))
                                (merge-project
                                 (r/make-project (subst q2 pa)
                                                 (r/project-query q2))))
                (r/make-project pa (merge-project pq))))))

        :else
        (r/make-project pa (merge-project pq))))

    (r/restrict? q)
    (r/make-restrict (r/restrict-exp q)
                     (merge-project (r/restrict-query q)))

    (r/restrict-outer? q)
    (r/make-restrict-outer (r/restrict-outer-exp q)
                           (merge-project (r/restrict-outer-query q)))

    (r/order? q)
    (r/make-order (r/order-alist q)
                  (merge-project (r/order-query q)))

    (r/group? q)
    (r/make-group (r/group-columns q)
                  (merge-project (r/group-query q)))

    (r/top? q)
    (r/make-top (r/top-offset q) (r/top-count q) (merge-project (r/top-query q)))

    (r/distinct-q? q)
    (r/make-distinct (merge-project (r/distinct-q-query q)))

    (r/combine? q)
    (r/make-combine (r/combine-rel-op q)
                    (merge-project (r/combine-query-1 q))
                    (merge-project (r/combine-query-2 q)))

    :else
    (c/assertion-violation `merge-project "unknown query" q)))

(declare push-restrict)

(defn push-restrict-restrict*
  [q]
  (let [is-restrict? (r/restrict? q)
        yank-query   (if is-restrict? r/restrict-query r/restrict-outer-query)
        yank-exp     (if is-restrict? r/restrict-exp r/restrict-outer-exp)
        rq           (lens/yank q yank-query)
        re           (lens/yank q yank-exp)
        maker        (if is-restrict? r/make-restrict r/make-restrict-outer)]
    (cond
      (and (r/project? rq)
           (not (r/aggregate? re)))
      (let [alist (r/project-alist rq)
            tail  (push-restrict
                   (maker (r/substitute-attribute-refs (into {} alist) re)
                          (r/project-query rq)))]
        (r/make-project alist tail))

      (r/combine? rq)
      (let [op    (r/combine-rel-op rq)
            q1    (r/combine-query-1 rq)
            q2    (r/combine-query-2 rq)
            attrs (r/expression-attribute-names re)]
        (cond
          (and (not= :difference op)
               (not= :quotient op)
               (not-any? (fn [[k v]]
                           (contains? attrs k))
                         (query->alist q1)))
          (r/make-combine op q1 (push-restrict (maker re q2)))

          (not-any? (fn [[k v]] (contains? attrs k)) (query->alist q2))
          (r/make-combine op (push-restrict (maker re q1)) q2)

          :else
          (maker re (push-restrict rq))))

      (r/restrict? rq)
      (let [pushed (push-restrict rq)]
        (if (r/restrict? pushed)
          (maker re pushed)
          (push-restrict (maker re pushed))))

      (r/restrict-outer? rq)
      (let [pushed (push-restrict rq)]
        (if (r/restrict-outer? pushed)
          (maker re pushed)
          (push-restrict (maker re pushed))))

      (r/order? rq)
      (r/make-order (r/order-alist rq)
                    (push-restrict
                     (maker re (r/order-query rq))))

      (r/group? rq)
      (r/make-group (r/group-columns rq)
                    (push-restrict
                     (maker re (r/group-query rq))))

      :else
      (maker re (push-restrict rq)))))

(defn push-restrict-order
  [q]
  {:pre [(r/order? q)]}
  (let [oq    (r/order-query q)
        alist (r/order-alist q)]
    (cond
      (r/project? oq)
      (let [palist     (r/project-alist oq)
            palist-map (into {} palist)
            new-alist  (map (fn [[k v]]
                              [(r/substitute-attribute-refs palist-map k) v])
                            alist)
            has-aggregations? (reduce (fn [acc [k v]]
                                        (or acc (r/aggregate? k)))
                                      false
                                      new-alist)]
        (if has-aggregations?
          (r/make-order alist (push-restrict oq))
          (r/make-project palist
                          (push-restrict
                           (r/make-order new-alist (r/project-query oq))))))

      (r/order? oq)
      (let [pushed (push-restrict oq)
            new    (r/make-order alist pushed)]
        (if (r/order? pushed)
          new
          (push-restrict new)))

      (r/top? oq)
      (let [pushed (push-restrict oq)
            new    (r/make-order alist pushed)]
        (if (r/top? pushed)
          new
          (push-restrict new)))

      :else
      (r/make-order alist (push-restrict oq)))))

(defn push-restrict-top
  [q]
  {:pre [(r/top? q)]}
  (let [tq     (r/top-query q)
        offset (r/top-offset q)
        count  (r/top-count q)]
    (cond
      (r/project? tq)
      (let [passoc            (r/project-alist tq)
            has-aggregations? (reduce (fn [acc [k v]] (or acc (r/aggregate? v)))
                                      false
                                      passoc)]
        (if has-aggregations?
          (r/make-top offset count (push-restrict tq))
          (r/make-project
           passoc
           (push-restrict (r/make-top offset count (r/project-query tq))))))

      (r/order? tq)
      (let [pushed (push-restrict tq)
            new    (r/make-top offset count pushed)]
        (if (r/order? pushed)
          new
          (push-restrict new)))

      (r/top? tq)
      (let [pushed (push-restrict tq)
            new    (r/make-top offset count pushed)]
        (if (r/top? pushed)
          new
          (push-restrict new)))

      :else
      (r/make-top offset count (push-restrict tq)))))

(defn push-restrict
  [q]
  {:post [(some? %)]}
  (cond
    (r/empty-query? q)   q
    (r/base-relation? q) q

    (r/project? q)
    (r/make-project (r/project-alist q) (push-restrict (r/project-query q)))

    (or (r/restrict? q) (r/restrict-outer? q))
    (push-restrict-restrict* q)

    (r/order? q)
    (push-restrict-order q)

    (r/group? q)
    (r/make-group (r/group-columns q) (push-restrict (r/group-query q)))

    (r/top? q)
    (push-restrict-top q)
 
    (r/distinct-q? q)
    (r/make-distinct (push-restrict (r/distinct-q-query q)))

    (r/combine? q)
    (r/make-combine (r/combine-rel-op q)
                    (push-restrict (r/combine-query-1 q))
                    (push-restrict (r/combine-query-2 q)))

    :else
    (c/assertion-violation `push-restrict "unknown query" q)))

(defn optimize-query
  "Takes a query and performs some optimizations."
  [q]
  (-> q
      push-restrict
      remove-dead
      merge-project))
