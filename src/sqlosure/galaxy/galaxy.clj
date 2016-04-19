(ns sqlosure.galaxy.galaxy
  (:require [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :as c]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.type :as t]
            [sqlosure.db-connection :as db]))

(define-record-type tuple
  (make-tuple expressions) tuple?
  [expressions tuple-expressions])

;; DONE
(define-record-type db-galaxy
  (really-make-db-galaxy name type setup-fn query) db-galaxy?
  [name db-galaxy-name
   type db-galaxy-type
   ^{:doc "Takes a db-connection, sets up virgin DB tables."}
   setup-fn db-galaxy-setup-fn
   query db-galaxy-query])

;; DONE
(def *db-galaxies* (atom {}))

;; TODO which universe?
(defn make&install-db-galaxy
  [name type setup-fn query]
  (let [dg (really-make-db-galaxy name type setup-fn query)
        rel (rel/make-base-relation name
                                    (rel/alist->rel-scheme {name type})
                                    sql/sql-universe
                                    dg)]
    (swap! *db-galaxies* assoc name rel)
    rel))

;; DONE
(defn initialize-db-galaxies!
  "Takes a connection and installs all galaxies currently stored in
  *db-galaxies* to the database."
  [conn]
  (dorun (map (fn [glxy]
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
  [name pred const->datum-fn datum->const-fn scheme reifier
   value->db-expression-fn
   & {:keys [ordered? numeric?]
      :or [ordered? false numeric? false]}]
  (t/make-base-type name pred const->datum-fn datum->const-fn
                    :universe sql/sql-universe
                    (make-db-type-data scheme reifier value->db-expression-fn)))

;; DONE
(define-record-type db-operator-data
  (make-db-operator-data base-query transformer-fn) db-operator-data?
  [base-query db-operator-data-base-query
   transformer-fn db-operator-data-transformer-fn])

;; DONE
(defn make-name-generator
  [prefix]
  (let [count (atom 0)]
    (fn []
      (let [c @count]
        (swap! count inc)
        (str prefix "_" c)))))

;; DONE
(defn list->product
  [ql]
  (if (empty? ql)
    (rel/the-empty)
    (rel/make-product (first ql)
                      (list->product (rest ql)))))

;; DONE
(defn apply-restrictions
  [rl q]
  (if (empty? rl)
    q
    (apply-restrictions (rest rl)
                        (rel/make-restrict (first rl) q))))

;; DONE
(defn restrict-to-scheme
  [scheme q]
  (rel/make-project
   (map (fn [k]
          [k (rel/make-attribute-ref
              (get (rel/rel-scheme-map scheme) k))])
        (rel/rel-scheme-columns scheme))
   q))

;; DONE
(defn make-new-names
  "Takes a string `base` and a list `lis` and returns a list of
  '(\"base_0\", ..., \"base_n\") where = `(count lis)`."
  [base lis]
  (loop [i 0
         lis lis
         rev '()]
    (if (empty? lis)
      (reverse rev)
      (recur (inc i) (rest lis) (cons (str base "_" i) rev)))))

(declare dbize-project dbize-expression)

;; TODO restrict-outer
(defn dbize-query*
  [q generate-name]
  (letfn
      [(worker [q generate-name]
         (cond
           (rel/empty-query? q) rel/the-empty-rel-scheme
           (rel/base-relation? q)
           (let [handle (rel/base-relation-handle q)]
             (if (db-galaxy? handle)
               (let [db-query (db-galaxy-query handle)
                     name (db-galaxy-name handle)
                     cols (rel/rel-scheme-columns (rel/query-scheme db-query))
                     new-names (make-new-names name cols)]
                 [(rel/make-project (map (fn [k new-name]
                                           [new-name (rel/make-attribute-ref k)])
                                         cols new-names))
                  (list
                   (cons name
                         (make-tuple (map rel/make-attribute-ref new-names))))])
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

;; TODO with a high possibility of errors.
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
(defn rename-query [q generate-name]
  (let [cols (rel/rel-scheme-columns (rel/query-scheme q))
        names (map #((generate-name)) cols)]
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
    (letfn [(worker [e]
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
                     '())
                    e))
                (rel/application? e)
                (let [rator (rel/application-rator e)
                      data (rel/rator-data rator)
                      base-query (db-operator-base-query data)
                      rands (rel/application-rands e)
                      initiate
                      (fn []
                        (apply (db-operator-data-transformer-fn data)
                               (concat
                                (map worker rands)
                                (map
                                 #(rel/expression-type underlying-scheme %)
                                 rands))))]
                  (if base-query
                    (let [[restriction-fn transform] (initiate)
                          base-query-refs (if base-query
                                            (let [[base-query-refs renamed-base-query]
                                                  (rename-query base-query generate-name)]
                                              (reset! base-queries (cons renamed-base-query @base-queries))
                                              base-query-refs)
                                            '())]
                      (when restriction-fn
                        (reset! restrictions (cons (apply restriction-fn base-query-refs) restrictions)))
                      (apply transform base-query-refs))
                    (initiate)))
                (tuple? e)
                (make-tuple (map worker (tuple-expressions e)))
                (rel/aggregation? e)
                (let [expr (worker (rel/aggregation-expr e))]
                  (if (= :count (rel/aggregation-operator e))
                    (rel/make-aggregation :count
                                          (loop [expr expr]
                                            (if (tuple? expr)
                                              (recur (first (tuple-expressions expr)))  ;; doesn't matter
                                              expr)))))
                (rel/case-expr? e)
                ;; FIXME: this is incomplete: we should commute this
                ;; with tuples inside the case if the result type is a
                ;; DB type
                (rel/make-case-expr (map (fn [[k v]]
                                           [(worker k) (worker v)])
                                         (rel/case-expr-alist ))
                                    (worker (rel/case-expr-default e)))
                :else (c/assertion-violation `dbize-expression
                                             "unknown expression" e)))]
      (let [dbized (worker e)]
        [dbized @base-queries @restrictions]))))
