(ns sqlosure.relational-algebra-sql
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [active.clojure.condition :as c]))

(defn x->sql-select
  "Takes a sql expression and turns it into a sql-select."
  [sql]
  (cond
    (sql/sql-select-empty? sql) (sql/new-sql-select)
    (and (sql/sql-select? sql)
         (empty? (sql/sql-select-attributes sql))) sql
    :else (let [new (sql/new-sql-select)]
            (sql/set-sql-select-tables new [[nil sql]]))))

(defn ^{:test true} aggregation-op->sql
  "Takes an op keyword and returns the corresponding sql-op. If there is no
  sql-op, return the name of the keyword."
  [op]
  (case op
    :count sql/op-count
    :sum sql/op-sum
    :avg sql/op-avg
    :min sql/op-min
    :max sql/op-max
    :std-dev sql/op-std-dev
    :std-dev-p sql/op-std-dev-p
    :var sql/op-var
    :var-p sql/op-var-p
    (name op)))

(declare query->sql)

(defn expression->sql
  "Takes a relational algebra expression and returns the corresponding
  sql-statement."
  [expr]
  (cond
    (rel/attribute-ref? expr) (sql/make-sql-expr-column
                               (rel/attribute-ref-name expr))
    (rel/const? expr) (sql/make-sql-expr-const (rel/const-val expr))
    (rel/application? expr) (apply sql/make-sql-expr-app
                                   (rel/rator-data (rel/application-rator expr))
                                   (map expression->sql
                                        (rel/application-rands expr)))
    (rel/tuple? expr) (sql/make-sql-expr-tuple
                       (map expression->sql (rel/tuple-expressions expr)))
    (rel/aggregation? expr) (sql/make-sql-expr-app
                             (aggregation-op->sql
                              (rel/aggregation-operator expr))
                             (expression->sql (rel/aggregation-expr expr)))
    (rel/case-expr? expr) (sql/make-sql-expr-case
                           (into {} (map (fn [[k v]]
                                           [(expression->sql k)
                                            (expression->sql v)])
                                         (rel/case-expr-alist expr)))
                           (expression->sql (rel/case-expr-default expr)))
    (rel/scalar-subquery? expr) (sql/make-sql-expr-subquery
                                 (query->sql (rel/scalar-subquery-query expr)))
    (rel/set-subquery? expr) (sql/make-sql-expr-subquery
                              (query->sql (rel/set-subquery-query expr)))
    :else (c/assertion-violation (str 'expression->subquery ": unknown expression " expr))))

(defn ^{:test true} alist->sql
  "Takes a map and returns a corresponding sql statement."
  [alist]
  (into {} (map (fn [[k v]] [k (expression->sql v)])) alist))

(defn ^{:test true} add-table
  "Takes an sql-select statement and adds a table to its select-tables list."
  [sql q]
  (sql/set-sql-select-tables sql
                             (conj (sql/sql-select-tables sql) [nil q])))

(defn ^{:test true} add-left-outer-table
  "Takes an sql-select statement and adds an outer (join) table to its select-tables list."
  [sql q]
  (sql/set-sql-select-outer-tables sql
                                   (conj (sql/sql-select-outer-tables sql) [nil q])))

(defn query->sql
  "Takes a query in abstract relational algegbra and returns the corresponding
  abstract sql."
  [q]
  (cond
    (rel/base-relation? q)
    (if-not (sql/sql-table? (rel/base-relation-handle q))
      (c/assertion-violation 'query->sql "base relation not a SQL table" q)
      (sql/make-sql-select-table (sql/sql-table-name (rel/base-relation-handle q))))
    (rel/project? q) (let [sql (x->sql-select (query->sql
                                               (rel/project-query q)))
                           alist (rel/project-alist q)]
                       (if (empty? alist)
                         (-> sql
                             (sql/set-sql-select-attributes
                              {"dummy" (sql/make-sql-expr-const "dummy")})
                             (sql/set-sql-select-nullary? true))
                         (sql/set-sql-select-attributes sql (alist->sql alist))))
    (rel/restrict? q) (let [sql (x->sql-select (query->sql
                                                (rel/restrict-query q)))]
                        (-> sql
                            (sql/set-sql-select-criteria
                             (cons (expression->sql (rel/restrict-exp q))
                                   (sql/sql-select-criteria sql)))))
    (rel/restrict-outer? q) (let [sql (x->sql-select (query->sql
                                                      (rel/restrict-outer-query q)))]
                              (-> sql
                                  (sql/set-sql-select-outer-criteria
                                   (cons (expression->sql (rel/restrict-outer-exp q))
                                         (sql/sql-select-outer-criteria sql)))))
    (rel/grouping-project? q)
    (let [sql (x->sql-select (query->sql (rel/grouping-project-query q)))
          alist (rel/grouping-project-alist q)]
      (-> sql
          (sql/set-sql-select-attributes (alist->sql alist))
          (sql/set-sql-select-group-by
           (concat
            (map expression->sql
                 (filter (fn [e] (not (rel/aggregate? e)))
                         (vals (rel/grouping-project-alist q))))
            (sql/sql-select-group-by sql)))))
    (rel/combine? q)
    (let [q1 (rel/combine-query-1 q)
          q2 (rel/combine-query-2 q)
          op (rel/combine-rel-op q)]
      (case op
        :product
        (let [sql1 (query->sql q1)
              sql2 (query->sql q2)]
          
          (cond
            (and (sql/sql-select? sql1) (empty? (sql/sql-select-attributes sql1)))
            (add-table sql1 sql2)
            
            (and (sql/sql-select? sql2) (empty? (sql/sql-select-attributes sql2)))
            (add-table sql2 sql1)
            
            :else
            (sql/set-sql-select-tables (sql/new-sql-select) [[nil sql1]
                                                             [nil sql2]])))

        :left-outer-product
        (let [sql1 (query->sql q1)
              sql2 (query->sql q2)]
          (if (sql/sql-select? sql1)
            (add-left-outer-table sql1 sql2)
            (-> (sql/new-sql-select)
                (sql/set-sql-select-tables [[nil sql1]])
                (sql/set-sql-select-outer-tables [[nil sql2]]))))

        :quotient
        (let [scheme-1 (rel/query-scheme q1)
              scheme-2 (rel/query-scheme q2)
              diff-scheme (rel/rel-scheme-difference scheme-1 scheme-2)]
          (if (rel/rel-scheme-unary? scheme-2)
            (let [sql1 (query->sql q1)
                  sql2 (query->sql q2)
                  name-2 (ffirst (rel/rel-scheme-alist scheme-2))
                  diff-alist (rel/rel-scheme-alist diff-scheme)
                  sql (sql/new-sql-select)]
              (-> sql
                  (add-table sql1)
                  (sql/set-sql-select-attributes
                   (map
                    (fn [[k _]] [k (sql/make-sql-expr-column k)]) diff-alist))
                  (sql/set-sql-select-criteria
                   (list (sql/make-sql-expr-app
                          sql/op-in
                          (sql/make-sql-expr-column name-2)
                          (sql/make-sql-expr-subquery sql2))))
                  (sql/set-sql-select-group-by
                   (map
                    (fn [[k _]] (sql/make-sql-expr-column k)) diff-alist))
                  (sql/set-sql-select-having
                   (sql/make-sql-expr-app
                    sql/op-=
                    (sql/make-sql-expr-app
                     sql/op-count
                     (sql/make-sql-expr-column
                      (ffirst (rel/rel-scheme-alist diff-scheme))))
                    (sql/make-sql-expr-subquery
                     (let [sql* (sql/new-sql-select)]
                       (-> sql*
                           (add-table sql2)
                           (sql/set-sql-select-attributes
                            (list [nil (sql/make-sql-expr-app
                                        sql/op-count
                                        (sql/make-sql-expr-column name-2))])))))))))
            
            (let [diff-project-alist (map (fn [[k _]] [k (rel/make-attribute-ref k)])
                                          (rel/rel-scheme-alist diff-scheme))
                  q1-project-alist (map (fn [[k _]] [k (rel/make-attribute-ref k)])
                                        (rel/rel-scheme-alist scheme-1))
                  pruned (rel/make-project diff-project-alist q1)]
              (query->sql (rel/make-difference pruned
                                               (rel/make-project diff-project-alist
                                                                 (rel/make-difference
                                                                  (rel/make-project q1-project-alist
                                                                                    (rel/make-product q2 pruned))
                                                                  q1)))))))
        
        (sql/make-sql-select-combine op (query->sql q1) (query->sql q2))))
        

    (rel/order? q)
    (let [sql (x->sql-select (query->sql (rel/order-query q)))
          new-order (map (fn [[k v]] [(expression->sql k) v])
                         (rel/order-alist q))]
      (sql/set-sql-select-order-by sql (concat new-order (sql/sql-select-order-by sql))))
    (rel/top? q)
    (let [sql (x->sql-select (query->sql (rel/top-query q)))]
      (sql/set-sql-select-extra sql (cons (str "LIMIT " (rel/top-count q))
                                          (sql/sql-select-extra sql))))
    (rel/empty-val? q) sql/the-sql-select-empty
    :else (c/assertion-violation 'query->sql "unknown query" (pr-str q))))
