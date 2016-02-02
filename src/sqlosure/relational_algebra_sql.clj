(ns sqlosure.relational-algebra-sql
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.type :as t]
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

(defn aggregation-op->sql
  "Takes an op keyword and returns the corresponding sql-op. If there is no
  sql-op, return the name of the keyword."
  [op]
  (case op
    :count sql/op-count
    :count-all sql/op-count-all
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
    (rel/const? expr) (sql/make-sql-expr-const (rel/const-type expr) (rel/const-val expr))
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
    (rel/aggregation*? expr) (sql/make-sql-expr-app
                              (aggregation-op->sql
                               (rel/aggregation*-operator expr))
                              (sql/make-sql-expr-column "*"))
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
    :else (c/assertion-violation 'expression->sql": unknown expression " expr)))

(defn alist->sql
  "Takes a map and returns a corresponding sql statement."
  [alist]
  (into {} (map (fn [[k v]] [k (expression->sql v)])) alist))

(defn add-table
  "Takes an sql-select statement and adds a table to its select-tables list."
  [sql q]
  (sql/set-sql-select-tables sql
                             (conj (sql/sql-select-tables sql) [nil q])))

(defn add-left-outer-table
  "Takes an sql-select statement and adds an outer (join) table to its select-tables list."
  [sql q]
  (sql/set-sql-select-outer-tables sql
                                   (conj (sql/sql-select-outer-tables sql) [nil q])))

(defn fold-sql-expression
  [on-column on-app on-const on-tuple on-case on-exists on-subquery expr]
  (cond
    (sql/sql-expr-column? expr) (on-column (sql/sql-expr-column-name expr))
    (sql/sql-expr-app? expr) (let [rator (sql/sql-expr-app-rator expr)
                                   rands (sql/sql-expr-app-rands expr)]
                               (on-app rator (map fold-sql-expression rands)))
    (sql/sql-expr-const? expr) (let [t (sql/sql-expr-const-type expr)
                                     v (sql/sql-expr-const-val expr)]
                                 (on-const t v))
    (sql/sql-expr-tuple? expr) (on-tuple (map fold-sql-expression (sql/sql-expr-tuple-expressions expr)))
    (sql/sql-expr-case? expr) (let [branches (sql/sql-expr-case-branches expr)
                                    default (sql/sql-expr-case-default expr)]
                                (on-case (map (fn [[k v]]
                                                [(fold-sql-expression k) (fold-sql-expression v)])
                                              branches)
                                         (fold-sql-expression default)))
    (sql/sql-expr-exists? expr) (let [select (sql/sql-expr-exists-select expr)]
                                  (on-exists select))
    (sql/sql-expr-subquery? expr) (let [sub (sql/sql-expr-subquery-query expr)]
                                    (on-subquery sub))
    :else (c/assertion-violation `fold-sql-expression "invalid sql expression" expr)))

(defn groupable?
  "Takes a Sql-expression and determines if the selection should be grouped.
  Expressions are groupable if there are only columns, non-aggregates or
  expressions that contain only such values. If an expression contains any
  groupable values then the whole expression is groupable."
  [expr]
  (fold-sql-expression
   (constantly true) ;; column
   (constantly true) ;; app
   (constantly false) ;; const
   (fn [exprs] (reduce (fn [acc b] (and acc b)) exprs)) ;; tuple
   (fn [branches default] (and (map (fn [[k v]] (or k v)) branches) default)) ;; case
   (constantly false) ;; exists
   ;; subquery
   identity
   expr))

(defn groupables
  "Takes a Sql-expression and returns all groupable attribues."
  [expr]
  (if (sql/sql-select? expr)
    (filter groupable? (map second (sql/sql-select-attributes expr)))
    (c/assertion-violation `groupables "invalid expr" expr)))

(defn substitute-expr
  "Taḱes a map of aliases and substitutes all column aliases with their values."
  [aliases expr]
  (fold-sql-expression
   (fn [e] (sql/make-sql-expr-column (get aliases e e))) ;; column
   sql/make-sql-expr-app ;; app
   sql/make-sql-expr-const ;; const
   sql/make-sql-expr-tuple ;; tuple
   sql/make-sql-expr-case ;; case
   sql/make-sql-expr-exists ;; exists
   sql/make-sql-expr-subquery;; subquery
   expr))

(defn substitute-group
  [aliases group-attrs]
  (when group-attrs
    (into {} (map (fn [[col expr]] [(get aliases col) expr]) group-attrs))))

(defn substitute
  "Rename projected columns in a select. Since we did not create another layer
  of SELECT, we have to propagate the associtation list provided in the current
  query or it will not create columns with the right names."
  [alist select]
  (if (sql/sql-select? select)
    (let [attrs (sql/sql-select-attributes select)
          criteria (sql/sql-select-criteria select)
          groupby (sql/sql-select-group-by select)
          orderby (sql/sql-select-order-by select)
          aliases (into {} (map (fn [[alias col]] [(rel/attribute-ref-name col) alias]) alist))]
      (-> select
          (sql/set-sql-select-attributes (map (fn [[curr-col expr]] [(get aliases curr-col curr-col) expr])
                                              attrs))
          (sql/set-sql-select-criteria (map (fn [c] (substitute-expr aliases c)) criteria))
          (sql/set-sql-select-group-by (substitute-group aliases groupby))
          (sql/set-sql-select-order-by (map (fn [[expr ord]] [(substitute-expr aliases expr) ord]) attrs))))
    (c/assertion-violation `substitute "invalid expr" select)))
(defn has-aggregations?
  "Takes an alist and checks if there are any aggregations on the right sides."
  [alist]
  (not= 0 (count (filter rel/aggregate? (map second alist)))))
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
                              ;; FIXME: what type is this dummy?
                              {"dummy" (sql/make-sql-expr-const t/any% "dummy")})
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
      (sql/set-sql-select-extra sql (cons (if-let [off (rel/top-offset q)]
                                            ;; TODO: there are a lot of different syntax versions for this
                                            ;; (http://stackoverflow.com/questions/1528604/how-universal-is-the-limit-statement-in-sql)
                                            (str "LIMIT " (rel/top-count q) " OFFSET " off)
                                            ;; no offset should be the same as offset 0 though:
                                            (str "LIMIT " (rel/top-count q)))
                                          (sql/sql-select-extra sql))))
    (rel/empty-val? q) sql/the-sql-select-empty
    :else (c/assertion-violation 'query->sql "unknown query" (pr-str q))))
