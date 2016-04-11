(ns sqlosure.relational-algebra-sql
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.type :as t]
            [clojure.set :as set]
            [active.clojure.condition :as c]
            [active.clojure.lens :as lens]))

(defn x->sql-select
  [sql]
  (cond
    (sql/sql-select-empty? sql) (sql/new-sql-select)

    (sql/sql-select-table? sql)
    (lens/shove (sql/new-sql-select) sql/sql-select-tables-lens [[nil sql]])

    (sql/sql-select-combine? sql)
    (lens/shove (sql/new-sql-select) sql/sql-select-tables-lens [[nil sql]])

    (sql/sql-select? sql)
    (cond
      (nil? (sql/sql-select-attributes sql)) sql

      (some? (sql/sql-select-group-by sql))
      (lens/shove (sql/new-sql-select) sql/sql-select-tables-lens [[nil sql]])

       :else
      (-> (sql/new-sql-select)
          (lens/shove sql/sql-select-tables-lens
                      [[nil (lens/shove sql sql/sql-select-group-by-lens nil)]])
          (lens/shove sql/sql-select-group-by-lens (sql/sql-select-group-by sql))))))

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
    :else (c/assertion-violation `expression->sql": unknown expression " expr)))

(defn project-alist->sql
  "Takes a projection alist and returns a corresponding sql statement."
  [alist]
  (map (fn [[k v]] [k (expression->sql v)]) alist))

(defn add-table
  "Takes an sql-select statement and adds a table to its select-tables list."
  [sql q]
  (lens/shove sql sql/sql-select-tables-lens
              (conj (sql/sql-select-tables sql) [nil q])))

(defn project->sql
  "Takes a project query and returns the abstract Sql representation."
  [q]
  (let [alist (rel/project-alist q)]
    (lens/shove
     (lens/shove (x->sql-select (query->sql (rel/project-query q)))
                 sql/sql-select-attributes-lens
                 (project-alist->sql alist))
     sql/sql-select-nullary?-lens
     (empty? alist))))

(defn query->sql
  "Takes a query in abstract relational algegbra and returns the corresponding
  abstract sql."
  [q]
  (cond
    (rel/empty-query? q) (sql/the-sql-select-empty)
    (rel/base-relation? q)
    (if-not (sql/sql-table? (rel/base-relation-handle q))
      (c/assertion-violation `query->sql "base relation not a SQL table" q)
      ;; FIXME: results in select * from, but should select in the order of column in rel:
      (sql/make-sql-select-table (sql/sql-table-name (rel/base-relation-handle q))))
    (rel/project? q) (project->sql q)
    (rel/restrict? q) (let [sql (x->sql-select (query->sql
                                                (rel/restrict-query q)))
                            exp (rel/restrict-exp q)]
                        (if (rel/aggregate? exp)
                          (lens/shove sql sql/sql-select-having-lens
                                      (cons (expression->sql exp)
                                            (sql/sql-select-having sql)))
                          (lens/shove sql sql/sql-select-criteria-lens
                                      (cons (expression->sql exp)
                                            (sql/sql-select-criteria sql)))))
    (rel/restrict-outer? q) (let [sql (x->sql-select (query->sql
                                                      (rel/restrict-outer-query q)))]
                              (lens/shove sql sql/sql-select-outer-criteria-lens
                                          (cons (expression->sql (rel/restrict-outer-exp q))
                                                (sql/sql-select-outer-criteria sql))))
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
            (lens/shove (sql/new-sql-select) sql/sql-select-tables-lens
                        [[nil sql1]
                         [nil sql2]])))

        :left-outer-product
        (let [sql1 (query->sql q1)
              sql2 (query->sql q2)]
          ;; we must not simply add sql2 to sql1's outer tables,
          ;; as this might trigger x->sql (called in the surrounding
          ;; make-restrict-outer) to wrap another SQL query around the
          ;; whole thing, thus moving the ON to a place where it's invalid
          (-> (sql/new-sql-select)
              (lens/shove sql/sql-select-tables-lens [[nil sql1]])
              (lens/shove sql/sql-select-outer-tables-lens [[nil sql2]])))

        :quotient
        (let [scheme-1 (rel/query-scheme q1)
              scheme-2 (rel/query-scheme q2)
              diff-scheme (rel/rel-scheme-difference scheme-1 scheme-2)]
          (if (rel/rel-scheme-unary? scheme-2)
            (let [sql1 (query->sql q1)
                  sql2 (query->sql q2)
                  name-2 (first (rel/rel-scheme-columns scheme-2))
                  diff-columns (rel/rel-scheme-columns diff-scheme)
                  sql (sql/new-sql-select)]
              (-> sql
                  (add-table sql1)
                  (lens/shove sql/sql-select-attributes-lens
                              (map
                               (fn [k] [k (sql/make-sql-expr-column k)]) diff-columns))
                  (lens/shove sql/sql-select-criteria-lens
                              (list (sql/make-sql-expr-app
                                     sql/op-in
                                     (sql/make-sql-expr-column name-2)
                                     (sql/make-sql-expr-subquery sql2))))
                  (lens/shove sql/sql-select-group-by-lens
                              (map
                               (fn [k] (sql/make-sql-expr-column k)) diff-columns))
                  (lens/shove sql/sql-select-having-lens
                              [(sql/make-sql-expr-app
                                sql/op-=
                                (sql/make-sql-expr-app
                                 sql/op-count
                                 (sql/make-sql-expr-column
                                  (first diff-columns)))
                                (sql/make-sql-expr-subquery
                                 (let [sql* (sql/new-sql-select)]
                                   (-> sql*
                                       (add-table sql2)
                                       (lens/shove sql/sql-select-attributes-lens
                                                   (list [nil (sql/make-sql-expr-app
                                                               sql/op-count
                                                               (sql/make-sql-expr-column name-2))]))))))])))

            (let [diff-project-alist (map (fn [k] [k (rel/make-attribute-ref k)])
                                          (rel/rel-scheme-columns diff-scheme))
                  q1-project-alist (map (fn [k] [k (rel/make-attribute-ref k)])
                                        (rel/rel-scheme-columns scheme-1))
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
      (lens/shove sql sql/sql-select-order-by-lens
                  (concat new-order (sql/sql-select-order-by sql))))


    (rel/group? q)
    (let [sql (x->sql-select (query->sql (rel/group-query q)))]
      (lens/shove sql sql/sql-select-group-by-lens
                  (set/union (sql/sql-select-group-by sql)
                             (rel/group-columns q))))
    
    (rel/top? q)
    (let [sql (x->sql-select (query->sql (rel/top-query q)))]
      (lens/shove sql sql/sql-select-extra-lens
                  (cons (if-let [off (rel/top-offset q)]
                          ;; TODO: there are a lot of different syntax versions for this
                          ;; (http://stackoverflow.com/questions/1528604/how-universal-is-the-limit-statement-in-sql)
                          (str "LIMIT " (rel/top-count q) " OFFSET " off)
                          ;; no offset should be the same as offset 0 though:
                          (str "LIMIT " (rel/top-count q)))
                        (sql/sql-select-extra sql))))
    (rel/empty-query? q) sql/the-sql-select-empty
    :else (c/assertion-violation `query->sql "unknown query" (pr-str q))))
