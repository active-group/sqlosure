(ns ^{:doc "Printing SQL"}
    sqlosure.sql-put
  (:require [sqlosure.sql :as sql]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :refer [add-table]]
            [sqlosure.type :as t]
            [sqlosure.utils :refer [third]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.string :as s]))

(defmacro with-out-str-and-value
  "See http://stackoverflow.com/a/7151125.
  Runs the body and collects the prints as a string and the value returned.
  Returns [prints-string value]."
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*out* s#]
       (let [v# ~@body]
         (vector (str s#)
                 v#)))))

(define-record-type sql-put-parameterization
  (make-sql-put-parameterization alias-proc combine-proc)
  sql-put-parameterization?
  [alias-proc sql-put-parameterization-alias-proc
   combine-proc sql-put-parameterization-combine-proc])

(defn put-space
  "Print a single space character."
  []
  (print " "))

(defn put-padding-if-non-null
  "When lis is not empty, prepend a space to (proc lis)."
  [lis proc]
  (when (seq lis)
    (put-space)
    (proc lis)))

(defn default-put-alias
  "When alias is not nil, print \" AS alias\"."
  [alias]
  (when alias (print " AS" alias)))

(defn put-dummy-alias
  "Always print \" AS alias\", even if there is none."
  [alias]
  (print " AS"
         (or alias
             (str (gensym)))))

(defn default-put-literal
  [type val]
  (do
    (print "?")
    [[type val]]))

(defn put-literal
  "Apply `params` literal-proc to val."
  [type val]
  (default-put-literal type val))

(defn put-alias
  "Apply `params` alias-proc to the alias."
  [alias]
  (default-put-alias alias))

(declare put-sql-select-1)

(defn put-sql-select
  "Takes a sql-put-parameterization and an sql-select and attempts to print it."
  [sel]
  (cond
    (or (sql/sql-select? sel)
        (sql/sql-select-combine? sel))
    (put-sql-select-1 sel)

    (sql/sql-select-table? sel)
    (do
      (print "SELECT * FROM ")
      (print (sql/sql-select-table-name sel))
      [])
    :else
    (assertion-violation `put-sql-select (str "unhandled query " (pr-str sel)))))

(defn put-joining-infix
  "Intersperse `between` between `lis`'s elements and print via `proc`."
  [lis between proc]
  (when (seq lis)
    (doall (concat (proc (first lis))
                   (mapcat (fn [x]
                             (print between)
                             (proc x))
                           (rest lis))))))

(defn put-tables
  "Takes a sql-put-parameterization and the sql-select-tables field of a
  sql-select and prints them as a sql statement."
  [tables between]
  (put-joining-infix tables between
                     (fn [[alias select]]
                       (if (sql/sql-select-table? select)
                         (do
                           (print (sql/sql-select-table-name select))
                           (put-alias alias))
                         (let [_ (print "(")
                               v1 (put-sql-select select)
                               _ (print ")")
                               _ (put-alias alias)]
                           v1)))))

(defn default-put-combine
  [op left right]
  (let [_ (print "(")
        v1 (put-sql-select left)
        _ (print ") ")
        _ (print (case op
                   :union "UNION"
                   :intersection "INTERSECT"
                   :difference "EXCEPT"))
        _ (print " (")
        v2 (put-sql-select right)
        _ (print ")")]
    (concat v1 v2)))

(def default-sql-put-parameterization
  (make-sql-put-parameterization
   default-put-alias
   default-put-combine))

(declare put-sql-expression)

(defn put-when
  [p]
  (let [_ (print "WHEN ")
        v1 (put-sql-expression (first p))
        _ (print " THEN ")
        v2 (put-sql-expression (second p))]
    (concat v1 v2)))

(defn put-condition
  [exprs]
  (put-joining-infix exprs " AND " (fn [b] (put-sql-expression b))))

(defn put-where
  [exprs]
  (print "WHERE ")
  (put-condition exprs))

(defn put-on
  [exprs]
  (print "ON ")
  (put-condition exprs))

(defn put-group-by
  "Takes a seq of sql-expr."
  [group-by]
  (print "GROUP BY ")
  (put-joining-infix group-by ", " print))

(defn put-order-by
  "Takes a seq of [sql-expr, sql-order]."
  [order-by]
  (print "ORDER BY ")
  (put-joining-infix order-by ", " (fn [[a b]]
                                     (let [v (put-sql-expression a)]
                                       (print (case b
                                                :ascending " ASC"
                                                :descending " DESC"))
                                       v))))

(defn put-having
  [exprs]
  (print "HAVING ")
  (put-condition exprs))

(defn put-attributes
  [attributes]
  (if (empty? attributes)
    (print "*")
    (put-joining-infix attributes ", "
                       (fn [[col expr]]
                         (if (and (sql/sql-expr-column? expr)
                                  (= col (sql/sql-expr-column-name expr)))
                           (print col)
                           (let [v (put-sql-expression expr)]
                             (default-put-alias col)
                             v))))))

(defn put-sql-join
  "Put the tables involved in the join of a SQL select."
  [tables outer-tables]
  (if (or (empty? outer-tables)
          (= (count tables) 1))
    (let [t (put-padding-if-non-null tables
                                     (fn [tables]
                                       (print "FROM ")
                                       (put-tables tables ", ")))
          o (if (not-empty outer-tables)
              (do
                (print " LEFT JOIN ")
                ;; every LEFT JOIN needs an ON
                (put-tables outer-tables " ON (1=1) LEFT JOIN "))
              [])]
      (concat t o))
    ;; outer tables AND more than one regular tablew
    (let [_ (print " FROM (SELECT * FROM ")
          t  (put-tables tables ", ")
          _ (do (print ")")
                (put-alias nil))
          o (do
              (print " LEFT JOIN ")
              (put-tables outer-tables " ON (1=1) LEFT JOIN" ))]
      (concat t o))))

(defn put-sql-select-1
  [sel]
  (cond
    (sql/sql-select? sel)
    (let [_ (print "SELECT")
          v1 (put-padding-if-non-null (sql/sql-select-options sel)
                                      #(print (s/join " " %)))
          _ (put-space)
          v2 (put-attributes (sql/sql-select-attributes sel))

          v3 (put-sql-join (sql/sql-select-tables sel)
                           (sql/sql-select-outer-tables sel))

          v5 (put-padding-if-non-null (sql/sql-select-outer-criteria sel)
                                      #(put-on %))

          v6 (put-padding-if-non-null (sql/sql-select-criteria sel)
                                      #(put-where %))

          v7 (put-padding-if-non-null (sql/sql-select-group-by sel)
                                      #(put-group-by %))

          v8 (let [h (sql/sql-select-having sel)]
               (when-not (empty? h)
                 (do (put-space) (put-having h))))
          v9 (put-padding-if-non-null (sql/sql-select-order-by sel)
                                      #(put-order-by %))
          _ (let [extra (sql/sql-select-extra sel)]
              (when-not (empty? extra)
                (put-space)
                (print (s/join " " extra))))]
      (concat v1 v2 v3 v5 v6 v7 v8 v9))

    (sql/sql-select-combine? sel) (default-put-combine
                                   (sql/sql-select-combine-op sel)
                                   (sql/sql-select-combine-left sel)
                                   (sql/sql-select-combine-right sel))

    (sql/sql-select-table? sel) (print (sql/sql-select-table-name sel))

    (sql/sql-select-empty? sel) (print "")  ;; woot woot
    :else (assertion-violation `put-sql-select-1 (str "unknown select " (pr-str sel)))))

(defn put-sql-expression
  [expr]
  (cond
    (sql/sql-expr-column? expr)
    (do (print (sql/sql-expr-column-name expr))
        [])
    (sql/sql-expr-app? expr)
    (let [op (sql/sql-expr-app-rator expr)
          rands (sql/sql-expr-app-rands expr)
          name (sql/sql-operator-name op)]
      (case (sql/sql-operator-arity op)
        ;; postfix
        -1 (let [_ (print "(")
                 v1 (put-sql-expression (first rands))
                 _ (print ")")
                 _ (put-space)
                 _ (print name)]
             v1)
        ;; prefix
        1 (let [_ (print name)
                _ (print "(")
                v1 (put-sql-expression (first rands))
                _ (print ")")]
            v1)
        ;; prefix 2
        -2 (let [_ (print name)
                 _ (print "(")
                 v1 (put-sql-expression (first rands))
                 _ (print ",")
                 _ (put-space)
                 v2 (put-sql-expression (second rands))
                 _ (print ")")]
             (concat v1 v2))
        ;; infix 2
        2 (let [_ (print "(")
                v1 (put-sql-expression (first rands))
                _ (put-space)
                _ (print name)
                _ (put-space)
                v2 (put-sql-expression (second rands))
                _ (print ")")]
            (concat v1 v2))
        3 (let [_ (print "(")
                _ (put-sql-expression (first rands))
                _ (put-space)
                _ (print name)
                _ (put-space)
                v1 (put-sql-expression (second rands))
                _ (put-space)
                _ (print "AND")
                _ (put-space)
                v2 (put-sql-expression (third rands))
                _ (print ")")]
            (concat v1 v2))
        (assertion-violation `put-sql-expression
                             (str "unhandled operator arity " (pr-str op)))))
    (sql/sql-expr-const? expr)
    (put-literal (sql/sql-expr-const-type expr)
                 (sql/sql-expr-const-val expr))
    (sql/sql-expr-tuple? expr)
    (let [_ (print "(")
          v (put-joining-infix (sql/sql-expr-tuple-expressions expr)
                               ", " (fn [b] (put-sql-expression b)))
          _ (print ")")]
      v)
    (sql/sql-expr-case? expr)
    (let [_ (print "(CASE ")
          v1 (mapcat #(put-when %) (sql/sql-expr-case-branches expr))
          _ (print " ELSE ")
          v2 (put-sql-expression (sql/sql-expr-case-default expr))
          _ (print ")")]
      (concat v1 v2))
    (sql/sql-expr-exists? expr)
    (let [_ (print "EXISTS ")
          _ (print "(")
          v (put-sql-select (sql/sql-expr-exists-select expr))
          _ (print ")")]
      v)
    (sql/sql-expr-subquery? expr)
    (let [_ (print "(")
          v (put-sql-select (sql/sql-expr-subquery-query expr))
          _ (print ")")]
      v)
    :else (assertion-violation `put-sql-expression (str "unhandled expression" (pr-str expr)))))

(defn sql-expression->string
  [expr]
  (let [[s params] (with-out-str-and-value (put-sql-expression expr))]
    (cons s params)))

(defn sql-select->string
  [sel]
  (let [[s params] (with-out-str-and-value (put-sql-select sel))]
    (cons s params)))
