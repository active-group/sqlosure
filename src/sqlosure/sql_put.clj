(ns sqlosure.sql-put
  (:require [sqlosure.sql :as sql]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :refer [add-table]]
            [sqlosure.type :as t]
            [sqlosure.utils :refer [third]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.string :as s]))

(defmacro with-out-str-and-value
  "See http://stackoverflow.com/a/7151125"
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*out* s#]
       (let [v# ~@body]
         (vector (str s#)
                 v#)))))

(define-record-type sql-put-parameterization
  (make-sql-put-parameterization alias-proc combine-proc literal-proc)
  sql-put-parameterization?
  [alias-proc sql-put-parameterization-alias-proc
   combine-proc sql-put-parameterization-combine-proc
   literal-proc sql-put-parameterization-literal-proc])

(defn put-space
  "Print a single space character."
  []
  (print " "))

(defn put-padding-if-non-null
  "When lis is not empty, prepend a space to (proc lis)."
  [lis proc]
  (when-not (empty? lis)
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

(defn put-literal
  "Apply `params` literal-proc to val."
  [param val]
  ((sql-put-parameterization-literal-proc param) val))

(declare put-sql-select-1)

(defn put-sql-select
  "Takes a sql-put-parameterization and an sql-select and attempts to print it."
  [param sel]
  (cond
    (or (sql/sql-select? sel)
        (sql/sql-select-combine? sel))
    (put-sql-select-1 param sel)

    (sql/sql-select-table? sel)
    (do
      (print "SELECT * FROM ")
      (print (sql/sql-select-table-name sel))
      [])
    :else
    (assertion-violation 'put-sql-select "unhandled query" sel)))

(defn put-joining-infix
  "Intersperse `between` between `lis`'s elements and print via `proc`."
  [lis between proc]
  (if (empty? lis)
    identity
    (let [v (proc (first lis))
          vs (loop [xs (rest lis)
                   vs nil]
              (if-not (empty? xs)
              (do (print between)
                  (recur (rest xs) (concat vs (proc (first xs)))))
              vs))]
      (concat v vs))))

(defn put-tables
  "Takes a sql-put-parameterization and the sql-select-tables field of a
  sql-select and prints them as a sql statement."
  [param tables]
  (print "FROM ")
  (put-joining-infix tables ", "
                     (fn [[alias select]]
                       (if (sql/sql-select-table? select)
                         (do
                           (print (sql/sql-select-table-name select))
                           ((sql-put-parameterization-alias-proc param) alias))
                         (let [_ (print "(")
                               v1 (put-sql-select param select)
                               _ (print ")")
                               _ ((sql-put-parameterization-alias-proc param) alias)]
                           v1)))))

(defn default-put-literal
  [val]
  (if (or (number? val) (string? val) (nil? val) (= true) (= false))
    (do
      (print "?")
      val)
    (assertion-violation 'default-put-literal "unhandeled literal" val)))

(defn default-put-combine
  [param op left right]
  (let [_ (print "(")
        v1 (put-sql-select param left)
        _ (print ") ")
        _ (print (case op
                   :union "UNION"
                   :intersection "INTERSECT"
                   :difference "EXCEPT"))
        _ (print " (")
        v2 (put-sql-select param right)
        _ (print ")")]
    (concat v1 v2)))

(def default-sql-put-parameterization
  (make-sql-put-parameterization default-put-alias default-put-combine default-put-literal))

(declare put-sql-expression)

(defn put-when
  [param p]
  (let [_ (print "WHEN ")
        v1 (put-sql-expression param (first p))
        _ (print " THEN ")
        v2 (put-sql-expression param (second p))]
    (concat v1 v2)))

(defn put-where
  [param exprs]
  (print "WHERE ")
  (let [v (put-joining-infix exprs " AND " (fn [b] (put-sql-expression param b)))]
    v))

(defn put-group-by
  [param group-by]
  (print "GROUP BY ")
  (put-joining-infix group-by ", " (fn [b] (put-sql-expression param b))))

(defn put-order-by
  [param order-by]
  (print "ORDER BY ")
  (flatten (put-joining-infix order-by ", " (fn [[a b]]
                                              (put-sql-expression param a)
                                              (print (case b
                                                       :ascending " ASC"
                                                       :descending " DESC"))))))

(defn put-having
  [param expr]
  (print "HAVING ")
  (let [v (put-sql-expression param expr)]
    v))

(defn put-attributes
  [param attributes]
  (if (empty? attributes)
    (print "*")
    (put-joining-infix attributes ", "
                       (fn [[col expr]]
                         (if (and (sql/sql-expr-column? expr)
                                  (= col (sql/sql-expr-column-name expr)))
                           (print col)
                           (do (put-sql-expression param expr)
                               (default-put-alias col)))))))

(defn put-sql-select-1
  [param sel]
  (cond
    (sql/sql-select? sel)
    (let [_ (print "SELECT")
          v1 (put-padding-if-non-null (sql/sql-select-options sel)
                                     #(print (s/join " " %)))
          _ (put-space)
          v2 (put-attributes param (sql/sql-select-attributes sel))
          v2 (put-padding-if-non-null (sql/sql-select-tables sel)
                                     #(put-tables param %))
          v3 (put-padding-if-non-null (sql/sql-select-criteria sel)
                                     #(put-where param %))
          v4 (put-padding-if-non-null (sql/sql-select-group-by sel)
                                     #(put-group-by param %))
          v5 (when-let [h (sql/sql-select-having sel)]
               (put-space)
               (put-having param h))
          v6 (put-padding-if-non-null (sql/sql-select-order-by sel)
                                     #(put-order-by param %))
          v7 (let [extra (sql/sql-select-extra sel)]
               (when-not (empty? extra)
                 (do (put-space)
                     (print (s/join " " extra)))))]
      (concat v1 v2 v3 v4 v5 v6 v7))
    (sql/sql-select-combine? sel) ((sql-put-parameterization-combine-proc param)
                                   param
                                   (sql/sql-select-combine-op sel)
                                   (sql/sql-select-combine-left sel)
                                   (sql/sql-select-combine-right sel))
    (sql/sql-select-table? sel) (print (sql/sql-select-table-name sel))
    (sql/sql-select-empty? sel) (print "")  ;; woot woot
    :else (assertion-violation 'put-sql-select-1 "unknown select" sel)))

(defn put-sql-expression
  [param expr]
  (cond
    (sql/sql-expr-column? expr) (print (sql/sql-expr-column-name expr))
    (sql/sql-expr-app? expr) (let [op (sql/sql-expr-app-rator expr)
                                   rands (sql/sql-expr-app-rands expr)
                                   name (sql/sql-operator-name op)]
                               (case (sql/sql-operator-arity op)
                                 -1 (let [_ (print "(")
                                          v1 (put-sql-expression param (first rands))
                                          _ (print ")")
                                          _ (put-space)
                                          _ (print name)]
                                      v1)
                                 1 (let [_ (print name)
                                         _ (print "(")
                                         v1 (put-sql-expression param (first rands))
                                         _ (print ")")]
                                     v1)
                                 2 (let [_ (print "(")
                                         v1 (put-sql-expression param (first rands))
                                         _ (put-space)
                                         _ (print name)
                                         _ (put-space)
                                         v2 (put-sql-expression param (second rands))
                                         _ (print ")")]
                                     (concat v1 v2))
                                 3 (let [_ (print "(")
                                         _ (put-sql-expression param (first rands))
                                         _ (put-space)
                                         _ (print name)
                                         _ (put-space)
                                         v1 (put-sql-expression param (second rands))
                                         _ (put-space)
                                         _ (print "AND")
                                         _ (put-space)
                                         v2 (put-sql-expression param (third rands))
                                         _ (print ")")]
                                     (concat v1 v2))
                                 :else (assertion-violation 'put-sql-expression
                                                            "unhandled operator arity " op)))
    (sql/sql-expr-const? expr) (let [v (put-literal param (sql/sql-expr-const-val expr))]
                                 [v])
    (sql/sql-expr-tuple? expr)
    (let [_ (print "(")
          v (put-joining-infix (sql/sql-expr-tuple-expressions expr)
                             ", " (fn [b] (put-sql-expression param b)))
          _ (print ")")]
      v)
    (sql/sql-expr-case? expr)
    (let [_ (print "(CASE ")
          v1 (map #(put-when param %) (sql/sql-expr-case-branches expr))
          _ (print " ELSE ")
          v2 (put-sql-expression param (sql/sql-expr-case-default expr))
          _ (print ")")]
      (concat v1 v2))
    (sql/sql-expr-exists? expr)
    (let [_ (print "EXISTS ")
          _ (print "(")
          v (put-sql-select param (sql/sql-expr-exists-select expr))
          _ (print ")")]
      v)
    (sql/sql-expr-subquery? expr)
    (let [_ (print "(")
          v (put-sql-select param (sql/sql-expr-subquery-query expr))
          _ (print ")")]
      v)
    :else (assertion-violation 'put-sql-expression "unhandled expression" expr)))

(defn sql-expression->string
  [param expr]
  (flatten (with-out-str-and-value (put-sql-expression param expr))))

(defn sql-select->string
  [param sel]
  (flatten (with-out-str-and-value (put-sql-select param sel))))
