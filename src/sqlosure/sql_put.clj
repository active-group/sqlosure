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

(defn put-literal
  "Apply `params` literal-proc to val."
  [param type val]
  ((sql-put-parameterization-literal-proc param) type val))

(defn put-alias
  "Apply `params` alias-proc to the alias."
  [param alias]
  ((sql-put-parameterization-alias-proc param) alias))

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
  [param tables between]
  (put-joining-infix tables between
                     (fn [[alias select]]
                       (if (sql/sql-select-table? select)
                         (do
                           (print (sql/sql-select-table-name select))
                           (put-alias param alias))
                         (let [_ (print "(")
                               v1 (put-sql-select param select)
                               _ (print ")")
                               _ (put-alias param alias)]
                           v1)))))

(defn default-put-literal
  [type val]
  (if true ;; look wrong in any case: (or (number? val) (string? val) (nil? val) (= true) (= false))
    (do
      (print "?")
      [[type val]])
    (assertion-violation `default-put-literal (str "unhandeled literal " (pr-str val)))))

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

(defn put-condition
  [param exprs]
  (put-joining-infix exprs " AND " (fn [b] (put-sql-expression param b))))

(defn put-where
  [param exprs]
  (print "WHERE ")
  (put-condition param exprs))

(defn put-on
  [param exprs]
  (print "ON ")
  (put-condition param exprs))

(defn put-group-by
  "Takes a seq of sql-expr."
  [param group-by]
  (print "GROUP BY ")
  (put-joining-infix group-by ", " print))

(defn put-order-by
  "Takes a seq of [sql-expr, sql-order]."
  [param order-by]
  (print "ORDER BY ")
  (put-joining-infix order-by ", " (fn [[a b]]
                                     (let [v (put-sql-expression param a)]
                                       (print (case b
                                                :ascending " ASC"
                                                :descending " DESC"))
                                       v))))

(defn put-having
  [param exprs]
  (print "HAVING ")
  (put-condition param exprs))

(defn put-attributes
  [param attributes]
  (if (empty? attributes)
    (print "*")
    (put-joining-infix attributes ", "
                       (fn [[col expr]]
                         (if (and (sql/sql-expr-column? expr)
                                  (= col (sql/sql-expr-column-name expr)))
                           (print col)
                           (let [v (put-sql-expression param expr)]
                             (default-put-alias col)
                             v))))))

(defn put-sql-join
  "Put the tables involved in the join of a SQL select."
  [param tables outer-tables]
  (if (or (empty? outer-tables)
          (= (count tables) 1))
    (let [t (put-padding-if-non-null tables
                                     (fn [tables]
                                       (print "FROM ")
                                       (put-tables param tables ", ")))
          
          o (if (not-empty outer-tables)
              (do
                (print " LEFT JOIN ")
                ;; every LEFT JOIN needs an ON
                (put-tables param outer-tables " ON (1=1) LEFT JOIN "))
              [])]
      (concat t o))
    ;; outer tables AND more than one regular tablew
    (let [_ (print " FROM (SELECT * FROM ")
          t  (put-tables param tables ", ")
          _ (do (print ")")
                (put-alias param nil))
          o (do
              (print " LEFT JOIN ")
              (put-tables param outer-tables " ON (1=1) LEFT JOIN" ))]
      (concat t o))))

(defn put-sql-select-1
  [param sel]
  (cond
    (sql/sql-select? sel)
    (let [_ (print "SELECT")
          v1 (put-padding-if-non-null (sql/sql-select-options sel)
                                      #(print (s/join " " %)))
          _ (put-space)
          v2 (put-attributes param (sql/sql-select-attributes sel))

          v3 (put-sql-join param
                           (sql/sql-select-tables sel)
                           (sql/sql-select-outer-tables sel))

          v5 (put-padding-if-non-null (sql/sql-select-outer-criteria sel)
                                      #(put-on param %))

          v6 (put-padding-if-non-null (sql/sql-select-criteria sel)
                                      #(put-where param %))

          v7 (put-padding-if-non-null (sql/sql-select-group-by sel)
                                      #(put-group-by param %))
          
          v8 (if-let [h (sql/sql-select-having sel)]
               (do
                 (put-space)
                 (put-having param h))
               [])
          v9 (put-padding-if-non-null (sql/sql-select-order-by sel)
                                      #(put-order-by param %))
          _ (let [extra (sql/sql-select-extra sel)]
              (when-not (empty? extra)
                (put-space)
                (print (s/join " " extra))))]
      (concat v1 v2 v3 v5 v6 v7 v8 v9))
      
    (sql/sql-select-combine? sel) ((sql-put-parameterization-combine-proc param)
                                   param
                                   (sql/sql-select-combine-op sel)
                                   (sql/sql-select-combine-left sel)
                                   (sql/sql-select-combine-right sel))
    
    (sql/sql-select-table? sel) (print (sql/sql-select-table-name sel))
    
    (sql/sql-select-empty? sel) (print "")  ;; woot woot
    :else (assertion-violation `put-sql-select-1 (str "unknown select " (pr-str sel)))))

(defn put-sql-expression
  [param expr]
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
                 v1 (put-sql-expression param (first rands))
                 _ (print ")")
                 _ (put-space)
                 _ (print name)]
             v1)
        ;; prefix
        1 (let [_ (print name)
                _ (print "(")
                v1 (put-sql-expression param (first rands))
                _ (print ")")]
            v1)
        ;; prefix 2
        -2 (let [_ (print name)
                 _ (print "(")
                 v1 (put-sql-expression param (first rands))
                 _ (print ",")
                 _ (put-space)
                 v2 (put-sql-expression param (second rands))
                 _ (print ")")]
             (concat v1 v2))
        ;; infix 2
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
        (assertion-violation `put-sql-expression
                             (str "unhandled operator arity " (pr-str op)))))
    (sql/sql-expr-const? expr)
    (put-literal param (sql/sql-expr-const-type expr)
                 (sql/sql-expr-const-val expr))
    (sql/sql-expr-tuple? expr)
    (let [_ (print "(")
          v (put-joining-infix (sql/sql-expr-tuple-expressions expr)
                               ", " (fn [b] (put-sql-expression param b)))
          _ (print ")")]
      v)
    (sql/sql-expr-case? expr)
    (let [_ (print "(CASE ")
          v1 (mapcat #(put-when param %) (sql/sql-expr-case-branches expr))
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
    :else (assertion-violation `put-sql-expression (str "unhandled expression" (pr-str expr)))))

(defn sql-expression->string
  [param expr]
  (let [[s params] (with-out-str-and-value (put-sql-expression param expr))]
    (cons s params)))

(defn sql-select->string
  [param sel]
  (let [[s params] (with-out-str-and-value (put-sql-select param sel))]
    (cons s params)))
