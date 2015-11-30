(ns sqlosure.sql-put
  (:require [sqlosure.sql :as sql]
            [active.clojure.record :refer [define-record-type]]
            [clojure.string :as s]))

(define-record-type sql-put-parameterization
  (make-sql-put-parameterization combine-proc literal-proc)
  sql-put-parameterization?
  [combine-proc sql-put-parameterization-combine-proc
   literal-proc sql-put-parameterization-literal-proc])

(defn put-space
  "Print a single space character."
  []
  (print " "))

(defn put-adding-if-non-null
  "When lis is not empty, prepend a space to (proc lis)."
  [lis proc]
  (when-not (empty? lis)
    (put-space)
    (proc lis)))

(defn put-as
  "When alias is not nil, print \" AS alias\"."
  [alias]
  (when alias (print " AS" alias)))

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
        (sql/sql-select-combine? sel)) (put-sql-select-1 param sel)
    (sql/sql-select-table? sel) (do
                                  (print "SELECT * FROM ")
                                  (print (sql/sql-select-table-name sel)))
    :else (throw (Exception. (str 'put-sql-select ": unhandled query " sel)))))

(defn put-joining-infix
  "Intersperse `between` between `lis`'s elements and print via `proc`."
  [lis between proc]
  (if (empty? lis)
    identity
    (do (proc (first lis))
        (loop [xs (rest lis)]
          (when-not (empty? xs)
            (print between)
            (proc (first xs))
            (recur (rest xs)))))))

(defn put-tables
  "Takes a sql-put-parameterization and the sql-select-tables field of a
  sql-select and prints them as a sql statement."
  [param tables]
  (print "FROM ")
  (put-joining-infix tables ", "
                     (fn [[alias select]]
                       (do
                         (if (sql/sql-select-table? select)
                           (print (sql/sql-select-table-name select))
                           (do (print "(")
                               (put-sql-select param select)
                               (print ")")))
                         (put-as alias)))))

(defn default-put-literal
  [val]
  (cond
    (number? val) (print val)
    (string? val) (do (print "'")
                      (print val)
                      (print "'"))
    (nil? val) (print "NULL")
    (or (= true val) (= false val)) (if val (print "TRUE") (print "FALSE"))
    :else (throw (Exception. (str 'default-put-literal
                                  ": unhandled literal " val)))))

(defn default-put-combine
  [param op left right]
  (print "(")
  (put-sql-select param left)
  (print ") ")
  (print (case op
           :union "UNION"
           :intersection "INTERSECT"
           :difference "EXCEPT"))
  (print " (")
  (put-sql-select param right)
  (print ")"))

(def default-sql-put-parameterization
  (make-sql-put-parameterization default-put-combine default-put-literal))

(declare put-sql-expression)

(defn put-when
  [param p]
  (print "WHEN ")
  (put-sql-expression param (first p))
  (print " THEN ")
  (put-sql-expression param (second p)))

(defn put-where
  [param exprs]
  (print "WHERE ")
  (put-joining-infix exprs " AND " (fn [b] (put-sql-expression param b))))

(defn put-group-by
  [param group-by]
  (print "GROUP BY ")
  (put-joining-infix group-by ", " (fn [b] (put-sql-expression param b))))

(defn put-order-by
  [param order-by]
  (print "ORDER BY ")
  (put-joining-infix order-by ", " (fn [[a b]]
                                     (put-sql-expression param a)
                                     (print (case b
                                              :ascending " ASC"
                                              :descending " DESC")))))

(defn put-having
  [param expr]
  (print "HAVING ")
  (put-sql-expression param expr))

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
                               (put-as col)))))))

(defn put-sql-select-1
  [param sel]
  (cond
    (sql/sql-select? sel)
    (do
      (print "SELECT")
      (put-adding-if-non-null (sql/sql-select-options sel)
                              #(print (s/join " " %)))
      (put-space)
      (put-attributes param (sql/sql-select-attributes sel))
      (put-adding-if-non-null (sql/sql-select-tables sel)
                              #(put-tables param %))
      (put-adding-if-non-null (sql/sql-select-criteria sel)
                              #(put-where param %))
      (put-adding-if-non-null (sql/sql-select-group-by sel)
                              #(put-group-by param %))
      (when-let [h (sql/sql-select-having sel)]
        (put-space)
        (put-having param h))
      (put-adding-if-non-null (sql/sql-select-order-by sel)
                              #(put-order-by param %))
      (let [extra (sql/sql-select-extra sel)]
        (when-not (empty? extra)
          (do (put-space)
              (s/join " " extra)))))
    (sql/sql-select-combine? sel) ((sql-put-parameterization-combine-proc param)
                                   param
                                   (sql/sql-select-combine-op sel)
                                   (sql/sql-select-combine-left sel)
                                   (sql/sql-select-combine-right sel))
    (sql/sql-select-table? sel) (print (sql/sql-select-table-name sel))
    (sql/sql-select-empty? sel) (print "")  ;; woot woot
    :else (throw (Exception. (str 'put-sql-select-1 ": unknown select " sel)))))

(defn put-sql-select
  [param sel]
  (cond
    (or (sql/sql-select? sel)
        (sql/sql-select-combine? sel)) (put-sql-select-1 param sel)
    (sql/sql-select-table? sel) (do (print "SELECT * FROM ")
                                    (print (sql/sql-select-table-name sel)))
    :else (throw (Exception. (str 'put-sql-select ": unhandled query " sel)))))

(defn put-sql-expression
  [param expr]
  (cond
    (sql/sql-expr-column? expr) (print (sql/sql-expr-column-name expr))
    (sql/sql-expr-app? expr) (let [op (sql/sql-expr-app-rator expr)
                                   rands (sql/sql-expr-app-rands expr)
                                   name (sql/sql-operator-name op)]
                               (case (sql/sql-operator-arity op)
                                 -1 (do (print "(")
                                        (put-sql-expression param (first rands))
                                        (print ")")
                                        (put-space)
                                        (print name))
                                 1 (do (print name)
                                       (print "(")
                                       (put-sql-expression param (first rands))
                                       (print ")"))
                                 2 (do (print "(")
                                       (put-sql-expression param (first rands))
                                       (put-space)
                                       (print name)
                                       (put-space)
                                       (put-sql-expression param (second rands))
                                       (print ")"))
                                 :else (throw (Exception.
                                               (str 'put-sql-expression
                                                    ": unhandled operator arity " op)))))
    (sql/sql-expr-const? expr) (put-literal param (sql/sql-expr-const-val expr))
    (sql/sql-expr-tuple? expr)
    (do (print "(")
        (put-joining-infix (sql/sql-expr-tuple-expressions expr)
                           ", " (fn [b] (put-sql-expression param b)))
        (print ")"))
    (sql/sql-expr-case? expr)
    (do (print "(CASE ")
        (map #(put-when param %) (sql/sql-expr-case-branches expr))
        (print " ELSE ")
        (put-sql-expression param (sql/sql-expr-case-default expr))
        (print ")"))
    (sql/sql-expr-exists? expr)
    (do (print "EXISTS ")
        (print "(")
        (put-sql-select param (sql/sql-expr-exists-select expr))
        (print ")"))
    (sql/sql-expr-subquery? expr)
    (do (print "(")
        (put-sql-select param (sql/sql-expr-subquery-query expr))
        (print ")"))
    :else (throw (Exception.
                  (str 'put-sql-expression ": unhandled expression " expr)))))

(defn sql-expression->string
  [param expr]
  (with-out-str (put-sql-expression param expr)))

(defn sql-select->string
  [param sel]
  (with-out-str (put-sql-select param sel)))
