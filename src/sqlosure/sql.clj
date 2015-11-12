(ns ^{:doc "Structured representation of SQL Link to relational algebra"
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.sql
  (:require [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :refer [make-universe make-derived-universe]]
            [sqlosure.type :refer [boolean% numeric-type? type=? make-set-type]]
            [active.clojure.record :refer [define-record-type]]))

(define-record-type sql-table
  (really-make-sql-table name scheme) sql-table?
  [name sql-table-name  ;; (sql-table -> string)
   scheme sql-table-scheme  ;; (sql-table -> rel-scheme)
   ])

(defn ^{:test true} make-sql-table
  [name scheme & {:keys [universe]}]
  (make-base-relation (symbol name) scheme
                      :universe universe
                      :handle (really-make-sql-table name scheme)))

(define-record-type sql-select
  (make-sql-select options attributes nullary? tables criteria group-by having
                   order-by extra)
  sql-select?
  [options sql-select-options
   attributes sql-select-attributes
   nullary? sql-select-nullary?
   tables sql-select-tables
   criteria sql-select-criteria
   group-by sql-select-group-by
   having sql-select-having
   order-by sql-select-order-by
   extra sql-select-extra])

(defn new-sql-select
  "Create a new, empty sql-select."
  []
  (make-sql-select nil nil false nil nil nil false nil nil))

(def ^{:private true} sql-order #{:ascending :descending})

(defn sql-order?
  "Is a key a sql-order?"
  [k]
  (contains? sql-order k))

(define-record-type sql-select-combine
  (make-sql-select-combine op left right) sql-select-combine?
  [op sql-select-combine-op
   left sql-select-combine-left
   right sql-select-combine-right])

(def ^{:private true} sql-combine-op #{:union :intersection :difference})

(defn sql-combine-op?
  "Is a key a sql-combine-op?"
  [k]
  (contains? sql-combine-op k))

(define-record-type sql-select-empty
  (make-sql-select-empty) sql-select-empty? [])

(def the-sql-select-empty (make-sql-select-empty))

;; sql-expr is one of:
;;  :sql-expr-column
;;  :sql-expr-app
;;  :sql-expr-case
;;  :sql-expr-exists
(define-record-type sql-expr-column
  (make-sql-expr-column name) sql-expr-column?
  [name sql-expr-column-name])

(define-record-type sql-expr-app
  (really-make-sql-expr-app rator rands) sql-expr-app?
  [rator sql-expr-app-rator
   rands sql-expr-app-rands])

(define-record-type sql-operator
  (make-sql-operator name arity) sql-operator?
  [name sql-operator-name
   ;; -1 means postfix
   arity sql-operator-arity])

(defn make-sql-expr-app
  [rator & rands]
  (if (= (Math/abs (sql-operator-arity rator))
         (count rands))
    (really-make-sql-expr-app rator rands)
    (throw (Exception. (str 'make-sql-expr-app
                            " number or arguments does not match arity of "
                            'rator)))))

(def op-= (make-sql-operator "=" 2))
(def op-< (make-sql-operator "<" 2))
(def op-> (make-sql-operator ">" 2))
(def op-<= (make-sql-operator "<=" 2))
(def op->= (make-sql-operator ">=" 2))
(def op-<> (make-sql-operator "<>" 2))
(def op-and (make-sql-operator "AND" 2))
(def op-or (make-sql-operator "OR" 2))
(def op-like (make-sql-operator "LIKE" 2))
(def op-in (make-sql-operator "IN" 2))
(def op-cat (make-sql-operator "CAT" 2))
(def op-+ (make-sql-operator "+" 2))
(def op-- (make-sql-operator "-" 2))
(def op-* (make-sql-operator "*" 2))
;; Used to be op//
(def op-div (make-sql-operator "/" 2))
(def op-mod (make-sql-operator "MOD" 2))
(def op-bit-not (make-sql-operator "~" 1))
(def op-bit-and (make-sql-operator "&" 2))
(def op-bit-or (make-sql-operator "|" 2))
(def op-bit-xor (make-sql-operator "^" 2))
(def op-asg (make-sql-operator "=" 2))

(def op-not (make-sql-operator "NOT" 1))
(def op-null? (make-sql-operator "IS NULL" -1))
(def op-not-null? (make-sql-operator "IS NOT NULL" -1))
(def op-length (make-sql-operator "LENGTH" 1))

(def op-count (make-sql-operator "COUNT" 1))
(def op-sum (make-sql-operator "SUM" 1))
(def op-avg (make-sql-operator "AVG" 1))
(def op-min (make-sql-operator "MIN" 1))
(def op-max (make-sql-operator "MAX" 1))
(def op-std-dev (make-sql-operator "StdDev" 1))
(def op-std-dev-p (make-sql-operator "StdDevP" 1))
(def op-var (make-sql-operator "Var" 1))
(def op-var-p (make-sql-operator "VarP" 1))

;; What to do about the (define-syntax ...) part? Is this just convinience or
;; is it really necessary?

(define-record-type sql-expr-const
  (make-sql-expr-const val) sql-expr-const?
  [val sql-expr-const-val])

(def the-sql-null (make-sql-expr-const nil))

(define-record-type sql-expr-tuple
  (make-sql-expr-tuple expressions) sql-expr-tuple?
  [expressions sql-expr-tuple-expressions])

(define-record-type sql-expr-case
  (make-sql-expr-case branches default) sql-expr-case?
  [branches sql-expr-case-branches
   default sql-expr-case-default])

(define-record-type sql-expr-exists
  (make-sql-expr-exists select) sql-expr-exists?
  [select sql-expr-exists-select])

(define-record-type sql-expr-subquery
  (make-sql-expr-subquery query) sql-expr-subquery?
  [query sql-expr-subquery-query])

(def sql-universe (make-universe))

(defn ^{:test true} make-sql-universe
  []
  (make-derived-universe sql-universe))

(defn ^{:test true} check-numerical [t fail]
  (if (numeric-type? t)
    true
    (fail 'numerical-type t)))

(def not$ (make-monomorphic-combinator 'not [boolean%] boolean%
                                       not
                                       :universe sql-universe))

(def or$ (make-monomorphic-combinator 'or [boolean% boolean%] boolean%
                                      (fn [a b]
                                        (or a b (if (or (empty? a)
                                                        (empty? b))
                                                  nil false)))
                                      :universe sql-universe
                                      :data op-or))

(def and$ (make-monomorphic-combinator 'and [boolean% boolean%] boolean%
                                       (fn [a b]
                                         (and (not-empty a) (not-empty b) a b)
                                         :universe sql-universe
                                         :data op-and)))

(defn ^{:test true} >=$
  [expr1 expr2]
  (let [rator (make-rator '>=
                          (fn [fail t1 t2]
                            (when fail
                              (do
                                (check-numerical t1 fail)
                                (check-numerical t2 fail)
                                (when-not (type=? t1 t2) (fail t1 t2))))
                            boolean%)
                          (null-lift-binary-predicate >=)
                          :universe sql-universe
                          :data op->=)]
    (make-application rator expr1 expr2)))

(defn plus$
  [expr1 expr2]
  (let [rator (make-rator '+
                          (fn [fail t1 t2]
                            (when fail
                              (do
                                (check-numerical t1 fail)
                                (check-numerical t2 fail)))
                            t1)
                          +
                          :universe sql-universe
                          :data op-+)]
    (make-application rator expr1 expr2)))

(defn minus$
  [expr1 expr2]
  (let [rator (make-rator '-
                          (fn [fail t1 t2]
                            (when fail
                              (do
                                (check-numerical t1 fail)
                                (check-numerical t2 fail)
                                t1)
                              -
                              :universe sql-universe
                              :data op--)))]
    (make-application rator expr1 expr2)))

(defn =$
  [expr1 expr2]
  (let [rator (make-rator '=
                          (fn [fail t1 t2]
                            (when (and fail (not (type=? t1 t2)))
                              (fail t1 t2))
                            boolean%)
                          =
                          :universe sql-universe
                          :data op-=)]
    (make-application rator expr1 expr2)))

(make-rator '= (fn [fail t1 t2] (when (and fail (not (type=? t1 t2)))
                                  (fail t1 t2))
                 boolean%)
            =
            :universe sql-universe
            :data op-=)

(=$ (make-attribute-ref "two")
    (make-attribute-ref "one"))

(defn member
  [x xs]
  (if-let [res (not-empty (drop-while #(not= % x) xs))]
    res
    false))

(defn in$
  [expr1 expr2]
  (let [rator (make-rator 'in
                          (fn [fail t1 t2]
                            (when (and fail (not (type=? (make-set-type t1) t2)))
                              (fail (make-set-type t1) t2))
                            boolean%)
                          contains?
                          :universe sql-universe
                          :data op-in)]
    (make-application rator expr1 expr2)))
