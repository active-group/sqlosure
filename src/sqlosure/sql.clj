(ns ^{:doc "Structured representation of SQL Link to relational algebra"
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.sql
  (:require [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :refer [make-universe make-derived-universe]]
            [sqlosure.type :refer [boolean% numeric-type? ordered-type? type=? make-set-type null% any%
                                   non-nullable-type]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]]
            [clojure.string :as string]))

(define-record-type sql-table
  (really-make-sql-table name scheme) sql-table?
  [name sql-table-name  ;; (sql-table -> string)
   scheme sql-table-scheme  ;; (sql-table -> rel-scheme)
   ])

;; FIXME: clean up public api: (sql-table-name (make-sql-table "abc" ...)) would break, e.g. 
(defn make-sql-table
  [name scheme & {:keys [universe]
                  :or {universe nil}}]
  (make-base-relation name scheme
                      :universe universe
                      :handle (really-make-sql-table name scheme)))

;; sql-table-name = string
;; sql-column = string
;; sel-select is on of:
;; * sql-select
;; * sql-select-combine
;; * sql-select-table
;; * sql-select-empty
(define-record-type sql-select
  (make-sql-select options
                   attributes
                   nullary?
                   tables
                   outer-tables
                   criteria
                   outer-criteria
                   group-by
                   having
                   order-by
                   extra)
  sql-select?
  [;; [ string ]
   ;; DISTINCT, ALL, etc.
   options sql-select-options
   ;; list [sql-column sql-expr]
   ;; [] is for '*'
   ;; nil means open - can still add some
   ;; result
   attributes sql-select-attributes
   ;; true if the select represents a nullary relation. In this case,
   ;; attributes should contain a single dummy attribute.
   ;; TODO / FIXME: Is this comment still true?
   nullary? sql-select-nullary?
   ;; [ [alias sql-select-talbe] ]
   ;; FROM
   tables sql-select-tables  ;; (vec-of ["alias" sql-select-table])
   ;; [ sql-expr ]
   ;; WHERE
   ;; [ sql-expr ]
   ;; GROUP-BY
   outer-tables sql-select-outer-tables
   criteria sql-select-criteria
   outer-criteria sql-select-outer-criteria
   ^{:doc "set of SQL column names or `nil`."}
   group-by sql-select-group-by

   ^{:doc "List of SQL expressions or nil."}
   having sql-select-having
   ;; [ {sql-expr sql-order} ]
   ;; ORDER BY
   order-by sql-select-order-by
   ;; [ string ]
   ;; TOP n, etc.
   extra sql-select-extra])

; FIXME: these should be done with lenses instead

(defn set-sql-select-attributes [sql-select attributes]
  (assoc sql-select :attributes attributes))

(defn set-sql-select-nullary? [sql-select nullary?]
  (assoc sql-select :nullary? nullary?))

(defn set-sql-select-tables [sql-select tables]
  (assoc sql-select :tables tables))

(defn set-sql-select-outer-tables [sql-select outer-tables]
  (assoc sql-select :outer-tables outer-tables))

(defn set-sql-select-criteria [sql-select criteria]
  (assoc sql-select :criteria criteria))

(defn set-sql-select-outer-criteria [sql-select outer-criteria]
  (assoc sql-select :outer-criteria outer-criteria))

(defn set-sql-select-group-by [sql-select group-by*]
  (assoc sql-select :group-by group-by*))

(defn set-sql-select-having [sql-select having]
  (assoc sql-select :having having))

(defn set-sql-select-order-by [sql-select order-by]
  (assoc sql-select :order-by order-by))

(defn set-sql-select-extra [sql-select extra]
  (assoc sql-select :extra extra))

;; FIXME: Use proper representation of empty values instead of nil.
(defn ^{:test false} new-sql-select
  "Create a new, empty sql-select."
  []
  (make-sql-select nil    ;; options
                   nil    ;; attributes
                   false  ;; nullary?
                   nil    ;; tables
                   nil    ;; outer-tables
                   nil    ;; criteria
                   nil    ;; outer-criteria
                   nil    ;; group-by
                   nil    ;; having
                   nil    ;; order-by
                   nil    ;; extra
                   ))

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

(define-record-type sql-select-table
  ^{:doc "The whole SQL table."}
  (make-sql-select-table name) sql-select-table?
  [name sql-select-table-name])

(def ^{:private true} sql-combine-op #{:union :intersection :difference})

(defn sql-combine-op?
  "Is a key a sql-combine-op?"
  [k]
  (contains? sql-combine-op k))

(define-record-type sql-select-empty
  (make-sql-select-empty) sql-select-empty? [])

(def the-sql-select-empty (make-sql-select-empty))

;; sql-expr is one of:
;;   * sql-expr-column
;;   * sql-expr-app
;;   * sql-expr-const
;;   * sql-expr-case
;;   * sql-expr-exists
;;   * sql-expr-tuple
;;   * sql-expr-subquery
(define-record-type sql-expr-column
  (make-sql-expr-column name) sql-expr-column?
  [name sql-expr-column-name])

(define-record-type sql-expr-app
  (really-make-sql-expr-app rator rands) sql-expr-app?
  [rator sql-expr-app-rator  ;; Is one of the symbols below or a string.
   rands sql-expr-app-rands])

(define-record-type sql-operator
  (make-sql-operator name arity) sql-operator?
  [name sql-operator-name
   arity sql-operator-arity  ;; -1 means postfix, -2 prefix
   ])

(defn make-sql-expr-app
  [rator & rands]
  (if (= (Math/abs (sql-operator-arity rator))
         (count rands))
    (really-make-sql-expr-app rator rands)
    (assertion-violation `make-sql-expr-app
                         "number of arguments does not match arity of"
                         rator)))

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
(def op-between (make-sql-operator "BETWEEN" 3))
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

(def op-concat (make-sql-operator "CONCAT" -2))
(def op-lower (make-sql-operator "LOWER" 1))
(def op-upper (make-sql-operator "UPPER" 1))

(def op-not (make-sql-operator "NOT" 1))
(def op-null? (make-sql-operator "IS NULL" -1))
(def op-not-null? (make-sql-operator "IS NOT NULL" -1))
(def op-length (make-sql-operator "LENGTH" 1))

(def op-count (make-sql-operator "COUNT" 1))
(def op-count-all (make-sql-operator "COUNT" 1))
(def op-sum (make-sql-operator "SUM" 1))
(def op-avg (make-sql-operator "AVG" 1))
(def op-min (make-sql-operator "MIN" 1))
(def op-max (make-sql-operator "MAX" 1))
(def op-std-dev (make-sql-operator "StdDev" 1))
(def op-std-dev-p (make-sql-operator "StdDevP" 1))
(def op-var (make-sql-operator "Var" 1))
(def op-var-p (make-sql-operator "VarP" 1))

(define-record-type sql-expr-const
  (make-sql-expr-const type val) sql-expr-const?
  [type sql-expr-const-type
   val sql-expr-const-val])

(def the-sql-null (make-sql-expr-const null% nil))

(define-record-type sql-expr-tuple
  (make-sql-expr-tuple expressions) sql-expr-tuple?
  [expressions sql-expr-tuple-expressions])

(define-record-type sql-expr-case
  (make-sql-expr-case branches default) sql-expr-case?
  [branches sql-expr-case-branches  ;; list(pair(sql-expr, sql-expr))
   default sql-expr-case-default])

(define-record-type sql-expr-exists
  (make-sql-expr-exists select) sql-expr-exists?
  [select sql-expr-exists-select  ;; sql-select
   ])

(define-record-type sql-expr-subquery
  (make-sql-expr-subquery query) sql-expr-subquery?
  [query sql-expr-subquery-query])

(def sql-universe (make-universe))

(defn make-sql-universe
  []
  (make-derived-universe sql-universe))

(defn check-numerical [t fail]
  (if (numeric-type? t)
    true
    (fail 'numerical-type t)))

(defn check-ordered [t fail]
  (if (ordered-type? t)
    true
    (fail 'ordered-type t)))

(def not$ (make-monomorphic-combinator 'not [boolean%] boolean%
                                       not
                                       :universe sql-universe
                                       :data op-not))

(def is-null?$
  (let [rator (make-rator "IS NULL"
                          (fn [fail arg-type] boolean%)
                          nil?
                          :universe sql-universe
                          :data op-null?)]
    (fn is-null?$ [rand]
      (make-application rator rand))))

(def is-not-null?$
  (let [rator (make-rator "IS NOT NULL"
                          (fn [fail arg-type] boolean%)
                          some?
                          :universe sql-universe
                          :data op-not-null?)]
    (fn is-not-null?$ [rand]
      (make-application rator rand))))

(def or$ (make-monomorphic-combinator 'or
                                      [boolean% boolean%]
                                      boolean%
                                      (fn [a b]
                                        (or a b (when-not (or (empty? a)
                                                              (empty? b))
                                                  false)))
                                      :universe sql-universe
                                      :data op-or))

(def and$ (make-monomorphic-combinator 'and
                                       [boolean% boolean%]
                                       boolean%
                                       (fn [a b]
                                         (and (not-empty a) (not-empty b) a b))
                                       :universe sql-universe
                                       :data op-and))

(defn- make-ordering-combinator [sym clj op]
  (let [rator (make-rator sym
                          (fn [fail t1 t2]
                            (when fail
                              (do
                                (check-ordered t1 fail)
                                (check-ordered t2 fail)
                                (when-not (type=? (non-nullable-type t1) (non-nullable-type t2))
                                  (fail t1 t2))))
                            boolean%)
                          (null-lift-binary-predicate clj)
                          :universe sql-universe
                          :data op)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(def >=$ (make-ordering-combinator '>= >= op->=))
(def <=$ (make-ordering-combinator '<= <= op-<=))
(def <$ (make-ordering-combinator '< >= op-<))
(def >$ (make-ordering-combinator '> >= op->))

(def plus$
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
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(def minus$
  (let [rator (make-rator '-
                          (fn [fail t1 t2]
                            (when fail
                              (do
                                (check-numerical t1 fail)
                                (check-numerical t2 fail)))
                            t1)
                          -
                          :universe sql-universe
                          :data op--)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(def concat$
  (let [rator (make-rator 'concat
                          (fn [fail t1 t2]
                            #_(when fail
                              (do
                                ;; TODO: check-string maybe?
                                ;; (check-numerical t1 fail)
                                ;; (check-numerical t2 fail)
                                ))
                            t1)
                          str
                          :universe sql-universe
                          :data op-concat)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(defn- make-string-converter [sym clj op]
  (let [rator (make-rator sym
                          (fn [fail t]
                            #_(when fail
                                (do
                                  ;; TODO: check-string maybe?
                                  (check-numerical t fail)))
                            t)
                          clj
                          :universe sql-universe
                          :data op)]
    (fn [expr]
      (make-application rator expr))))

(def lower$ (make-string-converter 'lower string/lower-case op-lower))
(def upper$ (make-string-converter 'upper string/upper-case op-upper))

(def =$
  (let [rator (make-rator '=
                          (fn [fail t1 t2]
                            (when (and fail (not (type=? (non-nullable-type t1) (non-nullable-type t2))))
                              (fail t1 t2))
                            boolean%)
                          =
                          :universe sql-universe
                          :data op-=)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(def like$
  (let [rator (make-rator 'like
                          (fn [fail t1 t2]
                            #_(when fail
                              (do
                                ;; TODO: check-string maybe?
                                ;; (check-numerical t1 fail)
                                ;; (check-numerical t2 fail)
                                ))
                            boolean%)
                          = ;; TODO: not really =
                          :universe sql-universe
                          :data op-like)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(defn member
  "Locates the first element of xs that is equal to x. If such an element
  exists, the rest of xs starting with that element is returned. Otherwise,
  the result is false (Scheme's member function)."
  [x xs]
  (if-let [res (not-empty (drop-while #(not= % x) xs))]
    res
    false))

(def in$
  (let [rator (make-rator 'in
                          (fn [fail t1 t2]
                            (when (and fail (not (type=? (make-set-type t1) t2)))
                              (fail (make-set-type t1) t2))
                            boolean%)
                          contains?
                          :universe sql-universe
                          :data op-in)]
    (fn [expr1 expr2]
      (make-application rator expr1 expr2))))

(def between$
  (let [rator (make-rator 'between
                          (fn [fail t1 t2 t3]
                            (when (and fail (or (not= t2 t3)
                                                (not (attribute-ref? t1))))
                              (fail t1 t2 t3))
                            boolean%)
                          nil
                          :universe sql-universe
                          :data op-between)]
    (fn [expr1 expr2 expr3]
      (make-application rator expr1 expr2 expr3))))
