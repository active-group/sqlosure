(ns sqlosure.core
  (:require [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.query-comprehension :as qc]
            [sqlosure.sql-put :as put]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.optimization :as opt]
            [sqlosure.type :as t]
            [active.clojure.monad :refer :all]
            [active.clojure.condition :refer [assertion-violation]]))

(defmacro deftable
  "`deftable` can be used to define tables for sqlosure. It will define a
  (Clojure) value named `?name`. `?sql-name` ist the name of the table in the
  DBMS, `?map` is a map of column-name -> sqlosure.type-type. `opts` may contain
  a :universe key and a universe (defaults to `nil`)."
  [?name ?sql-name ?map & ?opts]
  (let [?opts-m (apply hash-map ?opts)
        universe? (get ?opts-m :universe)]
    `(def ~?name
       (sql/make-sql-table ~?sql-name
                           ~(rel/alist->rel-scheme ?map)
                           :universe ~universe?))))

(defmacro query
  "`query` takes a series of relational algebra statements (projection,
  selection, grouping, etc.) and returns an executable relational algebra query.
  The last statement must be a monadic return (either return or projection)."
  [& ?forms]
  `(qc/get-query (monadic ~@?forms)))


;; -----------------------------------------------------------------------------
;; -- QUERY COMPREHENSION OPERATORS
;; -----------------------------------------------------------------------------

;; Reexports of query-monad operators.
(def <- qc/embed)
(def ! qc/!)
(def restrict qc/restrict)
(def group qc/group)
(def project qc/project)
(def top qc/top)


;; -----------------------------------------------------------------------------
;; -- Shortcuts for aggretations functions.
;; -----------------------------------------------------------------------------

(defn aggregate
  "`aggregate` takes an aggregation-operator (:count, :avg, etc.) an optionally
  an `sqlosure.relational-algebra/attribute-ref` and returns an aggregation.
  If no `attribute` is provided, returns an aggregation on the whole relation,
  otherwise on the specified attribute."
  [aggregation-op & attribute]
  (doseq [a attribute]
    (when-not (rel/attribute-ref? a)
      (assertion-violation `aggregate "expected attribute-ref" a)))
  (apply rel/make-aggregation aggregation-op attribute))

;; Aggregations on attributes.
(defn $count [aref] (aggregate :count aref))
(defn $sum [aref] (aggregate :sum aref))
(defn $avg [aref] (aggregate :avg aref))
(defn $min [aref] (aggregate :min aref))
(defn $max [aref] (aggregate :max aref))
(defn $std-dev [aref] (aggregate :std-dev aref))
(defn $std-dev-p [aref] (aggregate :std-dev-p aref))
(defn $var [aref] (aggregate :var aref))
(defn $var-p [aref] (aggregate :var-p aref))

;; Aggregations on relations (count(*) etc.).
(def $count* (aggregate :count))
(def $sum* (aggregate :sum))
(def $avg* (aggregate :avg))
(def $min* (aggregate :min))
(def $max* (aggregate :max))
(def $std-dev (aggregate :std-dev))
(def $std-dev-p (aggregate :std-dev-p))
(def $var (aggregate :var))
(def $var-p (aggregate :var-p))

;; -----------------------------------------------------------------------------
;; -- SQL operators defined in sqlosure.sql.
;; -----------------------------------------------------------------------------

;; Operators
(def $not sql/not$)
(def $is-null? sql/is-null?$)
(def $is-not-null? sql/is-not-null?$)
(def $or sql/or$)
(def $and sql/and$)
(def $>= sql/>=$)
(def $<= sql/<=$)
(def $> sql/<$)
(def $< sql/<$)
(def $plus sql/plus$)
(def $minus sql/minus$)
(def $concat sql/concat$)
(def $lower sql/lower$)
(def $upper sql/upper$)
(def $= sql/=$)
(def $like sql/like$)
(def $in sql/in$)
(def $between sql/between$)


;; -----------------------------------------------------------------------------
;; -- Shortcuts for types and type constructors.
;; -----------------------------------------------------------------------------

;; Constructors
(defn $string [val] (rel/make-const t/string% val))
(defn $integer [val] (rel/make-const t/integer% val))
(defn $double [val] (rel/make-const t/double% val))
(defn $boolean [val] (rel/make-const t/boolean% val))
(defn $null [val] (rel/make-const t/null% val))
(defn $any [val] (rel/make-const t/any% val))
(defn $date [val] (rel/make-const t/date% val))
(defn $timestamp [val] (rel/make-const t/timestamp% val))
(defn $blob [val] (rel/make-const t/blob% val))
;; Nullable types
(defn $string-null [val] (rel/make-const t/string%-nullable val))
(defn $integer-null [val] (rel/make-const t/integer%-nullable val))
(defn $double-null [val] (rel/make-const t/double%-nullable val))
(defn $blob-null [val] (rel/make-const t/blob%-nullable val))

;; Type shortcuts
(def $string-t t/string%)
(def $integer-t t/integer%)
(def $double-t t/double%)
(def $boolean-t t/boolean%)
(def $date-t t/date%)
(def $timestamp-t t/timestamp%)
(def $blob-t t/blob%)
;; Nullable types
(def $string-null-t t/string%-nullable)
(def $integer-null-t t/integer%-nullable)
(def $double-null-t t/double%-nullable)

;; -----------------------------------------------------------------------------
;; -- Helper
;; -----------------------------------------------------------------------------
(defn put-query
  "`put-query` takes a (relational algebra) query and returns it's (SQL-) string
  representation. Uses the default printer from `sqlosure.sql-put`."
  [q]
  (->> q opt/optimize-query rsql/query->sql
       (put/sql-select->string put/default-sql-put-parameterization)))
