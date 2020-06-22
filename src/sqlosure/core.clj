(ns sqlosure.core
  (:require [active.clojure.monad :as monad]
            [active.clojure.condition :as c]
            [active.clojure.monad :refer :all]
            [sqlosure.db-connection :as db]
            [sqlosure.optimization :as opt]
            [sqlosure.query-comprehension :as qc]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.sql :as sql]
            [sqlosure.sql-put :as put]
            [sqlosure.type :as t]
            [sqlosure.utils :as utils]))

(defn db-connect
  "`db-connect` takes a connection map and returns a `db-connection`-record for
  that backend. Dispatches on the `:classname` key in `db-spec`."
  [db-spec backend]
  (db/make-db-connection db-spec backend))

(defn variadic->alist
  [arg1 arg2 args]
  (when-not (even? (count args))
    (c/assertion-violation `variadic->alist "args must be even" args))
  (let [pairs (partition 2 args)]
    (cons (list arg1 arg2) pairs)))

(defn table
  "Returns a `sqlosure.relational-algebra/base-relation`.
  `sql-name` ist the name of the table in the DBMS, `map` is a map of column-name
  -> sqlosure.type-type. `opts` may contain a :universe key and a universe
  (defaults to `nil`)."
  [sql-name map & opts]
  (let [opts-m (apply hash-map opts)
        universe? (get opts-m :universe)]
    (sql/base-relation sql-name
                       (rel/alist->rel-scheme map)
                       :universe universe?)))

(defmacro query
  "`query` takes a series of relational algebra statements (projection,
  selection, grouping, etc.) and returns an executable relational algebra query.
  The last statement must be a monadic return (either return or projection)."
  [& ?forms]
  `(qc/get-query (monadic ~@?forms)))

(defn run-db-with-state
  "Runs the database program `prog` using the provided `command-config`.
  Returns the result plus the final state."
  [command-config prog]
  (monad/run-free-reader-state-exception command-config prog))

(defn run-db
  "Runs the database program `prog` using the provided `command-config`.
  Returns the result or the program."
  [command-config prog]
  (first (run-db-with-state command-config prog)))

;; -----------------------------------------------------------------------------
;; -- QUERY COMPREHENSION OPERATORS
;; -----------------------------------------------------------------------------

;; Reexports of query-monad operators.
(defn embed
  "Embed a RA query into the current query."
  [q]
  (qc/embed q))

(defn <-
  "Embed a RA query into the current query."
  [q]
  (qc/embed q))

(defn !
  "`!` selects an attribute from a relation.

      Example: (! t \"id\") corresponds to SQL \"t.id\"."
  [rel name]
  (qc/! rel name))

(defn restrict!
  "Restrict the current query by a condition.

  expr -> query(nil)

  Note this doesn't return anything."
  [expr]
  (qc/restrict expr))

(defn restrict
  [expr]
  (utils/print-deprecation-warning! `restrict `restrict!)
  (restrict! expr))

(defn restrict-outer!
  "Restrict outer part of the current query by a condition.

  expr -> query(nil)

  Note: this is a monadic action that doesn't return anything."
  [expr]
  (qc/restrict-outer expr))

(defn restrict-outer
  [expr]
  (utils/print-deprecation-warning! `restrict-outer `restrict-outer!)
  (restrict-outer! expr))

(defn restricted!
  "Restrict the current query by a condition. Returns the resulting state.

  Example:
      (query [t (<- embed t-table)]
                (restricted t ($<= (! t \"id\")
                                   ($integer 1))

  The corresponding SQL statement would be \"SELECT <all cols of t> FROM t WHERE id <= 1\"

  relation expr -> query(relation)."
  [expr]
  (qc/restricted expr))

(defn restricted
  [expr]
  (utils/print-deprecation-warning! `restricted `restricted!)
  (restricted! expr))

(defn group!
  "Group by specified seq of column references `[rel name]`.
  Example
      (query [t (<- embed t-table)]
             (group [t \"some_field\"])"
  [alist]
  (qc/group alist))

(defn group
  [alist]
  (utils/print-deprecation-warning! `group `group!)
  (group! alist))

(defn project
  "Project some columns of the current query. Returns the resulting state.

  Example:
      (query [t (<- embed t-table)]
                (project {\"foo\" (! t \"foo\")
                          \"bar\" (! t \"bar\"))

  The corresponding SQL statemant would be \"SELECT foo, bar FROM t\"."
  ([alist-or-relation]
   (if (qc/relation? alist-or-relation)
     (return alist-or-relation)
     (qc/project alist-or-relation)))  ; This is just for backwards compatibility.
  ([project-name project-field & more-projections]
   (qc/project (variadic->alist project-name project-field more-projections))))

(defn order!
  "Takes an alist of [[attribute-ref] :descending/:ascending] to order
  the result by this attribute.

  Example:
      (query [t (embed t-table)]
             (order {(! t \"foo\") :ascending})
             (project {\"foo\" (! t \"foo\")}))

  The corresponding SQL statemant would be \"SELECT foo FROM t ORDER BY foo ASC\"."
  ([alist]
   (qc/order alist))
  ([attr direction & more-orders]
   (qc/order (variadic->alist attr direction more-orders))))

(defn order
  [alist]
  (utils/print-deprecation-warning! `order `order!)
  (order! alist))

(defn top!
  "`top` is used to define queries that return a cerain number of entries.
  When called with one argument `n`, top constructs a query that only returns
  the first `n` elements.
  Whan called with two arguments `offset` and `n`, top constructs a query that
  returns the first `n` elements with an offset of `offset`."
  ([n] (qc/top nil n))
  ([offset n] (qc/top offset n)))

(def top top!)

(def distinct!
  "`distinct!` is used to define queries that only return distinct rows.
  Will result in a `DISTINCT` clause in the resulting SQL."
  qc/distinct!)

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
      (c/assertion-violation `aggregate "expected attribute-ref" a)))
  (apply rel/make-aggregation aggregation-op attribute))

;; Aggregations on attributes.
(defn $count
  "Aggregation. Count column `aref`."
  [aref]
  (aggregate :count aref))

(defn $sum
  "Aggregation. Sum column `aref`."
  [aref]
  (aggregate :sum aref))

(defn $avg
  "Aggregation. Calculates average of column `aref`."
  [aref]
  (aggregate :avg aref))

(defn $min
  "Aggregation. Calculates minimum of column `aref`."
  [aref]
  (aggregate :min aref))

(defn $max
  "Aggretation. Calculates maximum of column `aref`."
  [aref]
  (aggregate :max aref))

(defn $std-dev
  "Aggregation. Calculates the standard deviation of column `aref`."
  [aref]
  (aggregate :std-dev aref))

(defn $std-dev-p [aref] (aggregate :std-dev-p aref))

(defn $var
  "Aggregation. Calculates the variance of column `aref`."
  [aref]
  (aggregate :var aref))

(defn $var-p [aref] (aggregate :var-p aref))

;; Aggregations on relations (count(*) etc.).
(def ^{:doc "Aggregation. Count whole relation (`COUNT(*)`)."}
  $count* (aggregate :count-all))
(def ^{:doc "Aggregation. Sum up whole relation (`SUM(*)`)."}
  $sum* (aggregate :sum))
(def ^{:doc "Aggregation. Calculate average of whole relation (`AVG(*)`)."}
  $avg* (aggregate :avg))
(def ^{:doc "Aggregation. Calculate minimum of whole relation (`MIN(*)`)."}
  $min* (aggregate :min))
(def ^{:doc "Aggregation. Calculate maximum of whole relation (`MAX(*)`)."}
  $max* (aggregate :max))
(def ^{:doc "Aggregation. Calculate standart deviation of whole relation (`STDEV(*)`)."}
  $std-dev (aggregate :std-dev))
(def $std-dev-p (aggregate :std-dev-p))
(def ^{:doc "Aggregation. Calculate variance of whole relation (`VAR(*)`)."}
  $var (aggregate :var))
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
(def $> sql/>$)
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
(defn $bytea [val] (rel/make-const t/bytea% val))
;; Nullable types
(defn $string-null [val] (rel/make-const t/string%-nullable val))
(defn $integer-null [val] (rel/make-const t/integer%-nullable val))
(defn $double-null [val] (rel/make-const t/double%-nullable val))
(defn $blob-null [val] (rel/make-const t/blob%-nullable val))
(defn $bytea-null [val] (rel/make-const t/bytea%-nullable val))

;; Type shortcuts
(def $string-t t/string%)
(def $integer-t t/integer%)
(def $double-t t/double%)
(def $boolean-t t/boolean%)
(def $date-t t/date%)
(def $timestamp-t t/timestamp%)
(def $blob-t t/blob%)
(def $bytea-t t/bytea%)
;; Nullable types
(def $string-null-t t/string%-nullable)
(def $integer-null-t t/integer%-nullable)
(def $double-null-t t/double%-nullable)
(def $blob-null-t t/blob%-nullable)
(def $bytea-null-t t/bytea%-nullable)
(def $date-null-t t/date%-nullable)

;; -----------------------------------------------------------------------------
;; -- Helper
;; -----------------------------------------------------------------------------
(defn put-query
  "`put-query` takes a (relational algebra) query and returns it's (SQL-) string
  representation. Uses the default printer from `sqlosure.sql-put`."
  [q & {:keys [optimize?]}]
  (let [optimize (if optimize? opt/optimize-query identity)]
    (-> q
        optimize
        rsql/query->sql
        (put/sql-select->string put/default-sql-put-parameterization))))
