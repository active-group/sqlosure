(ns sqlosure.sql-put
  "Utilities for printing SQL.
  Functions can be combined to monadic programs. The result of such program can be executed via `run`,
  provided a valid [[sqlosure.sql-put/sql-put-parameterization]]."
  (:require [active.clojure.condition :as c]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.monad :as m]
            [clojure.string :as string]

            [sqlosure.sql :as sql]))

(define-record-type sql-put-parameterization
  (make-sql-put-parameterization alias-proc combine-proc) sql-put-parameterization?
  [alias-proc   sql-put-parameterization-alias-proc
   combine-proc sql-put-parameterization-combine-proc])

(defn write!
  [s & ss]
  (m/sequ_
   (map
    #(m/update-state-component! ::write-out conj %)
    (cons s ss))))

(defn default-put-alias
  "When alias is not nil, print \" AS alias\"."
  [alias]
  (if alias
    (write! "AS" alias)
    (m/return nil)))

(declare put-sql-select)

(defn default-put-combine
  [op left right]
  (m/monadic (write! "(")
             (put-sql-select left)
             (write! ")")
             (write!(case op
                      :union        "UNION"
                      :intersection "INTERSECT"
                      :difference   "EXCEPT"))
             (write! "(")
             (put-sql-select right)
             (write! ")")))

(def default-sql-put-parameterization
  (make-sql-put-parameterization
   default-put-alias
   default-put-combine))

(def ask-put-parameterization! (m/get-env-component ::put-parameterization))
(def ask-put-alias
  (m/monadic [pp ask-put-parameterization!]
             (m/return (or (sql-put-parameterization-alias-proc pp)
                           default-put-alias))))
(def ask-put-combine
  (m/monadic [pp ask-put-parameterization!]
             (m/return (or (sql-put-parameterization-combine-proc pp)
                           default-put-combine))))

(defn put-alias
  [alias]
  (m/monadic [put-alias* ask-put-alias]
             (put-alias* alias)))

(defn put-combine
  [op left right]
  (m/monadic [put-combine* ask-put-combine]
             (put-combine* op left right)))

(defn add-argument!
  [type val]
  (m/update-state-component! ::arguments conj [type val]))

;; Actual writing functions.

(defn put-padding-if-non-null
  "When xs is not empty, prepend a space to (proc lis)."
  [xs proc]
  (if (empty? xs)
    (m/return nil)
    (proc xs)))

(defn put-literal
  [type val]
  (m/monadic (write! "?")
             (add-argument! type val)))

(declare put-sql-select-1)

(defn put-sql-select
  [sel]
  (cond
    (or (sql/sql-select? sel)
        (sql/sql-select-combine? sel))
    (put-sql-select-1 sel)

    (sql/sql-select-table? sel)
    (m/monadic (write! "SELECT * FROM")
               (let [table-name (sql/sql-select-table-name sel)])
               (if-let [table-space (sql/sql-select-table-space sel)]
                 (write! (string/join "." [table-space table-name]))
                 (write! table-name)))

    :else
    (c/assertion-violation `put-sql-select (str "unhandled query " (pr-str sel)))))

(defn put-joining-infix
  [xs between proc]
  (if (empty? xs)
    (m/return nil)
    (m/monadic (proc (first xs))
               (m/sequ_ (map (fn [x]
                               (m/monadic (if (empty? between)
                                            (m/return nil)
                                            (write! between))
                                          (proc x)))
                             (rest xs))))))

(defn put-table
  [[alias select]]
  (if (sql/sql-select-table? select)
    (m/monadic
     (let [table-name (sql/sql-select-table-name select)
           table-space (sql/sql-select-table-space select)])
     (if table-space
       (write! (string/join "." [table-space table-name]))
       (write! table-name))
     (put-alias alias))
    (m/monadic (write! "(")
               (put-sql-select select)
               (write! ")")
               (put-alias alias))))

(defn put-tables
  [tables between]
  (put-joining-infix tables between put-table))

(declare put-sql-expression)

(defn put-when
  [p]
  (m/monadic (write! "WHEN")
             (put-sql-expression (first p))
             (write! "THEN")
             (put-sql-expression (second p))))

(defn put-condition
  [exprs]
  (put-joining-infix exprs "AND" put-sql-expression))

(defn put-with-condition
  [clause]
  (fn put-with-condition-fn [exprs]
    (m/monadic (write! clause)
               (put-condition exprs))))

(def put-where (put-with-condition "WHERE"))
(def put-on (put-with-condition "ON"))
(def put-having (put-with-condition "HAVING"))

(defn put-group-by
  "Takes a seq of sql-expr."
  [group-by]
  (m/monadic (write! "GROUP BY")
             (put-joining-infix group-by "," write!)))

(defn put-order-by
  "Takes a seq of [sql-expr, sql-order]."
  [order-by]
  (m/monadic (write! "ORDER BY")
             (put-joining-infix order-by ","
                                (fn [[a b]]
                                  (m/monadic (put-sql-expression a)
                                             (write! (case b
                                                       :ascending  "ASC"
                                                       :descending "DESC")))))))

(defn put-attributes
  [attributes]
  (if (empty? attributes)
    (write! "*")
    (put-joining-infix attributes ","
                       (fn [[col expr]]
                         (if (and (sql/sql-expr-column? expr)
                                  (= col (sql/sql-expr-column-name expr)))
                           (write! col)
                           (m/monadic (put-sql-expression expr)
                                      (put-alias col)))))))

(defn put-sql-join
  "Put the tables involved in the join of a SQL select."
  [tables outer-tables]
  (cond
    (nil? tables)
    (m/return nil)

    (or (empty? outer-tables)
          (= (count tables) 1))
    (m/monadic
     (put-padding-if-non-null tables
                              (fn [tables]
                                (m/monadic (write! "FROM")
                                           (put-tables tables ","))))
     (if (not-empty outer-tables)
       (m/monadic
        (write! "LEFT JOIN")
        ;; every LEFT JOIN needs an ON
        (put-tables outer-tables "ON (1=1) LEFT JOIN"))
       (m/return nil)))

    :else
    ;; outer tables AND more than one regular table
    (m/monadic
     (write! "FROM (SELECT * FROM")
     (put-tables tables ",")
     (write! ")")
     (put-alias nil)
     (write! "LEFT JOIN")
     (put-tables outer-tables "ON (1=1) LEFT JOIN"))))

(defn put-sql-select-1
  [sel]
  (cond
    ;; This is just an empty select.
    (= sel (sql/new-sql-select))
    (m/return nil)

    (sql/sql-select? sel)
    (m/monadic (write! "SELECT")
               (put-padding-if-non-null (sql/sql-select-options sel)
                                        #(apply write! %))
               (put-attributes (sql/sql-select-attributes sel))
               (put-sql-join (sql/sql-select-tables sel)
                             (sql/sql-select-outer-tables sel))
               (put-padding-if-non-null (sql/sql-select-outer-criteria sel) put-on)
               (put-padding-if-non-null (sql/sql-select-criteria sel) put-where)
               (put-padding-if-non-null (sql/sql-select-group-by sel) put-group-by)

               (let [h (sql/sql-select-having sel)])
               (if (empty? h) (m/return nil) (put-having h))

               (put-padding-if-non-null (sql/sql-select-order-by sel) put-order-by)

               (let [extra (sql/sql-select-extra sel)])
               (if (empty? extra) (m/return nil) (apply write! extra)))

    (sql/sql-select-combine? sel)
    (put-combine
     (sql/sql-select-combine-op sel)
     (sql/sql-select-combine-left sel)
     (sql/sql-select-combine-right sel))

    (sql/sql-select-table? sel)
    (write! (sql/sql-select-table-name sel))

    (sql/sql-select-empty? sel)
    (write! "")

    :else
    (c/assertion-violation `put-sql-select-1 (str "unknown select " (pr-str sel)))))

(defn put-sql-expression
  [expr]
  (cond
    (sql/sql-expr-column? expr)
    (write! (sql/sql-expr-column-name expr))

    (sql/sql-expr-app? expr)
    (let [op         (sql/sql-expr-app-rator expr)
          [r1 r2 r3] (sql/sql-expr-app-rands expr)
          name       (sql/sql-operator-name op)]
      (case (sql/sql-operator-arity op)
        ;; postfix
        -1
        (m/monadic (write! "(")
                   (put-sql-expression r1)
                   (write! ")" name))

        ;; prefix
        1
        (m/monadic (write! name "(")
                   (put-sql-expression r1)
                   (write! ")"))

        ;; prefix 2
        -2
        (m/monadic (write! name "(")
                   (put-sql-expression r1)
                   (write! ",")
                   (put-sql-expression r2)
                   (write! ")"))

        ;; infix 2
        2
        (m/monadic (write! "(")
                   (put-sql-expression r1)
                   (write! name)
                   (put-sql-expression r2)
                   (write! ")"))

        3
        (m/monadic (write! "(")
                   (put-sql-expression r1)
                   (write! name)
                   (put-sql-expression r2)
                   (write! "AND")
                   (put-sql-expression r3)
                   (write! ")"))
        (c/assertion-violation `put-sql-expression
                               (str "unhandled operator arity " (pr-str op)))))

    (sql/sql-expr-const? expr)
    (put-literal (sql/sql-expr-const-type expr) (sql/sql-expr-const-val expr))

    (sql/sql-expr-tuple? expr)
    (m/monadic (write! "(")
               (put-joining-infix (sql/sql-expr-tuple-expressions expr) "," put-sql-expression)
               (write! ")"))

    (sql/sql-expr-case? expr)
    ;; FIXME : Case wird dargestellt:
    ;; CASE val?
    ;; (WHEN ... THEN ...)+
    ;; (ELSE ...)? END
    ;; -> val optional! (kann Zeile, Expression oder Subquery mit
    ;; Einelementigem Rückgabewert sein)
    ;; (-> ELSE muss nicht vorhanden sein)
    ;; -> END fehlt
    ;; (an PostgreSQL getestet)
    (m/monadic (write! "(CASE")
               (m/sequ_ (map put-when (sql/sql-expr-case-branches expr)))
               (write! "ELSE")
               (put-sql-expression (sql/sql-expr-case-default expr))
               (write! "END)"))

    (sql/sql-expr-exists? expr)
    (m/monadic (write! "EXISTS (")
               (put-sql-select (sql/sql-expr-exists-select expr))
               (write! ")"))

    (sql/sql-expr-subquery? expr)
    (m/monadic (write! "(")
               (put-sql-select (sql/sql-expr-subquery-query expr))
               (write! ")"))

    :else
    (c/assertion-violation `put-sql-expression (str "unhandled expression" (pr-str expr)))))

(defn run
  [put-parameterization m]
  (let [[ret state]
        (m/run-free-reader-state-exception
         (m/null-monad-command-config {::put-parameterization put-parameterization}
                                      {::write-out []
                                       ::arguments []})
         m)]
    [(clojure.string/join " " (::write-out state)) (::arguments state)]))

(defn sql-expression->string
  [expr put-parameterization]
  (run put-parameterization (put-sql-expression expr)))

(defn sql-select->string
  [expr put-parameterization]
  (run put-parameterization (put-sql-select expr)))
