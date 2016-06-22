(ns sqlosure.db-connection
  (:require [active.clojure
             [condition :as c]
             [record :refer [define-record-type]]]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [sqlosure
             [galaxy :as glxy]
             [optimization :as o]
             [relational-algebra :as rel]
             [relational-algebra-sql :as rsql]
             [sql :as sql]
             [sql-put :as put]
             [time :as time]
             [type :as t]])
  (:import [java.sql PreparedStatement ResultSet]))

(define-record-type
  ^{:doc "`db-connection` serves as a container for storing the current
          db-connection as well as backend specific conversion and printer
          functions."}
  db-connection
  (make-db-connection conn) db-connection?
  [^{:doc "The database connection map as used by jdbc."}
   conn db-connection-conn])

(def get-from-result-set-method
  "Creates the type method `::get-from-result-set` with a default get function
  using `.getObject` on every input value and type."
  (t/make-type-method ::get-from-result-set
                      (fn [^ResultSet rs ix]
                        (.getObject rs ix))))

(defn result-set-seq
  "Creates and returns a lazy sequence of maps corresponding to the rows in the
   java.sql.ResultSet rs."
  [^ResultSet rs col-types]
  (let [row-values (fn []
                     ;; should cache the method implementations
                     (map-indexed
                      (fn [^Integer i ty]
                        (t/invoke-type-method
                         ty
                         get-from-result-set-method
                         rs
                         (inc i)))
                      col-types))
        rows ((fn thisfn []
                (if (.next rs)
                  (cons (vec (row-values))
                        (lazy-seq (thisfn)))
                  (.close rs))))]
    rows))

(def set-parameter-method
  "Creates the type method `::set-parameter` with a default set function
  using `.setObject` on every input value and type."
  (t/make-type-method ::set-parameter
                      (fn [^PreparedStatement stmt ix val]
                        (.setObject stmt ix val))))

(defn- set-parameters
  "Add the parameters to the given statement.
  Uses the approriate jdbc get and set functions depending on the type attached
  to the value.
  `param-types+args` is a sequence of vectors `[[$sqlosure-type val] ...]`."
  [stmt param-types+args]
  ;; FIXME: don't do map
  (dorun (map-indexed (fn [ix [ty val]]
                        (t/invoke-type-method ty
                                              set-parameter-method
                                              stmt
                                              (inc ix)
                                              val))
                      param-types+args)))

(defn define-type-method-implementations
  "Conveniently define the `set-parameter` and `get-parameter` of a type
  simultaniously."
  [ty set-parameter-fn get-parameter-fn]
  (swap! (t/-method-map-atom ty)
         assoc
         (t/type-method-name set-parameter-method) set-parameter-fn
         (t/type-method-name get-from-result-set-method) get-parameter-fn))

;; Type method implementations
(define-type-method-implementations t/string%
  (fn [^PreparedStatement stmt ix val] (.setString stmt ix val))
  (fn [^ResultSet rs ix] (.getString rs ix)))

(define-type-method-implementations t/string%-nullable
  (fn [^PreparedStatement stmt ix val]
    (if (nil? val)
      (.setNull stmt ix java.sql.Types/VARCHAR)
      (.setString stmt ix val)))
  (fn [^ResultSet rs ix] (let [v (.getString rs ix)]
                           (when-not (.wasNull rs) v))))

(define-type-method-implementations t/integer%
  (fn [^PreparedStatement stmt ix val] (.setInt stmt ix val))
  (fn [^ResultSet rs ix] (.getInt rs ix)))

(define-type-method-implementations t/integer%-nullable
  (fn [^PreparedStatement stmt ix val]
    (if (nil? val)
      (.setNull stmt ix java.sql.Types/INTEGER)
      (.setInt stmt ix val)))
  (fn [^ResultSet rs ix] (let [v (.getInt rs ix)]
                           (when-not (.wasNull rs) v))))

(define-type-method-implementations t/double%
  (fn [^PreparedStatement stmt ix val] (.setDouble stmt ix val))
  (fn [^ResultSet rs ix] (.getDouble rs ix)))

(define-type-method-implementations t/double%-nullable
  (fn [^PreparedStatement stmt ix val]
    (if (nil? val)
      (.setNull stmt ix java.sql.Types/DOUBLE)
      (.setDouble stmt ix val)))
  (fn [^ResultSet rs ix] (let [v (.getDouble rs ix)]
                           (when-not (.wasNull rs) v))))

(define-type-method-implementations t/boolean%
  (fn [^PreparedStatement stmt ix val] (.setBoolean stmt ix val))
  (fn [^ResultSet rs ix] (.getBoolean rs ix)))

(define-type-method-implementations t/blob%
  (fn [^PreparedStatement stmt ix val] (.setBlob stmt ix val))
  (fn [^ResultSet rs ix] (.getBlob rs ix)))

(define-type-method-implementations t/blob%-nullable
  (fn [^PreparedStatement stmt ix val] (if (nil? val)
                                         (.setNull stmt ix java.sql.Types/BLOB)
                                         (.setBlob stmt ix val)))
  (fn [^ResultSet rs ix] (.getBlob rs ix)))

(define-type-method-implementations t/clob%
  (fn [^PreparedStatement stmt ix val] (.setClob stmt ix val))
  (fn [^ResultSet rs ix] (.getClob rs ix)))

(define-type-method-implementations t/date%
  ;; NOTE `val` here is a `java.time.LocalDate` which has to be coerced to and
  ;;      from `java.sql.Date` first.
  (fn [^PreparedStatement stmt ix val]
    (.setDate stmt ix (time/to-sql-date val)))
  (fn [^ResultSet rs ix]
    (time/from-sql-date (.getDate rs ix))))

(define-type-method-implementations t/date%-nullable
  ;; NOTE `val` here is a `java.time.LocalDate` which has to be coerced to and
  ;;      from `java.sql.Date` first.
  (fn [^PreparedStatement stmt ix val]
    (if (nil? val)
      (.setNull stmt ix java.sql.Types/DATE)
      (.setDate stmt ix (time/to-sql-date val))))
  (fn [^ResultSet rs ix]
    (time/from-sql-date (.getDate rs ix))))

(define-type-method-implementations t/timestamp%
  (fn [^PreparedStatement stmt ix val]
    (.setTimestamp stmt ix (time/to-sql-timestamp val)))
  (fn [^ResultSet rs ix]
    (time/from-sql-timestamp (.getTimestamp rs ix))))

(define-type-method-implementations t/timestamp%-nullable
  (fn [^PreparedStatement stmt ix val]
    (if (nil? val)
      (.setNull stmt ix java.sql.Types/TIMESTAMP)
      (.setTimestamp stmt ix (time/to-sql-timestamp val))))
  (fn [^ResultSet rs ix]
    (time/from-sql-timestamp (.getTimestamp rs ix))))

(defn- run-query*
  "Takes a database connection and a query and runs it against the database.

  Example:

      (def db-spec { ... })  ;; Your db-connection map.
      (def kv-table (table \"kv\" {\"key\" $integer-t
                                 \"value\" $string-t}))
      ;; Get all records from kv-table where \"key\" is less than 10.
      (run-query (db-connect db-spec)
                 (query [kv (<- kv-table)]
                        (restrict ($> (! k \"key\")
                                    ($integer 10)))
                        (project {\"value\" (! kv \"value\")})))"
  [conn q & [opts]]
  (let [qq (if (:optimize? opts) (o/optimize-query q) q)
        scheme (rel/query-scheme qq)
        col-types (rel/rel-scheme-types scheme)
        asql (rsql/query->sql qq)
        [sql & param-types+args] (put/sql-select->string asql)
        db (db-connection-conn conn)
        run-query-with-params
        (^{:once true} fn* [con]
         (let [^PreparedStatement stmt
               (apply jdbc/prepare-statement con sql
                      (dissoc opts :optimize?))]
           (set-parameters stmt param-types+args)
           (.closeOnCompletion stmt)
           (result-set-seq (.executeQuery stmt) col-types)))]
    (if-let [con (jdbc/db-find-connection db)]
      (doall
       (run-query-with-params con))
      (with-open [con (jdbc/get-connection db)]
        (doall ; sorry
         (run-query-with-params con))))))

(defn db-query-reified-results
  "Takes a db-connetion `conn` and a query `q` and runs the query against the
  connected database. The result is returnd reified, which means all data is
  transformed in the shape specified by it's corresponding data type."
  [db q & [opts]]
  (let [[db-q _] (glxy/dbize-query q)
        db-res (run-query* db db-q opts)
        scheme (rel/query-scheme q)]
    (mapv #(glxy/reify-query-result % scheme opts) db-res)))

;; TODO Add '?' to :as-maps
(defn run-query
  "Opts:

  - `:galaxy-query?` (boolean): should the results be refied? (default = false)
  - `:opimitze?` (boolean): should the query be optimized? (default = true)
  - `:as-maps` (boolean): should the result set be a vector of maps containing
                          the record-column'name name? (default = false)"
  [db q & [opts]]
  (if (get opts :galaxy-query?)
    (db-query-reified-results db q (dissoc opts :galaxy-query?))
    (run-query* db q (dissoc opts :galaxy-query?))))

(defn- validate-scheme
  "`validate-scheme` takes two rel-schemes and checks if they obey the following
  rules:

  - `scheme` must not contain keys not present in `full-scheme`
  - `scheme` must contain all non-nullable fields present in `full-scheme`
  - `scheme`'s vals for each key must not differ in type from those in
    `full-scheme`"
  [full-scheme scheme]
  (letfn [(no-extra-keys [ks1 ks2]
            (empty? (set/difference (set ks2) (set ks1))))
          (no-type-difference [m1 m2]
            (reduce
             (fn [acc [k v]] (and acc (= v (get m1 k)))) true m2))
          (no-missing-non-nullable-types [m1 m2]
            (reduce
             (fn [acc [k v]]
               (if (not (t/-nullable? v))
                 (and acc (get m2 k))
                 acc)) true m1))]
    (let [[full-columns columns] [(rel/rel-scheme-columns full-scheme)
                                  (rel/rel-scheme-columns scheme)]
          [full-m m] [(rel/rel-scheme-map full-scheme)
                      (rel/rel-scheme-map scheme)]
          extra (no-extra-keys full-columns columns)
          missing (no-missing-non-nullable-types full-m m)
          type-diff (no-type-difference full-m m)]
      (cond
        (not extra)
        (c/assertion-violation
         `validate-scheme
         "scheme contains extra keys not present in relation"
         (clojure.data/diff full-columns columns))
        (not missing)
        (c/assertion-violation
         `validate-scheme
         "scheme is missing non-nullable keys"
         (clojure.data/diff full-m m))
        (not type-diff)
        (c/assertion-violation
         `validate-scheme
         "scheme contains values that do not match types with relation"
         (clojure.data/diff full-m m))
        :else true))))

(defn- insert-statement-string
  [table-name scheme]
  (let [values (->> (rel/rel-scheme-columns scheme)
                    (map (constantly "?"))
                    (interpose ", ")
                    (apply str))]
    (str "INSERT INTO " table-name " ("
         (apply str (interpose ", " (rel/rel-scheme-columns scheme))) ") "
         "VALUES (" values ")")))

(defn- rel-is-galaxy?
  "Returns true if the argument represents a galaxy rather than a sql-table."
  [rel]
  (glxy/db-galaxy? (rel/base-relation-handle rel)))

(defn extract-table
  "Takes a `sqlosure.relational-algebra/base-relation` and checks, whether its
  handle is a sql-table or a galaxy. In case of a galaxy, it extracts the
  underlying query (which in turn should be a base-relation). Otherwise, it just
  returns it's input."
  [table-or-galaxy]
  (assert (rel/base-relation? table-or-galaxy))
  (if (rel-is-galaxy? table-or-galaxy)
    (-> table-or-galaxy
        rel/base-relation-handle
        glxy/db-galaxy-query)
    table-or-galaxy))

(defn insert!
  "`insert!` takes a db-connection and an sql-table and some rest `args` and
  attempts to insert them into the connected databases table.
  The table to insert into is the `relational-algebra/base-relation-handle` of
  `sql-table`.

  `args` can either be:

   - a set of values to insert if there is a value for each column. Those
     must be in the order of the columns in the table.
   - a rel-scheme, specifying the columns to insert the values into follwed
     by the desired values.

  Example:

      (def db-spec { ... })  ;; Your db-connection map.
      ;; For inserts that contain every column of the specified table:
      (insert! (db-connect db-spec) some-table arg1 arg2 ... argN)

      ;; For inserts that only specify a subset of all columns:
      (insert! (db-connect db-spec) some-table
                                    (alist->rel-scheme {\"foo\" $integer-t
                                                        \"bar\" $string-t}
                                    integer-value string-value))"
  [conn sql-table & args]
  (let [;; Check if the queried table is a galaxy. If so, replace it with ther
        ;; actual table reference.
        ;; FIXME This may become unnecessary as soon as we're able to insert
        ;; real records instead of a list of arguments into galaxies.
        sql-table* (extract-table sql-table)
        [scheme vals] (if (and (seq args) (rel/rel-scheme? (first args)))
                        [(first args) (rest args)]
                        [(rel/query-scheme sql-table*)
                         (if (rel-is-galaxy? sql-table)
                           (vals (first args))
                           args)])
        db (db-connection-conn conn)
        run-query-with-params
        (^{:once true} fn* [con]
         (let [^PreparedStatement stmt
               (jdbc/prepare-statement
                con
                (insert-statement-string
                 (rel/base-relation-name sql-table)
                 scheme))
               types+vals (map (fn [ty val]
                                 (if (t/-galaxy-type? ty)
                                   [t/integer% (:id val)]
                                   [ty val]))
                               (rel/rel-scheme-types scheme) vals)]
           (set-parameters stmt types+vals)
           (.closeOnCompletion stmt)
           (.executeUpdate stmt)))]
    (if-let [con (jdbc/db-find-connection db)]
      (run-query-with-params con)
      (with-open [con (jdbc/get-connection db)]
        (doall (run-query-with-params con))))))

(defn- delete-statement-string
  [table-name crit-s]
  (str "DELETE FROM " table-name " WHERE "
       crit-s))

(defn delete!
  "`delete!` takes a db-connection, a sql-table and a criterion function and
  deletes all entries in `sql-table` that satisfy the criteria.
  `criterion-proc` is a function that takes as many arguments as the relation
  has fields. In it's body one can specify the criteria in the same manner as
  in regular sqlosure queries.

  Example:

      (def db-spec { ... })  ;; Your db-connection map.
      (def kv-table (table \"kv\" {\"key\" $integer-t
                                 \"value\" $string-t}))
      ;; Delete all records where key > 10.
      (delete! (db-connect db-spec) kv-table
               (fn [k v]
                 ($> k ($integer 10))))"
  [conn sql-table criterion-proc]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme (rel/base-relation-scheme sql-table)
        cols (rel/rel-scheme-columns scheme)
        db (db-connection-conn conn)
        [crit-s & crit-vals]
        (put/sql-expression->string
         (rsql/expression->sql (apply criterion-proc
                                      (map rel/make-attribute-ref cols))))
        run-query-with-params
        (^{:once true} fn* [con]
         (let [^PreparedStatement stmt
               (jdbc/prepare-statement
                con
                (delete-statement-string
                 (rel/base-relation-name sql-table)
                 crit-s))]
           (set-parameters stmt crit-vals)
           (.closeOnCompletion stmt)
           (.executeUpdate stmt)))]
    (if-let [con (jdbc/db-find-connection db)]
      (run-query-with-params con)
      (with-open [con (jdbc/get-connection db)]
        (run-query-with-params con)))))

(defn- update-statement-string
  [table-name set-vals crit-s? crit-vals?]
  (let [set-vals-string (->> set-vals
                             (map (fn [[k v]]
                                    (str k "=?")))
                            (interpose ", ")
                            (apply str))
        set-vals-vals (->> set-vals
                           (map second)
                                (map (fn [v]
                                       [(sql/sql-expr-const-type v)
                                        (sql/sql-expr-const-val v)])))
        crit-string (->> crit-s?
                         first)
        types+vals (concat set-vals-vals crit-vals?)]
    [(str "UPDATE " table-name " SET " set-vals-string
           (when crit-s?
             (str " WHERE " crit-s?)))
     types+vals]))

(defn update!
  "`update!` takes a db-connection, a sql-table, a criterion-proc and and map
  and updates all entries of `sql-table` that satisfy `criterion-proc` with the
  new key->value pairs specified in `alist-first` function.

  Example:

      (def db-spec { ... })  ;; Your db-connection map.
      (def kv-table (table \"kv\" {\"key\" $integer-t
                                 \"value\" $string-t}))
      ;; Update the \"value\" field all records where key > 10 to \"NEWVAL\".
      (update! (db-connect db-spec) kv-table
               (fn [k v]
                 ($> k ($integer 10)))  ;; Read: \"key\" > 10
               (fn [k v]
                 {\"value\" ($string \"NEWVAL\")})"
  [conn sql-table criterion-proc alist-first & args]
  (let [name (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme (rel/query-scheme sql-table)
        db (db-connection-conn conn)
        attr-exprs (map rel/make-attribute-ref
                        (rel/rel-scheme-columns scheme))
        alist (map (fn [[k v]]
                     [k (rsql/expression->sql v)])
                   (if (fn? alist-first)
                     (apply alist-first attr-exprs)
                     (when args
                       (cons alist-first args)
                       alist-first)))
        [crit-s & crit-vals]
        (put/sql-expression->string
         (rsql/expression->sql (apply criterion-proc attr-exprs)))
        [update-string update-types+vals]
        (update-statement-string
         (rel/base-relation-name sql-table)
         alist crit-s crit-vals)
        run-query-with-params
        (^{:once true} fn* [con]
         (let [^PreparedStatement stmt
               (jdbc/prepare-statement
                con
                update-string)]
           (set-parameters stmt update-types+vals)
           (.closeOnCompletion stmt)
           (.executeUpdate stmt)))]
    ;; update-types+vals
    (if-let [con (jdbc/db-find-connection db)]
      (run-query-with-params con)
      (with-open [con (jdbc/get-connection db)]
        (run-query-with-params con)))))

(defn run-sql
  "Takes a db-connection `conn` and a raw SQL query and executes it (calling out
  to jdbc's execute function)."
  [conn sql]
  (jdbc/execute! (db-connection-conn conn) sql))
