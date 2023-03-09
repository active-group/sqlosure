(ns sqlosure.db-connection
  (:require [active.clojure.condition :as c]
            [active.clojure.record :refer [define-record-type]]
            [clojure.data :as data]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]

            [sqlosure.backend :as backend]
            [sqlosure.optimization :as o]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.sql :as sql]
            [sqlosure.sql-put :as put]
            [sqlosure.time :as time]
            [sqlosure.type :as t]
            [sqlosure.type-implementation :as ti])
  (:import [java.sql PreparedStatement ResultSet]))

(define-record-type
  ^{:doc "`db-connection` serves as a container for storing the current
          db-connection as well as backend specific conversion and printer
          functions."}
  db-connection
  (make-db-connection conn backend) db-connection?
  [^{:doc "The database connection map as used by jdbc."}
   conn db-connection-conn
   backend db-connection-backend])

(defn result-set-seq
  "Creates and returns a lazy sequence of maps corresponding to the rows in the
   java.sql.ResultSet rs."
  [^ResultSet rs col-types backend]
  (let [row-values (fn []
                     ;; should cache the method implementations
                     (map-indexed
                      (fn [^Long i ty]
                        (let [type-implementation (backend/get-type-implementation backend ty)
                              sql->type           (ti/type-implementation-from-sql type-implementation)]
                          (sql->type rs (inc i))))
                      col-types))
        rows       ((fn thisfn []
                      (if (.next rs)
                        (cons (vec (row-values))
                              (lazy-seq (thisfn)))
                        (.close rs))))]
    rows))

(defn- log-statement [sql-str]
  ;;(println "Executing sql:" (pr-str sql-str))
  nil)

(defn- set-parameters
  "Add the parameters to the given statement.
  Uses the approriate jdbc get and set functions depending on the type attached
  to the value.
  `param-types+args` is a sequence of vectors `[[$sqlosure-type val] ...]`."
  [stmt param-types+args backend]
  (loop [idx 0
         pta param-types+args]
    (when-not (empty? pta)
      (let [[ty v]              (first pta)
            type-implementation (backend/get-type-implementation backend ty)
            type->sql           (ti/type-implementation-to-sql type-implementation)]
        (type->sql stmt (inc idx) v)
        (recur (inc idx) (rest pta))))))

(defn with-open-jdbc-con [conn f]
  (let [db (db-connection-conn conn)]
    (if-let [con (jdbc/db-find-connection db)]
      (f con)
      (with-open [con (jdbc/get-connection db)]
        (f con (db-connection-backend conn))))))

(defn- compile-query [q backend {:keys [optimize?] :or {optimize? true} :as opts-map}]
  (let [qq                     (if optimize? (o/optimize-query q) q)
        scheme                 (rel/query-scheme qq)
        col-types              (rel/rel-scheme-types scheme)
        [sql param-types+args] (put/sql-select->string
                                (rsql/query->sql qq)
                                (backend/backend-put-parameterization backend))
        jdbc-opts              (dissoc opts-map :optimize?)]
    (fn [con]
      (log-statement sql)
      (let [^PreparedStatement stmt
            (apply jdbc/prepare-statement con sql jdbc-opts)]
        (set-parameters stmt param-types+args backend)
        (.closeOnCompletion stmt)
        (result-set-seq (.executeQuery stmt) col-types backend)))))

(defn run-query
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
  [conn q & {:keys [optimize?] :or {optimize? true} :as opts-map}]
  (let [run (compile-query q (db-connection-backend conn) opts-map)]
    (with-open-jdbc-con conn
      (fn [con]
        (doall (run con))))))

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
         (data/diff full-columns columns))
        (not missing)
        (c/assertion-violation
         `validate-scheme
         "scheme is missing non-nullable keys"
         (data/diff full-m m))
        (not type-diff)
        (c/assertion-violation
         `validate-scheme
         "scheme contains values that do not match types with relation"
         (data/diff full-m m))
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

(defn- prepare-insert [sql-table scheme]
  (let [sql           (insert-statement-string
                       (rel/base-relation-name sql-table)
                       scheme)]
    (fn [con backend]
      (log-statement sql)
      (let [^PreparedStatement stmt
            (jdbc/prepare-statement con sql
                                    {:return-keys true})]
        (fn [vals]
          (let [types+vals (map (fn [ty val]
                                  [ty val])
                                (rel/rel-scheme-types scheme) vals)]
            (.clearParameters stmt)
            (set-parameters stmt types+vals backend)
            (.closeOnCompletion stmt)
            (.executeUpdate stmt)
            (let [^ResultSet result-set (.getGeneratedKeys stmt)]
              (if (.next result-set)
                (.getObject result-set 1)
                result-set))))))))

(defn insert-multi!
  "`insert-multi!` takes a db-connection and an sql-table and a list of lists of values and
  attempts to insert them into the connected databases table, in a more efficient way than calling [insert!] multiple times.
  The table to insert into is the `relational-algebra/base-relation-handle` of
  `sql-table`. If a `scheme` is given, then a subset of the table's columns can be inserted.

  Example:

      (def db-spec { ... })  ;; Your db-connection map.
      ;; For inserts that contain every column of the specified table:
      (insert-multi! (db-connect db-spec) some-table [[arg1 arg2 ... argN] ...])

      ;; For inserts that only specify a subset of all columns:
      (insert-multi! (db-connect db-spec) some-table
                                          (alist->rel-scheme {\"foo\" $integer-t
                                                              \"bar\" $string-t}
                                          [[int1 str1] [int2 str2] ...]))"
  ([conn sql-table vals-lists]
   (insert-multi! conn sql-table (rel/query-scheme sql-table) vals-lists))
  ([conn sql-table scheme vals-lists]
   (let [cq (prepare-insert sql-table scheme)]
     (with-open-jdbc-con conn
       (fn [con]
         (let [run (cq con (db-connection-backend conn))]
           (doall (map run
                       vals-lists))))))))

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
  (let [[scheme vals] (if (and (seq args) (rel/rel-scheme? (first args)))
                        [(first args) (rest args)]
                        [(rel/query-scheme sql-table) args])]
    (first (insert-multi! conn sql-table [vals]))))

(defn- delete-statement-string
  [table-name crit-s]
  (str "DELETE FROM " table-name " WHERE "
       crit-s))

(defn- compile-delete [backend sql-table criterion-proc]
  (let [name                 (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme               (rel/base-relation-scheme sql-table)
        cols                 (rel/rel-scheme-columns scheme)
        put-parameterization (backend/backend-put-parameterization backend)
        [crit-s crit-vals]
        (put/sql-expression->string
         (rsql/expression->sql (apply criterion-proc
                                      (map rel/make-attribute-ref cols)))
         put-parameterization)
        sql                  (delete-statement-string
                              (rel/base-relation-name sql-table)
                              crit-s)]
    (fn [con]
      (log-statement sql)
      (let [^PreparedStatement stmt
            (jdbc/prepare-statement con sql)]
        (set-parameters stmt crit-vals backend)
        (.closeOnCompletion stmt)
        (.executeUpdate stmt)))))

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
  (let [cq (compile-delete (db-connection-backend conn) sql-table criterion-proc)]
    (with-open-jdbc-con conn cq)))

(defn- update-statement-string+vals
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

(defn- compile-update [backend sql-table criterion-proc alist-first & args]
  (let [name                 (sql/sql-table-name (rel/base-relation-handle sql-table))
        scheme               (rel/query-scheme sql-table)
        attr-exprs           (map rel/make-attribute-ref
                                  (rel/rel-scheme-columns scheme))
        alist                (map (fn [[k v]]
                                    [k (rsql/expression->sql v)])
                                  (if (fn? alist-first)
                                    (apply alist-first attr-exprs)
                                    (when args
                                      (cons alist-first args)
                                      alist-first)))
        [crit-s crit-vals] (put/sql-expression->string
                            (rsql/expression->sql (apply criterion-proc attr-exprs))
                            (backend/backend-put-parameterization backend))
        
        [update-string update-types+vals] (update-statement-string+vals
                                           (rel/base-relation-name sql-table)
                                           alist crit-s crit-vals)]
  (fn [con]
    (log-statement update-string)
    (let [^PreparedStatement stmt
          (jdbc/prepare-statement con update-string)]
      (set-parameters stmt update-types+vals backend)
      (.closeOnCompletion stmt)
      (.executeUpdate stmt)))))

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
  (let [cq (apply compile-update (db-connection-backend conn)
                  sql-table criterion-proc alist-first args)]
    (with-open-jdbc-con conn cq)))

(defn run-sql
  [conn sql]
  (jdbc/execute! (db-connection-conn conn) sql))
