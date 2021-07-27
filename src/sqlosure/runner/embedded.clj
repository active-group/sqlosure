(ns sqlosure.runner.embedded
  "Definition of an 'embedded' query runner that operates on Clojure data-structures.
  The data-structure this runner operates on is a map from strings (corresponding
  to the [[sqlosure.relational-algebra/base-relation-handle]]'s name to a vector
  of maps.

  Example:

  (def table (sqlosure.core/table \"foo\" [[\"a\" sqlosure.core/$integer-t]
                                           [\"b\" sqlosure.core/$boolean-t]]))

  (def db {\"foo\" #{{\"a\" 42, \"b\" true}
                     {\"a\" 23, \"b\" false}}})

  (sqlosure.runner/run-query (runner db) table)
  ;; => [[42 true], [23 false]]
  "
  (:require [active.clojure.condition :as c]
            [active.clojure.lens :as lens]
            [active.clojure.monad :as monad]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.lang :as lang]))

(defn alist-lookup-value
  "Return all keys to a value from an alist."
  [alist val]
  (reduce (fn [acc [k v]]
            (if (= val v)
              (conj acc k)
              acc))
          []
          alist))

(defn extend-keys
  "Takes two alists `m` and `extensions` and 'extends' `m` be each pair in `extensions`.
  This means that each key in `m` is looked up in the keys of `extensions`.
  If the key exists in the values of `extensions`, the corresponding pair from
  `extesions` is added to `m`."
  [m extensions]
  (reduce (fn [acc [k v]]
            (let [ex (alist-lookup-value extensions k)]
              (if (empty? ex)
                (assoc acc k v)
                (merge acc
                       (into {} [[k v]])
                       (into {} (map (fn [e] [e v]) ex))))))
          {}
          m))

(defn rand->value
  [row r]
  (cond
    (rel/const? r)
    (rel/const-val r)

    (rel/attribute-ref? r)
    (get row (rel/attribute-ref-name r))))

(defn some-aggregation?
  "Returns truthy when `thing` is an aggregation expression."
  [thing]
  (or (rel/aggregation? thing)
      (rel/aggregation-all? thing)))

(defn unroll-project-alist
  "Takes a projection's alist and returns an unrolled alist.
  Unrolled means that [[sqlosure.relational-algrebra/attribute-ref]]s are
  replaced with the reference name. Aggregations remain untouched."
  [alist]
  (mapv (fn [[k v]]
          [k (if (some-aggregation? v)
               v
               (rel/attribute-ref-name v))])
        alist))

(defn alist->aggregations
  "Takes an `alist` and returns all pairs that contain an aggregation as their
  right-hand-side value."
  [alist]
  (filter (comp some-aggregation? second) alist))

(defn interpret-aggregation
  [[field aggr] rows]
  (cond
    (rel/aggregation? aggr)
    (let [attr (rel/attribute-ref-name (rel/aggregation-expr aggr))
          getter (fn [attr row]
                   (if-let [res (get row attr)]
                     res
                     (c/assertion-violation `interpret-aggregation "attribute not found in record" row attr)))
          rows (map (partial getter attr) rows)]
      {field (case (rel/aggregation-operator aggr)
               :count (count rows)
               :sum   (reduce + 0 rows)
               :avg   (/ (reduce + 0 rows)
                         (count rows))
               :min   (apply min rows)
               :max   (apply max rows)
               (:std-dev :std-dev-p :var :var-p)
               (throw (java.lang.UnsupportedOperationException. "no implemented yet")))})

    (rel/aggregation-all? aggr)
    {field (case (rel/aggregation-all-operator aggr)
             :count-all (count rows)

             (c/assertion-violation `interpret-aggregation "not a valid aggregation" aggr))}

    :else
    (c/assertion-violation `interpret-aggregation "not a valid aggregation" aggr)))

(defn base-relation->handle
  [q]
  (let [handle (rel/base-relation-handle q)]
    (cond
      (string? handle)        handle
      (sql/sql-table? handle) (sql/sql-table-name handle)

      :else
      (c/assertion-violation `interpret "not a valid handle" handle))))

(defn unroll-record
  "Takes a `query` and a `record` (map) and returns the `record`'s values
  according to the `query`s scheme."
  [query record]
  (let [scheme-cols (rel/rel-scheme-columns (rel/query-scheme query))]
    (map (partial get record) scheme-cols)))

(declare apply-restriction)

(defn unroll-rand
  [row operand]
  (cond
    (rel/const? operand)
    (rel/const-val operand)

    (rel/attribute-ref? operand)
    (get row (rel/attribute-ref-name operand))

    (rel/application? operand)
    (apply-restriction operand row)))

(defn translate-comparison-op
  [sym]
  (cond
    (= sym '<)
    (fn [a b] (.isBefore a b))

    (= sym '>)
    (fn [a b] (.isAfter a b))

    (= sym '<=)
    (fn [a b] (or (.isBefore a b)
                  (= a b)))
    (= sym '>=)
    (fn [a b] (or (.isAfter a b)
                  (= a b)))

    (= sym '=)  =))

(defn apply-restriction
  [restriction row]
  ;; a restriction is always an application
  (when-not (rel/application? restriction)
    (c/assertion-violation `apply-restriction "not an application" restriction))
  (let [rator          (rel/application-rator restriction)
        rands          (rel/application-rands restriction)
        unrolled-rands (map (partial unroll-rand row) rands)]
    ;; We need to be a little more careful when applying functions here.
    ;; For example, timestamps and dates are valid rands for <, but
    ;; must be compared by .isBefore, etc.
    (cond
      ;; Handle dates explicitly
      (or (every? #(instance? java.time.LocalDateTime %) unrolled-rands)
          (every? #(instance? java.time.LocalDateTime %) unrolled-rands))
      (apply (translate-comparison-op (rel/rator-name rator)) unrolled-rands)
      :else
      (apply (rel/rator-proc rator) unrolled-rands))))

(defn run-query
  [db q]
  (letfn [(interpret* [db q]
            (cond
              (rel/empty-query? q)
              nil

              (rel/base-relation? q)
              (get db (base-relation->handle q))

              (rel/project? q)
              (let [sub-q        (rel/project-query q)
                    alist        (rel/project-alist q)
                    extensions   (unroll-project-alist alist)
                    sub-res      (interpret* db sub-q)
                    aggregations (alist->aggregations extensions)]
                ;; We need to look ahead for grouping sub-queries because
                ;; there are some restrictions on how those need to be evaluated.
                ;; Especially, a grouping query must be projected via an aggregation.
                (cond
                  ;; Base case
                  (and (empty? aggregations)
                       (not (rel/group? sub-q)))
                  (let [select (mapv (comp rel/attribute-ref-name second) alist)]
                    (mapv (fn [row]
                            (extend-keys (select-keys row select) extensions))
                          sub-res))

                  (and aggregations
                       (not (rel/group? sub-q)))
                  (let [aggr (first aggregations)]
                    [(interpret-aggregation aggr sub-res)])

                  (and aggregations
                       (rel/group? sub-q))
                  (let [groups      sub-res
                        aggr        (first aggregations)
                        projections (filter (comp (comp not some-aggregation?) second) extensions)
                        select      (mapv second projections)
                        res         (map (fn [[grp rows]]
                                           (let [aggr-row      (interpret-aggregation aggr rows)
                                                 non-aggr-rows (map (fn [row]
                                                                      (extend-keys (select-keys row select) projections))
                                                                    rows)]
                                             (merge aggr-row (first non-aggr-rows))))
                                         groups)]
                    res)))

              (rel/restrict? q)
              (let [app   (rel/restrict-exp q)
                    query (rel/restrict-query q)
                    sub   (interpret* db query)]
                (if (rel/restrict? query)
                  ;; TODO How to handle multiple restrict queries in sequence?
                  (c/assertion-violation `interpret-query "sequential restricts are not supported" q)
                  (into [] (filter (partial apply-restriction app) sub))))

              (rel/restrict-outer? q)
              (throw (java.lang.UnsupportedOperationException. "no implemented yet"))

              (rel/combine? q)
              (throw (java.lang.UnsupportedOperationException. "no implemented yet"))

              (rel/order? q)
              (let [[ref direction] (first (rel/order-alist q))
                    attr            (rel/attribute-ref-name ref)
                    sub             (interpret* db (rel/order-query q))
                    res             (sort-by #(get % attr) sub)]
                (if (= :ascending direction)
                  res
                  (reverse res)))

              (rel/group? q)
              (let [cols (rel/group-columns q)
                    sub  (interpret* db (rel/group-query q))]
                (group-by #(get % (first cols)) sub))

              (rel/top? q)
              (let [cnt    (rel/top-count q)
                    offset (rel/top-offset q)
                    sub    (interpret* db (rel/top-query q))]
                ;; Depends on the order!
                (take cnt (drop (or offset 0) sub)))

              (rel/distinct-q? q)
              (let [sub (interpret* db (rel/distinct-q-query q))]
                (distinct sub))

              :else
              (c/assertion-violation `interpret "not a query" q)))
          (record->vector [cols record]
            (mapv #(get record %) cols))]
    (mapv (comp (partial into []) (partial unroll-record q)) (interpret* db q))))

(defn ensure-schema!
  [relation record]
  (when-not (= (count (rel/rel-scheme-columns (rel/base-relation-scheme relation))) (count record))
    (c/assertion-violation `ensure-schema! "record does not match relation's schema" relation record)))

(defn run-insert
  [db relation record]
  (ensure-schema! relation record)
  (let [handle  (base-relation->handle relation)
        cols    (rel/rel-scheme-columns (rel/base-relation-scheme relation))
        record  (into {} (map vector cols record))
        records (conj (into #{} (get db handle)) record)]
    (assoc db handle records)))

(declare interpret-application)

(defn interpret-rand
  [rand]
  (cond
    (rel/const? rand)       (rel/const-val rand)
    (rel/application? rand) (interpret-application rand)
    :else
    rand))

(defn interpret-rands
  [rands]
  (map interpret-rand rands))

(defn interpret-application
  [application]
  (when-not (rel/application? application)
    (c/assertion-violation `interpret-application "not an application" application))
  (let [rator          (rel/application-rator application)
        rands          (rel/application-rands application)
        resolved-rands (interpret-rands rands)
        proc           (rel/rator-proc rator)]
    (apply proc resolved-rands)))

(defn run-delete
  "Takes a `database` (map), a relation and a predicate function that takes the
  values of one record and returns a [[sqlosure.relational-algebra/Application]].
  Returns a tuple of [number-of-affected-rows new-db]."
  [db relation predicate-fn]
  (let [handle (base-relation->handle relation)
        [affected-rows-count new-records]
        (->> (get db handle)
             (reduce (fn [[affected-rows-count records] record]
                       (if (interpret-application (apply predicate-fn (unroll-record relation record)))
                         [(inc affected-rows-count) records]
                         [affected-rows-count (conj records record)]))
                     [0 []])
             (into []))]
    [affected-rows-count (assoc db handle (into #{} new-records))]))

(defn apply-updates
  [relation records predicate-fn update-fn]
  (letfn [(prepare-replacement [update-fn record]
            (into {} (map (fn [[k v]] [k (rel/const-val v)]) (apply update-fn (unroll-record relation record)))))]
    (->> records
         (reduce (fn [[affected-rows-count records] record]
                   (if (interpret-application (apply predicate-fn (unroll-record relation record)))
                     (let [new-record (merge record (prepare-replacement update-fn record))]
                       [(inc affected-rows-count) (conj records new-record)])
                     [affected-rows-count (conj records record)]))
                 [0 []]))))

(defn run-update
  "Takes a `database` (map), a relation, a predicate function that takes the
  values of one record and returns a [[sqlosure.relational-algebra/Application]]
  and an updated function that takes the values of a record and returns a map
  with the updated fields (must only contain the attributes that actually should
  be changed).
  Returns a tuple of [number-of-affected-rows new-db]."
  [db relation predicate-fn update-fn]
  (let [handle                            (base-relation->handle relation)
        [affected-rows-count new-records] (apply-updates relation (get db handle) predicate-fn update-fn)]
    [affected-rows-count (assoc db handle (into #{} new-records))]))

(defn run-db-action
  [lens run-any env state m]
  (let [db (lens/yank state lens)]
    (cond
      (lang/create? m)
      (let [new-db (run-insert db (lang/create-table m) (lang/create-record m))]
        [nil
         (lens/shove state lens new-db)])

      (lang/read? m)
      [(run-query db (lang/read-query m))
       state]

      (lang/update? m)
      (let [[affected-rows-count new-db] (run-update db
                                                     (lang/update-table m)
                                                     (lang/update-predicate-fn m)
                                                     (lang/update-update-fn m))]
        [(lang/make-write-result affected-rows-count)
         (lens/shove state lens new-db)])

      (lang/delete? m)
      (let [[affected-rows-count new-db] (run-delete db
                                                     (lang/delete-table m)
                                                     (lang/delete-predicate-fn m))]
        [(lang/make-write-result affected-rows-count)
         (lens/shove state lens new-db)])

      :else
      monad/unknown-command)))

(defn command-config
  "Takes a `db` and returns command config for running queries with the embedded
  runner.
  The database will be 'isolated' in the sense that it will use its own part of
  the state without interfering with anything else."
  [db]
  (monad/make-monad-command-config (partial run-db-action (lens/member ::db)) {} {::db db}))

(defn integrated-command-config
  "Takes a `lens` and returns command config for running queries with the
  embedded runner.
  The lens focuses on some part of the state this command-config is embedded in.
  It is in this sense 'integrated'.

  Use this if you have some larger state where the embedded db is supposed to be
  at some specific place in the state rather than just under `::db`."
  [lens]
  (monad/make-monad-command-config (partial run-db-action lens) {} {}))
