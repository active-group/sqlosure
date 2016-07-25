(ns sqlosure.db-type
  (:require [active.clojure
             [condition :as c]
             [record :as r]]
            [clojure.java.jdbc :as jdbc]
            [clojure.spec :as s]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [galaxy :as glxy]
             [relational-algebra :as rel]
             [sql :as sql]
             [type :as t]
             [universe :as universe]
             [utils :as u]]))

;; SEE https://github.com/active-group/active-clojure/blob/master/src/active/clojure/record.cljc
;; Slight modification of the `active-clojure.record/define-record-type`
;; which prepends an id field to every record.
(defmacro define-record-type
  "Attach doc properties to the type and the field names to get reasonable
  docstrings."
  [?type ?constructor-call ?predicate ?field-specs & ?opt+specs]
  (when-not (and (list? ?constructor-call)
                 (not (empty? ?constructor-call)))
    (throw (IllegalArgumentException.
            (str "constructor call must be a list in " *ns* " " (meta &form)))))
  (when-not (vector? ?field-specs)
    (throw (IllegalArgumentException.
            (str "field specs must be a vector in " *ns* " " (meta &form)))))
  (when-not (even? (count (remove seq? ?field-specs)))
    (throw (IllegalArgumentException.
            (str "odd number of elements in field specs in "
                 *ns* " " (meta &form)))))
  (when-not (every? true? (map #(= 3 (count %1)) (filter seq? ?field-specs)))
    (throw (IllegalArgumentException.
            (str "wrong number of elements in field specs with lens in "
                 *ns* " " (meta &form)))))
  (let [?field-triples
        (loop [specs (seq ?field-specs)
               ;; Always prepend the id selector.
               triples (list ['id (symbol (str ?type "-id")) nil])]
          (if (empty? specs)
            (reverse triples)
            (let [spec (first specs)]
              (cond
                (list? spec)
                (do
                  (when-not (and (= 3 (count spec))
                                 (every? symbol spec))
                    (IllegalArgumentException.
                     (str "invalid field spec " spec " in "
                          *ns* " " (meta &form))))
                  (recur (rest specs) (list* spec triples)))
                (symbol? spec)
                (do
                  (when (empty? (rest specs))
                    (throw (IllegalArgumentException.
                            (str "incomplete field spec for " spec " in "
                                 *ns* " " (meta &form)))))
                  (when-not (symbol? (fnext specs))
                    (throw (IllegalArgumentException.
                            (str "invalid accessor " (fnext specs) " for " spec
                                 " in " *ns* " " (meta &form)))))
                  (recur (nnext specs)
                         (list* [spec (fnext specs) nil] triples)))
                :else
                (throw (IllegalArgumentException.
                        (str "invalid field spec " spec " in " *ns* " "
                             (meta &form))))))))
        ?constructor (first ?constructor-call)
        ?constructor-args (cons 'id (rest ?constructor-call))
        ?constructor-args-set (set ?constructor-args)
        document (fn [n doc]
                   (vary-meta n
                              (fn [m]
                                (if (contains? m :doc)
                                  m
                                  (assoc m :doc doc)))))
        document-with-arglist
        (fn [n arglist doc]
          (vary-meta n
                     (fn [m]
                       (let [m (if (contains? m :doc)
                                 m
                                 (assoc m :doc doc))]
                         (if (contains? m :arglists)
                           m
                           (assoc m :arglists `'(~arglist)))))))
        name-doc (fn [field]
                   (if-let [doc (:doc (meta field))]
                     (str " (" doc ")")
                     ""))

        ?field-names (map first ?field-triples)
        reference (fn [name]
                    (str "[[" (ns-name *ns*) "/" name "]]"))
        ?docref (str "See " (reference ?constructor) ".")]
    (let [?field-names-set (set ?field-names)]
      (doseq [?constructor-arg ?constructor-args]
        (when-not (contains? ?field-names-set ?constructor-arg)
          (throw (IllegalArgumentException.
                  (str "constructor argument " ?constructor-arg
                       " is not a field in " *ns* " " (meta &form)))))))
    `(do
       (defrecord ~?type
                  [~@(map first ?field-triples)]
         ~@?opt+specs)
       (def ~(document-with-arglist
              ?predicate '[thing]
              (str "Is object a `" ?type "` record? " ?docref))
         (fn [x#]
           (instance? ~?type x#)))
       (def ~(document-with-arglist
              ?constructor
              (vec ?constructor-args)
              (str "Construct a `" ?type "`"
                   (name-doc ?type)
                   " record.\n"
                   (apply str
                          (map (fn [[?field ?accessor ?lens]]
                                 (str "\n`" ?field "`" (name-doc ?field)
                                      ": access via " (reference ?accessor)
                                      (if ?lens
                                        (str ", lens " (reference ?lens)) "")))
                               ?field-triples))))
         (fn [~@?constructor-args]
           (new ~?type
                ~@(map (fn [[?field _]]
                         (if (contains? ?constructor-args-set ?field)
                           `~?field
                           `nil))
                       ?field-triples))))
       (declare ~@(map (fn [[?field ?accessor ?lens]] ?accessor)
                       ?field-triples))
       ~@(mapcat
          (fn [[?field ?accessor ?lens]]
            (let [?rec (with-meta `rec# {:tag ?type})]
              `((def ~(document-with-arglist
                       ?accessor (vector ?type)
                       (str "Access `" ?field "` field"
                            (name-doc ?field)
                            " from a [[" ?type "]] record. " ?docref))
                  (fn [~?rec]
                    (. ~?rec ~(symbol (str "-" ?field)))))
                ~@(if ?lens
                    (let [?data `data#
                          ?v `v#]
                      `((def ~(document ?lens
                                        (str "Lens for the `" ?field "` field"
                                             (name-doc ?field)
                                             " from a [[" ?type "]] record."
                                             ?docref))
                          (active.clojure.lens/lens
                           ~?accessor
                           (fn [~?data ~?v]
                             (~?constructor
                              ~@(map
                                 (fn [[?shove-field ?shove-accessor]]
                                   (if (= ?field ?shove-field)
                                     ?v
                                     `(~?shove-accessor ~?data)))
                                 ?field-triples)))))))
                    '()))))
          ?field-triples))))

(defn- sql-universe-lookup!
  [function what]
  (or (function sql/sql-universe what)
      (c/assertion-violation `sql-universe-lookup!
                             "No such value" function what)))

(defn get-type!
  [type-name]
  (sql-universe-lookup! universe/universe-lookup-type type-name))

(defn get-base-relation!
  [rel-name]
  (sql-universe-lookup! universe/universe-lookup-base-relation rel-name))

(defn cons-id-field
  [m]
  (into {} (cons {"id" t/integer%} (map (fn [[k v]] [(name k) v]) m))))

(defn db-name-of
  [thing]
  (clojure.string/replace (name thing) #"-" "_"))

(defmacro define-record
  [?name ?fields]
  `(define-record-type ~?name
     ~(apply list (symbol (str "really-make-" ?name))
             (map #(-> % db-name-of symbol) ?fields))
     ~(symbol (str ?name "?"))
     ~(into [] (apply concat (for [f# ?fields]
                               [(symbol (db-name-of f#))
                                (symbol (str ?name "-" (db-name-of f#)))])))))

(defn table*
  [sql-name type-map & [opts]]
  (let [universe (get opts :universe)]
    (sql/make-sql-table
     (db-name-of sql-name)
     (rel/alist->rel-scheme type-map)
     :universe universe)))

(defn scheme*
  [alist]
  (rel/alist->rel-scheme alist))

(defn db-installer-strings
  [type-map]
  (mapv (fn [[f t]] [(name f) (t/-to-string (if (t/-galaxy-type? t)
                                              t/integer%
                                              t))]) type-map))

(defn installer*
  [nom type-map]
  (fn [db]
    (jdbc/db-do-prepared
     (db/db-connection-conn db)
     (jdbc/create-table-ddl nom (db-installer-strings type-map)))))

(defn make-id->val
  [glxy-name db->val]
  (fn id->val [id]
    (let [table (rel/base-relation-handle
                 (get-base-relation! glxy-name))
          v
          (first (db/run-query @*current-db-connection*
                               (query [vs (<- (get-base-relation! glxy-name))]
                                      (restrict ($= (! vs "id")
                                                    ($integer id)))
                                      (project vs))))]
      (db->val v))))

(s/fdef make-db->val
        :args (s/cat :constr fn? :type-map ::type-map)
        :ret  (s/fspec :args (s/cat :vs (s/coll-of (constantly true) []))
                       :ret  rel/const?))

(defn make-db->val
  [glxy-name constr type-map]
  (if-not (reduce (fn [acc v] (or acc (t/-galaxy-type? v)))
                  false (vals type-map))
    ;; If this is the case, we need not worry. Just apply the constructor to the
    ;; result and return it.
    (fn [vs]
      (if (= 1 (count vs))
        (let [querier (t/atomic-type-const->datum-fn
                       (get-type! glxy-name))]
          (querier (first vs)))
        (apply constr vs)))
    ;; Otherwise, things get a little more complicated and we have to assume
    ;; that every non-default-type already is defined with it's own type/galaxy/
    ;; etc.
    (fn [vs]
      (let [types (vec (vals type-map))]
        (loop [i 0
               ts (mapv #(t/-galaxy-type? %) types)
               vs vs
               res []]
          (if (empty? ts)
            (apply constr res)
            (if-not (first ts)
              ;; Base case, just keep the value.
              (recur (inc i)
                     (vec (rest ts))
                     (vec (rest vs))
                     (conj res (first vs)))
              ;; 'Complex' type. This means what we have here is an id for some
              ;; record stored in another galaxy.
              (let [typ (get types i)
                    querier (t/atomic-type-const->datum-fn typ)
                    v (querier (first vs))]
                (recur (inc i)
                       (vec (rest ts))
                       (vec (rest vs))
                       (conj res v))))))))))

(defn make-val->db
  [type-map]
  (fn [v]
    (glxy/make-tuple (into [] (mapv (fn [v' t]
                                      (rel/make-const t v')) 
                                    (vals v) (vals type-map))))))

(defn all-sqlosure-types?
  "Is every value in the seq `ts` a `sqlosure` type?"
  [ts]
  (every? #(satisfies? t/base-type-protocol %) ts))

(defn check-types
  [vs type-map]
  (let [vs (keys type-map)
        ts (vals type-map)]
    (when (not= (count vs) (count ts))
      (c/assertion-violation `check-types "unequal lenghts" vs ts))
    (let [bools (map (fn [ix v t]
                       [ix ((t/atomic-type-predicate t) v)])
                     (range 0 (count vs)) vs ts)]
      (cond
        (not (all-sqlosure-types? ts))
        (c/assertion-violation
         `check-types "types vector contains non-sqlosure type values" ts)
        (u/count= vs ts)
        (or (u/and? (map second bools))
            (let [idx (ffirst (filter #(-> % second false?) bools))]
              [idx (get vs idx) (get ts idx)]))
        :else
        (c/assertion-violation
         `check-types "sequences of unequal length" vs ts)))))

(defn constructor*
  [data-constructor-fn type-map]
  (fn constructor [& args]
    (let [checked-args (check-types args (vals type-map))]
      (if-not (vector? checked-args)
        (apply data-constructor-fn args)
        (c/assertion-violation
         `constructor*
         (str "mismatch, expected val of type " (t/-name (get checked-args 2))
              ", got " (type (get checked-args 1))
              " (" (get checked-args 1) ")"
              " full value" checked-args))))))

(defn make-selector
  "This function returns a query-monad operation to query for one field of a
  galaxy record. Not intended for usage outside of `define-product-type`.

  Args:

  * `selector-name`: The name of this selector (SQL-name)
  * `in-type`: Type that goes into the generated function
  * `out-type`: Type that is returned by this function
  * `selector`: How to select the value from a record (data level)
  * `idx`: Position of the value in the tuple returned from the query-monad"
  [selector-name in-type out-type selector idx]
  (let [rator (rel/make-rator selector-name
                              (fn [fail arg-type]
                                (when-not (= arg-type in-type)
                                  fail)
                                out-type)
                              selector
                              :universe sql/sql-universe
                              :data (glxy/make-db-operator-data
                                     nil
                                     (fn [rec & _]
                                       (get (glxy/tuple-expressions rec) idx))))]
    (fn [rand]
      (rel/make-application rator rand))))

(r/define-record-type product-type
  (really-make-product-type scheme table installer id->value-fn db->value-fn type)
  product-type?
  [scheme product-type-scheme
   table product-type-table
   installer product-type-installer
   id->value-fn product-type-id->value-fn
   db->value-fn product-type-db->value-fn
   type product-type-type])

(defn make-product-type
  [nom constructor predicate id-selector type-map*]
  (let [type-map  (->> (map (fn [[k v]] [(db-name-of k) v])
                            (cons-id-field type-map*))
                       (into {}))
        scheme    (scheme* type-map)
        table     (table* nom type-map)
        installer (installer* nom type-map)
        db->val   (make-db->val nom constructor type-map)
        id->val   (make-id->val nom db->val)
        val->db   (make-val->db type-map)
        the-type  (glxy/make-db-type nom
                                     predicate
                                     id->val
                                     id-selector
                                     scheme
                                     db->val
                                     val->db)
        sql-sel-names (map (fn [k]
                             (symbol (str "$" nom "-" k))) (keys type-map))
        sql-selectors
        (map-indexed (fn [i [k v]]
                       (let [sel-name (symbol (str "$" nom "-" k))]
                         (make-selector sel-name the-type v sel-name i)))
                     type-map)]
    ;; Insert all values into the current (as in 'calling') namespace.
    (intern *ns* (symbol (str nom "-scheme")) scheme)
    (intern *ns* (symbol (str nom "-table")) table)
    (intern *ns* (symbol (str nom "-installer")) installer)
    (intern *ns* (symbol (str nom "-id->val")) id->val)
    (intern *ns* (symbol (str nom "-db->val")) db->val)
    (intern *ns* (symbol (str nom "-val->db")) db->val)
    (intern *ns* (symbol (str "$" nom "-t")) the-type)
    (intern *ns* (symbol (str nom "-galaxy"))
            (glxy/make&install-db-galaxy nom the-type installer table
                                         {:universe sql/sql-universe}))
    ;; Create a set of selector functions.
    (loop [sql-sel-names sql-sel-names
           sql-selectors sql-selectors]
      (let [n (first sql-sel-names)
            s (first sql-selectors)]
        (when n
          (intern *ns* n s)
          (recur (rest sql-sel-names) (rest sql-selectors)))))
    the-type))
