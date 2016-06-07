(ns sqlosure.sugar
  (:require [active.clojure
             [condition :as c]
             [record :refer [define-record-type]]]
            [clojure.java.jdbc :as jdbc]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [type :as t]]
            [sqlosure.galaxy.galaxy :as glxy]))

(defn cons-id-field
  [m]
  (into {} (cons {"id" '$integer-t}
                 (map (fn [[k v]]
                        [(name k) v]) m))))

;; NOTE This seems hella hacky -- there must be another way for this and all
;;      it's uses.
(defn- symbol->value
  [sym]
  (when-let [v (first (remove nil? (map #(ns-resolve % sym) (all-ns))))]
    (var-get v)))

(defn check-types
  "Check the types of values passed to a function. If the vectors match up,
  returns true, otherwise returns `[failure-index failure-val failure-type]` of
  first mismatch.'

  Args:

  * `vs`: a vector of values to be type-checked
  * `ts`: a vector of `sqlosure.type`s the `vs` should satisfy"
  [vs ts]
  (let [bools (map (fn [ix v t] [ix ((t/atomic-type-predicate t) v)])
                   (range 0 (count vs)) vs ts)]
    (if (every? #(satisfies? t/base-type-protocol %) ts)
      (cond
        (= (count vs) (count ts))
        (or (every? true? (map second bools))
            (let [idx (ffirst (filter #(-> % second false?) bools))]
              [idx (get vs idx) (get ts idx)]))
        :else (c/assertion-violation `check-types
                                     "sequences of unequal length" vs ts))
      (c/assertion-violation `check-types
                             "types vector contains non-sqlosure type values"
                             ts))))

(defn make-db->val
  [constructor]
  (fn [vs]
    (apply constructor vs)))

(defn make-val->db
  [ts]
  (c/assert (every? #(satisfies? t/base-type-protocol %) ts) "invalid type(s).")
  (fn [v]
    (glxy/make-tuple (into [] (mapv (fn [v' t]
                                      (rel/make-const t v')) 
                                    (vals v) ts)))))

;; TODO Write tests.
(defmacro define-db-type
  [nom pred ts]
  (let [db-type-name# (symbol (str "$" nom "-t"))
        constr-name# (symbol (str "$" nom))]
    `(def ~db-type-name#
       (glxy/make-db-type ~(str nom)
                          ~pred
                          nil nil
                          ~(symbol (str nom "-scheme"))
                          (def ~(symbol (str "db->" nom))
                            ~(str "Takes a db-record and returns a " nom ".")
                            (make-db->val ~constr-name#))
                          (def ~(symbol (str nom "->db"))
                            ~(str "Takes a " nom " and returns it's db-representation.")
                            (make-val->db ~ts))))))

(defn make-selector
  "Args:

  * `selector-name`: The name of this selector (SQL-name)
  * `in-type`: Type that goes into the generated function
  * `out-type`: Type that is returned by this function
  * `selector`: How to select the value from a record (data level)
  * `idx`: Position of the value in the tuple returned from the query-monad"
  [selector-name in-type out-type selector idx]
  (let [rator (rel/make-rator selector-name
                              (fn [fail arg-typ]
                                (when-not (= arg-typ
                                             (if (symbol? in-type)
                                               (symbol->value in-type)
                                               in-type))
                                  fail)
                                (if (symbol? out-type)
                                  (symbol->value out-type)))
                              selector
                              :universe sql/sql-universe
                              :data (glxy/make-db-operator-data
                                     nil
                                     (fn [rec & _]
                                       (get (glxy/tuple-expressions rec) idx))))]
    (fn [rand]
      (rel/make-application rator rand))))

;; TODO Write tests.
(defmacro define-constructor
  "Define a type-checking constructor function.

  Args:

  * function-name `fn-name?`
  * `fields` vector which will serve as the arguments vector to the
    constructor
  * `types` vector representing the types of the constructor arguments."
  [fn-name fields types]
  (let [fn-name# (symbol (str "$" fn-name))
        constr-name# (symbol (str "really-make-" fn-name))
        field-names# (mapv #(-> % name symbol) fields)
        args# (into [] field-names#)]
    `(defn ~fn-name#
       ~args#
       (let [res# (~check-types ~args# ~types)]
         (if-not (vector? res#)
           (~constr-name# ~@field-names#)
           ;; Throw an assertion if the types do not match the constructor.
           (c/assertion-violation `(str fn-name#)
                                  (str "mismatch, expected val of type "
                                       (t/-name (get res# 2))
                                       ", got "
                                       (type (get res# 1))
                                       " (" (get res# 1) ")")))))))

;; TODO Write tests. (really?)
(defn zip
  [xs ys]
  (mapv (fn [x y] [x y]) xs ys))

(defn db-installer-strings
  [fields types]
  (c/assert (and (seq fields) (seq types)) "fields and types must not be empty")
  (into [] (for [[f t] (zip fields types)]
             [(name f) (t/-to-string
                        (if (symbol? t) (symbol->value t) t))])))

;; TODO Write tests.
(defn make-db-installer!
  [nom fields types]
  (fn [db]
    (jdbc/db-do-prepared (db/db-connection-conn db)
                         (jdbc/create-table-ddl
                          nom
                          (db-installer-strings fields types)))))

(defn- table*
  [sql-name the-map & opts]
  (let [opts-m (apply hash-map opts)
        universe? (get opts-m :universe)]
    (sql/make-sql-table
     (clojure.string/replace (str sql-name) #"\-" "_")
     (rel/alist->rel-scheme
      (into {} (map (fn [[k t]]
                      [k (symbol->value t)])
                    the-map)))
     :universe universe?)))

(defn- scheme*
  [sql-name the-map]
  (rel/alist->rel-scheme (into {} (map (fn [[k t]]
                                         [k (symbol->value t)])
                                       the-map))))

(defn- galaxy*
  [nom fields types]
  (let [nom* (str nom)]
    (glxy/make&install-db-galaxy
     nom*
     (symbol->value (symbol (str "$" nom "-t")))
     (symbol->value (symbol (str "install-" nom "-table!")))
     (symbol->value (symbol (str nom "-table"))))))

(defmacro define-product-type
  "Args:

  * `?name`: name of the type to be defined. Will subsequently be used as the
             base-name for all other names.
  * `?map`: A map of field-names to `sqlosure.type` types. Those can either be
            atomic-types predefined in `sqlosure.type` or user-defined types.
  * `?opts`: Optional arguments to customize the defined type."
  [?name ?map & ?opts]
  (let [?map-with-id# (cons-id-field ?map)
        ?fields# (into [] (keys ?map-with-id#))
        ?types# (into [] (vals ?map-with-id#))
        ?args-vec# (mapv #(-> % name symbol) ?fields#)
        ?sel-names# (mapv #(symbol (str ?name "-" (name %))) ?fields#)
        ?sql-sel-names# (mapv #(symbol (str "$" ?name "-" (name %))) ?fields#)]
    `(do
       (define-record-type ~?name
         ~(apply list (symbol (str "really-make-" ?name))
                 (map (fn [[k _]] (symbol (name k))) ?map-with-id#))
         ~(symbol (str ?name "?"))
         ~(into [] (apply concat (for [[k _] ?map-with-id#]
                                   [(symbol (name k))
                                    (symbol (str ?name "-" (name k)))]))))
       (def ~(symbol (str ?name "-table")) ~(table* ?name ?map-with-id#))
       (def ~(symbol (str ?name "-scheme")) ~(scheme* ?name ?map-with-id#))
       (define-constructor ~?name ~?fields# ~?types#)
       (define-db-type ~?name (symbol (str ~?name "?")) ~?types#)
       (def ~(symbol (str "install-" ?name "-table!"))
         ~(make-db-installer! ?name ?fields# ?types#))
       (def ~(symbol (str ?name "-galaxy")) ~(galaxy* ?name ?fields# ?types#))
       ;; Create a set of query-monad accessor functions.
       ~@(for [i (range 0 (count ?sel-names#))]
           `(def ~(get ?sql-sel-names# i)
              ~(make-selector (str (get ?sel-names# i))
                              (symbol (str "$" ?name "-t"))
                              (get ?types# i)
                              (get ?sel-names# i) i))))))
