(ns sqlosure.sugar
  (:require [active.clojure
             [condition :as c]
             [record :refer [define-record-type]]]
            [clojure.java.jdbc :as jdbc]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [type :as t]]
            [sqlosure.galaxy.galaxy :as glxy]))

(def ... nil)

(defn cons-id-field
  [m]
  (into {} (cons {"id" '$integer-t} m)))

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
    (or (every? true? (map second bools))
        (let [idx (ffirst (filter #(-> % second false?) bools))]
          [idx (get vs idx) (get ts idx)]))))

(defn make-db->val
  [constructor]
  (fn [& vs]
    (apply constructor vs)))

(defn make-val->db
  [ts]
  (fn [v]
    (glxy/make-tuple (into [] (mapv (fn [v' t]
                                      (rel/make-const t v')) 
                                    (vals v) ts)))))

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

  * `t`: the (db-)type this selector is for.
  * `field`: which field to select"
  [selector-name in-type out-type selector idx]
  (rel/make-monomorphic-combinator selector-name
                                   [in-type]
                                   out-type
                                   selector
                                   :data
                                   (glxy/make-db-operator-data
                                    nil
                                    (fn [rec & args]
                                      (get (glxy/tuple-expressions rec) idx)))))

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


(defn zip
  [xs ys]
  (mapv (fn [x y] [x y]) xs ys))

(defn make-db-installer
  [nom fields types]
  (fn [db]
    (jdbc/db-do-prepared (db/db-connection-conn db)
                         (jdbc/create-table-ddl
                          nom
                          (into [] (for [[f t] (zip fields types)]
                                     [(name f) (t/-to-string (symbol->value t))]))))))

(defmacro define-galaxy
  [?nom ?t ?fields ?types ?table]
  `(do
     (def ~(symbol (str "install-" ?nom "-table!"))
       ~(make-db-installer (name ?nom)
                           ?fields ?types))
     (def ~(symbol (str ?nom "-galaxy"))
       ~(glxy/make&install-db-galaxy (str ?nom) ?t
                                     (symbol (str "install-" ?nom "-table!"))
                                     ?table))))

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
    (println (type (first ?types#)))
    `(do
       (define-record-type ~?name
         ~(apply list (symbol (str "really-make-" ?name))
                 (map (fn [[k _]] (symbol (name k))) ?map-with-id#))
         ~(symbol (str ?name "?"))
         ~(into [] (apply concat (for [[k _] ?map-with-id#]
                                   [(symbol (name k))
                                    (symbol (str ?name "-" (name k)))]))))
       (define-table+scheme ~?name ~?map-with-id#)
       (define-constructor ~?name ~?fields# ~?types#)
       (define-db-type ~?name (symbol (str ~?name "?")) ~?types#)
       (define-galaxy ~?name (symbol (str ~?name "-t")) ~?fields# ~?types#
         (symbol (str ~?name "-table")))
       ~@(for [i (range 0 (count ?sel-names#))]
           `(def ~(get ?sql-sel-names# i)
              ~(make-selector (str (get ?sel-names# i))
                              (symbol (str "$" ?name "-t"))
                              (get ?types# i)
                              (get ?sel-names# i) i))))))
