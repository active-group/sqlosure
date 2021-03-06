(ns sqlosure.type
  (:require [active.clojure.condition :refer [assertion-violation]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.lens :as lens]
            [sqlosure.universe :refer [register-type! universe-lookup-type]]
            [sqlosure.utils :refer [zip]])
  (:import java.io.Writer
           [java.time LocalDate LocalDateTime]))

(defprotocol TypeProtocol
  "Protocol for base types."
  (-name [this] "Get name of the type.")
  (-contains? [this val] "Does non-null value belong to this base type?")
  (-nullable? [this] "Is this type nullable?")
  (-nullable [this] "Get us nullable version of this type.")
  (-non-nullable [this] "Get us non-nullable version of this type.")
  (-numeric? [this] "Is this type numeric?")
  (-ordered? [this] "Is this type ordered?")
  (-data [this] "Domain-specific data, for outside use."))

(defn nullable-type?
  "Is type nullable?"
  [ty]
  (and (satisfies? TypeProtocol ty)
       (-nullable? ty)))

(declare make-atomic-type)

(define-record-type AtomicType
  (make-atomic-type name nullable? numeric? ordered? predicate data)
  atomic-type?
  [name atomic-type-name
   nullable? atomic-type-nullable?
   numeric? atomic-type-numeric?
   ordered? atomic-type-ordered?
   predicate atomic-type-predicate
   data atomic-type-data]

  TypeProtocol
  (-name [_] name)
  (-contains? [_ val] (predicate val))
  (-nullable? [_] nullable?)
  (-nullable [_] (make-atomic-type name true numeric? ordered? predicate data))
  (-non-nullable [_] (make-atomic-type name false numeric? ordered? predicate data))
  (-numeric? [_] numeric?)
  (-ordered? [_] ordered?)
  (-data [_] data))

(defmethod print-method AtomicType [r, ^Writer w]
  (.write w "#")
  (.write w (.getName (class r)))
  (print-method {:name (atomic-type-name r)
                 :nullable? (atomic-type-nullable? r)}
                w))

(defmethod print-dup AtomicType [r, ^Writer w]
  (.write w "#")
  (.write w (.getName (class r)))
  (let [mp {:name (atomic-type-name r)
            :nullable? (atomic-type-nullable? r)}]
    (if *verbose-defrecords*
      (print-dup mp w)
      (print-dup (vec (vals mp)) w))))

(declare really-make-bounded-string-type)

(define-record-type BoundedStringType
  (really-make-bounded-string-type max-size nullable?) bounded-string-type?
  [max-size bounded-string-type-max-size
   nullable? bounded-string-type-nullable?]

  TypeProtocol
  (-name [_] (list 'bounded-string max-size))
  (-contains? [_ val] (and (string? val) (<= (count val) max-size)))
  (-nullable? [_] nullable?)
  (-nullable [_] (really-make-bounded-string-type max-size true))
  (-non-nullable [_] (really-make-bounded-string-type max-size false))
  (-numeric? [_] false)
  (-ordered? [_] true))

(declare make-nullable-type)

(define-record-type ProductType
  (really-make-product-type components) product-type?
  [components product-type-components]

  TypeProtocol
  (-name [this] "tuple")
  (-contains?
   [this vs]
   (and (seqable? vs)
        (= (count vs) (count components))
        (loop [vs  vs
               cs  components
               res true]
          (if (empty? vs)
            res
            (recur (rest vs) (rest cs)
                   (and res (-contains? (first cs) (first vs))))))))
  (-nullable? [_] false)
  (-nullable
   [this]
   (map make-nullable-type components))
  (-non-nullable [this] this)
  (-numeric? [this] false)
  (-ordered? [this] false)
  (-data
   [this]
   (throw (java.lang.UnsupportedOperationException. "not implemented yet"))))

(defn make-product-type
  [components]
  #_(when-not (every? #(satisfies? TypeProtocol components) components)
    (assertion-violation `make-product-type (str "each components must satisfy " `TypeProtocol) components))
  (really-make-product-type components))

(define-record-type SetType
  (really-make-set-type member-type) set-type?
  [member-type set-type-member-type])

(defn make-set-type
  [member-type]
  (when-not (satisfies? TypeProtocol member-type)
    (assertion-violation `make-set-type (str "member-type must satisfy " `TypeProtocol) member-type))
  (really-make-set-type member-type))

(defn make-base-type
  "Returns a new base type as specified.
  If :universe is supplied, the new type will be registered in the universe and
  this function returns a vector containing `[type universe]`."
  [name predicate & [opts]]
  (let [opts (merge {:universe nil
                     :numeric? false
                     :ordered? false
                     :data     nil}
                    opts)
        t    (make-atomic-type name
                               false
                               (:numeric? opts)
                               (:ordered? opts)
                               predicate
                               (:data opts))]
    (when-let [universe (:universe opts)]
      (register-type! universe name t))
    t))

(defn make-bounded-string-type
  "Create string type with given maximum number of chars."
  [max-size]
  (really-make-bounded-string-type max-size false))

(defn non-nullable-type
  "Yield non-nullable version of type."
  [base]
  (if (satisfies? TypeProtocol base)
    (-non-nullable base)
    base))

(defn type?
  "Is a `thing` a type?"
  [thing]
  (or (atomic-type? thing) (product-type? thing) (set-type? thing)))

(defn base-type-name
  "Yield name of base type."
  [t]
  (-name t))

(defn make-nullable-type
  "Make type nullable."
  [t]
  (cond
    (atomic-type? t)  (-nullable t)
    (product-type? t) (map make-nullable-type (product-type-components t))
    (set-type? t)     t))

(defn null?
  [v]
  (if (or (vector? v) (map? v) (list? v) (set? v))
    (empty? v)
    (nil? v)))

(defn type-member?
  "Checks if `thing` is a member of a type."
  [thing ty]
  (if (satisfies? TypeProtocol ty)
    (if (nil? thing)
      (-nullable? ty)
      (-contains? ty thing))
    (cond
      (product-type? ty) (let [cs (product-type-components ty)]
                           (and (vector? thing)
                                (= (count thing) (count cs))
                                (reduce
                                 (fn [acc [k v]] (and acc (type-member? k v)))
                                 (zip thing cs))))
      (set-type? ty) (let [mem (set-type-member-type ty)]
                       (and (or (vector? thing) (seq? thing))
                            (every? #(type-member? % mem) thing)))
      :else (assertion-violation `type-member? "unhandled type" thing))))

(defn numeric-type?
  "Is type numeric, in the sense of the server's capability to call standard
  operations like MAX and AVG on them."
  [ty]
  (and (satisfies? TypeProtocol ty)
       (-numeric? ty)))

(defn ordered-type?
  "Is type ordered, in the sense of the servers' capability to make an
  'order by' on them."
  [ty]
  (and (satisfies? TypeProtocol ty)
       (-ordered? ty)))

;; Checks if two types are the same.
;; Verbose definition unnecessary because of Clojures sensible equality (?).

;; SEE: http://stackoverflow.com/a/14797271
(defn test-array
  [t]
  (let [check (type (t []))]
    (fn [arg] (instance? check arg))))

(def byte-array?
  (test-array byte-array))

(def char-array?
  (test-array char-array))

(defn date?
  "checks whether a value is of type java.util.Date."
  [x]
  (instance? LocalDate x))

(defn timestamp?
  "Returns true if d is a java.time.LocalDateTime."
  [d]
  (instance? LocalDateTime d))

;; Some base types
(def string%
  (make-base-type 'string string? {:ordered? true}))
(def string%-nullable (make-nullable-type string%))

(def integer%
  (make-base-type 'integer integer? {:numeric? true :ordered? true}))
(def integer%-nullable (make-nullable-type integer%))

(def double% (make-base-type 'double double? {:numeric? true :ordered? true}))
(def double%-nullable (make-nullable-type double%))

(def boolean% (make-base-type 'boolean boolean?))
(def boolean%-nullable (make-nullable-type boolean%))

;; Used to represent the type of sql NULL. Corresponds to nil in Clojure.
(def null% (make-base-type 'unknown nil?))
(def any% (make-base-type 'any (constantly true)))

(def date% (make-base-type 'date date? {:ordered? true}))
(def date%-nullable (make-nullable-type date%))

(def timestamp% (make-base-type 'timestamp timestamp?
                                {:ordered? true}))
(def timestamp%-nullable (make-nullable-type timestamp%))

(def blob% (make-base-type 'blob byte-array?))
(def blob%-nullable (make-nullable-type blob%))

(def clob% (make-base-type 'clob char-array?))
(def clob%-nullable (make-nullable-type clob%))

(def bytea% (make-base-type 'bytea byte-array?))
(def bytea%-nullable (make-nullable-type bytea%))

 ;; Serialization

#_(defn type->datum
  "`type->datum` takes a type and returns it into a recursive list of it's
  subtypes. If the `type` is invalid, raises assertion-violation.
  Examples:
  * `(type->datum string%) => (string)`
  * `(type->datum (make-product-type [string% double%])) => (product (string) (double)`"
  [t]
  (cond
    (satisfies? TypeProtocol t)
    (if (-nullable? t)
      (list 'nullable
            (type->datum (-non-nullable t)))
      (-name t))
    
    (product-type? t) (list 'product (mapv type->datum
                                           (product-type-components t)))
    (set-type? t) (list 'set
                        (type->datum (set-type-member-type t)))
    :else (assertion-violation `type->datum "unknown type" t)))

#_(defn datum->type
  "`datum->type` takes a datum (as produced by `type->datum`) and a universe and
  returns the corresponding type. If the type is not a base type defined in
  `type.clj`, try looking it up in the supplied universe. If it's not found,
  throws an exception.
  Examples:
  * `(datum->type (string) => string`
  * `(datum->type (product (string) (double))) => (make-product-type [string% double%])`"
  [d universe]
  ;; TODO: Make this a multimethod to support externally defined types or something?
  (cond
    (symbol? d)
    ;; TODO: If we actually decide we need this: aren't these types registered with the universe, too?
    (or (universe-lookup-type universe d)
        (assertion-violation `datum->type "unknown type" (first d)))
    (case d
      string string%
      integer integer%
      double double%
      boolean boolean%
      date date%
      timestamp timestamp%
      blob blob%

      )

    (and (coll? d) (not-empty d))
    (case (first d)
      nullable (-nullable (datum->type (second d) universe))
      product (make-product-type (map #(datum->type % universe)
                                      (second d)))
      set (make-set-type (datum->type (second d) universe))
      bounded-string (make-bounded-string-type (second d))
      (or (universe-lookup-type universe d)
          (assertion-violation `datum->type "unknown type" (first d))))

    :else
    (or (universe-lookup-type universe d)
        (assertion-violation `datum->type "unknown type" (first d)))))

#_(defn const->datum
  "`const->datum` takes a type and a value and applies the types and returns the
  corresponding clojure value. If the type or value are invalid, throws an
  exception.
  Example:
  * `(const->datum string% \"foobar\") => \"foobar\"`
  * `(const->datum (make-product-type [string% integer%]) [\"foo\" 42])
     => [\"foo\" 42]`"
  [t val]
  (cond
    (satisfies? TypeProtocol t)
    (when-not (and (-nullable? t) (nil? val))
      (-const->datum t val))

    (product-type? t) (cond
                        (or (null? val) (sequential? val))
                        (map const->datum (product-type-components t) val)
                        :else (assertion-violation `const->datum
                                                  "invalid product-type value"
                                                  t val))
    (set-type? t) (let [mem (set-type-member-type t)]
                    (map (fn [v] (const->datum mem v)) val))
    :else (assertion-violation `const->datum "invalid type" t val)))

#_(defn datum->const
  "`datum->const` takes a type and a datum and turns the datum in the
  corresponding clojure data. If type is invalid or datum does not match the
  type, throws an exception.
  Example:
  * `(datum->const double% 42) => 42`'
  * `(datum->const (make-set-type integer%) [42 23]) => [42 23]`"
  [t d]
  (cond
    (satisfies? TypeProtocol t)
    (when-not (and (-nullable? t) (nil? d))
      (-datum->const t d))
    (product-type? t) (cond
                        (or (pair? d) (nil? d))
                        ;; Maybe add type check here?
                        (map datum->const (product-type-components t) d)
                        :else (assertion-violation `datum->const
                                                   "invalid product type datum for type"
                                                   t d))
    (set-type? t) (cond
                    (or (pair? d) (nil? d))
                    (let [mem (set-type-member-type t)]
                      (map (fn [dd] (datum->const mem dd)) d))
                    :else (assertion-violation `datum->const
                                               "invalid set type datum for type"
                                               t d))
    :else (assertion-violation `datum->const "invalid type" t)))
