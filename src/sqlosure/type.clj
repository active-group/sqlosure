(ns ^{:doc "Types."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.type
  (:require [sqlosure.universe :refer [register-type! universe-lookup-type]]
            [sqlosure.utils :refer [zip]]
            [active.clojure.record :refer [define-record-type]]
            [active.clojure.condition :refer [assertion-violation]])
  (:import [java.time LocalDate LocalDateTime]))

(defprotocol base-type-protocol
  "Protocol for base types."
  (-name [this] "Get name of the type.")
  (-contains? [this val] "Does non-null value belong to this base type?")
  (-nullable? [this] "Is this type nullable?")
  (-nullable [this] "Get us nullable version of this type.")
  (-non-nullable [this] "Get us non-nullable version of this type.")
  (-numeric? [this] "Is this type numeric?")
  (-ordered? [this] "Is this type ordered?")
  (-const->datum [this val] "Convert value to datum.")
  (-datum->const [this datum] "Convert datum to value."))

(defn nullable-type?
  "Is type nullable?"
  [ty]
  (and (satisfies? base-type-protocol ty)
       (-nullable? ty)))

(defn base-type-name
  "Yield name of base type."
  [t]
  (-name t))

(declare make-atomic-type)

(define-record-type atomic-type
  (make-atomic-type name nullable? numeric? ordered? predicate const->datum-fn datum->const-fn)
  atomic-type?
  [name atomic-type-name
   nullable? atomic-type-nullable?
   numeric? atomic-type-numeric?
   ordered? atomic-type-ordered?
   predicate atomic-type-predicate
   const->datum-fn atomic-type-const->datum-fn
   datum->const-fn atomic-type-datum->const-fn]
  base-type-protocol
  (-name [_] name)
  (-contains? [_ val] (predicate val))
  (-nullable? [_] nullable?)
  (-nullable [_] (make-atomic-type name true numeric? ordered? predicate const->datum-fn datum->const-fn))
  (-non-nullable [_] (make-atomic-type name false numeric? ordered? predicate const->datum-fn datum->const-fn))
  (-numeric? [_] numeric?)
  (-ordered? [_] ordered?)
  (-const->datum [_ val] (const->datum-fn val))
  (-datum->const [_ datum] (datum->const-fn datum)))

;; FIXME: custom printer

(defn make-base-type
  "Returns a new base type as specified.
  If :universe is supplied, the new type will be registered in the universe and
  this function returns a vector containing `[type universe]`."
  [name predicate const->datum-proc datum->const-proc & {:keys [universe numeric? ordered?]}]
  (let [t (make-atomic-type name false
                            (boolean numeric?) (boolean ordered?)
                            predicate
                            const->datum-proc datum->const-proc)]
    (when universe
      (register-type! universe name t))
    t))

(declare really-make-bounded-string-type)

(define-record-type bounded-string-type
  (really-make-bounded-string-type max-size nullable?) bounded-string-type?
  [max-size bounded-string-type-max-size
   nullable? bounded-string-type-nullable?]
  base-type-protocol
  (-name [_] (list 'bounded-string max-size))
  (-contains? [_ val] (and (string? val) (<= (count val) max-size)))
  (-nullable? [_] nullable?)
  (-nullable [_] (really-make-bounded-string-type max-size true))
  (-non-nullable [_] (really-make-bounded-string-type max-size false))
  (-numeric? [_] false)
  (-ordered? [_] true)
  (-const->datum [_ val] val)
  (-datum->const [_ datum] datum))

(defn make-bounded-string-type
  "Create string type with given maximum number of chars."
  [max-size]
  (really-make-bounded-string-type max-size false))

(defn make-nullable-type
  "Make type nullable."
  [base]
  (-nullable base))

(defn non-nullable-type
  "Yield non-nullable version of type."
  [base]
  (if (satisfies? base-type-protocol base)
    (-non-nullable base)
    base))

(define-record-type product-type
  (make-product-type components) product-type?
  [components product-type-components])

(define-record-type set-type
  (make-set-type member-type) set-type?
  [member-type set-type-member-type])

(defn null?
  [v]
  (if (or (vector? v) (map? v) (list? v) (set? v))
    (empty? v)
    (nil? v)))

(defn all?
  [bs]
  (when bs
    (reduce #(and %1 %2) true bs)))

(defn any?
  [bs]
  (when bs
    (reduce #(or %1 %2) false bs)))

(defn type-member?
  "Checks if `thing` is a member of a type."
  [thing ty]
  (if (satisfies? base-type-protocol ty)
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
  "Is type numeric, in the sense of the server's capability to call standard operations like MAX and AVG on them."
  [ty]
  (and (satisfies? base-type-protocol ty)
       (-numeric? ty)))

(defn ordered-type?
  "Is type ordered, in the sense of the servers' capability to make an 'order by' on them."
  [ty]
  (and (satisfies? base-type-protocol ty)
       (-ordered? ty)))

;; Checks if two types are the same.
;; Verbose definition unnecessary because of Clojures sensible equality (?).
(def type=? =)

;; Standard types

(defn double?
  "checks if a value is of type double."
  [x]
  (instance? Double x))

(defn boolean?
  "Checks if a value if of type boolean. This includes booleans and nil."
  [x]
  (or (nil? x) (instance? Boolean x)))

;; SEE: http://stackoverflow.com/a/14797271
(defn test-array
  [t]
  (let [check (type (t []))]
    (fn [arg] (instance? check arg))))

(def byte-array?
  (test-array byte-array))

(defn date?
  "checks whether a value is of type java.util.Date."
  [x]
  (instance? LocalDate x))

(defn timestamp?
  "Returns true if d is a java.time.LocalDateTime."
  [d]
  (instance? LocalDateTime d))

;; Some base types
(def string% (make-base-type 'string string? identity identity
                             :ordered? true))
(def integer% (make-base-type 'integer integer? identity identity
                              :numeric? true :ordered? true))
(def double% (make-base-type 'double double? identity identity
                              :numeric? true :ordered? true))
(def boolean% (make-base-type 'boolean boolean? identity identity))

;; Used to represent the type of sql NULL. Corresponds to nil in Clojure.
(def null% (make-base-type 'unknown nil? identity identity))
(def any% (make-base-type 'any (constantly true) identity identity))

(def date% (make-base-type 'date date? identity identity
                           :ordered true))
(def timestamp% (make-base-type 'timestamp timestamp? identity identity
                                :ordered true))

(def blob% (make-base-type 'blob byte-array? 'lose 'lose))

(def nullable-integer% (make-nullable-type integer%))

(def string%-nullable (make-nullable-type string%))
(def integer%-nullable (make-nullable-type integer%))
(def double%-nullable (make-nullable-type double%))
(def blob%-nullable (make-nullable-type blob%))

 ;; Serialization

(defn type->datum
  "`type->datum` takes a type and returns it into a recursive list of it's
  subtypes. If the `type` is invalid, raises assertion-violation.
  Examples:
  * `(type->datum string%) => (string)`
  * `(type->datum (make-product-type [string% double%])) => (product (string) (double)`"
  [t]
  (cond
    (satisfies? base-type-protocol t)
    (if (-nullable? t)
      (list 'nullable
            (type->datum (-non-nullable t)))
      (-name t))
    
    (product-type? t) (list 'product (mapv type->datum
                                           (product-type-components t)))
    (set-type? t) (list 'set
                        (type->datum (set-type-member-type t)))
    :else (assertion-violation `type->datum "unknown type" t)))

(defn datum->type
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
    ;; FIXME: aren't these types registered with the universe, too?
    (case d
      string string%
      integer integer%
      double double%
      boolean boolean%
      date date%
      timestamp timestamp%
      blob blob%

      (or (universe-lookup-type universe d)
          (assertion-violation `datum->type "unknown type" (first d))))

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


(defn pair?
  "Returns true if v is a sequence not empty (like schemes pair? function)."
  [v]
  (if (or (list? v) (vector? v))
    (not (empty? v))
    false))

(defn const->datum
  "`const->datum` takes a type and a value and applies the types and returns the
  corresponding clojure value. If the type or value are invalid, throws an
  exception.
  Example:
  * `(const->datum string% \"foobar\") => \"foobar\"`
  * `(const->datum (make-product-type [string% integer%]) [\"foo\" 42])
     => [\"foo\" 42]`"
  [t val]
  (cond
    (satisfies? base-type-protocol t)
    (if (and (-nullable? t) (nil? val))
      nil
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

(defn datum->const
  "`datum->const` takes a type and a datum and turns the datum in the
  corresponding clojure data. If type is invalid or datum does not match the
  type, throws an exception.
  Example:
  * `(datum->const double% 42) => 42`'
  * `(datum->const (make-set-type integer%) [42 23]) => [42 23]`"
  [t d]
  (cond
    (satisfies? base-type-protocol t)
    (if (and (-nullable? t) (nil? d))
      nil
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
