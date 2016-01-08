(ns ^{:doc "Types."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.type
  (:require [sqlosure.universe :refer [register-type! universe-lookup-type]]
            [sqlosure.utils :refer [zip]]
            [active.clojure.record :refer [define-record-type]])
  (:import [java.time LocalDate LocalDateTime]))

;;; ----------------------------------------------------------------------------
;;; --- TYPES
;;; ----------------------------------------------------------------------------
(define-record-type base-type
  (really-make-base-type name predicate const->datum-proc datum->const-proc data)
  base-type?
  [name base-type-name
   predicate base-type-predicate
   const->datum-proc base-type-const->datum-proc
   datum->const-proc base-type-datum->const-proc
   data base-type-data])

(defn make-base-type
  "Returns a new base type as specified.
  If :universe is supplied, the new type will be registered in the universe and
  this function returns a vector containing `[type universe]`."
  [name predicate const->datum-proc datum->const-proc & {:keys [universe data]}]
  (let [t (really-make-base-type name predicate const->datum-proc
                                 datum->const-proc data)]
    (when universe
      (register-type! universe name t))
    t))

(defn values
  "kind of resembles schemes `values` function but instead returns a vector of
  all its arguments. i don't know if this is really necessary or the way to go."
  [& values]
  (vec values))

(define-record-type bounded-string-type
  (make-bounded-string-type max-size) bounded-string-type?
  [max-size bounded-string-type-max-size])

(define-record-type nullable-type
  (really-make-nullable-type underlying) nullable-type?
  [underlying nullable-type-underlying])

(defn make-nullable-type
  "if base is a nullable-type, return it. otherwise wrap it in nullable-type."
  [base]
  (if (nullable-type? base)
    base
    (really-make-nullable-type base)))

(define-record-type product-type
  (make-product-type components) product-type?
  [components product-type-components])

(define-record-type set-type
  (make-set-type member-type) set-type?
  [member-type set-type-member-type])

(defn null?
  [v]
  (if (seq? v)
    (empty? v)
    (nil? v)))

(defn all?
  [bs]
  (reduce #(and %1 %2) true bs))

(defn any?
  [bs]
  (reduce #(or %1 %2) false bs))

(defn type-member?
  "Checks if `thing` is a member of a type."
  [thing ty]
  (cond
    (nullable-type? ty) (or (null? thing)
                            (type-member? thing
                                          (nullable-type-underlying ty)))
    (base-type? ty) ((base-type-predicate ty) thing)
    (bounded-string-type? ty) (and (string? thing)
                                   (<= (count thing)
                                       (bounded-string-type-max-size ty)))
    (product-type? ty) (let [cs (product-type-components ty)]
                         (and (vector? thing)
                              (= (count thing) (count cs))
                              (reduce
                               (fn [acc [k v]] (and acc (type-member? k v)))
                               (zip thing cs))))
    (set-type? ty) (let [mem (set-type-member-type ty)]
                     (and (or (vector? thing) (seq? thing))
                          (every? #(type-member? % mem) thing)))
    :else (throw (Exception. (str 'type-member? ": unhandled type: "
                                  (if (nil? thing) "nil" thing))))))

(defmulti numeric-type? "Defines if a base-type is numeric, in the sense of the server's capability to call standard operations like MAX and AVG on them."
  (fn [t] (base-type-name t))
  :default ::default)

(defmethod numeric-type? ::default
  ;; per default, types are not numeric.
  [t] false)

(defmulti ordered-type? "Defines if a base-type is ordered, in the sense of the servers' capability to make an 'order by' on them."
  (fn [t] (base-type-name t))
  :default ::default)

(defmethod ordered-type? ::default
  ;; per default, all numeric types are ordered.
  [t] (numeric-type? t))

(defn type=?
  "Checks if two types are the same."
  [t1 t2]
  (cond
    (base-type? t1) (= t1 t2)
    (base-type? t2) false
    (nullable-type? t1) (and (nullable-type? 2)
                             (type=? (nullable-type-underlying t1)
                                     (nullable-type-underlying t2)))
    (bounded-string-type? t1) (and (bounded-string-type? t2)
                                   (= (bounded-string-type-max-size t1)
                                      (bounded-string-type-max-size t2)))
    (product-type? t1) (and (product-type? t2)
                            (let [c1 (product-type-components t1)
                                  c2 (product-type-components t2)]
                              (and (= (count c1) (count c2))
                                   (every? (fn [[t1 t2]] (type=? t1 t2)) (zip c1 c2)))))
    (set-type? t1) (and (set-type? t2)
                        (type=? (set-type-member-type t1)
                                (set-type-member-type t2)))
    :else (throw (Exception. (str 'type=? ": unknown type: " t1)))))

;; Standard types

(defn double?
  "checks if a value is of type double."
  [x]
  (instance? Double x))

(defn boolean?
  "checks if a value if of type boolean"
  [x]
  (instance? Boolean x))

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
(def string% (make-base-type 'string string? identity identity))
(def integer% (make-base-type 'integer integer? identity identity))
(def double% (make-base-type 'double double? identity identity))
(def boolean% (make-base-type 'boolean boolean? identity identity))

(def date% (make-base-type 'date date? identity identity))
(def timestamp% (make-base-type 'timestamp timestamp? identity identity))

(def blob% (make-base-type 'blob byte-array? 'lose 'lose))

(def nullable-integer% (make-nullable-type integer%))

(def string%-nullable (make-nullable-type string%))
(def integer%-nullable (make-nullable-type integer%))
(def double%-nullable (make-nullable-type double%))
(def blob%-nullable (make-nullable-type blob%))

(defmethod numeric-type? 'integer [_] true)
(defmethod numeric-type? 'double [_] true)

(defmethod ordered-type? 'string [_] true)

;; Serialization

(defn type->datum
  "`type->datum` takes a type and returns it into a recursive list of it's
  subtypes. If the `type` is invalid, throws an exception.
  Examples:
  * `(type->datum string%) => (string)`
  * `(type->datum (make-product-type [string% double%])) => (product (string) (double)`"
  [t]
  (cond
    (nullable-type? t) (list 'nullable
                             (type->datum (nullable-type-underlying t)))
    (product-type? t) (list 'product (mapv type->datum
                                           (product-type-components t)))
    (set-type? t) (list 'set
                        (type->datum (set-type-member-type t)))
    (bounded-string-type? t) (list 'bounded-string
                                   (bounded-string-type-max-size t))
    (base-type? t) (list (base-type-name t))
    :else (throw (Exception. (str 'type->datum ": unknown type: " t)))))

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
  (case (first d)
    nullable (really-make-nullable-type (datum->type (second d) universe))
    product (make-product-type (map #(datum->type % universe)
                                    (second d)))
    set (make-set-type (datum->type (second d) universe))
    string string%
    integer integer%
    double double%
    boolean boolean%
    date date%
    timestamp timestamp%
    blob blob%
    bounded-string (make-bounded-string-type (second d))
    (or (universe-lookup-type universe (first d))
        (throw (Exception. (str 'datum->type ": unknown type " (first d)))))))

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
    (base-type? t) ((base-type-const->datum-proc t) val)
    (nullable-type? t) (when-not (null? val)
                         (const->datum (nullable-type-underlying t) val))
    (bounded-string-type? t) val  ;; Maybe check here for correct value again?
    (product-type? t) (cond
                       (or (empty? val) (pair? val))
                       (map const->datum (product-type-components t) val)
                        :else (throw
                               (Exception. (str 'const->datum
                                                ": invalid product-type value "
                                                t val))))
    (set-type? t) (let [mem (set-type-member-type t)]
                    (map (fn [v] (const->datum mem v)) val))
    :else (throw (Exception. (str 'const->datum ": invalid type " t val)))))

(defn datum->const
  "`datum->const` takes a type and a datum and turns the datum in the
  corresponding clojure data. If type is invalid or datum does not match the
  type, throws an exception.
  Example:
  * `(datum->const double% 42) => 42`'
  * `(datum->const (make-set-type integer%) [42 23]) => [42 23]`"
  [t d]
  (cond
    (base-type? t) ((base-type-datum->const-proc t) d)
    (nullable-type? t) (when-not (null? d)
                         (datum->const (nullable-type-underlying t) d))
    (bounded-string-type? t) d
    (product-type? t) (cond
                        (or (pair? d) (nil? d))
                        ;; Maybe add type check here?
                        (map datum->const (product-type-components t) d)
                        :else (throw
                               (Exception.
                                (str 'datum->const
                                     ": invalid product type datum for type "
                                     t " with value " d))))
    (set-type? t) (cond
                    (or (pair? d) (nil? d))
                    (let [mem (set-type-member-type t)]
                      (map (fn [dd] (datum->const mem dd)) d))
                    :else (throw
                           (Exception. (str 'datum->const
                                            ": invalid set type datum for type "
                                            t " with value " d))))
    :else (throw (Exception. (str 'datum->const " invalid type " t)))))
