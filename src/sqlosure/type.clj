(ns ^{:doc "Types."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.type
  (:require [sqlosure.universe :refer [register-type! universe-lookup-type]]
            [active.clojure.record :refer [define-record-type]]))

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

(defn ^{:test true} make-base-type
  "Returns a new base type as specified.
  If :universe is supplied, the new type will be registered in the universe and
  this function returns a vector containing `[type universe]`."
  [name predicate const->datum-proc datum->const-proc & {:keys [universe data]}]
  (let [t (really-make-base-type name predicate const->datum-proc
                                 datum->const-proc data)]
    (when universe
      (register-type! universe name t))
    t))

(defn ^{:test true} values
  "kind of resembles schemes `values` function but instead returns a vector of
  all its arguments. i don't know if this is really necessary or the way to go."
  [& values]
  (vec values))

(defn ^{:test true} double?
  "checks if a value is of type double."
  [x]
  (instance? Double x))

(defn ^{:test true} boolean?
  "checks if a value if of type boolean"
  [x]
  (instance? Boolean x))

(defn ^{:test false} byte-vector? [x]
  (and (vector? x) (every? #(instance? byte %) x)))

;; Some base types
(def string% (make-base-type 'string string? identity identity))
(def integer% (make-base-type 'integer integer? identity identity))
(def double% (make-base-type 'double double? identity identity))
(def boolean% (make-base-type 'boolean boolean? identity identity))
;; Clojure itself has no byte type. Maybe use Java's?
(def blob% (make-base-type 'blob byte-vector? 'lose 'lose))

(define-record-type bounded-string-type
  (make-bounded-string-type max-size) bounded-string-type?
  [max-size bounded-string-type-max-size])

(define-record-type nullable-type
  (really-make-nullable-type underlying) nullable-type?
  [underlying nullable-type-underlying])

(defn ^{:test true} make-nullable-type
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

(def nullable-integer% (make-nullable-type integer%))

(defn null?
  [v]
  (if (seq? v)
    (empty? v)
    (nil? v)))

(defn zip
  [xs ys]
  (mapv (fn [k v] [k v]) xs ys))

(defn all?
  [bs]
  (reduce #(and %1 %2) true bs))

(defn any?
  [bs]
  (reduce #(or %1 %2) false bs))

(defn ^{:test true} type-member?
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

(defn ^{:test true} numeric-type?
  "checks if a type is numeric."
  [t]
  (or (= t integer%)
      (= t double%)))

(defn ^{:test true} ordered-type?
  [t]
  (or (numeric-type? t)
      (= t string%)
      ;; todo: implement calendar-type
      #_(= t calendar-time%)))

(def string%-nullable (make-nullable-type string%))
(def integer%-nullable (make-nullable-type integer%))
(def double%-nullable (make-nullable-type double%))
(def blob%-nullable (make-nullable-type blob%))

(defn ^{:test true} type=?
  "checks if two types are the same."
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

(defn ^{:test true} type->datum
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

(defn ^{:test true} datum->type
  [d universe]
  (case (first d)
    nullable (really-make-nullable-type (datum->type (second d) universe))
    product (make-product-type (map #(datum->type % universe)
                                    (second d)))
    set (make-set-type (datum->type (second d) universe))
    string string%
    integer integer%
    double double%
    boolean boolean%
    ;; calendar-time calendar-time%
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

(defn ^{:test true} const->datum
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

(defn ^{:test true} datum->const
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
