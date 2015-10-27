(ns ^{:doc "Types."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.type
  (:require [sqlosure.universe :refer [register-type]]
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

(defn make-base-type
  "Returns a new base type as specified.
  If :universe is supplied, the new type will be registered in the universe and
  this function returns a vector containing `[type universe]`."
  [name predicate const->datum-proc datum->const-proc & {:keys [universe data]}]
  (let [t (really-make-base-type name predicate const->datum-proc
                                 datum->const-proc data)]
    (if universe  ;; todo: register-type! ?
      [t (register-type universe name t)]
      t)))

(defn values
  "Kind of resembles schemes `values` function but instead returns a vector of
  all its arguments. I don't know if this is really necessary or the way to go."
  [& values]
  (vec values))

(defn double?
  "Checks if a value is of type Double."
  [x]
  (instance? Double x))

(defn boolean?
  "Checks if a value if of type Boolean"
  [x]
  (instance? Boolean x))

(defn byte-vector? [x]
  (and (vector? x) (every? #(instance? Byte %) x)))

;; Some base types
(def string% (make-base-type 'string string? values values))
(def integer% (make-base-type 'integer integer? values values))
(def double% (make-base-type 'double double? values values))
(def boolean% (make-base-type 'boolean boolean? values values))
;; Clojure itself has no byte type. Maybe use Java's?
(def blob% (make-base-type 'blob byte-vector? 'lose 'lose))

(define-record-type bounded-string-type
  (make-bounded-string-type max-size) bounded-string-type?
  [max-size bounded-string-type-max-size])

(define-record-type nullable-type
  (really-make-nullable-type underlying) nullable-type?
  [underlying nullable-type-underlying])

(defn nullable
  "If base is a nullable-type, return it. Otherwise wrap it in nullable-type."
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

(def nullable-integer& (nullable integer%))

(defn type-member?
  "Checks if `thing` is a member of a type."
  [thing ty]
  (cond
    (nullable-type? thing) (or (empty? thing)
                               (type-member? thing
                                             (nullable-type-underlying ty)))
    (base-type? ty) ((base-type-predicate ty) thing)
    (bounded-string-type? ty) (and (string? thing)
                                   (<= (count thing)
                                       (bounded-string-type-max-size ty)))
    (product-type? ty) (let [cs (product-type-components ty)]
                         (and (vector? thing)
                              (= (count thing) (count cs))
                              (every? type-member? thing cs)))
    (set-type? ty) (let [mem (set-type-member-type ty)]
                     (and (seq? thing)
                          (every? #(type-member? % mem) thing)))
    :else (throw (Exception. (str 'type-member? ": unhandled type: "
                                  (if (nil? thing) "nil" thing))))))

(defn numeric-type?
  "Checks if a type is numeric."
  [t]
  (or (= t integer%)
      (= t double%)))

(defn ordered-type?
  [t]
  (or (numeric-type? t)
      (= t string%)
      ;; TODO: implement calendar-type
      #_(= t calendar-time%)))

(def string%-nullable (nullable string%))
(def integer%-nullable (nullable integer%))
(def double%-nullable (nullable double%))
(def blob%-nullable (nullable blob%))

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
                                   (every? type=? c1 c2))))
    (set-type? t1) (and (set-type? t2)
                        (type=? (set-type-member-type t1)
                                (set-type-member-type t2)))
    :else (throw (Exception. (str 'type=? ": unknown type: " t1)))))

(defn type->datum
  [t]
  (cond
    (nullable-type? t) (list 'nullable (nullable-type-underlying t))
    (product-type? t) (cons 'product (map type->datum
                                          (product-type-components t)))
    (set-type? t) (cons 'set-type (type->datum (set-type-member-type t)))
    (bounded-string-type? t) (list 'bounded-string (bounded-string-type-max-size t))
    (base-type? t) (list (base-type-name t))
    :else (throw (Exception. (str 'type->datum ": unknown type: " t)))))

(defn datum->type
  [d universe]
  (case (first d)
    nullable (really-make-nullable-type (datum->type (second d) universe))
    product (make-product-type (map #(datum->type % universe) (second d)))
    set (make-set-type (datum->type (second d) universe))
    string string%
    integer integer%
    double double%
    boolean boolean%
                                        ; calendar-time calendar-time%
    blob blob%
    bounded-string (make-bounded-string-type (second d))
    ;; TODO
    :else nil #_(or (universe-lookup-type universe ()))))

(defn pair?
  "Returns true if v is a sequence not empty (like schemes pair? function)."
  [v]
  (if (or (list? v) (vector? v))
    (not (empty? v))
    false))

(defn const->datum
  [t val]
  (cond
    (base-type? t) ((base-type-const->datum-proc t) val)
    (nullable-type? t) (when-not (empty? val)
                         (const->datum (nullable-type-underlying t) val))
    (bounded-string-type? t) val
    (product-type? t) (cond
                        (or (empty? val) (pair? val))
                        (map const->datum (product-type-components t) val)
                        (vector? val) (map const->datum
                                           (product-type-components t)
                                           val)
                        :else (throw
                               (Exception. (str 'const->datum
                                                ": invalid product-type value "
                                                t val))))
    (set-type? t) (let [mem (set-type-member-type t)]
                    (map (fn [v] (const->datum mem v)) val))
    :else (throw (Exception. (str 'const->datum ": invalid type " t val)))))

(defn datum->const
  [t d]
  (cond
    (base-type? t) ((base-type-datum->const-proc t) d)
    (nullable-type? t) (when (empty? d)
                         (datum->const (nullable-type-underlying t) d))
    (bounded-string-type? t) d
    (product-type? t) (cond
                        (or (empty? d) (pair? d))
                        (map datum->const (product-type-components t) d)
                        (vector? d)
                        (map datum->const (product-type-components t))
                        :else (throw
                               (Exception. (str 'datum->const
                                                " invalid product type datum"
                                                t d))))
    (set-type? t) (let [mem (set-type-member-type t)]
                    (map (fn [dd] (datum->const mem dd)) d))
    :else (throw (Exception. (str 'datum->const " invalid type " t)))))
