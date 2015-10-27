(ns ^{:doc "Implementation of relational algebra based on Mike Sperbers
relational-algebra.scm.
Replaced alist with hash-map."
      :author "Marco Schneider"}
    sqlosure.relational-algebra
  (:require [sqlosure.universe :refer [register-base-relation
                                       universe-base-relation-table
                                       register-rator]]
            [clojure.set :refer [difference]]
            [active.clojure.record :refer [define-record-type]]))

(define-record-type rel-scheme
  (make-rel-scheme alist) rel-scheme?
  [alist rel-scheme-alist])

(def the-empty-rel-scheme (make-rel-scheme nil))
(def the-empty-environment nil)

(defn rel-scheme=?
  "Returns true if t1 and t2 are the same."
  [t1 t2]
  ;; TODO: Couldn't this just be (= t1 t2)?
  (= (rel-scheme-alist t1) (rel-scheme-alist t2)))

(defn rel-scheme-difference
  "Return a new rel-scheme resulting of the (set-)difference of s1's and s2's
  alist."
  [s1 s2]
  (make-rel-scheme (into {} (difference (set (rel-scheme-alist s1))
                                        (set (rel-scheme-alist s2))))))

(defn rel-scheme-unary?
  "Returns true if the rel-scheme's alist consist of only one pair."
  [scheme]
  (= 1 (count (rel-scheme-alist scheme))))

(defn rel-scheme->environment
  "??"
  [s]
  (rel-scheme-alist s))

(defn compose-environments
  "Combine two environments. e1 takes precedence over e2."
  [e1 e2]
  (cond
    (empty? e1) e2
    (empty? e2) e1
    :else (merge e2 e1)))

(defn lookup-env
  "Lookup a name in an environment.
  TODO: Should this return `false` as in the original?"
  [name env]
  (get env name))

;;; ----------------------------------------------------------------------------
;;; --- Primitive relations, depending on the domain universe
;;; ----------------------------------------------------------------------------

(define-record-type base-relation
  (really-make-base-relation name scheme handle) base-relation?
  [name base-relation-name
   scheme base-relation-scheme
   handle base-relation-handle])

(defn make-base-relation
  "Returns a new base relation.
  If :handle is supplied, use ist as base-relation-handle, defaults to nil.
  If :universe is supplied, return a vector of [relation universe]"
  [name scheme & {:keys [universe handle]}]
  (let [rel (really-make-base-relation name scheme handle)]
    (if universe
      [rel (register-base-relation universe name rel)]
      rel)))

;;; ----------------------------------------------------------------------------
;;; --- EXPRESSIONS
;;; ----------------------------------------------------------------------------
(define-record-type attribute-ref
  (make-attribute-ref name) attribute-ref?
  [name attribute-ref-name])

(define-record-type const
  (make-const type val) const?
  [type const-type
   val const-val])

(define-record-type null
  (make-null type) null?
  [type null-type])

(define-record-type application
  (really-make-application rator rands) application?
  [rator application-rator
   rands application-rands])

(defn make-application
  [rator & rands]
  (really-make-application rator rands))

(define-record-type rator
  (really-make-rator name range-type-proc proc data) rator?
  [name rator-name
   range-type-proc rator-range-type-proc
   proc rator-proc
   data rator-data])

(defn make-rator
  [name range-type-proc proc & {:keys [universe data]}]
  (let [r (really-make-rator name range-type-proc proc data)]
    (if universe
      [r (register-rator universe name r)]
      r)))

;;; ----------------------------------------------------------------------------
;;; --- QUERYS
;;; ----------------------------------------------------------------------------

(define-record-type project
  (really-make-project alist query) project?
  [alist project-alist
   query project-query])

(defn make-project
  [alist query]
  (if (empty? alist)
    (if (project? query)
      (make-project alist (project-query query))
      (really-make-project alist query))
    (really-make-project alist query)))

(defn make-extend
  [alist query]
  (make-project (concat alist (map (fn [p]
                                     (cons (first p)
                                           (make-attribute-ref (first p))))
                                   (rel-scheme-alist (query-scheme query))))))

(defn query?
  [obj]
  (or (empty? obj) (base-relation? obj)))
