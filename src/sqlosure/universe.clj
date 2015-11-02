(ns ^{:doc "A universe defines a the context needed to access a particular
application domain."
      :author "Marco Schneider, based on Mike Sperbers schemeql2"}
    sqlosure.universe
  (:require [active.clojure.record :refer [define-record-type]]))

(define-record-type universe
  (really-make-universe base-relation-table
                        type-table
                        rator-table)
  universe?
  [base-relation-table universe-base-relation-table
   type-table universe-type-table
   rator-table universe-rator-table])


(defn make-universe
  "Return a new, empty universe."
  []
  (atom (really-make-universe (hash-map) (hash-map) (hash-map))))

(defn make-derived-universe
  "This doesn't look right at all. The original also basically only made a copy
  of the old universe so I'm not quite sure if this is necessary here.
  NOTE: If I'm right, shouldn't this not be called copy-universe or something
  along those lines?"
  [old]
  (let [old-universe @old]
    (atom (really-make-universe (universe-base-relation-table old-universe)
                                (universe-type-table old-universe)
                                (universe-rator-table old-universe)))))

(defn register-type!
  "Takes a universe, a name for a type and a type and returns a new universe
  which contains the new name->type mapping."
  [universe name type]
  (let [new-universe @(make-derived-universe universe)]
    (atom (assoc-in new-universe [:type-table name] type))))

(defn universe-lookup-type
  "Look up the type with name `name` in the `universe`. Returns nil if not
  present."
  [universe name]
  (get (universe-type-table @universe) name))

(defn register-base-relation!
  "Takes a universe, a name for a base relation and returns a new universe
  which contains the new name->base-relation mapping."
  [universe name base-relation]
  (let [new-universe @(make-derived-universe universe)]
    (atom (assoc-in new-universe [:base-relation-table name] base-relation))))

(defn universe-lookup-base-relation
  "Look up the base-relation with name `name` in the `universe`. Returns nil if
  not present."
  [universe name]
  (get (universe-base-relation-table @universe) name))

(defn register-rator!
  "Takes a universe, a name for a rator and returns a new universe
  which contains the new name->rator-relation mapping.
  In Mikes implementation, this was a mutation. I'd like to try to make it work
  immutable first."
  [universe name rator]
  (let [new-universe @(make-derived-universe universe)]
    (atom (assoc-in new-universe [:rator-table name] rator))))

(defn universe-lookup-rator
  "Look up the rator with name `name` in the `universe`. Returns nil if
  not present."
  [universe name]
  (get (universe-rator-table @universe) name))
