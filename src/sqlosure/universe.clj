(ns ^{:doc "A universe defines a the context needed to access a particular
application domain."
      :author "Marco Schneider"}
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
  (really-make-universe (hash-map) (hash-map) (hash-map)))

(defn make-derived-universe
  "This doesn't look right at all. The original also basically only made a copy
  of the old universe so I'm not quite sure if this is necessary here.
  NOTE: If I'm right, shouldn't this not be called copy-universe or something
  along those lines?"
  [old]
  (really-make-universe (universe-base-relation-table old)
                        (universe-type-table old)
                        (universe-rator-table old)))

(defn register-type
  "Takes a universe, a name for a type and a type and returns a new universe
  which contains the new name->type mapping.
  In Mikes implementation, this was a mutation. I'd like to try to make it work
  immutable first."
  [universe name type]
  (really-make-universe
   (universe-base-relation-table universe)
   (assoc (universe-type-table universe) name type)
   (universe-rator-table universe)))

(defn universe-lookup-type
  "Look up the type with name `name` in the `universe`. Returns nil if not
  present."
  [universe name]
  (get (universe-type-table universe) name))

(defn register-base-relation
  "Takes a universe, a name for a base relation and returns a new universe
  which contains the new name->base-relation mapping.
  In Mikes implementation, this was a mutation. I'd like to try to make it work
  immutable first."
  [universe name base-relation]
  (really-make-universe
   (assoc (universe-base-relation-table universe) name base-relation)
   (universe-type-table universe)
   (universe-rator-table universe)))

(defn universe-lookup-base-relation
  "Look up the base-relation with name `name` in the `universe`. Returns nil if
  not present."
  [universe name]
  (get (universe-base-relation-table universe) name))

(defn register-rator
  "Takes a universe, a name for a rator and returns a new universe
  which contains the new name->rator-relation mapping.
  In Mikes implementation, this was a mutation. I'd like to try to make it work
  immutable first."
  [universe name rator]
  (really-make-universe
   (universe-base-relation-table universe)
   (universe-type-table universe)
   (assoc (universe-rator-table universe) name rator)))

(defn universe-lookup-rator
  "Look up the rator with name `name` in the `universe`. Returns nil if
  not present."
  [universe name]
  (get (universe-rator-table universe) name))
