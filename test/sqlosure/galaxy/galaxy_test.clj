(ns sqlosure.galaxy.galaxy-test
  (:require [sqlosure.core :refer :all]
            [sqlosure.db-connection :as db]
            [sqlosure.galaxy.galaxy :refer :all]
            [active.clojure.record :refer [define-record-type]]
            [sqlosure.time :as time]
            [clojure.java.jdbc :as jdbc]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.type :as t]
            [sqlosure.sql :as sql]
            [sqlosure.relational-algebra-sql :as rsql]
            [sqlosure.sql-put :as put]))

;; Just a test to get a feel for db-types.
(define-record-type person
  (make-person id first last birthday sex) person?
  [id person-id
   first person-first
   last person-last
   birthday person-birthday
   ^{:doc "false == male"}
   sex person-sex])

(def person-scheme
  (rel/alist->rel-scheme {"id" $integer-t
                          "first" $string-t
                          "last" $string-t
                          "birthday" $date-t
                          "sex" $boolean-t}))

(def person-table
  (table "person" (rel/rel-scheme-map person-scheme)))

(defn install-person-db-tables!
  [db]
  (jdbc/db-do-prepared
   db
   (jdbc/create-table-ddl
    [[:id "INT"]
     [:first "TEXT"]
     [:last "TEXT"]
     [:birthday "DATE"]
     [:sex "BOOLEAN"]])))

(defn id->person
  [db id]
  (db/run-query
   db
   (query [p (<- person-table)]
          (restrict ($= (! p "id")
                        ($integer id)))
          (project p))))

(defn db->person
  [db id]
  (when-let [p (id->person db id)]
    (apply make-person (first p))))

(defn person->db-expression
  [p]
  (make-tuple [($integer (person-id p))
               ($string (person-first p))
               ($string (person-last p))
               ($date (person-birthday p))
               ($boolean (person-sex p))]))

(def $person-t (make-db-type "person" person?
                             person-id id->person
                             person-scheme
                             db->person
                             person->db-expression))

(def person-galaxy
  (make&install-db-galaxy
   "person" $person-t
   (fn [db] (install-person-db-tables! db))
   person-table))

;; (dbize-query (table "foo" (rel/alist->rel-scheme {"id" $integer-t})))
(def p1 (make-person 0 "Marco" "Schneider" (time/make-date 1989 10 31) false))

(def $person-id
  (rel/make-monomorphic-combinator "person-id"
                                   (list $person-t)
                                   $integer-t
                                   person-id
                                   :universe sql/sql-universe))

(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(db-query-reified-result (db-connect db-spec) (query [p (<- person-galaxy)]
                                                     (restrict ($= ($person-id (! p))
                                                                   ($integer 1)))
                                                     (project p)))

(dbize-query (query [p (<- person-galaxy)]
                    (restrict ($= ($person-id (! p))
                                  ($integer 1)))
                    (project p)))
