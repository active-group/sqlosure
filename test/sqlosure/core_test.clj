(ns sqlosure.core-test
  (:require [active.clojure.monad :as monad]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]

            [sqlosure.core :as sql]
            [sqlosure.lang :as db]
            [sqlosure.backends.h2 :as h2]
            [sqlosure.runner.database :as db-runner]
            [sqlosure.runner.embedded :as embedded-runner]))

;; setup/utils
(def table (sql/table "t1" [["a" sql/$integer-t]
                            ["b" sql/$string-t]]))

(def db-spec {:classname   "org.h2.Driver"
              :subprotocol "h2:mem"
              :subname     "tests;MODE=PostgreSQL"
              :mode        "PostgreSQL"})

(defn with-db
  [f]
  (jdbc/with-db-connection [db {:classname   "org.h2.Driver"
                                :subprotocol "h2:mem"
                                :subname     "tests;MODE=PostgreSQL"
                                :mode        "PostgreSQL"}]
    (jdbc/db-do-prepared db
                         (jdbc/create-table-ddl
                          "t1"
                          [["a" :int]
                           ["b" "VARCHAR(100)"]]))
    (f db)))

(def prog (monad/monadic [read-table-1 (db/read! table)]
                         (db/create! table [1 "foo"])
                         (db/create! table [2 "bar"])
                         [read-table-2 (db/read! table)]
                         (db/update! table
                                     (fn [a b]
                                       (sql/$= a (sql/$integer 1)))
                                     (fn [a b]
                                       {"b" (sql/$string "updated")}))
                         [read-table-3 (db/read! table)]
                         (db/delete! table (fn [a b] (sql/$= a (sql/$integer 2))))
                         [read-table-4 (db/read! table)]
                         (monad/return [read-table-1
                                        read-table-2
                                        read-table-3
                                        read-table-4])))

(deftest run-db-test
  (with-db
    (fn [db]
      (let [command-config-embedded (embedded-runner/command-config {})
            command-config-h2 (db-runner/command-config (sql/db-connect db h2/implementation))
            res [#{} #{[2 "bar"] [1 "foo"]}  ]]
        (testing "embedded"
          (let [[one two three four] (sql/run-db command-config-embedded prog)]
            (is (empty? one))
            (is (= #{[1 "foo"] [2 "bar"]} two))
            (is (= #{[1 "updated"] [2 "bar"]} three))
            (is (= #{[1 "updated"]} four))))
        (testing "h2"
          (let [[one two three four] (sql/run-db command-config-h2 prog)]
            (is (empty? one))
            (is (= [[1 "foo"] [2 "bar"]] two))
            (is (= [[1 "updated"] [2 "bar"]] three))
            (is (= [[1 "updated"]] four))))))))
