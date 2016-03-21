(ns sqlosure.database-test
  (:require [sqlosure.core :refer :all]
            [sqlosure.database :refer :all]
            [sqlosure.db-sqlite3 :as sqlite]
            [sqlosure.test-utils :refer [db-spec with-actor-db person-table]]
            [sqlosure.time :as time]
            [active.clojure.monad :refer [return]]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [sqlosure.relational-algebra :as rel]))

(deftest insert!-test
  (testing "without explicit rel-scheme"
    (with-actor-db db-spec
      (fn [db]
        (let [conn (sqlite/open-db-connection-sqlite3 db)]
          (let [axel {"id" -1 "first" "Axel" "last" "Hacke" "birthday" (time/make-date 1956 1 20) "sex" false}
                get-1 (query [person (<- person-table)]
                             (restrict ($= (! person "id") ($integer -1)))
                             (return person))]
            (is (empty? (run-query conn get-1)))
            (apply insert! conn person-table (vals axel))
            (is (= #{axel} (set (run-query conn get-1))))))))))
