(ns sqlosure.relational-algebra-sql-test
  (:require [sqlosure.relational-algebra-sql :refer :all]
            [sqlosure.sql :as sql]
            [clojure.test :refer :all]))

(deftest x->sql-select-test
  (is (= (sql/new-sql-select) (x->sql-select sql/the-sql-select-empty)))
  (let [sel (sql/set-sql-select-attributes
             (sql/new-sql-select) {"one" (sql/make-sql-expr-column "one")})]
    (is (= (sql/set-sql-select-tables (sql/new-sql-select) {false sel})
           (x->sql-select sel))))
  (let [sel (sql/set-sql-select-nullary? (sql/new-sql-select) true)]
    (is (= sel (x->sql-select sel)))))
