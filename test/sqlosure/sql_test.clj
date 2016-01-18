(ns sqlosure.sql-test
  (:require [sqlosure.sql :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :as u :refer [make-universe universe? universe-base-relation-table]]
            [sqlosure.type :refer [integer% double% string%]]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(deftest make-sql-table-test
  (let [test-universe (make-universe)
        test-table (make-sql-table "tbl1"
                                   (alist->rel-scheme [["one" string%]
                                                       ["two" integer%]]))
        test-table2 (make-sql-table
                     "tbl2"
                     (alist->rel-scheme [["one" string%]
                                         ["two" integer%]])
                     :universe test-universe)]
    (is (= (base-relation-name test-table) 'tbl1))
    (is (= (sql-table-scheme (base-relation-handle test-table))
           (alist->rel-scheme [["one" string%] ["two" integer%]])))
    (is (and (= (sql-table-scheme (base-relation-handle test-table2))
                (alist->rel-scheme [["one" string%] ["two" integer%]]))
             (= ((u/universe-get-base-relation-table test-universe) 'tbl2)
                test-table2)))))

(deftest sql-order?-test
  (is (every? sql-order? [:ascending :descending]))
  (is (not-any? sql-order? [:any :thing :else])))

(deftest sql-combine-op?-test
  (is (every? sql-combine-op? [:union :intersection :difference]))
  (is (not-any? sql-combine-op? [:any :thing :else])))

(deftest make-sql-expr-app-test
  (let [test-expr (make-sql-expr-app op-+ 1 2)]
    (is (and (= (sql-operator-name (sql-expr-app-rator test-expr)) "+")
             (= (sql-operator-arity (sql-expr-app-rator test-expr)) 2)))
    (is (= (sql-expr-app-rands test-expr) '(1 2))))
  (is (thrown? Exception (make-sql-expr-app op-+ 1 2 3))))

(deftest check-numerical-test
  (let [fail (fn [source t] (throw (Exception. (str source ": not numerical " t))))]
    (is (and (check-numerical integer% fail) (check-numerical double% fail)))
    (is (thrown? Exception (check-numerical string% fail)))))

(deftest make-sql-universe-test
  (is (= @sql-universe @(make-sql-universe))))

(deftest member-test
  (is (false? (member 4 [1 2 3])))
  (is (= [2 3] (member 2 [1 2 3])))
  (is (false? (member 42 nil))))
