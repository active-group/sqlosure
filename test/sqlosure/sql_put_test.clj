(ns sqlosure.sql-put-test
  (:require [sqlosure.sql-put :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.relational-algebra-sql :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [sqlosure.type :refer [string% integer% double% boolean%]]
            [clojure.test :refer :all]
            [clojure.string :as s]))

(def tbl1 (make-sql-table "tbl1"
                          {"one" string%
                           "two" double%}
                          :universe sql-universe
                          :handle "tbl1"))

(deftest put-space-test
  (is (= (with-out-str (print \space))) " "))

(deftest put-adding-if-non-null-test
  (is (nil? (put-adding-if-non-null nil identity)))
  (is (nil? (put-adding-if-non-null '() identity)))
  (is (= " foo bar baz"
         (with-out-str (put-adding-if-non-null ["foo" "bar" "baz"]
                                               #(print (s/join " " %)))))))

(deftest put-as-test
  (is (nil? (put-as nil)))
  (is (= " AS FOO" (with-out-str (put-as "FOO")))))

(deftest put-sql-select-test
  (let [put-sql-select*
        (partial put-sql-select default-sql-put-parameterization)]
    (is (= "SELECT *" (with-out-str
                        (put-sql-select* (new-sql-select)))))
    (is (= "SELECT * FROM CUSTOMERS"
           (with-out-str
             (put-sql-select* (make-sql-select-table "CUSTOMERS")))))
    (let [q1 (-> (new-sql-select)
                 (set-sql-select-tables
                  [["S" (make-sql-select-table "SUPPLIERS")]
                   [nil (make-sql-select-table "CUSTOMERS")]])
                 (set-sql-select-attributes
                  {"UID" (make-sql-expr-column "UID")})
                 (set-sql-select-order-by
                  [[(make-sql-expr-column "uid") :ascending]])
                 (set-sql-select-criteria
                  [(make-sql-expr-app op-<
                                      (make-sql-expr-column "foo")
                                      (make-sql-expr-const 10))
                   (make-sql-expr-app op-=
                                      (make-sql-expr-column "uid")
                                      (make-sql-expr-const 5))]))
          q2 (-> (new-sql-select)
                 (set-sql-select-attributes
                  {"cost" (make-sql-expr-column "cost")})
                 (add-table (make-sql-select-table "PARTS"))
                 (set-sql-select-criteria
                  [(make-sql-expr-app op-<
                                      (make-sql-expr-column "cost")
                                      (make-sql-expr-const 100))]))]
      (is (= (with-out-str (put-sql-select* q1))
             (str "SELECT UID "
                  "FROM SUPPLIERS AS S, CUSTOMERS "
                  "WHERE (foo < 10) AND (uid = 5) "
                  "ORDER BY uid ASC")))
      (is (= (with-out-str (put-sql-select*
                            (make-sql-select-combine :union q1 q2)))
             (str "(SELECT UID FROM SUPPLIERS AS S, CUSTOMERS "
                  "WHERE (foo < 10) AND (uid = 5) "
                  "ORDER BY uid ASC) "
                  "UNION "
                  "(SELECT cost "
                  "FROM PARTS "
                  "WHERE (cost < 100))"))))))
