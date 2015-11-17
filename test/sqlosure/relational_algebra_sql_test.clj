(ns sqlosure.relational-algebra-sql-test
  (:require [sqlosure.relational-algebra-sql :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [sqlosure.type :refer :all]
            [sqlosure.universe :refer [make-universe]]
            [clojure.pprint :refer [pprint]]
            [sqlosure.sql :as sql]
            [clojure.test :refer :all]))

(def test-universe (make-sql-universe))

(def tbl1 (make-sql-table "tbl1"
                          (make-rel-scheme
                           {"one" string%
                            "two" integer%})
                          :universe test-universe))

(def tbl2 (make-sql-table "tbl2"
                          (make-rel-scheme
                           {"three" blob%
                            "four" double%})
                          :universe test-universe))
(deftest x->sql-select-test
  (is (= (new-sql-select) (x->sql-select the-sql-select-empty)))
  (let [sel (set-sql-select-attributes
             (new-sql-select) {"one" (make-sql-expr-column "one")})]
    (is (= (set-sql-select-tables (new-sql-select) [[nil sel]])
           (x->sql-select sel))))
  (let [sel (set-sql-select-nullary? (new-sql-select) true)]
    (is (= sel (x->sql-select sel)))))

(deftest aggregation-op->sql-test
  (is (= op-count (aggregation-op->sql :count)))
  (is (= op-sum (aggregation-op->sql :sum)))
  ;; Everything else is basically the same.
  (is (= "foobar" (aggregation-op->sql :foobar))))

(deftest expression->sql-test
  (is (= (make-sql-expr-column "two") (expression->sql (make-attribute-ref "two"))))
  (is (= (make-sql-expr-const "foobar") (expression->sql (make-const string% "foobar"))))
  (is (= (make-sql-expr-app op-= (make-sql-expr-const true) (make-sql-expr-const "bar"))
         (expression->sql (=$ (make-const boolean% true)
                              (make-const string% "bar")))))
  (is (= (make-sql-expr-tuple [(make-sql-expr-const 42.0)
                               (make-sql-expr-const "foobar")
                               (make-sql-expr-column "ref")])
         (expression->sql (make-tuple [(make-const double% 42.0)
                                       (make-const string% "foobar")
                                       (make-attribute-ref "ref")]))))
  (is (= (make-sql-expr-app
          op-count
          (make-sql-expr-tuple [(make-sql-expr-column "two")
                                (make-sql-expr-app op-=
                                                   (make-sql-expr-const 42)
                                                   (make-sql-expr-const 23))]))
         (expression->sql (make-aggregation :count (make-tuple [(make-attribute-ref "two")
                                                                (=$ (make-const integer% 42)
                                                                    (make-const integer% 23))])))))
  (is (= (make-sql-expr-case
          {(make-sql-expr-app op-=
                              (make-sql-expr-const 42)
                              (make-sql-expr-const 42))
           (make-sql-expr-const true)}
          (make-sql-expr-const false))
         (expression->sql (make-case-expr {(=$ (make-const integer% 42)
                                                    (make-const integer% 42))
                                           (make-const boolean% true)}
                                          (make-const boolean% false))))))

(deftest alist->sql-test
  (is (= {"one" (make-sql-expr-column "one")
          "two" (make-sql-expr-column "two")}
         (alist->sql {"one" (make-attribute-ref "one")
                      "two" (make-attribute-ref "two")})))
  (is (thrown? Exception  ;; Should throw because string% and integer% are not
                          ;; expressions.
               (alist->sql {"one" string% "two" integer%}))))
