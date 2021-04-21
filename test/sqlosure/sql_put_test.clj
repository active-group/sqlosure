(ns sqlosure.sql-put-test
  (:require [active.clojure.lens :as lens]
            [clojure.test :as t :refer [deftest is testing]]
            [clojure.string :as string]

            [sqlosure.core :as c]
            [sqlosure.relational-algebra-sql :as rel-alg-sql]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.sql :as sql]
            [sqlosure.sql-put :as put]
            [sqlosure.type :refer [string% integer% double% boolean%]]))

(def tbl1 (sql/base-relation "tbl1"
                             [["one" string%]
                              ["two" double%]]
                             :universe sql/sql-universe
                             :handle "tbl1"))

(def test-put-parameterization
  (put/make-sql-put-parameterization
   (fn [alias]
     (if alias
       (put/write! "TESTAS" alias)
       (active.clojure.monad/return nil)))
   put/default-put-combine))

(def test-run (partial put/run test-put-parameterization))

(deftest put-padding-if-non-null-test
  (is (= ["" []] (test-run (put/put-padding-if-non-null nil put/write!))))
  (is (= ["foo bar baz" []]
         (test-run (put/put-padding-if-non-null ["foo" "bar" "baz"]
                                                #(put/write! (string/join " " %)))))))

(deftest put-alias-test
  (is (= ["TESTAS foo" []] (test-run (put/put-alias "foo"))))
  (is (= ["" []] (test-run (put/put-alias nil)))))

(deftest put-literal-test
  (is (= ["?" [[integer% 42]]] (test-run (put/put-literal integer% 42))))
  (is (= ["?" [[string% "foobar"]]] (test-run (put/put-literal string% "foobar"))))
  (is (= ["?" [[string% nil]]] (test-run (put/put-literal string% nil))))
  (is (= ["?" [[boolean% true]]] (test-run (put/put-literal boolean% true))))
  (is (= ["?" [[boolean% false]]] (test-run (put/put-literal boolean% false)))))

(deftest put-sql-select-test
  (is (= ["" []] (test-run (put/put-sql-select (sql/new-sql-select)))))
  (is (= ["SELECT * FROM CUSTOMERS" []]
         (test-run (put/put-sql-select (sql/make-sql-select-table nil "CUSTOMERS")))))
  (t/testing "table-spaces are printed correctly"
    (t/is (= ["SELECT * FROM prefix.table" []]
             (-> (sql/base-relation "table" [])
                 (rel/base-relation-table-space "prefix")
                 rel-alg-sql/query->sql
                 put/put-sql-select
                 test-run))))
  (let [o (rel/make-order
           {(rel/make-attribute-ref "one") :ascending}
           tbl1)
        q (rel-alg-sql/query->sql o)]

    (is (= ["SELECT * FROM tbl1 ORDER BY one ASC" []]
           (test-run (put/put-sql-select q)))))
  (let [q1 (-> (sql/new-sql-select)
               (lens/shove sql/sql-select-tables
                           [["S" (sql/make-sql-select-table nil "SUPPLIERS")]
                            [nil (sql/make-sql-select-table nil "CUSTOMERS")]])
               (lens/shove sql/sql-select-attributes
                           {"UID" (sql/make-sql-expr-column "UID")})
               (lens/shove sql/sql-select-order-by
                           [[(sql/make-sql-expr-column "uid") :ascending]])
               (lens/shove sql/sql-select-criteria
                           [(sql/make-sql-expr-app sql/op-<
                                                   (sql/make-sql-expr-column "foo")
                                                   (sql/make-sql-expr-const integer% 10))
                            (sql/make-sql-expr-app sql/op-=
                                                   (sql/make-sql-expr-column "uid")
                                                   (sql/make-sql-expr-const integer% 5))]))
        q2 (-> (sql/new-sql-select)
               (lens/shove sql/sql-select-attributes
                           {"cost" (sql/make-sql-expr-column "cost")})
               (rel-alg-sql/add-table (sql/make-sql-select-table nil "PARTS"))
               (lens/shove sql/sql-select-criteria
                           [(sql/make-sql-expr-app sql/op-<
                                                   (sql/make-sql-expr-column "cost")
                                                   (sql/make-sql-expr-const integer% 100))]))]
    (is (= ["SELECT UID FROM SUPPLIERS TESTAS S , CUSTOMERS WHERE ( foo < ? ) AND ( uid = ? ) ORDER BY uid ASC"
            [[integer% 10] [integer% 5]]]
           (test-run (put/put-sql-select q1))))
    (is (= [(str "( SELECT UID "
                 "FROM SUPPLIERS TESTAS S , CUSTOMERS "
                 "WHERE ( foo < ? ) AND ( uid = ? ) "
                 "ORDER BY uid ASC ) "
                 "UNION "
                 "( SELECT cost "
                 "FROM PARTS "
                 "WHERE ( cost < ? ) )")
            [[integer% 10] [integer% 5] [integer% 100]]]
           (test-run (put/put-sql-select
                      (sql/make-sql-select-combine :union q1 q2)))))))

(deftest put-sql-outer-join-test
  (testing "simple case"
    (let [t1  (sql/base-relation "t1"
                                 (rel/alist->rel-scheme [["C" string%]]))
          t2  (sql/base-relation "t2"
                                 (rel/alist->rel-scheme [["D" integer%]]))
          r   (rel/make-restrict-outer (sql/=$ (rel/make-attribute-ref "C")
                                               (rel/make-attribute-ref "D"))
                                       (rel/make-left-outer-product t1 t2))
          sql (rel-alg-sql/query->sql r)]
      (is (= ["SELECT * FROM t1 LEFT JOIN t2 ON ( C = D )" []]
             (test-run (put/put-sql-select sql))))))
  (testing "multiple tables on the left"
    (let [t1  (sql/base-relation "t1"
                                 (rel/alist->rel-scheme [["C" string%]]))
          t2  (sql/base-relation "t2"
                                 (rel/alist->rel-scheme [["D" integer%]]))
          t3  (sql/base-relation "t3"
                                 (rel/alist->rel-scheme [["E" integer%]]))
          r   (rel/make-restrict-outer (sql/=$ (rel/make-attribute-ref "C")
                                               (rel/make-attribute-ref "E"))
                                       (rel/make-left-outer-product (rel/make-product t1 t2) t3))
          sql (rel-alg-sql/query->sql r)]
      (is (= ["SELECT * FROM ( SELECT * FROM t1 , t2 ) LEFT JOIN t3 ON ( C = E )" []]
             (test-run (put/put-sql-select sql)))))))

(deftest put-joining-infix-test
  (is (= ["foo bar baz" []]
         (test-run (put/put-joining-infix ["foo" "bar" "baz"] "" put/write!))))
  (is (= ["foo - bar - baz" []]
         (test-run (put/put-joining-infix ["foo" "bar" "baz"] "-" put/write!)))))

(deftest put-tables-test
  (is (= ["foo" []]
         (test-run (put/put-tables [[nil (sql/make-sql-select-table nil "foo")]] ", "))))
  (is (= ["foo ,  bar TESTAS b" []]
         (test-run (put/put-tables [[nil (sql/make-sql-select-table nil "foo")]
                                    ["b" (sql/make-sql-select-table nil "bar")]]
                                   ", ")))))

(deftest default-put-combine-test
  (let [q1 (-> (sql/new-sql-select)
               (lens/shove sql/sql-select-tables
                           [["S" (sql/make-sql-select-table nil "SUPPLIERS")]
                            [nil (sql/make-sql-select-table nil "CUSTOMERS")]])
               (lens/shove sql/sql-select-attributes
                           {"UID" (sql/make-sql-expr-column "UID")})
               (lens/shove sql/sql-select-order-by
                           [[(sql/make-sql-expr-column "uid") :ascending]])
               (lens/shove sql/sql-select-criteria
                           [(sql/make-sql-expr-app sql/op-<
                                               (sql/make-sql-expr-column "foo")
                                               (sql/make-sql-expr-const integer% 10))
                            (sql/make-sql-expr-app sql/op-=
                                               (sql/make-sql-expr-column "uid")
                                               (sql/make-sql-expr-const integer% 5))]))
        q2 (-> (sql/new-sql-select)
               (lens/shove sql/sql-select-attributes
                           {"cost" (sql/make-sql-expr-column "cost")})
               (rel-alg-sql/add-table (sql/make-sql-select-table nil "PARTS"))
               (lens/shove sql/sql-select-criteria
                           [(sql/make-sql-expr-app sql/op-<
                                               (sql/make-sql-expr-column "cost")
                                               (sql/make-sql-expr-const integer% 100))]))]
    (is (= [ (str "( SELECT UID FROM SUPPLIERS TESTAS S , CUSTOMERS WHERE ( foo < ? ) AND ( uid = ? ) ORDER BY uid ASC ) "
                  "UNION "
                  "( SELECT cost FROM PARTS WHERE ( cost < ? ) )")
            [[integer% 10] [integer% 5] [integer% 100]]]
           (test-run (put/default-put-combine :union q1 q2))))))

(deftest put-when-test
  (is (= ["WHEN ? THEN ?" [[string% "foo"] [string% "bar"]]]
         (test-run (put/put-when [(sql/make-sql-expr-const string% "foo")
                                  (sql/make-sql-expr-const string% "bar")])))))

(deftest put-group-by-test
  (is (= ["GROUP BY cost" []] (test-run (put/put-group-by #{"cost"}))))
  (is (= ["GROUP BY cost , supplier" []] (test-run (put/put-group-by #{"cost" "supplier"})))))

(deftest put-order-by-test
  (is (= ["ORDER BY one ASC" []]
         (test-run (put/put-order-by [[(sql/make-sql-expr-column "one") :ascending]]))))
  (is (= ["ORDER BY one ASC , two DESC" []]
         (test-run (put/put-order-by [[(sql/make-sql-expr-column "one") :ascending]
                                      [(sql/make-sql-expr-column "two") :descending]])))))


(deftest put-attributes-test
  (is (= ["*" []] (test-run (put/put-attributes nil))))
  (is (= ["foo , bar TESTAS something-else , same" []]
         (test-run (put/put-attributes [[nil (sql/make-sql-expr-column "foo")]
                                        ["something-else" (sql/make-sql-expr-column "bar")]
                                        ["same" (sql/make-sql-expr-column "same")]])))))
