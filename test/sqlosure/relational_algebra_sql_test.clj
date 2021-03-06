(ns sqlosure.relational-algebra-sql-test
  (:require [sqlosure.relational-algebra-sql :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [sqlosure.type :refer :all]
            [sqlosure.universe :refer [make-universe]]
            [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [active.clojure.lens :as lens]))

(def test-universe (make-sql-universe))

(def tbl1 (base-relation "tbl1"
                         (alist->rel-scheme
                          [["one" string%]
                           ["two" integer%]])
                         :universe test-universe))

(def tbl2 (base-relation "tbl2"
                         (alist->rel-scheme
                          [["three" blob%]
                           ["four" double%]])
                         :universe test-universe))

(deftest x->sql-select-test
  (is (= (new-sql-select) (x->sql-select the-sql-select-empty)))
  (let [sel (lens/shove (new-sql-select) sql-select-attributes {"one" (make-sql-expr-column "one")})]
    (is (= (lens/shove (new-sql-select) sql-select-tables [[nil sel]])
           (x->sql-select sel))))
  (let [sel (lens/shove (new-sql-select) sql-select-nullary? true)]
    (is (= sel (x->sql-select sel)))))

(deftest aggregation-op->sql-test
  (is (= op-count (aggregation-op->sql :count)))
  (is (= op-sum (aggregation-op->sql :sum)))
  ;; Everything else is basically the same.
  (is (= "foobar" (aggregation-op->sql :foobar))))

(deftest expression->sql-test
  (is (= (make-sql-expr-column "two") (expression->sql (make-attribute-ref "two"))))
  (is (= (make-sql-expr-const string% "foobar") (expression->sql (make-const string% "foobar"))))
  (is (= (make-sql-expr-app op-= (make-sql-expr-const boolean% true) (make-sql-expr-const string% "bar"))
         (expression->sql (=$ (make-const boolean% true)
                              (make-const string% "bar")))))
  (is (= (make-sql-expr-tuple [(make-sql-expr-const double% 42.0)
                               (make-sql-expr-const string% "foobar")
                               (make-sql-expr-column "ref")])
         (expression->sql (make-tuple [(make-const double% 42.0)
                                       (make-const string% "foobar")
                                       (make-attribute-ref "ref")]))))
  (is (= (make-sql-expr-app
          op-count
          (make-sql-expr-tuple [(make-sql-expr-column "two")
                                (make-sql-expr-app op-=
                                                   (make-sql-expr-const integer% 42)
                                                   (make-sql-expr-const integer% 23))]))
         (expression->sql (make-aggregation :count (make-tuple [(make-attribute-ref "two")
                                                                (=$ (make-const integer% 42)
                                                                    (make-const integer% 23))])))))
  (is (= (make-sql-expr-case
          {(make-sql-expr-app op-=
                              (make-sql-expr-const integer% 42)
                              (make-sql-expr-const integer% 42))
           (make-sql-expr-const boolean% true)}
          (make-sql-expr-const boolean% false))
         (expression->sql (make-case-expr {(=$ (make-const integer% 42)
                                                    (make-const integer% 42))
                                           (make-const boolean% true)}
                                          (make-const boolean% false))))))

(deftest project-alist->sql-test
  (is (= [["one" (make-sql-expr-column "one")]
          ["two" (make-sql-expr-column "two")]]
         (project-alist->sql [["one" (make-attribute-ref "one")]
                              ["two" (make-attribute-ref "two")]])))
  (is (thrown? Exception  ;; Should throw because string% and integer% are not
                          ;; expressions.
               (doall (project-alist->sql {"one" string% "two" integer%})))))

(deftest add-table-test
  (is (= tbl1 (-> (new-sql-select)
                  (add-table tbl1)
                  sql-select-tables
                  first
                  second)))
  (let [q (-> (new-sql-select) (add-table tbl1) (add-table tbl2))]
    (is (= 2 (count (sql-select-tables q))))
    (is (= (list tbl2 tbl1)
           (map second (sql-select-tables q))))))

(deftest query->sql-test
  (is (= (make-sql-select-table nil "tbl1")
         (query->sql tbl1)))
  (testing "project"
    (let [p         (make-project [["two" (make-attribute-ref "two")]
                                   ["one" (make-attribute-ref "one")]]
                                  tbl1)
          res       (lens/shove (x->sql-select (query->sql tbl1))
                                sql-select-attributes
                                (project-alist->sql (project-alist p)))
          nullary-p (make-project [] tbl1)]
      (is (sql-select-nullary? (query->sql nullary-p)))
      (is (= res (query->sql p)))
      (testing "with aggregation"
        (let [grouping-p (query->sql
                          (make-project
                           [["one" (make-attribute-ref "one")]
                            ["count_twos" (make-aggregation :count (make-attribute-ref "two"))]]
                           (make-group #{"one"}
                                       tbl1)))]
          (is (= [["one" (make-sql-expr-column "one")]
                  ["count_twos" (make-sql-expr-app op-count
                                                   (make-sql-expr-column "two"))]]
                 (sql-select-attributes grouping-p)))
          (is (= [[nil (make-sql-select-table nil "tbl1")]] (sql-select-tables grouping-p)))
          
          (is (= #{"one"} (sql-select-group-by grouping-p)))))))
  (testing "restrict"
    (let [test-universe (make-universe)
          t1            (base-relation 't1
                                       (alist->rel-scheme [["C" string%]])
                                       :universe test-universe
                                       :handle "t1")
          r             (make-restrict (>=$ (make-const integer% 42)
                                            (make-attribute-ref "C"))
                                       t1)]
      (is (= (mapv second (sql-select-tables (query->sql r)))
             [(query->sql t1)]))
      (is (= [(expression->sql (>=$ (make-const integer% 42)
                                    (make-attribute-ref "C")))]
             (sql-select-criteria (query->sql r))))))

  (testing "product"

    (let [t1 (base-relation "t1"
                            (alist->rel-scheme [["A" string%]]))
          t2 (base-relation "t2"
                            (alist->rel-scheme [["B" integer%]]))
          q (make-product t1 t2)
          sql (query->sql q)]
      (is (= [[nil (make-sql-select-table nil "t1")]
              [nil (make-sql-select-table nil "t2")]]
             (sql-select-tables sql))))
    (testing "nested queries just get pushed into the SQL-tables"
      (let [t1 (base-relation "t1"
                              (alist->rel-scheme [["A" string%]]))
            q2 (make-project [["B" (make-attribute-ref "B")]]
                             (base-relation "t2"
                                            (alist->rel-scheme [["B" integer%]])))
            q (make-product t1 q2)
            sql (query->sql q)]
        (is (= [[nil (make-sql-select-table nil "t1")]
                [nil (query->sql q2)]]
               (sql-select-tables sql))))))

  (testing "outer product"
    (let [test-universe
          (make-universe)
          t1  (base-relation 't1
                             (alist->rel-scheme [["C" string%]])
                             :universe test-universe
                             :handle "t1")
          t2  (base-relation 't2
                             (alist->rel-scheme [["D" integer%]])
                             :universe test-universe
                             :handle "t2")
          r   (make-restrict (=$ (make-attribute-ref "C")
                                 (make-attribute-ref "D"))
                             (make-left-outer-product t1 t2))
          sql (query->sql r)]
      (is (= [[nil (make-sql-select-table nil 't1)]]
             (sql-select-tables sql)))
      (is (= [[nil (make-sql-select-table nil 't2)]]
             (sql-select-outer-tables sql)))))

  (testing "order"
    (let [tbl (base-relation "tbl1"
                             (alist->rel-scheme [["one" string%]
                                                 ["two" integer%]]))
          o   (make-order {(make-attribute-ref "one") :ascending}
                          (base-relation "tbl1"
                                         (alist->rel-scheme [["one" string%]
                                                             ["two" integer%]])))
          sql (query->sql o)]
      (is (= [[nil (make-sql-select-table nil "tbl1")]]
             (sql-select-tables sql)))
      (is (= [[(make-sql-expr-column "one") :ascending]]
             (sql-select-order-by sql)))))

  (testing "combining grouping with empty projection"
    (let [s (query->sql
             (make-project [["all" (make-aggregation :count-all)]]
                           (make-project []
                                         (make-group #{"one"}
                                                     (base-relation "tbl1"
                                                                    (alist->rel-scheme [["one" string%]
                                                                                        ["two" integer%]]))))))]
      ;; we want a subquery
      (is (sql-select? (second (first (sql-select-tables s))))))))
