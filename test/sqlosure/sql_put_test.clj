(ns sqlosure.sql-put-test
  (:require [active.clojure.lens :as lens]
            [clojure
             [string :as s]
             [test :refer :all]]
            [sqlosure
             [relational-algebra :refer :all]
             [relational-algebra-sql :refer :all]
             [sql :refer :all]
             [sql-put :refer :all]
             [type :refer [boolean% double% integer% string%]]]))

(def tbl1 (make-sql-table "tbl1"
                          {"one" string%
                           "two" double%}
                          :universe sql-universe
                          :handle "tbl1"))

(deftest put-space-test
  (is (= (with-out-str (print \space)) " ")))

(deftest put-padding-if-non-null-test
  (is (nil? (put-padding-if-non-null nil identity)))
  (is (nil? (put-padding-if-non-null '() identity)))
  (is (= " foo bar baz"
         (with-out-str (put-padding-if-non-null ["foo" "bar" "baz"]
                                                #(print (s/join " " %)))))))

(deftest default-put-alias-test
  (is (nil? (default-put-alias nil)))
  (is (= " AS FOO" (with-out-str (default-put-alias "FOO")))))

(deftest put-literal-test
  (is (= ["?" [[integer% 42]]]
         (with-out-str-and-value (put-literal integer% 42))))
  (is (= ["?" [[string% "foobar"]]]
         (with-out-str-and-value (put-literal string% "foobar"))))
  (is (= ["?" [[string% nil]]]
         (with-out-str-and-value (put-literal string% nil))))
  (is (= ["?" [[boolean% true]]]
         (with-out-str-and-value (put-literal boolean% true))))
  (is (= ["?" [[boolean% false]]]
         (with-out-str-and-value (put-literal boolean% false)))))

(deftest put-sql-select-test
  (is (= "SELECT *" (with-out-str
                      (put-sql-select (new-sql-select)))))
  (is (= "SELECT * FROM CUSTOMERS"
         (with-out-str
           (put-sql-select (make-sql-select-table "CUSTOMERS")))))
  (let [o (make-order {(make-attribute-ref "one") :ascending}
                      (make-sql-table "tbl1"
                                      (alist->rel-scheme [["one" string%]
                                                          ["two" integer%]])))
        q (query->sql o)
        [res-str res-args] (with-out-str-and-value
                             (put-sql-select q))]
    (is (= "SELECT * FROM tbl1 ORDER BY one ASC" res-str))
    (is (= res-args '())))
  (let [q1 (-> (new-sql-select)
               (lens/shove sql-select-tables-lens
                           [["S" (make-sql-select-table "SUPPLIERS")]
                            [nil (make-sql-select-table "CUSTOMERS")]])
               (lens/shove sql-select-attributes-lens
                           {"UID" (make-sql-expr-column "UID")})
               (lens/shove sql-select-order-by-lens
                           [[(make-sql-expr-column "uid") :ascending]])
               (lens/shove sql-select-criteria-lens
                           [(make-sql-expr-app
                             op-<
                             (make-sql-expr-column "foo")
                             (make-sql-expr-const integer% 10))
                            (make-sql-expr-app
                             op-=
                             (make-sql-expr-column "uid")
                             (make-sql-expr-const integer% 5))]))
        q2 (-> (new-sql-select)
               (lens/shove sql-select-attributes-lens
                           {"cost" (make-sql-expr-column "cost")})
               (add-table (make-sql-select-table "PARTS"))
               (lens/shove sql-select-criteria-lens
                           [(make-sql-expr-app
                             op-<
                             (make-sql-expr-column "cost")
                             (make-sql-expr-const integer% 100))]))]
    (let [[res-str res-args]
          (with-out-str-and-value (put-sql-select q1))]
      (is (= (str "SELECT UID "
                  "FROM SUPPLIERS AS S, CUSTOMERS "
                  "WHERE (foo < ?) AND (uid = ?) ORDER BY uid ASC")
             res-str))
      (is (= [[integer% 10] [integer% 5]] res-args)))
    (let [[res-str res-args]
          (with-out-str-and-value (put-sql-select
                                   (make-sql-select-combine :union q1 q2)))]
      (is (= (str "(SELECT UID "
                  "FROM SUPPLIERS AS S, CUSTOMERS "
                  "WHERE (foo < ?) AND (uid = ?) ORDER BY uid ASC) "
                  "UNION (SELECT cost FROM PARTS WHERE (cost < ?))")
             res-str))
      (is (= [[integer% 10] [integer% 5] [integer% 100]] res-args)))))

(deftest put-sql-outer-join-test
  (testing "simple case"
    (let [t1 (make-sql-table "t1"
                             (alist->rel-scheme [["C" string%]]))
          t2 (make-sql-table "t2"
                             (alist->rel-scheme [["D" integer%]]))
          r (make-restrict-outer (=$ (make-attribute-ref "C")
                                     (make-attribute-ref "D"))
                                 (make-left-outer-product t1 t2))
          sql (query->sql r)]
      (let [[res-str res-args]
            (with-out-str-and-value (put-sql-select sql))]
        (is (= "SELECT * FROM t1 LEFT JOIN t2 ON (C = D)"
               res-str)))))
  (testing "multiple tables on the left"
    (let [t1 (make-sql-table "t1"
                             (alist->rel-scheme [["C" string%]]))
          t2 (make-sql-table "t2"
                             (alist->rel-scheme [["D" integer%]]))
          t3 (make-sql-table "t3"
                             (alist->rel-scheme [["E" integer%]]))
          r (make-restrict-outer
             (=$ (make-attribute-ref "C")
                 (make-attribute-ref "E"))
             (make-left-outer-product (make-product t1 t2) t3))
          sql (query->sql r)]
      
      (let [[res-str res-args]
            (with-out-str-and-value (put-sql-select sql))]
        (is (= "SELECT * FROM (SELECT * FROM t1, t2) LEFT JOIN t3 ON (C = E)"
               res-str))))))


(deftest put-joining-infix-test
  (is (= "foo bar baz"
         (with-out-str (put-joining-infix ["foo" "bar" "baz"] " " print))))
  (is (= "foo-bar-baz"
         (with-out-str (put-joining-infix ["foo" "bar" "baz"] "-" print)))))

(deftest put-tables-test
  (is (= "foo"
         (with-out-str (put-tables [[nil (make-sql-select-table "foo")]]
                                   ", "))))
  (is (= "foo, bar AS b"
         (with-out-str (put-tables [[nil (make-sql-select-table "foo")]
                                    ["b" (make-sql-select-table "bar")]]
                                   ", ")))))

(deftest default-put-literal-test
  (is (= ["?" [[integer% 42]]]
         (with-out-str-and-value (default-put-literal integer% 42))))
  (is (= ["?" [[string% "foobar"]]]
         (with-out-str-and-value (default-put-literal string% "foobar"))))
  (is (= ["?" [[string% nil]]]
         (with-out-str-and-value (default-put-literal string% nil))))
  (is (= ["?" [[boolean% true]]]
         (with-out-str-and-value (default-put-literal boolean% true))))
  (is (= ["?" [[boolean% false]]]
         (with-out-str-and-value (default-put-literal boolean% false)))))

(deftest default-put-combine-test
  (let [q1 (-> (new-sql-select)
               (lens/shove sql-select-tables-lens
                           [["S" (make-sql-select-table "SUPPLIERS")]
                            [nil (make-sql-select-table "CUSTOMERS")]])
               (lens/shove sql-select-attributes-lens
                           {"UID" (make-sql-expr-column "UID")})
               (lens/shove sql-select-order-by-lens
                           [[(make-sql-expr-column "uid") :ascending]])
               (lens/shove sql-select-criteria-lens
                           [(make-sql-expr-app
                             op-<
                             (make-sql-expr-column "foo")
                             (make-sql-expr-const integer% 10))
                            (make-sql-expr-app
                             op-=
                             (make-sql-expr-column "uid")
                             (make-sql-expr-const integer% 5))]))
        q2 (-> (new-sql-select)
               (lens/shove sql-select-attributes-lens
                           {"cost" (make-sql-expr-column "cost")})
               (add-table (make-sql-select-table "PARTS"))
               (lens/shove sql-select-criteria-lens
                           [(make-sql-expr-app
                             op-<
                             (make-sql-expr-column "cost")
                             (make-sql-expr-const integer% 100))]))
        [res-str res-args]
        (with-out-str-and-value (default-put-combine :union q1 q2))]
    (is (= (str "(SELECT UID "
                "FROM SUPPLIERS AS S, CUSTOMERS "
                "WHERE (foo < ?) AND (uid = ?) ORDER BY uid ASC) "
                "UNION (SELECT cost FROM PARTS WHERE (cost < ?))" ) res-str))
    (is (= [[integer% 10] [integer% 5] [integer% 100]] res-args))))

(deftest put-when-test
  (is (= ["WHEN ? THEN ?" [[string% "foo"] [string% "bar"]]]
         (with-out-str-and-value
           (put-when [(make-sql-expr-const string% "foo")
                      (make-sql-expr-const string% "bar")])))))

(= '("foo" "bar") ["foo" "bar"] (list "foo" "bar"))

(deftest put-where-test
  (is (= ["WHERE (cost = ?) AND (foo = ?)" [[integer% 100] [string% "bar"]]]
         (with-out-str-and-value
           (put-where [(make-sql-expr-app
                        op-=
                        (make-sql-expr-column "cost")
                        (make-sql-expr-const integer% 100))
                       (make-sql-expr-app
                        op-=
                        (make-sql-expr-column "foo")
                        (make-sql-expr-const string% "bar"))])))))

(deftest put-group-by-test
  (is (= "GROUP BY cost"
         (with-out-str
           (put-group-by #{"cost"}))))
  (is (= "GROUP BY cost, supplier"
         (with-out-str
           (put-group-by #{"cost"
                           "supplier"})))))

(deftest put-order-by-test
  (is (= "ORDER BY one ASC"
         (with-out-str
           (put-order-by [[(make-sql-expr-column "one") :ascending]]))))
  (is (= "ORDER BY one ASC, two DESC"
         (with-out-str
           (put-order-by [[(make-sql-expr-column "one") :ascending]
                          [(make-sql-expr-column "two") :descending]])))))

(deftest put-having-test
  (is (= ["HAVING (year < ?)" [[integer% 2000]]]
         (with-out-str-and-value
           (put-having [(make-sql-expr-app
                         op-<
                         (make-sql-expr-column "year")
                         (make-sql-expr-const integer% 2000))]))))
  (is (= ["HAVING ((year < ?), (director = ?))"
          [[integer% 2000] [string% "Luc Besson"]]]
         (with-out-str-and-value
           (put-having
            [(make-sql-expr-tuple
              [(make-sql-expr-app
                op-<
                (make-sql-expr-column "year")
                (make-sql-expr-const integer% 2000))
               (make-sql-expr-app
                op-=
                (make-sql-expr-column "director")
                (make-sql-expr-const string% "Luc Besson"))])])))))

(deftest put-attributes-test
  (is (= "*" (with-out-str (put-attributes nil))))
  (is (= "foo, bar AS something-else, same"
         (with-out-str
           (put-attributes [[nil (make-sql-expr-column "foo")]
                            ["something-else" (make-sql-expr-column "bar")]
                            ["same" (make-sql-expr-column "same")]])))))
