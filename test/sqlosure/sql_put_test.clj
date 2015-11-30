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

(deftest put-literal-test
  (let [p default-sql-put-parameterization]
    (is (= "42" (with-out-str (put-literal p 42))))
    (is (= "'foobar'" (with-out-str (put-literal p "foobar"))))
    (is (= "NULL" (with-out-str (put-literal p nil))))
    (is (= "TRUE" (with-out-str (put-literal p true))))
    (is (= "FALSE" (with-out-str (put-literal p false))))))

(deftest put-sql-select-test
  (let [put-sql-select*
        (partial put-sql-select default-sql-put-parameterization)]
    (is (= "SELECT *" (with-out-str
                        (put-sql-select* (new-sql-select)))))
    (is (= "SELECT * FROM CUSTOMERS"
           (with-out-str
             (put-sql-select* (make-sql-select-table "CUSTOMERS")))))
    (let [o (make-order {(make-attribute-ref "one") :ascending}
                        (make-sql-table "tbl1"
                                        (make-rel-scheme {"one" string%
                                                          "two" integer%})))
          q (query->sql o)]
      (is (= "SELECT * FROM tbl1 ORDER BY one ASC"
             (with-out-str
               (put-sql-select default-sql-put-parameterization q)))))
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

(deftest put-joining-infix-test
  (is (= "foo bar baz"
         (with-out-str (put-joining-infix ["foo" "bar" "baz"] " " print))))
  (is (= "foo-bar-baz"
         (with-out-str (put-joining-infix ["foo" "bar" "baz"] "-" print)))))

(deftest put-tables-test
  (is (= "FROM foo"
         (with-out-str (put-tables default-sql-put-parameterization
                                   [[nil (make-sql-select-table "foo")]]))))
  (is (= "FROM foo, bar AS b"
         (with-out-str (put-tables default-sql-put-parameterization
                                   [[nil (make-sql-select-table "foo")]
                                    ["b" (make-sql-select-table "bar")]])))))

(deftest default-put-literal-test
  (is (= "42" (with-out-str (default-put-literal 42))))
  (is (= "'foobar'" (with-out-str (default-put-literal "foobar"))))
  (is (= "NULL" (with-out-str (default-put-literal nil))))
  (is (= "TRUE" (with-out-str (default-put-literal true))))
  (is (= "FALSE" (with-out-str (default-put-literal false)))))

(deftest default-put-combine-test
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
    (is (= (with-out-str (default-put-combine default-sql-put-parameterization
                                              :union q1 q2))
           (str "(SELECT UID FROM SUPPLIERS AS S, CUSTOMERS "
                "WHERE (foo < 10) AND (uid = 5) "
                "ORDER BY uid ASC) "
                "UNION "
                "(SELECT cost "
                "FROM PARTS "
                "WHERE (cost < 100))")))))

(deftest put-when-test
  (is (= "WHEN 'foo' THEN 'bar'"
         (with-out-str (put-when default-sql-put-parameterization
                                 [(make-sql-expr-const "foo")
                                  (make-sql-expr-const "bar")])))))

(deftest put-where-test
  (is (= "WHERE (foo = 'bar')")
      (with-out-str (put-where default-sql-put-parameterization [(make-sql-expr-app op-=
                                                                                    (make-sql-expr-column "cost")
                                                                                    (make-sql-expr-const 100))
                                                                 (make-sql-expr-app op-=
                                                                                    (make-sql-expr-column "foo")
                                                                                    (make-sql-expr-const "bar"))]))))

(deftest put-group-by-test
  (is (= "GROUP BY cost"
         (with-out-str
           (put-group-by default-sql-put-parameterization
                         [(make-sql-expr-column "cost")]))))
  (is (= "GROUP BY cost, supplier"
         (with-out-str
           (put-group-by default-sql-put-parameterization
                         [(make-sql-expr-column "cost")
                          (make-sql-expr-column "supplier")])))))

(deftest put-order-by-test
  (is (= "ORDER BY one ASC"
         (with-out-str
           (put-order-by default-sql-put-parameterization [[(make-sql-expr-column "one") :ascending]]))))
  (is (= "ORDER BY one ASC, two DESC"
         (with-out-str
           (put-order-by default-sql-put-parameterization
                         [[(make-sql-expr-column "one") :ascending]
                          [(make-sql-expr-column "two") :descending]])))))

(deftest put-having-test
  (is (= "HAVING (year < 2000)"
         (with-out-str (put-having default-sql-put-parameterization
                                   (make-sql-expr-app
                                    op-<
                                    (make-sql-expr-column "year")
                                    (make-sql-expr-const 2000))))))
  (is (= "HAVING ((year < 2000), (director = 'Luc Besson'))"
         (with-out-str (put-having
                        default-sql-put-parameterization
                        (make-sql-expr-tuple
                         [(make-sql-expr-app
                           op-<
                           (make-sql-expr-column "year")
                           (make-sql-expr-const 2000))
                          (make-sql-expr-app
                           op-=
                           (make-sql-expr-column "director")
                           (make-sql-expr-const "Luc Besson"))]))))))

(deftest put-attributes-test
  (is (= "*" (with-out-str (put-attributes default-sql-put-parameterization nil))))
  (is (= "foo, bar AS something-else, same"
         (with-out-str (put-attributes default-sql-put-parameterization
                                       [[nil (make-sql-expr-column "foo")]
                                        ["something-else" (make-sql-expr-column "bar")]
                                        ["same" (make-sql-expr-column "same")]])))))
