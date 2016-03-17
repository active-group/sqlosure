(ns sqlosure.relational-algebra-test
  (:require [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :refer :all]
            [sqlosure.sql :as sql :refer :all]
            [sqlosure.type :refer :all]
            [active.clojure.lens :as lens]
            [clojure.pprint :refer :all]
            [clojure.test :refer :all]))

(def test-scheme1 (alist->rel-scheme [[:foo :bar]
                                      [:fizz :buzz]]))
(def test-scheme2 (alist->rel-scheme [[:foo :bar]
                                      [:some :thing]]))
(def test-scheme3 (alist->rel-scheme [[:foo :bar]]))
(def test-scheme4 (alist->rel-scheme [[:fizz :buzz]]))

(deftest rel-scheme-types-test
  (is (= [:bar :buzz]
         (rel-scheme-types test-scheme1))))

(deftest rel-scheme-columns-test
  (is (= [:foo :fizz]
         (rel-scheme-columns test-scheme1))))

(deftest rel-scheme=?-test
  (is (rel-scheme=? (alist->rel-scheme []) the-empty-rel-scheme))
  (is (rel-scheme=? (alist->rel-scheme [[:foo "bar"]
                                        [:fizz "buzz"]])
                    (alist->rel-scheme [[:foo "bar"]
                                        [:fizz "buzz"]]))))

(deftest rel-scheme-concat-test
  (let [a [["one" "one"] ["two" "two"]]
        b [["three" "three"] ["four" "four"]]
        a-scheme (alist->rel-scheme a)
        b-scheme (alist->rel-scheme b)
        ab-scheme (alist->rel-scheme (concat a b))]
    (testing "with either one of the inputs nil"
      ;; FIXME I guess this should rather be an assertion violation?
      (is (= a-scheme (rel-scheme-concat a-scheme nil)))
      (is (= a-scheme (rel-scheme-concat nil a-scheme))))
    (testing "with either one of the input alists empty"
      (is (= a-scheme (rel-scheme-concat (alist->rel-scheme []) a-scheme)))
      (is (= a-scheme (rel-scheme-concat a-scheme (alist->rel-scheme [])))))
    (testing "with both alists not empty"
      (let [res (rel-scheme-concat a-scheme b-scheme)]
        (is (= ab-scheme res))
        (is (= (into {} (concat a b)) (rel-scheme-map res)))))
    (testing "with groupings"
      (let [a-scheme-grouped (assoc a-scheme :grouped #{"one"})
            b-scheme-grouped (assoc b-scheme :grouped #{"three"})
            ab-scheme-grouped (assoc ab-scheme :grouped #{"one"})
            ba-scheme-grouped (rel-scheme-concat b-scheme a-scheme-grouped)
            ab-scheme-grouped-both (assoc (alist->rel-scheme (concat a b))
                                          :grouped #{"one" "three"})]
        (is (= ab-scheme-grouped (rel-scheme-concat a-scheme-grouped b-scheme)))
        (is (= ba-scheme-grouped (rel-scheme-concat b-scheme a-scheme-grouped)))
        (is (= ab-scheme-grouped-both (rel-scheme-concat a-scheme-grouped b-scheme-grouped)))))))

(deftest rel-scheme-difference-test
  (is (= (rel-scheme-difference test-scheme1 test-scheme3)
         (alist->rel-scheme [[:fizz :buzz]])))
  (is (= (rel-scheme-difference test-scheme2 test-scheme3)
         (alist->rel-scheme [[:some :thing]])))
  (is (= (rel-scheme-difference test-scheme1 the-empty-rel-scheme)
         test-scheme1))
  (is (= (rel-scheme-difference (alist->rel-scheme [[:k1 "k1"] [:k2 "k2"] [:k3 "k3"]])
                                (alist->rel-scheme [[:k2 "just"] [:k3 "the"] [:k4 "keys"]]))
         (alist->rel-scheme [[:k1 "k1"]])))
  (is (= (rel-scheme-difference (alist->rel-scheme [[:k2 "just"] [:k3 "the"] [:k4 "keys"]])
                                (alist->rel-scheme [[:k1 "k1"] [:k2 "k2"] [:k3 "k3"]]))
         (alist->rel-scheme [[:k4 "keys"]])))
  (is (thrown? Exception (rel-scheme-difference
                          (alist->rel-scheme [[:foo "some foo"]])
                          (alist->rel-scheme [[:foo :bar]])))))

(deftest rel-scheme-unary?-test
  (is (rel-scheme-unary? test-scheme4))
  (is (not (rel-scheme-unary? test-scheme1))))

(deftest rel-scheme-nullable-test
  (let [scheme (alist->rel-scheme [["one" string%]
                                   ["two" integer%]])
        scheme-nullable (alist->rel-scheme [["one" (make-nullable-type string%)]
                                            ["two" (make-nullable-type integer%)]])]
    (is (= scheme-nullable (rel-scheme-nullable scheme)))
    (testing "nil should throw an assertion"
      (is (thrown? Exception (rel-scheme-nullable nil))))
    (testing "should result in input if all types are nullable"
      (is (= scheme-nullable (rel-scheme-nullable scheme-nullable))))))

(deftest rel-scheme->environment-test
  (is (= {:fizz :buzz} (rel-scheme->environment test-scheme4)))
  (is (= {:foo :bar :some :thing} (rel-scheme->environment test-scheme2))))

(deftest compose-environments-test
  (is (= the-empty-environment (compose-environments the-empty-environment the-empty-environment)))
  (is (= {:foo :bar} (compose-environments {:foo :bar} the-empty-environment)))
  ;; e1 should take precedence over e2!
  (is (= {:foo :bar :fizz :buzz} (compose-environments {:foo :bar}
                                                       {:foo :something-else :fizz :buzz}))))

(deftest lookup-env-test
  (is (= :bar (lookup-env :foo {:foo :bar}))))

(deftest make-base-relation-test
  (let [test-universe (make-universe)]
    (is (= (really-make-base-relation :name :scheme nil)
           (make-base-relation :name :scheme)))
    (is (= (really-make-base-relation :name :scheme :some-handle)
           (make-base-relation :name :scheme :handle :some-handle)))
    (let [rel (make-base-relation "name"
                                  "scheme"
                                  :universe test-universe)]
      (is (= {"name" rel} (universe-get-base-relation-table test-universe)))
      (is (= (really-make-base-relation "name" "scheme" nil) rel)))
    (let [rel (make-base-relation "name"
                                  "scheme"
                                  :universe test-universe
                                  :handle "some handle")]
      (is (= {"name" rel} (universe-get-base-relation-table test-universe)))
      (is (= (really-make-base-relation "name" "scheme" "some handle") rel)))))

(deftest make-application-test
  (let [app (make-application + 1 2 3)]
    (is (= (application-rator app) +)
        (= (application-rands app) [1 2 3]))))

(deftest make-rator-test
  (let [test-universe (make-universe)]
    (is (empty? (universe-get-rator-table test-universe)))
    (let [rator
          (make-rator '+ (fn [fail t1 t2]
                           (when fail
                             (do (sql/check-numerical t1 fail)
                                 (sql/check-numerical t2 fail)))
                           t1)
                      +
                      :universe test-universe)]
      (is (= (universe-lookup-rator test-universe '+)
             rator)))))

(let [test-universe (make-universe)]
  (def tbl1 (make-base-relation 'tbl1
                                (alist->rel-scheme [["one" string%]
                                                    ["two" integer%]])
                                :universe test-universe
                                :handle "tbl1"))

  (def tbl2 (make-base-relation 'tbl2
                                (alist->rel-scheme [["one" integer%]
                                                    ["two" string%]])
                                :universe test-universe
                                :handle "tbl2"))
  (def tbl3 (make-base-relation 'tbl3
                                (alist->rel-scheme [["three" integer%]
                                                    ["four" string%]])
                                :universe test-universe
                                :handle "tbl3")))

(deftest make-project-test
  (let [p (make-project [["two" (make-attribute-ref "two")]
                         ["one" (make-attribute-ref "one")]]
                        tbl1)]
    (testing "with alist"
      (is (= [["two" (make-attribute-ref "two")]
              ["one" (make-attribute-ref "one")]]
             (project-alist p)))
      (is (= tbl1 (project-query p))))
    (testing "with map"
      (let [p1 (make-project {"two" (make-attribute-ref "two")
                              "one" (make-attribute-ref "one")} tbl1)]
        (is (= p p1))))
    (testing "with nested projects"
      (let [pp (make-project [] p)]
        (is (= (make-project [] tbl1) pp))
        (is (= tbl1 (project-query pp)))))))

(deftest make-extend-test
  (let [p (make-extend [["three" (make-const integer% 3)]
                        ["four" (make-const integer% 4)]]
                       tbl1)]
    (is (= [["three" (make-const integer% 3)]
            ["four" (make-const integer% 4)]
            ["one" (make-attribute-ref "one")]
            ["two" (make-attribute-ref "two")]]
         (project-alist p)))
    (is (= tbl1
           (project-query p))))
  (testing "make-extend only copies grouped attributes"
    (let [p (make-extend [["three" (make-const integer% 3)]]
                         (make-group #{"one"} tbl1))]
      (is (= [["three" (make-const integer% 3)]
              ["one" (make-attribute-ref "one")]]
             (project-alist p))))))

(deftest make-combine-test
  (let [p1 (make-project {"two" (make-attribute-ref "two")} tbl1)
        p2 (make-project {"one" (make-attribute-ref "one")} tbl1)
        c (make-combine :union p1 p2)
        c2 (make-combine :product p1 the-empty)
        c3 (make-combine :product p2 the-empty)]
    (testing "with valid relational operator"
      (is (= :union (combine-rel-op c)))
      (is (= p1 (combine-query-1 c)))
      (is (= p2 (combine-query-2 c))))
    (testing "with either one query empty it should return the non empty query"
      (is (= p1 c2))
      (is (= p2 c3)))
    (testing "with invalid relational operator it should throw an assertion"
      (is (thrown? Exception (make-combine :non-existent-operator p1 p2))))))

(deftest combinations-test
  (let [p1 (make-project {"two" (make-attribute-ref "two")} tbl1)
        p2 (make-project {"one" (make-attribute-ref "one")} tbl1)]
    (is (= (make-combine :product p1 p2) (make-product p1 p2)))
    (is (= (make-combine :union p1 p2) (make-union p1 p2)))
    (is (= (make-combine :intersection p1 p2) (make-intersection p1 p2)))
    (is (= (make-combine :difference p1 p2) (make-difference p1 p2)))
    (is (= (make-combine :quotient p1 p2) (make-quotient p1 p2)))
    (is (= (make-combine :quotient p1 the-empty) (make-quotient p1 the-empty)))))

(deftest order-op?-test
  (is (order-op? :ascending))
  (is (order-op? :descending))
  (is (not (order-op? :anything-else))))

(deftest aggregations-op?-test
  (is (reduce #(and %1 (aggregations-op? %2)) true
              [:count :count-all :sum :avg :min
               :max :std-dev :std-dev-p :var :var-p]))
  (is (not (aggregations-op? :anything-else))))

(deftest make-aggregation-test
  (testing "for regular aggregations"
    (let [aggr (make-aggregation :count (make-attribute-ref "one"))]
      (is (aggregation? aggr))
      (is (= :count (aggregation-operator aggr)))
      (is (= (make-attribute-ref "one") (aggregation-expr aggr)))))
  (testing "for aggregation*"
    (let [aggr (make-aggregation :count)]
      (is (aggregation*? aggr))
      (is (= :count (aggregation*-operator aggr)))))
  (testing "for an invalid number of aruguments it should throw an assertion"
    (is (thrown? Exception (make-aggregation :count
                                             (make-attribute-ref "one")
                                             (make-attribute-ref "one"))))))

(deftest expression-type-test
  (let [one-ref (make-attribute-ref "one")
        string-const (make-const string% "foobar")
        string-null (make-null string%)
        my-aggregation (make-aggregation
                        :count (make-tuple [(make-const integer% 40)
                                            (make-const integer% 2)]))]
    (testing "attribute-ref"
      (is (thrown? Exception (expression-type the-empty-environment one-ref)))
      (is (= string% (expression-type
                      (rel-scheme->environment (base-relation-scheme tbl1))
                      one-ref))))
    (testing "const and const-null"
      (is (= string% (expression-type the-empty-environment string-const)))
      (is (= string% (expression-type the-empty-environment string-null))))
    (testing "application"
      (is (= integer% (expression-type the-empty-environment
                                       (sql/plus$ (make-const integer% 1)
                                                  (make-const integer% 41)))))
      (is (= boolean% (expression-type the-empty-environment
                                       (sql/>=$ (make-const integer% 1)
                                                (make-const integer% 2)))))
      (is (thrown? Exception (expression-type
                              (rel-scheme->environment (base-relation-scheme tbl1))
                              (sql/>=$ (make-const integer% 1)
                                       (make-attribute-ref "one"))))))
    (testing "tuple"
      (is (= (make-product-type [integer% string%])
             (expression-type the-empty-environment
                              (make-tuple [(make-const integer% 42)
                                           (make-const string% "foobar")]))))
      (is (= (make-product-type [string%])
             (expression-type {"string" string%}
                              (make-tuple [(make-attribute-ref "string")]))))
      (is (not= (make-product-type [string%])
                (expression-type the-empty-environment
                                 (make-tuple [(make-const integer% 42)])))))
    (testing "aggregation"
      (is (= integer% (expression-type the-empty-environment my-aggregation)))
      (is (= date% (expression-type
                    {"one" date%}
                    (make-aggregation :min (make-attribute-ref "one")))))
      (testing "should fail with non matching operators and field references \\
                with typechecking set to true"
        (is (thrown? Exception  ;; With typechecking, this expression should fail.
                     (expression-type the-empty-environment
                                      (make-aggregation
                                       :min
                                       (make-tuple [(make-const integer% 42)
                                                    (make-const integer% 23)])))))
        (is (thrown? Exception (expression-type
                                {"one" string%}
                                (make-aggregation :sum (make-attribute-ref "one"))))))
      (testing "should fail with anything but :count-all"
        (is (thrown? Exception (expression-type the-empty-environment
                                                (make-aggregation :count-something))))))
    (testing "case expression"
      (let [my-case (make-case-expr {(sql/=$ (make-const integer% 42)
                                             (make-const integer% 42))
                                     (make-const boolean% true)}
                                    (make-const boolean% false))
            invalid-case (make-case-expr {(sql/plus$ (make-const integer% 42)
                                                  (make-const integer% 42))
                                          (make-const boolean% true)}
                                         (make-const boolean% false))]
        (is (= boolean% (expression-type the-empty my-case)))
        (is (thrown? Exception  ;; With typechecking, this should fail
                     (expression-type
                      the-empty-environment
                      (make-case-expr
                       {(sql/plus$ (make-const string% "foobar")
                                   (make-const integer% 42))
                        (make-const boolean% true)}
                       (make-const boolean% false)))))))))

(deftest aggregate?-test
  (is (not (aggregate? (make-attribute-ref "one"))))
  (is (not (aggregate? (make-const string% "foobar"))))
  (is (not (aggregate? (make-null string%))))
  (is (not (aggregate? (sql/=$ (make-const boolean% true) (make-const boolean% false)))))
  (is (aggregate? (sql/=$ (make-const boolean% false)
                          (make-aggregation :min (make-tuple [(make-const integer% 42)
                                                              (make-const integer% 23)])))))
  ;; tuple + app
  (is (aggregate? (sql/=$ (make-aggregation :min (make-tuple [(make-const integer% 42)
                                                              (make-const integer% 23)]))
                          (make-const boolean% false))))
  (is (aggregate? (make-tuple [(make-const integer% 42)
                               (make-aggregation :count (make-attribute-ref "one"))])))
  (is (not (aggregate? (make-case-expr
                        {(sql/plus$ (make-const string% "foobar")
                                    (make-const integer% 42))
                         (make-const boolean% true?)}
                        (make-const boolean% false)))))
  ;; case
  (is (aggregate? (make-case-expr
                   {(sql/plus$ (make-const string% "foobar")
                               (make-aggregation :min (make-tuple [(make-const integer% 42)
                                                                   (make-const integer% 23)])))
                    (make-const boolean% true?)}
                   (make-const boolean% false))))
  (testing "scalar subquery"
    (is (not (aggregate? (make-scalar-subquery (make-attribute-ref "one")))))
    ;; FIXME I think this too should count as an aggregate but the old schemeql2 code didn't treat it as one..
    (is (not (aggregate? (make-scalar-subquery (make-aggregation :count-all))))))
  (testing "set subquery"
    (let [s (make-set-subquery (make-project {"one" (make-attribute-ref "one")} tbl1))
          sa (make-set-subquery (make-project {"one" (make-aggregation :count (make-attribute-ref "one"))}
                                              tbl1))]
      ;; FIXME I think this too should count as an aggregate but the old schemeql2 code didn't treat it as one..
      (is (not (aggregate? s)))
      (is (not (aggregate? sa)))))
  (testing "everything else should throw an assertion"
    (is (thrown? Exception (aggregate? :invalid-input)))))

(deftest query-scheme-test
  (let [test-universe (make-universe)
        SUBB (make-base-relation 'SUBB
                                 (alist->rel-scheme [["C" string%]])
                                 :universe test-universe
                                 :handle "SUBB")
        SUBA (make-base-relation 'SUBA
                                 (alist->rel-scheme [["C" string%]])
                                 :universe test-universe
                                 :handle "SUBA")]
    (testing "empty val"
      (is (= the-empty-rel-scheme (query-scheme the-empty))))

    (testing "base relation"
      (is (= (alist->rel-scheme [["one" string%]
                                 ["two" integer%]])
             (query-scheme tbl1))))

    (testing "projection"
      (let [p (make-project [["two" (make-attribute-ref "two")]
                             ["one" (make-attribute-ref "one")]]
                            tbl1)
            res (query-scheme p)]
        (is (= (rel-scheme-map res) {"two" integer% "one" string%})))
      (let [p2 (make-project [["count_twos" (make-aggregation :count (make-attribute-ref "two"))]]
                             tbl1)]
        (is (= {"count_twos" integer%} (rel-scheme-map (query-scheme p2))))))
    (testing "aggregations and product-types should make it fail"
      (is (thrown? Exception
                   (query-scheme (make-project
                                  [["two" (make-attribute-ref "two")]
                                   ["one" (make-aggregation
                                           :min
                                           (make-tuple [(make-const integer% 42)
                                                        (make-const integer% 23)]))]]
                                  tbl1)))))

    (testing "restriction"
      (let [r (make-restrict (sql/=$ (make-scalar-subquery
                                      (make-project [["C" (make-attribute-ref "C")]]
                                                    SUBB))
                                     (make-attribute-ref "C"))
                             SUBA)]
        (is (= {"C" string%} (rel-scheme-map (query-scheme r)))))
      (testing "should fail with typecheck on and non-boolean applications")
      (is (thrown? Exception (query-scheme (make-restrict (sql/plus$ (make-const integer% 41)
                                                                     (make-const integer% 1))
                                                          tbl1)))))

    (testing "subquery"
      (let [r (make-project [["X" (make-scalar-subquery
                                   (make-restrict
                                    (sql/=$ (make-attribute-ref "D")
                                            (make-attribute-ref "C"))
                                    SUBB))]]
                            (make-project [["D" (make-attribute-ref "C")]]
                                          SUBA))]
        (is (= {"X" string%} (rel-scheme-map (query-scheme r))))))
        

    (testing "outer restriction"
      (let [r (make-restrict-outer (sql/=$ (make-scalar-subquery
                                            (make-project [["C" (make-attribute-ref "C")]]
                                                          SUBB))
                                           (make-attribute-ref "C"))
                                   SUBA)]
        (is (= {"C" string%} (rel-scheme-map (query-scheme r)))))
      (testing "should fail with typecheck on and non-boolean applications"
        (is (thrown? Exception (query-scheme (make-restrict-outer
                                              (sql/plus$ (make-const integer% 41)
                                                         (make-const integer% 1))
                                              tbl1))))))

    (testing "grouping"
      (is (= (lens/shove (alist->rel-scheme [["one" string%]
                                             ["two" integer%]])
                         rel-scheme-grouped-lens
                         #{"one"})
             (query-scheme (make-group #{"one"} tbl1))))
      (is (= (lens/shove (alist->rel-scheme [["one" string%]
                                             ["two" integer%]])
                         rel-scheme-grouped-lens
                         #{"one" "two"})
             (query-scheme (make-group #{"two"} (make-group #{"one"} tbl1)))))
      ;; Note: make-extend is used by query-comprehension/project,
      ;; which currently keeps un-aggregated and un-grouped column,
      ;; making SQL server unhappy; only 'one' and 'cnt' may be there in the end (maybe not here)
      (is (= (alist->rel-scheme [["one*" string%]
                                 ["cnt" integer%]
                                 ["one" string%]
                                 ["two" integer%]])
             (query-scheme  (make-extend [["one*" (make-attribute-ref "one")]
                                          ["cnt" (make-aggregation :count-all)]]
                                         (make-group #{"one" "two"} tbl1)))))
      (is (thrown?
           Exception
           (query-scheme (make-project [["x" (make-attribute-ref "one")]
                                        ["y" (make-aggregation :max
                                                               (make-attribute-ref "two"))]]
                                       tbl1))))

      (is (thrown?
           Exception
           (query-scheme (make-project [["x" (make-attribute-ref "one")]]
                                       (make-group #{"two"} tbl1))))))

    (testing "scheme for various combinations"
      (let [test-universe (make-universe)
            rel1 (make-base-relation 'tbl1
                                     (alist->rel-scheme [["one" string%]
                                                         ["two" integer%]])
                                     test-universe
                                     "tbl1")
            rel2 (make-base-relation 'tbl2
                                     (alist->rel-scheme [["three" boolean%]
                                                         ["four" double%]])
                                     test-universe
                                     "tbl2")
            c (make-product rel1 rel2)
            q (make-quotient rel1 rel2)
            l (make-left-outer-product rel1 rel2)]
        (testing "product"
          (is (= (rel-scheme-concat (base-relation-scheme rel1)
                                    (base-relation-scheme rel2))
                 (query-scheme c))))
        (testing "quotient"
          (is (= (rel-scheme-difference (base-relation-scheme rel1)
                                        (base-relation-scheme rel2))
                 (query-scheme q))))
        (testing "left outer product"
          (is (= (rel-scheme-concat (base-relation-scheme rel1)
                                    (rel-scheme-nullable (base-relation-scheme rel2)))
                 (query-scheme l))))))
    (testing "order"
      (let [o (make-order {(make-attribute-ref "one") :ascending} tbl1)]
        (is (= (rel-scheme-map (query-scheme o))
               {"one" string% "two" integer%}))
        (is (= (alist->rel-scheme [["one" date%]])
               (query-scheme (make-order {(make-attribute-ref "one") :ascending}
                                         (make-base-relation 'rel
                                                             (alist->rel-scheme [["one" date%]])
                                                             :universe (make-universe)
                                                             :handle "rel")))))))
    (testing "combine"
      (let [p (make-project {"one" (make-attribute-ref "one")} tbl1)
            p2 (make-project {"one" (make-attribute-ref "one")} tbl2)
            r (make-restrict (sql/=$ (make-attribute-ref "one")
                                     (make-const string% "foobar"))
                             tbl1)
            cp (make-product p p)
            clop (make-left-outer-product p p)
            cu (make-union p p2)]
        (is (thrown? Exception (query-scheme clop)))
        (is (thrown? Exception (query-scheme cp)))
        (is (thrown? Exception (query-scheme clop)))
        (is (thrown? Exception (query-scheme cu))))))
  (testing "anything else should fail"
    (is (thrown? Exception (query-scheme nil)))))

(deftest query?-test
  ;; everything else is basically the same...
  (is (query? the-empty))
  (is (query? tbl1))
  (is (query? (make-project [["one" (make-attribute-ref "one")]
                             ["two" (make-attribute-ref "two")]]
                            tbl1)))
  (is (not (query? [(make-attribute-ref 42)])))
  (is (not (query? [])))
  (is (not (query? nil))))

(deftest expression->datum-test
  (is (= (list 'attribute-ref "two") (expression->datum
                                      (make-attribute-ref "two"))))
  (is (= '(const string "foobar")
         (expression->datum
          (make-const string% "foobar"))))
  (is (= '(null-type string)
         (expression->datum
          (make-null string%))))
  (is (= '(application >= ((const integer 42) (const integer 23)))
         (expression->datum (sql/>=$ (make-const integer% 42)
                                     (make-const integer% 23)))))
  (is (= '(tuple (const string "foobar") (const integer 42))
         (expression->datum (make-tuple [(make-const string% "foobar")
                                         (make-const integer% 42)]))))
  (is (= '(aggregation :count
                       (tuple
                        (const integer 40)
                        (const integer 2)))
         (expression->datum (make-aggregation :count (make-tuple [(make-const integer% 40)
                                                                  (make-const integer% 2)]))))))

(deftest query->datum-test
  (is (= (list 'empty-query) (query->datum the-empty)))
  (is (= (list 'base-relation 'tbl1) (query->datum tbl1)))
  (is (= (list 'project (list (list "two" 'attribute-ref "two")
                              (list "one" 'attribute-ref "one"))
               '(base-relation tbl1))
         (query->datum (make-project [["two" (make-attribute-ref "two")]
                                      ["one" (make-attribute-ref "one")]]
                                     tbl1))))
  (testing "restrict"
    (let [test-universe (make-universe)
          SUBB (make-base-relation 'SUBB
                                   (alist->rel-scheme [["C" string%]])
                                   test-universe
                                   "SUBB")
          SUBA (make-base-relation 'SUBA
                                   (alist->rel-scheme [["C" string%]])
                                   test-universe
                                   "SUBA")
          r (make-restrict (sql/>=$ (make-scalar-subquery
                                     (make-project [["C" (make-attribute-ref "C")]]
                                                   SUBB))
                                    (make-attribute-ref "C"))
                           SUBA)]
      (is (= (list 'restrict
                   (list 'application
                         '>=
                         (list (list 'scalar-subquery
                                     (list 'project
                                           (list (list "C" 'attribute-ref "C"))
                                           (list 'base-relation 'SUBB)))
                               (list 'attribute-ref "C")))
                   (list 'base-relation 'SUBA))
             (query->datum r)))))

  (testing "restrict outer"
    (let [test-universe (make-universe)
          SUBB (make-base-relation 'SUBB
                                   (alist->rel-scheme [["B" string%]])
                                   test-universe
                                   "SUBB")
          SUBA (make-base-relation 'SUBA
                                   (alist->rel-scheme [["A" string%]])
                                   test-universe
                                   "SUBA")
          r (make-restrict-outer (sql/=$ (make-attribute-ref "A")
                                         (make-attribute-ref "B"))
                                 (make-product SUBB SUBA))]
      (is (= (list 'restrict-outer
                   (list 'application
                         '=
                         (list (list 'attribute-ref "A")
                               (list 'attribute-ref "B")))
                   (list :product (list 'base-relation 'SUBB) (list 'base-relation 'SUBA)))
             (query->datum r)))))
  (testing "group"
    (let [grp (make-group #{"one"} tbl1)]
      (is (= (list 'group (group-columns grp) (query->datum tbl1))
             (query->datum grp)))))
  (testing "every other input should fail"
    (is (thrown? Exception (query->datum :not-a-query)))))

(deftest datum->query-test
  (let [test-universe (register-base-relation! (make-universe)
                                               'tbl1 tbl1)
        query->datum->query #(-> % query->datum (datum->query test-universe))]
    (is (= the-empty (datum->query '(empty-query) test-universe)))
    (is (= tbl1 (datum->query '(base-relation tbl1) test-universe)))
    (is (thrown? Exception  ;; Should throw because universe does not contain
                 ;; the relation.
                 (datum->query '(base-relation tbl1) (make-universe))))
    (let [p (make-project [["two" (make-attribute-ref "two")]
                           ["one" (make-attribute-ref "one")]]
                          tbl1)]
      (is (= p (query->datum->query p)))
      (is (thrown? Exception  ;; Should throw because tbl1 is not registered in
                   ;; universe.
                   (datum->query (query->datum p) (make-universe)))))
    (let [sql-universe* (make-derived-universe sql-universe)
          SUBB (make-base-relation 'SUBB
                                   (alist->rel-scheme [["C" string%]])
                                   :universe sql-universe*
                                   :handle "SUBB")
          SUBA (make-base-relation 'SUBA
                                   (alist->rel-scheme [["C" string%]])
                                   :universe sql-universe*
                                   :handle "SUBA")
          r (make-restrict (sql/=$ (make-scalar-subquery
                                    (make-project [["C" (make-attribute-ref "C")]]
                                                  SUBB))
                                   (make-attribute-ref "C"))
                           SUBA)]
      (is (= (make-restrict
              (make-application
               (universe-lookup-rator sql-universe* '=)
               (make-scalar-subquery (make-project [["C" (make-attribute-ref "C")]]
                                                   SUBB))
               (make-attribute-ref "C"))
              SUBA)
             (datum->query (query->datum r) sql-universe*)))
      (is (thrown? Exception  ;; Should throw because of unregistered
                              ;; relations.
                   (datum->query (query->datum r) sql-universe))))

    (let [sql-universe* (make-derived-universe sql-universe)
          SUBB (make-base-relation 'SUBB
                                   (alist->rel-scheme [["B" string%]])
                                   :universe sql-universe*
                                   :handle "SUBB")
          SUBA (make-base-relation 'SUBA
                                   (alist->rel-scheme [["A" string%]])
                                   :universe sql-universe*
                                   :handle "SUBA")
          r (make-restrict-outer (sql/=$ (make-attribute-ref "A")
                                         (make-attribute-ref "B"))
                                 (make-left-outer-product SUBB SUBA))]
      (is (= (make-restrict-outer
              (make-application
               (universe-lookup-rator sql-universe* '=)
               (make-attribute-ref "A")
               (make-attribute-ref "B"))
              (make-left-outer-product SUBB SUBA))
             (datum->query (query->datum r) sql-universe*))))
      
    (let [rel1 (make-base-relation 'tbl1
                                   (alist->rel-scheme [["one" string%]
                                                       ["two" integer%]])
                                   :universe test-universe
                                   :handle "tbl1")
          rel2 (make-base-relation 'tbl2
                                   (alist->rel-scheme [["three" boolean%]
                                                       ["four" double%]])
                                   :universe test-universe
                                   :handle "tbl2")
          p (make-product rel1 rel2)
          q (make-quotient rel1 rel2)
          u (make-union rel1 rel2)
          l (make-left-outer-product rel1 rel2)]
      (is (= p (query->datum->query p)))
      (is (= q (query->datum->query q)))
      (is (= u (query->datum->query u)))
      (is (= l (query->datum->query l))))
    (let [o (make-order [[(make-attribute-ref "one") :ascending]] tbl1)]
      (is (= o (query->datum->query o))))
    (let [t (make-top nil 10 tbl1)]
      (is (= t (query->datum->query t))))
    (let [t (make-top 5 10 tbl1)]
      (is (= t (query->datum->query t)))))
  (testing "every other input should fail"
    (is (thrown? Exception
                 (datum->query '(:invalid-argument) (make-universe))))))

(deftest datum->expression-test
  (let [test-universe (make-universe)
        expression->datum->expression #(-> %
                                           expression->datum
                                           (datum->expression test-universe))]
    (is (= (expression->datum->expression (make-attribute-ref "two"))
           (make-attribute-ref "two")))
    (is (= (expression->datum->expression (make-const string% "foobar"))
           (make-const string% "foobar")))
    (is (= (expression->datum->expression (make-null string%))
           (make-null string%)))
    (testing "application"
      (let [a (make-application (make-rator '+
                                            (fn [fail t1 t2]
                                              (when fail
                                                (do
                                                  (sql/check-numerical t1 fail)
                                                  (sql/check-numerical t2 fail)))
                                              t1)
                                            +
                                            :universe sql-universe
                                            :data op-+)
                                (make-const integer% 40)
                                (make-const integer% 2))]
        (is (= (datum->expression (expression->datum a) sql-universe)
               a)))
      (testing "application with unknown operator should fail"
        (is (thrown? Exception (datum->expression
                                (list 'application 'quux 2 2)
                                sql-universe)))))

    (testing "tuple"
      (is (= (expression->datum->expression (make-tuple [(make-const integer% 40)
                                                         (make-const integer% 2)]))
             (make-tuple [(make-const integer% 40)
                          (make-const integer% 2)]))))
    (testing "aggregation"
      (is (= (expression->datum->expression (make-aggregation
                                             :count (make-tuple [(make-const integer% 40)
                                                                 (make-const integer% 2)])))
             (make-aggregation
              :count (make-tuple [(make-const integer% 40)
                                  (make-const integer% 2)])))))
    (testing "aggregation*"
      (is (= (make-aggregation :count-all)
             (datum->expression (list 'aggregation* :count-all)
                                sql-universe))))
    (testing "case expression"
      (is (= (datum->expression (expression->datum
                                 (make-case-expr {(sql/=$ (make-const integer% 42)
                                                          (make-const integer% 42))
                                                  (make-const boolean% true)}
                                                 (make-const boolean% false)))
                                sql-universe)
             (make-case-expr {(make-application
                               (universe-lookup-rator sql-universe '=)
                               (make-const integer% 42)
                               (make-const integer% 42))
                              (make-const boolean% true)}
                             (make-const boolean% false))))))
  (testing "everything else should fail"
    (is (thrown? Exception (datum->expression (list 'doesnotexists) sql-universe)))))

(deftest expression-attribute-names-test
  (is (= #{"two"} (expression-attribute-names (make-attribute-ref "two"))))
  (is (= nil (expression-attribute-names (make-const string% "foobar"))))
  (is (= nil (expression-attribute-names (make-null string%))))
  (is (= nil
         (expression-attribute-names (sql/plus$ (make-const integer% 40)
                                                (make-const integer% 2)))))
  (is (= nil
         (expression-attribute-names (sql/plus$ (make-const integer% 21)
                                                (make-const integer% 21)))))
  ;; I'm not sure this is right...
  (is (= nil
         (expression-attribute-names (make-tuple [(make-const integer% 40)
                                                  (make-const integer% 2)]))))
  (is (= #{"two"}
         (expression-attribute-names (make-tuple [(make-const integer% 40)
                                                  (make-attribute-ref "two")]))))
  (is (= nil
         (expression-attribute-names (make-aggregation
                                      :count (make-tuple [(make-const integer% 40)
                                                          (make-const integer% 2)])))))
  (is (= #{"two"}
         (expression-attribute-names (make-aggregation
                                      :count (make-tuple [(make-const integer% 40)
                                                          (make-attribute-ref "two")])))))
  (is (= nil
         (expression-attribute-names (make-case-expr {(sql/=$ (make-const integer% 42)
                                                              (make-const integer% 42))
                                                      (make-const boolean% true)}
                                                     (make-const boolean% false)))))
  (is (= #{"three" "two"}
         (expression-attribute-names (make-case-expr {(sql/=$ (make-attribute-ref "two")
                                                              (make-const integer% 42))
                                                      (make-const boolean% true)}
                                                     (make-attribute-ref "three"))))))

(deftest query-attribute-names-test
  (is (nil? (query-attribute-names the-empty)))
  (testing "base-relation"
    (is (nil? (query-attribute-names tbl1))))
  (testing "project"
    (is (= #{"two" "one"} (query-attribute-names
                           (make-project [["two" (make-attribute-ref "two")]
                                          ["one" (make-attribute-ref "one")]]
                                         tbl1)))))
  (testing "restrict"
    (let [test-universe (make-universe)
          SUBB (make-base-relation 'SUBB
                                   (alist->rel-scheme [["B" string%]])
                                   :universe test-universe
                                   :handle "SUBB")
          SUBA (make-base-relation 'SUBA
                                   (alist->rel-scheme [["A" string%]])
                                   :universe test-universe
                                   :handle "SUBA")
          SUBC (make-base-relation 'SUBC
                                   (alist->rel-scheme [["C" string%]
                                                       ["D" string%]])
                                   :universe test-universe
                                   :handle "SUBC")
          r1 (make-restrict (sql/=$ (make-scalar-subquery
                                     (make-project [["C" (make-attribute-ref "C")]
                                                    ["D" (make-attribute-ref "D")]]
                                                   SUBC))
                                    (make-attribute-ref "A"))
                            SUBA)
          r2 (make-restrict (sql/=$ (make-attribute-ref "A")
                                    (make-attribute-ref "B"))
                            (make-left-outer-product SUBB SUBA))
          r3 (make-restrict-outer (sql/=$ (make-attribute-ref "A")
                                          (make-attribute-ref "B"))
                                  (make-left-outer-product SUBB SUBA))]
      (is (= #{"C" "D" "A"} (query-attribute-names r1)))
      (is (= #{"A" "B"} (query-attribute-names r2)))
      (is (= #{"A" "B"} (query-attribute-names r3)))))
  (let [test-universe (make-universe)
        rel1 (make-base-relation 'tbl1
                                 (alist->rel-scheme [["one" string%]
                                                     ["two" integer%]])
                                 test-universe
                                 "tbl1")
        rel2 (make-base-relation 'tbl2
                                 (alist->rel-scheme [["three" boolean%]
                                                     ["four" double%]])
                                 test-universe
                                 "tbl2")
        p1 (make-project [["two" (make-attribute-ref "two")]
                          ["one" (make-attribute-ref "one")]]
                         tbl1)
        p2 (make-project [["three" (make-attribute-ref "three")]
                          ["four" (make-attribute-ref "four")]]
                         tbl3)
        c (make-product rel1 p1)
        q (make-quotient p1 rel1)
        u (make-union p1 p2)
        l (make-left-outer-product rel1 p1)]
    (is (= #{"two" "one"} (query-attribute-names c)))
    (is (= #{"two" "one"} (query-attribute-names q)))
    (is (= #{"two" "one" "three" "four"} (query-attribute-names u)))
    (is (= #{"two" "one"} (query-attribute-names l))))
  (testing "group"
    (is (= #{"one" "two"} (query-attribute-names
                           (make-order {(make-attribute-ref "one") :ascending}
                                       tbl1)))))
  (testing "top"
    (is (nil? (query-attribute-names (make-top 5 10 tbl1)))))
  (testing "group"
    (is (nil? (query-attribute-names (make-group #{} tbl1))))
    (is (= #{"one" "two"}
           (query-attribute-names
            (make-group #{"one"}
                        (make-project
                         {"one" (make-attribute-ref "one")} tbl1))))))
  (testing "everything else should fail"
    (is (thrown? Exception (query-attribute-names nil)))))

(deftest substitute-attribute-refs-test
  (is (= (make-const string% "foobar")
         (substitute-attribute-refs {"two" (make-const string% "foobar")}
                                    (make-attribute-ref "two"))))
  (is (= (make-attribute-ref "one")
         (substitute-attribute-refs {} (make-attribute-ref "one"))))
  (is (= (make-const string% "foobar")
         (substitute-attribute-refs {} (make-const string% "foobar"))))
  (is (= (make-null string%)
         (substitute-attribute-refs {} (make-null string%))))
  (let [a (make-application (make-rator '+
                                        (fn [fail t1 t2]
                                          (when fail
                                            (do
                                              (sql/check-numerical t1 fail)
                                              (sql/check-numerical t2 fail)))
                                          t1)
                                        +
                                        :universe sql-universe
                                        :data op-+)
                            (make-attribute-ref "fourtytwo")
                            (make-const integer% 2))
        res (make-application (universe-lookup-rator sql-universe '+)
                              (make-const integer% 42)
                              (make-const integer% 2))]
    (is (= res (substitute-attribute-refs
                {"fourtytwo" (make-const integer% 42)} a))))
  (is (= (make-tuple [(make-const integer% 42)
                      (make-attribute-ref "three")
                      (make-const string% "foobar")])
         (substitute-attribute-refs {"two" (make-const integer% 42)}
                                    (make-tuple [(make-attribute-ref "two")
                                                 (make-attribute-ref "three")
                                                 (make-const string% "foobar")]))))
  (is (= (make-aggregation
          :count (make-tuple [(make-const integer% 40)
                              (make-const integer% 2)]))
         (substitute-attribute-refs
          {"fourty" (make-const integer% 40)}
          (make-aggregation
           :count (make-tuple [(make-attribute-ref "fourty")
                               (make-const integer% 2)])))))
  (is (= (make-case-expr {(make-application
                           (universe-lookup-rator sql-universe '=)
                           (make-const integer% 42)
                           (make-const integer% 42))
                          (make-const boolean% true)}
                         (make-const boolean% true))
         (substitute-attribute-refs
          {"true" (make-const boolean% true)
           "fourtytwo" (make-const integer% 42)}
          (make-case-expr {(make-application
                            (universe-lookup-rator sql-universe '=)
                            (make-attribute-ref "fourtytwo")
                            (make-const integer% 42))
                           (make-attribute-ref "true")}
                          (make-attribute-ref "true"))))))

(deftest cull-substitution-alist-test
  (is (empty? (cull-substitution-alist
               {"one" (make-attribute-ref "one")}
               tbl1)))
  (is (= {"three" (make-attribute-ref "three")}
         (cull-substitution-alist {"three" (make-attribute-ref "three")} tbl1))))

(deftest query-substitute-attribute-refs-test
  (testing "the empty"
    (is (= the-empty (query-substitute-attribute-refs {} the-empty))))
  (testing "base-relation"
    (is (= tbl1 (query-substitute-attribute-refs {} tbl1)))))

(deftest count-aggregations-test
  (let [aggr (make-aggregation
              :count (make-tuple [(make-const integer% 40)
                                  (make-const integer% 2)]))]
    (is (= 0 (count-aggregations (make-attribute-ref "foobar"))))
    (is (= 0 (count-aggregations (make-const string% "foobar"))))
    (is (= 0 (count-aggregations (make-null string%))))
    ;; application, tuple, aggregation
    (let [app1 (sql/plus$ (make-const integer% 42)
                          (make-const integer% 23))
          app2 (sql/plus$ aggr
                          aggr)
          app3 (sql/plus$ aggr
                          (make-aggregation
                           :count (make-tuple [(make-const integer% 40)
                                               (sql/plus$ aggr
                                                          aggr)])))]
      (is (= 0 (count-aggregations app1)))
      (is (= 2 (count-aggregations app2)))
      (is (= 4 (count-aggregations app3))))
    (let [c1 (make-case-expr {(sql/=$ (make-const integer% 42)
                                      (make-const integer% 42))
                              (make-const boolean% true)}
                             (make-const boolean% false))
          c2 (make-case-expr {(sql/=$ (make-const integer% 42)
                                      (make-const integer% 42))
                              (make-const boolean% true)}
                             aggr)]
      (is (= 0 (count-aggregations c1)))
      (is (= 1 (count-aggregations c2))))))
