(ns sqlosure.optimization-test
  (:require [sqlosure.optimization :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.type :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(deftest project-alist-substitute-attribute-refs-test
  (is (= [["one" (make-const integer% 42)]]
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  [["one" (make-attribute-ref "one")]])))
  (is (= [["one" (make-const integer% 42)]
          ["two" (make-attribute-ref "two")]]
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  [["one" (make-attribute-ref "one")]
                                                   ["two" (make-attribute-ref "two")]]))))

(deftest order-alist-attribute-names-test
  (let [alist [[(make-attribute-ref "one") :ascending]
               [(make-attribute-ref "two") :descending]]]
    (is (= #{"one" "two"}
           (order-alist-attribute-names alist)))
    (is (empty?
           (order-alist-attribute-names nil)))))

(deftest intersect-live-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (alist->rel-scheme [["one" string%]
                                                     ["two" integer%]])
                                 :handle "tbl1")
        p (make-project [["one" (make-attribute-ref "one")]
                         ["two" (make-attribute-ref "two")]]
                        tbl1)]
    (is (= #{"one"}
           (intersect-live #{"one"} p)))
    (is (= #{"two" "one"}
           (intersect-live #{"one" "two"} p)))))

(deftest remove-dead-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (alist->rel-scheme [["one" string%]
                                                     ["two" integer%]
                                                     ["three" double%]])
                                 :handle "tbl1")
        p1 (make-project [["one" (make-attribute-ref "one")]
                          ["two" (make-attribute-ref "two")]]
                         tbl1)
        p2 (make-project [["one" (make-attribute-ref "one")]]
                         (make-project [["one" (make-attribute-ref "one")]
                                        ["two" (make-attribute-ref "two")]]
                                       tbl1))]
    (testing "empty-val"
      (is (= (make-empty-val) (remove-dead (make-empty-val)))))
    (testing "base-relation"
      (is (= tbl1 (remove-dead tbl1))))
    (testing "project"
      (is (= p1 (remove-dead p1)))
      (is (= (alist->rel-scheme {"one" string%})
             (query-scheme (remove-dead p2)))))
    (testing "restrict"
      (let [r1 (make-restrict (=$ (make-attribute-ref "one")
                                  (make-const string% "foobar"))
                              tbl1)
            r2 (make-restrict (=$ (make-attribute-ref "one")
                                  (make-const string% "foobar"))
                              p2)]
        (is (= r1 (remove-dead r1)))
        (testing "removes dead projection from underlying project"
          (is (= (alist->rel-scheme {"one" string%})
                 (-> r2 remove-dead restrict-query project-query query-scheme))))))
    (testing "restrict-outer"
      (let [r1 (make-restrict-outer (=$ (make-attribute-ref "one")
                                        (make-const string% "foobar"))
                                    tbl1)
            r2 (make-restrict-outer (=$ (make-attribute-ref "one")
                                        (make-const string% "foobar"))
                                    p2)]
        (is (= r1 (remove-dead r1))
            (= (alist->rel-scheme {"one" string%})
               (-> r2 remove-dead restrict-outer-query project-query query-scheme)))))
    (testing "order"
      (let [o1 (make-order [[(make-attribute-ref "one") :ascending]]
                           tbl1)
            o2 (make-order [[(make-attribute-ref "one") :ascending]]
                           p2)]
        (is (= o1 (remove-dead o1)))
        (is (= (alist->rel-scheme {"one" string%})
               (-> o2 remove-dead order-query project-query query-scheme)))))
    (testing "group"
      (let [g1 (make-group #{"one"} tbl1)
            g2 (make-group #{"one"} p2)]
        (is (= g1 (remove-dead g1)))
        (is (= (alist->rel-scheme {"one" string%})
               (-> g2 remove-dead group-query project-query query-scheme)))))
    (testing "top"
      (let [t1 (make-top 0 1 tbl1)
            t2 (make-top 0 1 p2)]
        (is (= t1 (remove-dead t1)))
        (is (= (alist->rel-scheme {"one" string%})
               (-> t2 remove-dead top-query project-query query-scheme)))))))

(let [tbl1 (make-base-relation "tbl1"
               (-> r2 remove-dead restrict-outer-query project-query query-scheme)))))))
                 (-> r2 remove-dead restrict-query project-query query-scheme))))))))

(deftest merge-project-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (alist->rel-scheme [["one" string%]
                                                     ["two" integer%]
                                                     ["three" double%]])
                                 :handle "tbl1")]
    (is (= the-empty (merge-project the-empty))))

  (testing "optimization should not merge aggregates"
    (let [expr (let [tbl1 (make-base-relation "tbl1"
                                              (alist->rel-scheme [["one" string%]
                                                                  ["two" integer%]])
                                              :handle "tbl1")]
                 (make-project [["m" (make-aggregation :max (make-attribute-ref "c"))]]
                               (make-project [["c" (make-aggregation :count-all)]]
                                             (make-group #{"one"}
                                                         tbl1))))]
      (is (= expr (merge-project expr))))))

