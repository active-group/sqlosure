(ns sqlosure.optimization-test
  (:require [active.clojure.monad :as m :refer [monadic]]
            [sqlosure.optimization :as opt :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.type :as t :refer :all]
            [sqlosure.query-comprehension :as q]
            [sqlosure.relational-algebra :as rel :refer :all]
            [sqlosure.universe :refer [make-universe]]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [sqlosure.sql :as sql]))

(let [u (make-universe)]
  (def tbl1 (sql/base-relation 'tbl1
                               (alist->rel-scheme [["one" string%]
                                                   ["two" integer%]])
                               :univese u))
  (def tbl2 (sql/base-relation 'tbl2
                               (alist->rel-scheme [["three" blob%]
                                                   ["four" string%]])
                               :universe u)))

(deftest project-alist-substitute-attribute-refs-test
  (is (= [["one" (make-const integer% 42)]]
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  [["one" (make-attribute-ref "one")]])))
  (is (= [["one" (make-const integer% 42)]
          ["two" (make-attribute-ref "two")]]
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  [["one" (make-attribute-ref "one")]
                                                   ["two" (make-attribute-ref "two")]]))))

(deftest query->alist-test
  (let [p (make-project {"one" (make-attribute-ref "one")} tbl1)
        p2 (make-project {"one" (make-attribute-ref "one")}
                         (make-project {"one" (make-attribute-ref "one")
                                        "two" (make-attribute-ref "two")}
                                       tbl1))
        p3 (make-project {"one" (make-attribute-ref "one")
                          "two" (make-attribute-ref "two")}
                         (make-project {"one" (make-attribute-ref "one")
                                        "two" (make-attribute-ref "two")}
                                       tbl1))]
    (is (= {"one" string%} (query->alist p)))
    (is (= {"one" string%} (query->alist p2)))
    (is (= {"one" string% "two" integer%} (query->alist p3)))))

(deftest remove-dead-test
  (let [tbl1         (make-base-relation "tbl1"
                                         (alist->rel-scheme [["one" string%]
                                                             ["two" integer%]
                                                             ["three" double%]])
                                         :handle "tbl1")
        p1           (make-project [["one" (make-attribute-ref "one")]
                                    ["two" (make-attribute-ref "two")]]
                                   tbl1)
        p2           (make-project [["one" (make-attribute-ref "one")]]
                                   (make-project [["one" (make-attribute-ref "one")]
                                                  ["two" (make-attribute-ref "two")]]
                                                 tbl1))
        p2-optimized (make-project [["one" (make-attribute-ref "one")]]
                                   (make-project [["one" (make-attribute-ref "one")]]
                                                 tbl1))
        o1           (make-order [[(make-attribute-ref "one") :ascending]]
                                 tbl1)]
    (testing "empty-query"
      (is (= (make-empty-query) (remove-dead the-empty))))
    (testing "base-relation"
      (is (= tbl1 (remove-dead tbl1))))
    (testing "project"
      (is (= p1 (remove-dead p1)))
      (is (= p2-optimized
             (remove-dead p2))))
    (testing "restrict"
      (let [r1 (make-restrict (=$ (make-attribute-ref "one")
                                  (make-const string% "foobar"))
                              tbl1)
            r2   (make-restrict (=$ (make-attribute-ref "one")
                                    (make-const string% "foobar"))
                                p2)]
        (is (= r1 (remove-dead r1)))
        (testing "removes dead projection from underlying project"
          (is (= (make-restrict (=$ (make-attribute-ref "one")
                                    (make-const string% "foobar"))
                                p2-optimized)
                 (opt/remove-dead r2))))))
    (testing "restrict-outer"
      (let [r1 (make-restrict-outer (=$ (make-attribute-ref "one")
                                        (make-const string% "foobar"))
                                    tbl1)
            r2 (make-restrict-outer (=$ (make-attribute-ref "one")
                                        (make-const string% "foobar"))
                                    p2)]
        (is (= r1 (remove-dead r1)))
        (is (= (make-restrict-outer (=$ (make-attribute-ref "one")
                                        (make-const string% "foobar"))
                                    p2-optimized)
               (opt/remove-dead r2))) ))
    (testing "order"
        (let [o2 (make-order [[(make-attribute-ref "one") :ascending]] p2)]
        (is (= o1 (remove-dead o1)))
        (is (= (rel/make-order [[(make-attribute-ref "one") :ascending]] p2-optimized)
               (opt/remove-dead o2)))))
    (testing "group"
        (let [g1 (make-group #{"one"} tbl1)
              g2 (make-group #{"one"} p2)]
        (is (= g1 (remove-dead g1)))
        (is (= (rel/make-group #{"one"} p2-optimized)
               (opt/remove-dead g2)))))
    (testing "top"
        (let [t1 (make-top 0 1 tbl1)
              t2 (make-top 0 1 p2)]
        (is (= t1 (remove-dead t1)))
        (is (= (rel/make-top 0 1 p2-optimized)
               (opt/remove-dead t2)))))
    (testing "combine"
      (testing ":product"
        (let [o2 (make-project [["three" (make-attribute-ref "one")]] o1)
              c1 (make-combine :product p1 o2)
              c2 (make-combine :product p2 o2)]
          (is (= c1 (remove-dead c1)))
          (is (= (rel/make-product p2-optimized o2)
                 (opt/remove-dead c2)))))
      (testing ":quotient"
        (let [c1 (make-combine :quotient o1 p1)
              c2 (make-combine :quotient o1 p2)]
          (is (= c1 (remove-dead c1)))
          (is (= (alist->rel-scheme {"one" string%})
                 (-> c2 remove-dead combine-query-2 query-scheme)))))
      (testing ":difference"
        (let [c1 (make-difference (make-project [["one" (make-attribute-ref "one")] ["two" (make-attribute-ref "two")]] o1) p1)
              c2 (make-difference (make-project [["one" (make-attribute-ref "one")]] o1) p2)]
          (is (= c1 (remove-dead c1)))
          (is (= (alist->rel-scheme {"one" string%})
                 (-> c2 remove-dead combine-query-2 query-scheme)))))
      (testing "everything else should fail"
        (is (thrown? Exception (remove-dead nil)))))))

(deftest merge-project-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (alist->rel-scheme [["one" string%]
                                                     ["two" integer%]
                                                     ["three" double%]])
                                 :handle "tbl1")
        p1 (make-project [["one" (make-attribute-ref "one")]
                          ["two" (make-attribute-ref "two")]]
                         tbl1)
        p2 (make-project [["one" (make-attribute-ref "one")]] p1)
        p3 (make-project [["three" (make-attribute-ref "one")]
                          ["four" (make-attribute-ref "two")]]
                         p1)
        r1 (make-restrict (sql/=$ (make-attribute-ref "one")
                                  (make-const string% "foo"))
                          p1)]
    (testing "empty-queryue"
      (is (= (make-empty-query) (merge-project the-empty))))
    (testing "base-relation"
      (is (= tbl1 (merge-project tbl1))))
    (testing "project"
      (testing "with underlying project"
        (is (= p1 (merge-project p1)))
        (is (= (make-project [["one" (make-attribute-ref "one")]] tbl1)
               (merge-project p2)))
        (is (= (make-project [["three" (make-attribute-ref "one")]
                              ["four" (make-attribute-ref "two")]] tbl1)
               (merge-project p3))))
      (testing "with underlying combine"
        (let [c1 (make-project {"one" (make-attribute-ref "one")}
                               (make-product p1 p3))
              c2 (make-project {"one" (make-attribute-ref "one")}
                               (make-union p1 r1))]
          (is (= (make-project (project-alist c1)
                               (merge-project (project-query c1)))
                 (merge-project c1)))
          (is (= (make-project (project-alist c2)
                               (merge-project (project-query c2)))
                 (merge-project c2))))))
    (testing "restrict-outer"
      (let [ro1 (make-restrict-outer (sql/=$ (make-attribute-ref "one")
                                             (make-const string% "foobar"))
                                     tbl1)
            ro2 (make-restrict-outer (sql/=$ (make-attribute-ref "three")
                                             (make-const string% "foobar"))
                                     p3)]
        (is (= ro1 (merge-project ro1)))
        (is (= (make-restrict-outer (sql/=$ (make-attribute-ref "three")
                                            (make-const string% "foobar"))
                                    (merge-project p3))
               (merge-project ro2)))))
    (testing "order"
      (let [o1 (make-order {(make-attribute-ref "one") :ascending}
                           tbl1)
            o2 (make-order {(make-attribute-ref "three") :descending}
                           p3)]
        (is (= o1 (merge-project o1)))
        (is (= (make-order {(make-attribute-ref "three") :descending}
                           (merge-project p3))
               (merge-project o2)))))
    (testing "optimization should not merge aggregates"
      (let [expr (let [tbl1 (make-base-relation "tbl1"
                                                (alist->rel-scheme [["one" string%]
                                                                    ["two" integer%]])
                                                :handle "tbl1")]
                   (make-project [["m" (make-aggregation :max (make-attribute-ref "c"))]]
                                 (make-project [["c" (make-aggregation :count-all)]]
                                               (make-group #{"one"}
                                                           tbl1))))]
        (is (= expr (merge-project expr)))))
    (testing "evereything else should fail"
      (is (thrown? Exception (merge-project nil)))))
  (testing "shouldn't zap project over group"
    (let [c (make-project [["all" (make-aggregation :count-all)]]
                          (make-project []
                                        (make-group #{"one"} tbl1)))]
      (is (= c (merge-project c)))))
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

(deftest push-restrict-test
  (let [r1 (make-restrict (=$ (make-attribute-ref "one")
                              (make-const string% "foobar")) tbl1)
        p1 (make-project {"one" (make-attribute-ref "one")
                          "two" (make-attribute-ref "two")} tbl1)
        p2 (make-project {"one" (make-attribute-ref "one")} r1)
        r2 (make-restrict (=$ (make-attribute-ref "one")
                              (make-const string% "foobar")) p2)
        r* (fn [q] (make-restrict (=$ (make-attribute-ref "one")
                                      (make-const string% "foobar")) q))
        ro* (fn [q] (make-restrict-outer (=$ (make-attribute-ref "one")
                                             (make-const string% "foobar")) q))
        c1 (make-union p1 p2)
        c2 (make-difference p1 r1)
        r3 (r* c1)
        r4 (r* c2)
        r5 (r* (make-union (make-project {"two" (make-attribute-ref "two")}
                                         tbl1)
                           p2))
        r6 (r* (make-union p2
                           (make-project {"two" (make-attribute-ref "two")}
                                         tbl1)))
        r7 (r* (r* p1))
        ro1 (make-restrict-outer (=$ (make-attribute-ref "one")
                                     (make-const string% "foobar"))
                                 tbl1)
        ro2 (make-restrict-outer (=$ (make-attribute-ref "one")
                                     (make-const string% "foobar"))
                                 ro1)
        o1 (make-order {(make-attribute-ref "one") :ascending} tbl1)
        g1 (make-group #{"one"} tbl1)]
    (testing "empty val"
      (is (= the-empty (push-restrict the-empty))))
    (testing "base-relation"
      (is (= tbl1 (push-restrict tbl1))))
    (testing "project"
      (is (= p1 (push-restrict p1)))
      (is (= p2 (push-restrict p2))))
    (testing "restrict"
      (is (= r1 (push-restrict r1)))
      (is (= (make-project {"one" (make-attribute-ref "one")}
                           (make-restrict (=$ (make-attribute-ref "one")
                                              (make-const string% "foobar"))
                                          r1))
             (push-restrict r2)))
      (testing "with underlying restrict"
        (is (= (make-project (project-alist p1)
                             (make-restrict (=$ (make-attribute-ref "one")
                                                (make-const string% "foobar"))
                                            (make-restrict (=$ (make-attribute-ref "one")
                                                               (make-const string% "foobar"))
                                                           tbl1)))
               (push-restrict r7))))
      (testing "with underlying restrict-outer"
        (is (= (make-restrict (=$ (make-attribute-ref "one") (make-const string% "foobar"))
                              (push-restrict ro2))
               (push-restrict (r* ro2))))
        (is (= (push-restrict (make-restrict (=$ (make-attribute-ref "one")
                                                 (make-const string% "foobar"))
                                             (push-restrict ro1)))
               (push-restrict (r* ro1)))))
      (testing "with underlying order"
        (is (= (make-order (order-alist o1)
                           (make-restrict (=$ (make-attribute-ref "one")
                                              (make-const string% "foobar"))
                                          (order-query o1)))
               (push-restrict (r* o1)))))
      (testing "with underlying group"
        (is (= (make-group #{"one"}
                           (make-restrict (=$ (make-attribute-ref "one")
                                              (make-const string% "foobar"))
                                          tbl1))
               (push-restrict (r* g1)))))
      (testing "with underlying combine"
        (testing ":union"
          (is (= r3 (push-restrict r3)))
          (is (= (make-union
                  (make-project {"two" (make-attribute-ref "two")} tbl1)
                  (push-restrict (make-restrict (restrict-exp r5)
                                                (push-restrict p2))))
                 (push-restrict r5)))
          (is (= (make-union
                  (push-restrict (make-restrict (restrict-exp r5)
                                                (push-restrict p2)))
                  (make-project {"two" (make-attribute-ref "two")} tbl1))
                 (push-restrict r6))))
        (testing ":difference"
          (is (= r4 (push-restrict r4))))))
    (testing "restrict-outer"
      (testing "with underlying project"
        (is (= (make-project (project-alist p1)
                             (make-restrict-outer (=$ (make-attribute-ref "one")
                                                      (make-const string% "foobar"))
                                                  tbl1))
               (push-restrict (ro* p1)))))

      (testing "with underlying combine"
        (testing "with left outer product"
          (is (= (make-restrict-outer (=$ (make-attribute-ref "one")
                                          (make-const string% "foobar"))
                                      (make-combine :left-outer-product
                                                    p1 p2))
               (push-restrict (ro* (make-combine :left-outer-product
                                                 p1 p2))))))

        (testing "with union"
          (let [c (make-union p1 p1)
                p (make-project {"two" (make-attribute-ref "two")} tbl1)]
            #_(is (= (make-union p1
                               (push-restrict
                                (make-restrict-outer (=$ (make-attribute-ref "one")
                                                         (make-const string% "foobar")) p1)))
                   (push-restrict (ro* c))))
            #_(is (= (make-union (push-restrict (ro* p)) p1)
                   (push-restrict (ro* (make-union p p1)))))
            #_(let [rr (ro* (make-union p p))]
              (is (= (make-restrict-outer (restrict-outer-exp rr)
                                          (push-restrict (restrict-outer-query rr)))
                     (push-restrict rr)))))))

      (testing "with underlying restrict"
        (let [t (=$ (make-attribute-ref "one")
                    (make-const string% "foobar"))
              rr1 (ro* (make-restrict t (make-restrict t tbl1)))
              rr2 (ro* (make-restrict t tbl1))]
          (is (= (make-restrict-outer (restrict-outer-exp rr1)
                                      (push-restrict (restrict-outer-query rr1)))
                 (push-restrict rr1)))
          (is (= (push-restrict (make-restrict-outer t (push-restrict (restrict-outer-query rr2))))
                 (push-restrict rr2)))))
      (testing "with underlying order"
        (is (= (make-order {(make-attribute-ref "one") :ascending}
                           (push-restrict (ro* tbl1)))
               (push-restrict
                (make-restrict-outer (=$ (make-attribute-ref "one")
                                         (make-const string% "foobar"))
                                     (make-order {(make-attribute-ref "one") :ascending} tbl1)))))))
    (testing "order"
      (let [o1 (make-order {(make-attribute-ref "one") :descending} p1)
            o2 (make-order {(make-attribute-ref "one") :descending}
                           (make-project {"one" (make-aggregation :count (make-attribute-ref "one"))}
                                         tbl1))
            o* (fn [q] (make-order {(make-attribute-ref "one") :ascending} q))
            o3 (o* (make-order {(make-attribute-ref "two") :descending} tbl1))
            o4 (o* (make-order {(make-attribute-ref "two") :descending}
                               (make-order {(make-attribute-ref "one") :ascending} tbl1)))
            o5 (o* (make-top 0 1 (make-top 0 1 tbl1)))
            o6 (o* (make-top 0 1 tbl1))]
        (testing "with underlying project"
          (is (= (make-project (project-alist p1)
                               (push-restrict (make-order
                                               [[(make-attribute-ref "one") :descending]]
                                               (project-query p1))))
                 (push-restrict o1)))
          (is (= (make-order {(make-attribute-ref "one") :descending}
                             (push-restrict (make-project [["one" (make-aggregation :count (make-attribute-ref "one"))]]
                                                          tbl1)))
                 (push-restrict o2))))
        (testing "with underlying order"
          (is (= (make-order (order-alist o4) (push-restrict (order-query o4)))
                 (push-restrict o4)))
          (is (= (push-restrict (make-order (order-alist o3) (push-restrict (order-query o3)))) ;; == pushed != order
                 (push-restrict o3))))
        (testing "with underlying top"
          (is (= (make-order (order-alist o5) (push-restrict (order-query o5)))
                 (push-restrict o5)))
          (is (= (push-restrict (make-order (order-alist o6) (push-restrict (order-query o6))))
                 (push-restrict o6))))))
    (testing "group"
      (is (= (make-group #{"one"} tbl1)
             (push-restrict (make-group #{"one"} tbl1)))))
    (testing "top"
      (let [t* (fn [q] (make-top 0 1 q))
            t1 (t* (make-project {"one" (make-attribute-ref "one")} tbl1))
            t2 (t* (make-project {"one" (make-aggregation :count (make-attribute-ref "one"))} tbl1))
            t3 (t* (make-order {(make-attribute-ref "one") :ascending} tbl1))
            t4 (t* (make-order {(make-attribute-ref "one") :ascending}
                               (make-order {(make-attribute-ref "two") :desceding} tbl1)))]
        (testing "with underlying project"
          (is (= (make-project (project-alist (top-query t1))
                               (push-restrict (make-top 0 1 (project-query (top-query t1)))))
                 (push-restrict t1)))
          (is (= (make-top 0  1 (push-restrict (top-query t2)))
                 (push-restrict t2))))
        (testing "with underlying order"
          (is (= (push-restrict (make-top 0 1 (push-restrict (top-query t3))))
                 (push-restrict t3)))
          (is (= (make-top 0 1 (push-restrict (top-query t4)))
                 (push-restrict t4))))))
    (testing "combine"
      (is (= c1 (push-restrict c1))))
    (testing "everything else should fail"
      (is (thrown? Exception (push-restrict nil))))))

;; Imported from schemeql2 test suite
(deftest schemeql2-test
  (is (= (rel/alist->rel-scheme [["foo" t/integer%]])
         (rel/query-scheme
          (opt/optimize-query
           (q/get-query
            (monadic [t1 (q/embed tbl1)
                      t2 (q/embed tbl2)]
                     (q/restrict (sql/=$ (q/! t1 "one")
                                         (q/! t2 "four")))
                     (q/project [["foo" (q/! t1 "two")]])))))))
  (is (= (rel/alist->rel-scheme [["foo" t/string%]])
         (rel/query-scheme
          (opt/optimize-query
           (q/get-query
            (monadic [t1 (q/embed tbl1)
                      t2 (q/embed tbl2)]
                     (q/union (q/project [["foo" (q/! t1 "one")]])
                              (q/project [["foo" (q/! t2 "four")]]))))))))
  (is (= (rel/alist->rel-scheme [["foo" t/string%]])
         (rel/query-scheme
          (opt/optimize-query
           (q/get-query
            (monadic [t1 (q/embed tbl1)
                      t2 (q/embed tbl2)]
                     (q/restrict (sql/=$ (q/! t1 "one")
                                         (q/! t2 "four")))
                     (q/union (q/project [["foo" (q/! t1 "one")]])
                              (q/project [["foo" (q/! t2 "four")]])))))))))
