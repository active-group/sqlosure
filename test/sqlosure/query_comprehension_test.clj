(ns sqlosure.query-comprehension-test
  (:require [active.clojure.condition :as condition]
            [active.clojure.monad :as monad :refer :all]
            [clojure.test :as t :refer :all]

            [sqlosure.optimization :as opt]
            [sqlosure.query-comprehension :as qc :refer :all]
            [sqlosure.relational-algebra :as rel :refer :all]
            [sqlosure.relational-algebra-sql :refer :all]
            [sqlosure.sql :as sql :refer :all]
            [sqlosure.sql-put :as put]
            [sqlosure.type :as sql-t :refer :all]
            [sqlosure.universe :refer :all]))

(def test-universe (make-universe))

(def tbl1 (base-relation "tbl1"
                         (alist->rel-scheme [["one" string%]
                                             ["two" integer%]])
                         :universe test-universe))

(def tbl2 (base-relation "tbl2"
                         (alist->rel-scheme [["three" blob%]
                                             ["four" string%]])
                         :universe test-universe))

(defn test-put-query
  [query]
  (put/sql-select->string (query->sql query) put/default-sql-put-parameterization))

(deftest const-restrict-test
  (is (= ["SELECT foo_1 AS foo FROM ( SELECT two_0 AS foo_1 , one_0 , two_0 FROM ( SELECT one AS one_0 , two AS two_0 FROM tbl1 ) WHERE ( one_0 = ? ) )"
          [[string% "foobar"]]]
         (test-put-query
          (get-query
           (monadic
            [t1 (embed tbl1)]
            (restrict (=$ (! t1 "one")
                          (make-const string% "foobar")))
            (project [["foo" (! t1 "two")]])))))))

(deftest group-test
  (is (rel-scheme=?
       (alist->rel-scheme [["foo" integer%]])
       (query-scheme
        (get-query (monadic [t1 (embed tbl1)]
                            (group [t1 "one"])
                            (project [["foo" (make-aggregation :max (! t1 "two"))]])))))))


(deftest trivial
  (is (rel-scheme=?
       (alist->rel-scheme [["foo" integer%]])
       (query-scheme
        (get-query (monadic
                    [t1 (embed tbl1)]
                    [t2 (embed tbl2)]
                    (restrict (=$ (! t1 "one")
                                  (! t2 "four")))
                    (project [["foo" (! t1 "two")]])))))))

(deftest trivial-outer
  (is (rel-scheme=?
       (alist->rel-scheme [["foo" integer%]])
       (query-scheme
        (get-query (monadic
                    [t1 (embed tbl1)]
                    [t2 (outer tbl2)]
                    (restrict-outer (=$ (! t1 "one")
                                        (! t2 "four")))
                    (project [["foo" (! t1 "two")]])))))))


(deftest combine
  (is (rel-scheme=?
       (alist->rel-scheme [["foo" string%]])
       (query-scheme
        (get-query
         (monadic
          [t1 (embed tbl1)]
          [t2 (embed tbl2)]
          (union (project [["foo" (! t1 "one")]])
                 (project [["foo" (! t2 "four")]])))))))

  (is (rel-scheme=?
       (alist->rel-scheme [["foo" string%]])
       (query-scheme
        (get-query
         (monadic
          [t1 (embed tbl1)]
          [t2 (embed tbl2)]
          (subtract (project [["foo" (! t1 "one")]])
                    (project [["foo" (! t2 "four")]])))))))

  (is (rel-scheme=?
       (alist->rel-scheme [["bar" blob%]])
       (query-scheme
        (get-query
         (monadic
          [t1 (embed tbl1)
           t2 (embed tbl2)]
          (divide (project [["foo" (! t1 "one")]
                            ["bar" (! t2 "three")]])
                  (project [["foo" (! t2 "four")]]))))))))

(deftest order-t
  (is (rel-scheme=?
       (alist->rel-scheme [["foo" integer%]])
       (query-scheme
        (get-query
         (monadic
          [t1 (embed tbl1)
           t2 (embed tbl2)]
          (restrict (=$ (! t1 "one")
                        (! t2 "four")))
          (order {(! t1 "one") :ascending})
          (project [["foo" (! t1 "two")]])))))))

(t/deftest with-table-space-test
  (t/testing "with-table-space puts the correct value to the env"
    (t/is (= [{::qc/table-space "prefix"} {}]
             (monad/run-free-reader-state-exception
              (monad/null-monad-command-config {} {})
              (qc/with-table-space "prefix"
                (monad/monadic [env (monad/get-env)]
                               (monad/return env))))))))

(deftest table-space-test
  (let [br (rel/make-base-relation
            "name"
            (rel/alist->rel-scheme [["a" sql-t/integer%]]))]
    (t/testing "without a table space"
      (t/is (nil? (-> (qc/get-query (qc/embed br))
                      rel/project-query
                      rel/project-query
                      rel/base-relation-table-space))))
    (t/testing "with a table space"
      (let [q (qc/with-table-space "prefix" (qc/embed br))]
        (t/is (= "prefix"
                 (-> (qc/get-query q)
                     rel/project-query
                     rel/project-query
                     rel/base-relation-table-space)))))
    (t/testing "blank characters are prohibited"
      (try (qc/with-table-space "with blank" (qc/embed br))
           (catch Exception e
             (t/is (condition/assertion-violation? e)))))))
