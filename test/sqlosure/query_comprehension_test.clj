(ns sqlosure.query-comprehension-test
  (:require [active.clojure.monad :refer :all]
            [clojure.test :refer :all]
            [sqlosure
             [optimization :as opt]
             [query-comprehension :refer :all]
             [relational-algebra :refer :all]
             [relational-algebra-sql :refer :all]
             [sql :refer :all]
             [sql-put :as put]
             [type :refer :all]
             [universe :refer :all]]))

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
