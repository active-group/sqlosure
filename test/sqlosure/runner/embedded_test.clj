(ns sqlosure.runner.embedded-test
  (:require [active.clojure.condition :as c]
            [clojure.test :refer [deftest is testing]]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.core :as sqlosure]
            [sqlosure.sql :as sql]

            [sqlosure.runner.embedded :as i])
  (:import [java.time LocalDateTime]))

(deftest alist-lookup-value-test
  (is (= [] (i/alist-lookup-value [] :value)))
  (is (= [] (i/alist-lookup-value [[:foo "bar"]] :value)))
  (is (= [:foo] (i/alist-lookup-value [[:foo "bar"]] "bar")))
  (is (= [:key "bar"] (i/alist-lookup-value [[:key "foo"] ["bar" "foo"]] "foo"))))

(deftest extend-keys-test
  (is (= {"a" "a"} (i/extend-keys [["a" "a"]] nil)))
  (is (= {"a" "a", "b" "a"} (i/extend-keys [["a" "a"]] [["b" "a"]]))))

(deftest some-aggregation?-test
  (is (false? (i/some-aggregation? (rel/make-attribute-ref "a"))))
  (is (false? (i/some-aggregation? (rel/make-const sqlosure.core/$integer-t 42))))
  (is (true? (i/some-aggregation? (rel/make-aggregation :count-all))))
  (is (true? (i/some-aggregation? (rel/make-aggregation :count (rel/make-attribute-ref "a"))))))

(deftest unroll-project-alist-test
  (is (empty? (i/unroll-project-alist [])))
  (is (= [["a" "a"]] (i/unroll-project-alist [["a" (rel/make-attribute-ref "a")]])))
  (is (= [["a" "a"] ["b" "b"]]
         (i/unroll-project-alist [["a" (rel/make-attribute-ref "a")]
                                  ["b" (rel/make-attribute-ref "b")]])))
  (is (= [["a" "a"] ["b" (rel/make-aggregation :count-all)]]
         (i/unroll-project-alist [["a" (rel/make-attribute-ref "a")]
                                  ["b" (rel/make-aggregation :count-all)]]))))

(deftest alist->aggregations-test
  (is (empty? (i/alist->aggregations [["a" (rel/make-attribute-ref "a")]
                                      ["b" (rel/make-attribute-ref "b")]])))
  (is (= [["b" (rel/make-aggregation :count-all)]]
         (i/alist->aggregations [["a" (rel/make-attribute-ref "a")]
                                 ["b" (rel/make-aggregation :count-all)]]))))

(deftest interpret-aggregation-test
  (testing "if it is not an aggregation"
    (try (i/interpret-aggregation [["a" (rel/make-attribute-ref "a")]] #{})
         (catch Exception e
           (is (c/condition? e)))))
  (testing "error cases"
    (testing "when making a reference to a non-existing field"
      (try (i/interpret-aggregation ["aggr" (rel/make-aggregation :count (rel/make-attribute-ref "b"))] #{{"a" 1}})
           (catch Exception e
             (is (c/condition? e)))))
    (testing "when supplying a value that is not an aggregation"
      (try (i/interpret-aggregation ["aggr" (rel/make-attribute-ref "a")] #{{"a" 1}})
           (catch Exception e
             (is (c/condition? e))))))
  (testing "count"
    (is (= {"aggr" 2}
           (i/interpret-aggregation ["aggr" (rel/make-aggregation :count (rel/make-attribute-ref "a"))]
                                    #{{"a" 1} {"a" 2}}))))
  (testing "sum"
    (is (= {"aggr" 3}
           (i/interpret-aggregation ["aggr" (rel/make-aggregation :sum (rel/make-attribute-ref "a"))]
                                    #{{"a" 1} {"a" 2}}))))
  (testing "avg"
    (is (= {"aggr" 3/2}
           (i/interpret-aggregation ["aggr" (rel/make-aggregation :avg (rel/make-attribute-ref "a"))]
                                    #{{"a" 1} {"a" 2}}))))
  (testing "min"
    (is (= {"aggr" 1}
           (i/interpret-aggregation ["aggr" (rel/make-aggregation :min (rel/make-attribute-ref "a"))]
                                    #{{"a" 1} {"a" 2}}))))
  (testing "max"
    (is (= {"aggr" 2}
           (i/interpret-aggregation ["aggr" (rel/make-aggregation :max (rel/make-attribute-ref "a"))]
                                    #{{"a" 1} {"a" 2}})))))

(deftest base-relation->handle-test
  (is (= "handle"
         (i/base-relation->handle (rel/make-base-relation "rel" "scheme" :handle "handle"))))
  (is (= "handle"
         (i/base-relation->handle (sql/base-relation "handle" "scheme")))))

(deftest unroll-record-test
  (is (= ["a" "b" "c"]
         (i/unroll-record
          (rel/make-base-relation "rel"
                                  (rel/alist->rel-scheme [["a" sqlosure/$string-t]
                                                          ["b" sqlosure/$string-t]
                                                          ["c" sqlosure/$string-t]]))
          {"c" "c" "a" "a" "b" "b"}))))

(deftest unroll-rand-test
  (is (= 42 (i/unroll-rand {} (sqlosure/$integer 42))))
  (is (= 42 (i/unroll-rand {"a" 42} (rel/make-attribute-ref "a")))))

(deftest apply-restriction-test
  (testing "flat restriction"
    (is (false? (i/apply-restriction (sqlosure/$= (rel/make-attribute-ref "a")
                                                  (sqlosure/$integer 42))
                                     {"a" 23})))
    (is (true? (i/apply-restriction (sqlosure/$= (rel/make-attribute-ref "a")
                                                 (sqlosure/$integer 42))
                                    {"a" 42}))))
  (testing "nested restriction"
    (is (false? (i/apply-restriction (sqlosure/$or (sqlosure/$= (rel/make-attribute-ref "a")
                                                                (sqlosure/$integer 42))
                                                   (sqlosure/$= (rel/make-attribute-ref "a")
                                                                (sqlosure/$integer 23)))
                                     {"a" 1024})))
    (is (true? (i/apply-restriction (sqlosure/$or (sqlosure/$= (rel/make-attribute-ref "a")
                                                               (sqlosure/$integer 42))
                                                  (sqlosure/$= (rel/make-attribute-ref "a")
                                                               (sqlosure/$integer 23)))
                                    {"a" 23})))))

(deftest run-query-test
  (let [rel (rel/make-base-relation "rel"
                                    (rel/alist->rel-scheme [["a" sqlosure/$integer-t]
                                                            ["b" sqlosure/$integer-t]])
                                    :handle "handle")]
    (testing "the empty query"
      (is (empty? (i/run-query {} rel/the-empty))))

    (testing "base relation"
      (is (empty? (i/run-query {} rel)))
      (is (= [[142 123] [42 23]]
             (i/run-query {"handle" #{{"a" 42, "b" 23} {"a" 142, "b" 123}}} rel))))

    (testing "project"
      (testing "base case"
        (let [p (sqlosure/query [t (sqlosure/<- rel)]
                                (sqlosure/project [["a" (sqlosure/! t "a")]]))]
          (is (empty? (i/run-query {} p)))
          (is (= [[142] [42]]
                 (i/run-query {"handle" #{{"a" 42, "b" 23} {"a" 142, "b" 123}}} p)))))

      (testing "with aggregation, no grouping"
        (let [p (sqlosure/query [t (sqlosure/<- rel)]
                                (sqlosure/project [["a" (sqlosure/$count (sqlosure/! t "a"))]]))]
          (is (= [[0]] (i/run-query {} p)))
          (is (= [[2]]
                 (i/run-query {"handle" #{{"a" 42, "b" 23} {"a" 142, "b" 123}}} p)))))

      (testing "with aggregation plus no grouping"
        (let [p (sqlosure/query [t (sqlosure/<- rel)]
                                (sqlosure/group! [t "a"])
                                (sqlosure/project [["a" (sqlosure/! t "a")]
                                                   ["b" (sqlosure/$count (sqlosure/! t "b"))]]))]
          (is (empty? (i/run-query {} p)))
          (is (= [[142 2] [42 1]]
                 (i/run-query {"handle" #{{"a" 42, "b" 23}
                                          {"a" 142, "b" 123}
                                          {"a" 142, "b" 42}}}
                              p))))))

    (testing "restrict"
      (testing "flat restrict"
        (let [r (sqlosure/query [t (sqlosure/<- rel)]
                                (sqlosure/restrict! (sqlosure/$= (sqlosure/! t "a")
                                                                 (sqlosure/$integer 42)))
                                (sqlosure/project [["b" (sqlosure/! t "b")]]))]
          (is (empty? (i/run-query #{} r)))
          (is (= [[23]] (i/run-query {"handle" #{{"a" 42, "b" 23}
                                                 {"a" 142, "b" 123}}} r)))))
      (testing "nested restrict"
        (let [r (sqlosure/query [t (sqlosure/<- rel)]
                                (sqlosure/restrict!
                                 (sqlosure/$or (sqlosure/$= (sqlosure/! t "a")
                                                            (sqlosure/$integer 42))
                                               (sqlosure/$= (sqlosure/! t "b")
                                                            (sqlosure/$integer 123))))
                                (sqlosure/project [["b" (sqlosure/! t "b")]]))]
          (is (empty? (i/run-query #{} r)))
          (is (= [[123] [23]] (i/run-query {"handle" #{{"a" 42, "b" 23}
                                                       {"a" 142, "b" 123}}} r)))))
      #_(testing "sequential restricts"
          (let [r (sqlosure/query [t (sqlosure/<- rel)]
                                  (sqlosure/restrict! (sqlosure/$= (sqlosure/! t "b")
                                                                   (sqlosure/$integer 123)))
                                  (sqlosure/restrict! (sqlosure/$= (sqlosure/! t "a")
                                                                   (sqlosure/$integer 42)))
                                  (sqlosure/project [["b" (sqlosure/! t "b")]]))]
            (is (empty? (i/run-query #{} r)))
            (is (= #{[23] [123]} (i/run-query {"handle" #{{"a" 42, "b" 23}
                                                          {"a" 142, "b" 123}}} r))))))

    (testing "order"
      (let [o  (sqlosure/query [t (sqlosure/<- rel)]
                               (sqlosure/order! (sqlosure/! t "a") :ascending)
                               (sqlosure/project t))
            o2 (sqlosure/query [t (sqlosure/<- rel)]
                               (sqlosure/order! (sqlosure/! t "a") :descending)
                               (sqlosure/project t))]
        (is (empty? (i/run-query {} o)))
        (is (= [[1 "one"] [2 "two"] [3 "three"]]
               (i/run-query {"handle" #{{"a" 3 "b" "three"}
                                        {"a" 1 "b" "one"}
                                        {"a" 2 "b" "two"}}}
                            o)))
        (is (= [[3 "three"] [2 "two"] [1 "one"]]
               (i/run-query {"handle" #{{"a" 3 "b" "three"}
                                        {"a" 1 "b" "one"}
                                        {"a" 2 "b" "two"}}}
                            o2))))
      (let [r   (rel/make-base-relation "rel"
                                        (rel/alist->rel-scheme [["ts" sqlosure/$timestamp-t]])
                                        :handle "handle")
            ts1 (LocalDateTime/of 1989 10 31 10 30 23 123)
            ts2 (LocalDateTime/of 1989 10 31 10 30 23 124)
            ts3 (LocalDateTime/of 2020 4 4 0 0 0 0)
            q   (fn [direction]
                  (i/run-query {"handle" #{{"ts" ts1}
                                           {"ts" ts2}
                                           {"ts" ts3}}}
                               (sqlosure/query [t (sqlosure/<- r)]
                                               (sqlosure/order! [[(sqlosure/! t "ts") direction]])
                                               (sqlosure/project t))))]
        (is (= [[ts1] [ts2] [ts3]] (q :ascending)))
        (is (= [[ts3] [ts2] [ts1]] (q :descending)))))

    (testing "top"
      (let [t  (sqlosure/query [t (sqlosure/<- rel)]
                               (sqlosure/order! (sqlosure/! t "a") :ascending)
                               (sqlosure/top! 1)
                               (sqlosure/project t))
            t2 (sqlosure/query [t (sqlosure/<- rel)]
                               (sqlosure/order! (sqlosure/! t "a") :descending)
                               (sqlosure/top! 1 2)
                               (sqlosure/project t))]
        (is (empty? (i/run-query {} t)))
        (is (= [[1 "one"]]
               (i/run-query {"handle" #{{"a" 3 "b" "three"}
                                        {"a" 1 "b" "one"}
                                        {"a" 2 "b" "two"}}}
                            t)))
        (is (= [[2 "two"] [1 "one"]]
               (i/run-query {"handle" #{{"a" 3 "b" "three"}
                                        {"a" 1 "b" "one"}
                                        {"a" 2 "b" "two"}}}
                            t2)))))
    ;; NOTE This test is wonky in terms of Clojure's set semantics -- we can't have a set with a duplicate "record".
    ;; (testing "distinct"
    ;;   (let [d (sqlosure/query [t (sqlosure/<- rel)]
    ;;                           (sqlosure/order! (sqlosure/! t "a") :ascending)
    ;;                           sqlosure/distinct!
    ;;                           (sqlosure/project t))]
    ;;     (is (empty? (i/run-query {} d)))
    ;;     (is (= #{[1 "one"] [2 "two"]}
    ;;            (i/run-query {"handle" #{{"a" 2 "b" "two"}
    ;;                                           {"a" 1 "b" "one"}
    ;;                                           {"a" 2 "b" "two"}}}
    ;;                               d)))))
    ))

(deftest interpret-insert-test
  (let [rel (rel/make-base-relation "rel"
                                    (rel/alist->rel-scheme [["a" sqlosure/$integer-t]
                                                            ["b" sqlosure/$integer-t]])
                                    :handle "handle")]
    (testing "inserting into an empty database"
      (is (= {"handle" #{{"a" 42 "b" 23}}}
             (i/run-insert {} rel [42 23]))))
    (testing "adding to an existing database"
      (is (= {"handle" #{{"a" 42 "b" 23}
                         {"a" 32 "b" 64}}}
             (i/run-insert {"handle" #{{"a" 42 "b" 23}}} rel [32 64]))))
    (testing "fails when the record does not match the relation's schema"
      (try (i/run-insert {} rel [42 23])
           (catch Exception e
             (is (c/condition? e)))))))

(deftest interpret-delete-test
  (let [rel (rel/make-base-relation "rel"
                                    (rel/alist->rel-scheme [["a" sqlosure/$integer-t]
                                                            ["b" sqlosure/$integer-t]])
                                    :handle "handle")]
    (testing "deleting a record that does not exist"
      (is (= [0 {"handle" #{{"a" 42 "b" 23}}}]
             (i/run-delete {"handle" #{{"a" 42 "b" 23}}}
                           rel
                           (fn [a b]
                             (sqlosure/$= a (sqlosure/$integer 23)))))))
    (testing "deleting a record that exists"
      (is (= [1 {"handle" #{}}]
             (i/run-delete {"handle" #{{"a" 42 "b" 23}}}
                           rel
                           (fn [a b]
                             (sqlosure/$= a (sqlosure/$integer 42)))))))))

(deftest interpret-updated-test
  (let [rel (rel/make-base-relation "rel"
                                    (rel/alist->rel-scheme [["a" sqlosure/$integer-t]
                                                            ["b" sqlosure/$integer-t]])
                                    :handle "handle")]
    (testing "updating a record that does not exist"
      (is (= [0 {"handle" #{{"a" 42 "b" 23}}}]
             (i/run-update {"handle" #{{"a" 42 "b" 23}}}
                           rel
                           (fn [a b]
                             (sqlosure/$= a (sqlosure/$integer 23)))
                           (fn [a b]
                             {"a" (sqlosure/$integer 0)})))))
    (testing "updating a record that exists"
      (is (= [1 {"handle" #{{"a" 0 "b" 23}
                            {"a" 142 "b" 123}}}]
             (i/run-update {"handle" #{{"a" 42 "b" 23}
                                       {"a" 142 "b" 123}}}
                           rel
                           (fn [a b]
                             (sqlosure/$= a (sqlosure/$integer 42)))
                           (fn [a b]
                             {"a" (sqlosure/$integer 0)})))))
    (testing "updating multiple records"
      (is (= [2 {"handle" #{{"a" 0 "b" 23}
                            {"a" 142 "b" 123}
                            {"a" 0 "b" 123}}}]
             (i/run-update {"handle" #{{"a" 42 "b" 23}
                                       {"a" 142 "b" 123}
                                       {"a" 42 "b" 123}}}
                           rel
                           (fn [a b]
                             (sqlosure/$= a (sqlosure/$integer 42)))
                           (fn [a b]
                             {"a" (sqlosure/$integer 0)})))))))
