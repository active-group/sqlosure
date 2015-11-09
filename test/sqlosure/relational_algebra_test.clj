(ns sqlosure.relational-algebra-test
  (:require [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :refer :all]
            [sqlosure.sql :as sql :refer :all]
            [sqlosure.type :refer :all]
            [clojure.pprint :refer :all]
            [clojure.test :refer :all]))

(def test-scheme1 (make-rel-scheme {:foo :bar
                                    :fizz :buzz}))
(def test-scheme2 (make-rel-scheme {:foo :bar
                                    :some :thing}))
(def test-scheme3 (make-rel-scheme {:foo :bar}))
(def test-scheme4 (make-rel-scheme {:fizz :buzz}))


(deftest rel-schem=?-test
  (is (rel-scheme=? (make-rel-scheme nil) the-empty-rel-scheme))
  (is (rel-scheme=? (make-rel-scheme {:foo "bar"
                                      :fizz "buzz"})
                    (make-rel-scheme {:fizz "buzz"
                                      :foo "bar"}))))

(deftest rel-scheme-difference-test
  (is (= (rel-scheme-difference test-scheme1 test-scheme3) test-scheme4))
  (is (= (rel-scheme-difference test-scheme2 test-scheme3) (make-rel-scheme {:some :thing})))
  (is (= (rel-scheme-difference test-scheme1 the-empty-rel-scheme) test-scheme1)))

(deftest rel-scheme-unary?-test
  (is (rel-scheme-unary? test-scheme4))
  (is (not (rel-scheme-unary? test-scheme1))))

(deftest rel-scheme->environment-test
  (is (= {:fizz :buzz} (rel-scheme->environment test-scheme4)))
  (is (= {:foo :bar :some :thing} (rel-scheme->environment test-scheme2))))

(deftest compose-environments-test
  (is (= the-empty-environment) (compose-environments the-empty-environment the-empty-environment))
  (is (= {:foo :bar} (compose-environments {:foo :bar} the-empty-environment)))
  ;; e1 should take precedence over e2!
  (is (= {:foo :bar :fizz :buzz} (compose-environments {:foo :bar} {:foo :something-else :fizz :buzz}))))

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
                             (do (check-numerical t1 fail)
                                 (check-numerical t2 fail)))
                           t1)
                      +
                      :universe test-universe)]
      (is (= (universe-lookup-rator test-universe '+)
             rator)))))

(def tbl1 (make-base-relation 'tbl1
                              (make-rel-scheme {"one" string%
                                                "two" integer%})
                              (make-universe)
                              "tbl1"))

(deftest make-project-test
  (let [p (make-project {"two" (make-attribute-ref "two")
                         "one" (make-attribute-ref "one")}
                        tbl1)]
    (is (= (project-alist p) {"two" (make-attribute-ref "two")
                              "one" (make-attribute-ref "one")}))
    (is (= (project-query p) tbl1)))
  (is (thrown? Exception (make-project [] tbl1))))

;; TODO: what should be the result?
#_(deftest make-extend-test
  (let [p (make-extend {"three" (make-attribute-ref "three")
                        "four" (make-attribute-ref "four")} tbl1)]
    (is (= (project-alist p) (merge {"three" (make-attribute-ref "three")
                                     "four" (make-attribute-ref "four")}
                                    {"one" string%
                                     "two" integer%})))
    (is (= (project-query p) tbl1))))

(deftest expression-type-test
  (let [one-ref (make-attribute-ref "one")
        string-const (make-const string% "foobar")
        string-null (make-null string%)
        my-aggregation (make-aggregation
                        :count (make-tuple [(make-const integer% 40)
                                            (make-const integer% 2)]))
        my-case (make-case-expr {(sql/=$ (make-const integer% 42)
                                         (make-const integer% 42))
                                 (make-const boolean% true)}
                                (make-const boolean% false))]
    ;; attribute ref
    (is (thrown? Exception (expression-type the-empty-environment one-ref)))
    (is (= string% (expression-type
                    (rel-scheme->environment (base-relation-scheme tbl1))
                    one-ref)))
    ;; const
    (is (= (base-type? (expression-type the-empty-environment string-const))))
    (is (= string% (expression-type the-empty-environment string-const)))
    ;; const-null
    (is (= string% (expression-type the-empty-environment string-null)))
    ;; application
    (is (= integer% (expression-type the-empty-environment
                                     (sql/plus$ (make-const integer% 1)
                                                (make-const integer% 41)))))
    (is (= boolean% (expression-type the-empty-environment
                                     (sql/>=$ (make-const integer% 1)
                                              (make-const integer% 2)))))
    (is (thrown? Exception (expression-type
                            (rel-scheme->environment (base-relation-scheme tbl1))
                            (sql/>=$ (make-const integer% 1)
                                     (make-attribute-ref "one"))
                            :typecheck? true)))
    ;; tuple
    (is (= (make-product-type [integer% string%])
           (expression-type the-empty-environment
                            (make-tuple [(make-const integer% 42)
                                         (make-const string% "foobar")]))))
    (is (= (make-product-type [string%])
           (expression-type {"string" string%}
                            (make-tuple [(make-attribute-ref "string")]))))
    (is (not= (make-product-type [string%])
              (expression-type the-empty-environment
                               (make-tuple [(make-const integer% 42)]))))
    ;; aggregation
    (is (= integer% (expression-type the-empty-environment my-aggregation)))
    (is (= (make-product-type [integer% integer%])
           (expression-type the-empty-environment
                            (make-aggregation
                             :min
                             (make-tuple [(make-const integer% 42)
                                          (make-const integer% 23)])))))
    (is (thrown? Exception  ;; With typechecking, this expression should fail.
                 (expression-type the-empty-environment
                                  (make-aggregation
                                   :min
                                   (make-tuple [(make-const integer% 42)
                                                (make-const integer% 23)]))
                                  :typecheck? true)))
    ;; case expression
    (is (= boolean% (expression-type the-empty my-case)))
    (is (thrown? Exception  ;; With typechecking, this should fail
                 (expression-type
                  the-empty-environment
                  (make-case-expr
                   {(sql/plus$ (make-const string% "foobar")
                               (make-const integer% 42))
                    (make-const boolean% true)}
                   (make-const boolean% false))
                  :typecheck? true)))
    ;; scalar subquery
    ))

(deftest query-scheme-test
  (let [test-universe (make-universe)
        SUBB (make-base-relation 'SUBB
                                 (make-rel-scheme {"C" string%})
                                 test-universe
                                 "SUBB")
        SUBA (make-base-relation 'SUBA
                                 (make-rel-scheme {"C" string%})
                                 test-universe
                                 "SUBA")]
    ;; empty val
    (is (= the-empty-rel-scheme (query-scheme (make-empty-val))))
    ;; base relation
    (is (= (make-rel-scheme {"one" string%
                             "two" integer%})
           (query-scheme tbl1 :typecheck? true)))
    ;; projection
    (let [p (make-project {"two" (make-attribute-ref "two")
                           "one" (make-attribute-ref "one")}
                          tbl1)
          res (query-scheme p :typecheck? true)]
      (is (= (rel-scheme-alist res) {"two" integer% "one" string%}))
      (is (thrown? Exception  ;; should fail with typechecking because of aggregation
                   (query-scheme (make-project
                                  {"two" (make-attribute-ref "two")
                                   "one" (make-aggregation
                                          :min
                                          (make-tuple [(make-const integer% 42)
                                                       (make-const integer% 23)]))}
                                  tbl1)
                                 :typecheck? true))))
    (let [r (make-restrict (sql/=$ (make-scalar-subquery
                                    (make-project {"C" (make-attribute-ref "C")}
                                                  SUBB))
                                   (make-attribute-ref "C"))
                           SUBA)]
      (is (= (rel-scheme-alist (query-scheme r)) {"C" string%}))
      (is (thrown? Exception (query-scheme r :typecheck? true))))
    ;; combine
    (let [test-universe (make-universe)
          rel1 (make-base-relation 'tbl1
                                   (make-rel-scheme {"one" string%
                                                     "two" integer%})
                                   test-universe
                                   "tbl1")
          rel2 (make-base-relation 'tbl2
                                   (make-rel-scheme {"three" boolean%
                                                     "four" double%})
                                   test-universe
                                   "tbl2")
          c (make-product rel1 rel2)
          q (make-quotient rel1 rel2)
          u (make-union rel1 rel2)]
      (is (= (rel-scheme-alist (query-scheme c))
             (into (rel-scheme-alist (base-relation-scheme rel1))
                   (rel-scheme-alist (base-relation-scheme rel2)))))
      (is (= (rel-scheme-alist (query-scheme q))
             (rel-scheme-alist (rel-scheme-difference (base-relation-scheme rel1)
                                                      (base-relation-scheme rel2))))))))

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
  ;; scalar
  (is (not (aggregate? (make-scalar-subquery tbl1)))))

#_ ((deftest query?-test
       (is (query? '())

           (query? nil))
       (is (query? (make-base-relation "name" "scheme"))
           (query? (make-base-relation "name" "scheme" :universe (make-universe)
                                       :handle "some handle"))))

    (deftest relational-op?-test
      (is (every? relational-op? [:product :union :intersection :quotient :difference]))
      (is (not-every? relational-op? [:some-key "some string" 42 'some-symbol])))

    (def test-universe (make-universe))


    (deftest query-scheme-test
      (let [project-q (make-project {"two" (make-attribute-ref "two")
                                     "one" (make-attribute-ref "one")}
                                    tbl1)
            order-q (make-order {(make-attribute-ref "one") :ascending} tbl1)]
        (is (rel-scheme=? (query-scheme tbl1) (query-scheme order-q)))
        (is (rel-scheme=? (query-scheme tbl1) (query-scheme
                                               (make-project {"eq" (=$ (make-attribute-ref "two")
                                                                       (make-attribute-ref "two"))}
                                                             tbl1))))))

    (make-project {"eq" (=$ (make-attribute-ref "two")
                            (make-attribute-ref "two"))}
                  tbl1)

    (deftest make-combine-test
      (is (= (make-combine :union "some query" "other query")
             (make-union "some query" "other query")))
      (is (thrown? Exception (make-combine :not-valid "some query" "other query"))))

    (deftest make-project-test
      (let [q (make-project {"two" (make-attribute-ref "two")
                             "one" (make-attribute-ref "one")} "some query")]
        (is (and (= (project-alist q) {"two" (make-attribute-ref "two")
                                       "one" (make-attribute-ref "one")})
                 (= (project-query q) "some query")))
        (is (thrown? Exception (make-project ["not" "a" "map"] "some query")))))

    ;; union, difference, intersection and quotient are basically the same...
    (deftest make-product-test
      (is (= (make-product "some query" "other query") (really-make-combine :product "some query" "other query")))
      (is (= (make-product "some query" nil) (make-product nil "some query") "some query")))

    (deftest order-op?-test
      (is (and (order-op? :ascending) (order-op? :descending)))
      (is (not (or (order? :some-key) (order-op? nil)))))

    (deftest aggregations-op?-test
      (is (and (aggregations-op? :count) (aggregations-op? :sum)
               (aggregations-op? :avg) (aggregations-op? :min)
               (aggregations-op? :max) (aggregations-op? :std-dev)
               (aggregations-op? :std-dev-p) (aggregations-op? :var)
               (aggregations-op? :var-p)))
      (is (not (or (aggregations-op? :some-key) (aggregations-op? nil)))))

    ;; A few example expressions
    (def project-1 (make-project {"one" (make-attribute-ref "one")
                                  "two" (make-attribute-ref "two")}
                                 tbl1))

    (def project-2
      (make-project
       {"foo"
        (make-case-expr
         {(=$ (make-attribute-ref "two")   ;; alist
              (make-attribute-ref "two"))
          (make-attribute-ref "one")}      ;; default
         (make-attribute-ref "one"))}
       tbl1))
    )
