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

(def tbl1 (make-base-relation 'tbl1
                              (alist->rel-scheme [["one" string%]
                                                  ["two" integer%]])
                              :universe (make-universe)
                              :handle "tbl1"))

(deftest make-project-test
  (let [p (make-project [["two" (make-attribute-ref "two")]
                         ["one" (make-attribute-ref "one")]]
                        tbl1)]
    (is (= [["two" (make-attribute-ref "two")]
            ["one" (make-attribute-ref "one")]]
           (project-alist p)))
    (is (= tbl1 (project-query p)))))

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
    (is (base-type? (expression-type the-empty-environment string-const)))
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
      (is (= the-empty-rel-scheme (query-scheme (make-empty-val)))))

    
    (testing "base relation"
      (is (= (alist->rel-scheme [["one" string%]
                                 ["two" integer%]])
             (query-scheme tbl1 :typecheck? true))))

    (testing "projection"
      (let [p (make-project [["two" (make-attribute-ref "two")]
                             ["one" (make-attribute-ref "one")]]
                            tbl1)
            p2 (make-project [["two" (make-attribute-ref "two")]
                              ["count_twos" (make-aggregation :count (make-attribute-ref "two"))]]
                             tbl1)
            res (query-scheme p :typecheck? true)]
        (is (= (rel-scheme-alist res) {"two" integer% "one" string%}))
        (is (= {"two" integer% "count_twos" integer%} (rel-scheme-alist (query-scheme p2))))
        (is (thrown? Exception  ;; should fail with typechecking because of aggregation
                     (query-scheme (make-project
                                    [["two" (make-attribute-ref "two")]
                                     ["one" (make-aggregation
                                             :min
                                             (make-tuple [(make-const integer% 42)
                                                          (make-const integer% 23)]))]]
                                    tbl1)
                                   :typecheck? true)))))

    (testing "restriction"
      (let [r (make-restrict (sql/=$ (make-scalar-subquery
                                      (make-project [["C" (make-attribute-ref "C")]]
                                                    SUBB))
                                     (make-attribute-ref "C"))
                             SUBA)]
        (is (= (rel-scheme-alist (query-scheme r)) {"C" string%}))
        (is (thrown? Exception (query-scheme r :typecheck? true)))))
      

    (testing "outer restriction"
      (let [r (make-restrict-outer (sql/=$ (make-scalar-subquery
                                            (make-project [["C" (make-attribute-ref "C")]]
                                                          SUBB))
                                           (make-attribute-ref "C"))
                                   SUBA)]
        (is (= (rel-scheme-alist (query-scheme r)) {"C" string%}))
        (is (thrown? Exception (query-scheme r :typecheck? true)))))

    (testing "grouping"
      (is (lens/overhaul (alist->rel-scheme [["one" string%]
                                             ["two" integer%]])
                         rel-scheme-grouped-lens
                         #{"one"})
          (query-scheme (make-group #{"one"} tbl1)))
      (is (lens/overhaul (alist->rel-scheme [["one" string%]
                                             ["two" integer%]])
                         rel-scheme-grouped-lens
                         #{"one" "two"})
          (query-scheme (make-group #{"two"} (make-group #{"one"} tbl1))))
      (is (thrown-with-msg?
           Throwable
           #"type violation"
           (query-scheme (make-project [["x" (make-attribute-ref "one")]
                                        ["y" (make-aggregation :max
                                                               (make-attribute-ref "two"))]]
                                       tbl1)
                         :typecheck? true)))

      (is (thrown-with-msg?
           Throwable
           #"type violation"
           (query-scheme (make-project [["x" (make-attribute-ref "one")]]
                                       (make-group #{"two"} tbl1))
                         :typecheck? true))))


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
    ;; order
    (let [o (make-order {(make-attribute-ref "one") :ascending} tbl1)]
      (is (= (rel-scheme-alist (query-scheme o))
             {"one" string% "two" integer%})))))

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
  (is (= (list 'const (list 'string) "foobar")
         (expression->datum
          (make-const string% "foobar"))))
  (is (= (list 'null-type (list 'string))
         (expression->datum
          (make-null string%))))
  (is (= (list 'application '>= (list (list 'const '(integer) 42)
                                      (list 'const '(integer) 23)))
         (expression->datum (sql/>=$ (make-const integer% 42)
                                     (make-const integer% 23)))))
  (is (= (list 'tuple
               (list 'const '(string) "foobar")
               (list 'const '(integer) 42))
         (expression->datum (make-tuple [(make-const string% "foobar")
                                         (make-const integer% 42)]))))
  (is (= (list 'aggregation :count
               (list 'tuple
                     (list 'const '(integer) 40)
                     (list 'const '(integer) 2)))
         (expression->datum (make-aggregation :count (make-tuple [(make-const integer% 40)
                                                                  (make-const integer% 2)]))))))

(deftest query->datum-test
  (is (= (list 'empty-val) (query->datum (make-empty-val))))
  (is (= (list 'base-relation 'tbl1) (query->datum tbl1)))
  (is (= (list 'project (list (list "two" 'attribute-ref "two")
                              (list "one" 'attribute-ref "one"))
               '(base-relation tbl1))
         (query->datum (make-project [["two" (make-attribute-ref "two")]
                                      ["one" (make-attribute-ref "one")]]
                                     tbl1))))
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
           (query->datum r))))


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

(deftest datum->query-test
  (let [test-universe (register-base-relation! (make-universe)
                                               'tbl1 tbl1)
        query->datum->query #(-> % query->datum (datum->query test-universe))]
    (is (= the-empty (datum->query '(empty-val) test-universe)))
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
      (is (= t (query->datum->query t))))))

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
    (is (= (expression->datum->expression (make-tuple [(make-const integer% 40)
                                                       (make-const integer% 2)]))
           (make-tuple [(make-const integer% 40)
                        (make-const integer% 2)])))
    (is (= (expression->datum->expression (make-aggregation
                                           :count (make-tuple [(make-const integer% 40)
                                                               (make-const integer% 2)])))
           (make-aggregation
            :count (make-tuple [(make-const integer% 40)
                                (make-const integer% 2)]))))
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
  (is (nil? (query-attribute-names tbl1)))
  (is (= #{"two" "one"} (query-attribute-names
                            (make-project [["two" (make-attribute-ref "two")]
                                           ["one" (make-attribute-ref "one")]]
                                          tbl1))))
  (let [test-universe (make-universe)
        SUBB (make-base-relation 'SUBB
                                 (alist->rel-scheme [["C" string%]])
                                 :universe test-universe
                                 :handle "SUBB")
        SUBA (make-base-relation 'SUBA
                                 (alist->rel-scheme [["C" string%]])
                                 :universe test-universe
                                 :handle "SUBA")
        r1 (make-restrict (sql/=$ (make-scalar-subquery
                                   (make-project [["C" (make-attribute-ref "C")]
                                                  ["D" (make-attribute-ref "D")]]
                                                 SUBB))
                                  (make-attribute-ref "C"))
                          SUBA)
        r2 (make-restrict (sql/=$ (make-attribute-ref "C")
                                  (make-attribute-ref "C"))
                          (make-left-outer-product SUBB SUBA))]
    (is (= #{"C" "D"} (query-attribute-names r1)))
    (is (= #{"C"} (query-attribute-names r2))))
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
                         tbl1) ; FIXME: doesn't typecheck
        c (make-product rel1 p1)
        q (make-quotient p1 rel1)
        u (make-union p1 p2)
        l (make-left-outer-product rel1 p1)]
    (is (= #{"two" "one"} (query-attribute-names c)))
    (is (= #{"two" "one"} (query-attribute-names q)))
    (is (= #{"two" "one" "three" "four"} (query-attribute-names u)))
    (is (= #{"two" "one"} (query-attribute-names l))))
  (is (= #{"one" "two"} (query-attribute-names
                          (make-order {(make-attribute-ref "one") :ascending}
                                      tbl1))))
  (is (nil? (query-attribute-names (make-top 5 10 tbl1)))))

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
  (is (= the-empty (query-substitute-attribute-refs {} the-empty)))
  (is (= tbl1 (query-substitute-attribute-refs {} tbl1))))

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
