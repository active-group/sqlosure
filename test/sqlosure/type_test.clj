(ns sqlosure.type-test
  (:require [sqlosure.type :refer :all]
            [sqlosure.universe :as u]
            [sqlosure.time :as time]
            [clojure.test :refer :all]))

(deftest make-base-type-test
  (let [test-universe (u/make-universe)
        string-type (make-base-type 'string string? {:universe test-universe})]
    (is (and (= (base-type-name string-type) 'string)
             (u/universe-lookup-type test-universe 'string)))))

(deftest date?-test
  (is (date? (time/make-date 2000 1 1)))
  (is (date? (java.time.LocalDate/of 1989 10 31)))
  (is (not (date? 42))))

(deftest byte-array?-test
  (testing "an empty byte array of fixed length"
    (is (byte-array? (byte-array 10))))
  (testing "a byte array filled with some values"
    (is (byte-array? (byte-array [(byte 0x43) (byte 0x6c) (byte 0x6f)]))))
  (testing "nil or another seq should not be a byte array"
    (is (not (byte-array? nil)))
    (is (not (byte-array? [1 2 3])))
    (is (not (byte-array? (range 0 10))))))

(deftest make-nullable-type-test
  (let [really-nullable (make-nullable-type string%)]
    (is (not (nullable-type? string%)))
    (is (nullable-type? really-nullable))
    (is (= really-nullable (make-nullable-type really-nullable)))
    (is (= (non-nullable-type (make-nullable-type boolean%)) boolean%))))

(deftest null?-test
  (is (null? []))
  (is (null? '()))
  (is (null? {}))
  (is (null? #{}))
  (is (null? nil)))

(deftest type-member?-test
  (testing "base types"
    (is (type-member? 42 integer%))
    (is (not (type-member? "false" integer%)))
    (is (not (type-member? 42.0 integer%))))
  (testing "nullable types"
    (is (type-member? nil integer%-nullable))
    (is (type-member? 1 integer%-nullable))
    (is (not (type-member? "string" integer%-nullable))))
  (testing "bounded string type"
    (let [string-5% (make-bounded-string-type 6)]
      (is (type-member? "foobar" string-5%))
      (is (type-member? "foo" string-5%))
      (is (not (type-member? "arteides" string-5%)))))
  (testing "product type"
    (let [my-product% (make-product-type [string% integer%])]
      (is (type-member? ["foobar" 42] my-product%))
      (is (not (type-member? [42 "foobar"] my-product%)))))
  (testing "set type"
    (let [my-set% (make-set-type string%)]
      (is (type-member? ["foobar" "fizzbuzz"] my-set%))
      (is (not (type-member? ["foobar" 42] my-set%)))
      (is (not (type-member? [42 "foobar"] my-set%)))
      (is (not (type-member? [42 false] my-set%)))))
  (is (thrown? Exception (type-member? String "foobar"))))

(deftest numeric-type?-test
  (is (numeric-type? integer%))
  (is (numeric-type? double%))
  (is (not (numeric-type? boolean%)))
  (is (not (numeric-type? string%))))

(deftest ordered-type?-test
  (is (ordered-type? integer%))
  (is (ordered-type? double%))
  (is (ordered-type? string%))
  (is (ordered-type? timestamp%))
  (is (not (ordered-type? boolean%))))

#_(deftest type->datum-test
  (is (= (type->datum string%) 'string))
  (is (= (type->datum integer%) 'integer))
  (is (= (type->datum string%-nullable) '(nullable string)))
  (is (= (type->datum (make-product-type [string% integer%]))
         '(product [string integer])))
  (is (= (type->datum (make-set-type string%))
         '(set string)))
  (is (= (type->datum (make-bounded-string-type 5))
         '(bounded-string 5)))
  (testing "anything else should return an assertion"
    (is (thrown? Exception (type->datum nil)))))

#_(deftest datum->type-test
  (let [test-universe (u/make-universe)
        my-bounded-string% (make-bounded-string-type 6)
        my-product% (make-product-type [string% integer%])
        my-set% (make-set-type double%)]
    (is (= (datum->type 'string test-universe) string%))
    (is (= (datum->type 'integer test-universe) integer%))
    (is (= (datum->type 'double test-universe) double%))
    (is (= (datum->type 'boolean test-universe) boolean%))
    (is (= (datum->type 'blob test-universe) blob%))
    (is (= (datum->type 'date test-universe) date%))
    (is (= (datum->type 'timestamp test-universe) timestamp%))
    (is (= (datum->type '(bounded-string 6) test-universe)
           my-bounded-string%))
    (is (= (datum->type '(nullable string) test-universe)
           (make-nullable-type string%)
           string%-nullable))
    (is (= (datum->type '(product [string integer])
                        test-universe)
           my-product%))
    (is (= (datum->type '(set double) test-universe)
           my-set%))
    (do
      (is (thrown? Exception (datum->type 'long) test-universe))
      (u/register-type! test-universe 'long double%)
      (is (= (datum->type 'long test-universe)
             double%)))))

#_(deftest const->datum-test
  (let [my-bounded% (make-bounded-string-type 5)
        my-product% (make-product-type [integer% string%])
        my-set% (make-set-type integer%)]
    (is (= (const->datum string% "foobar") "foobar"))
    (is (= (const->datum double% 42.0) 42.0))
    (is (= (const->datum double%-nullable 42.0) 42.0))
    (is (= (const->datum double%-nullable nil) nil))
    (is (= (const->datum my-bounded% "foobar") "foobar"))
    (testing "product"
      (is (= (const->datum my-product% [42 "foobar"]) [42 "foobar"]))
      (is (= (const->datum my-product% '(42 "foobar")) [42 "foobar"])))
    (is (= (const->datum my-set% [23 42]) [23 42]))
    (is (thrown? Exception (const->datum my-product% 42)))
    (is (thrown? Exception (const->datum :non-existent-type 42.0)))
    (is (thrown? Exception (const->datum my-product% 42)))))

#_(deftest datum->const-test
  (let [my-bounded% (make-bounded-string-type 5)
        my-product% (make-product-type [integer% string%])
        my-set% (make-set-type integer%)
        my-nullable% (make-nullable-type string%)]
    (is (= (datum->const double% 42.0) 42.0))
    (is (= (datum->const string% "foobar") "foobar"))
    (is (= (datum->const my-bounded% "foobar") "foobar"))
    (is (= (datum->const my-product% [42 "foobar"]) [42 "foobar"]))
    (is (= (datum->const my-set% ["foo" "bar"]) ["foo" "bar"]))
    (testing "nullable"
      (is (= (datum->const my-nullable% "foo") "foo"))
      (is (= (datum->const my-nullable% nil) nil)))
    (is (thrown? Exception (datum->const my-product% 42)))
    (is (thrown? Exception (datum->const my-set% 42)))
    (is (thrown? Exception (datum->const String "foobar")))))
