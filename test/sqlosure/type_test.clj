(ns sqlosure.type-test
  (:require [sqlosure.type :refer :all]
            [sqlosure.universe :as u]
            [sqlosure.time :as time]
            [clojure.test :refer :all]))

(deftest make-base-type-test
  (let [test-universe (u/make-universe)
        string-type (make-base-type 'string string? values values
                                    :universe test-universe)]
    (is (and (= (base-type-name string-type) 'string)
             ;; I was not aware one could test fns on equality. Why does this work?
             ;; Of even more important, why is this still wrong?
             (= (base-type-predicate string-type) string?)
             (= (base-type-const->datum-proc string-type) values)
             (= (base-type-datum->const-proc string-type) values)
             (nil? (base-type-data string-type))
             (u/universe-lookup-type test-universe 'string)))))


(deftest values-test
  (is (= [1 2 3] (values 1 2 3)))
  (is (not= [1 2 3] (values 1 2)))
  (is (= [1] (values 1))))

(deftest double?-test
  (is (double? 0.3))
  (is (double? 42.0))
  (is (not (double? 1)))
  (is (not (double? nil)))
  (is (not (double? "string")))
  (is (not (double? :keyword))))

(deftest boolean?-test
  (is (boolean? true))
  (is (boolean? false))
  (is (not (boolean? 42)))
  (is (not (boolean? "string")))
  (is (not (boolean? :keyword))))

(deftest date?-test
  (is (date? (time/make-date 2000 1 1)))
  (is (date? (java.time.LocalDate/of 1989 10 31)))
  (is (not (date? 42))))

(deftest make-nullable-type-test
  (let [really-nullable (really-make-nullable-type 'string)]
    (is (not (nullable-type? string%)))
    (is (nullable-type? (make-nullable-type really-nullable)))
    (is (nullable-type? (make-nullable-type 'boolean)))
    (is (= (nullable-type-underlying (make-nullable-type 'boolean)) 'boolean))
    (is (not= (nullable-type-underlying (make-nullable-type 'string)) nil))))

;; TODO: Test type-member?
(deftest type-member?-test
  ;; base types
  (is (type-member? 42 integer%))
  (is (not (type-member? "false" integer%)))
  (is (not (type-member? 42.0 integer%)))
  ;; nullable
  (is (type-member? nil nullable-integer%))
  (is (type-member? 1 nullable-integer%))
  (is (not (type-member? "string" nullable-integer%)))
  ;; bounded-string-type
  (let [string-5% (make-bounded-string-type 6)]
    (is (type-member? "foobar" string-5%))
    (is (type-member? "foo" string-5%))
    (is (not (type-member? "arteides" string-5%))))
  ;; product-type
  (let [my-product% (make-product-type [string% integer%])]
    (is (type-member? ["foobar" 42] my-product%))
    (is (not (type-member? [42 "foobar"] my-product%))))
  ;; set-type -- okay now i really don't know what a set type is supposed to be...
  (let [my-set% (make-set-type string%)]
    (is (type-member? ["foobar" "fizzbuzz"] my-set%))
    (is (not (type-member? ["foobar" 42] my-set%)))
    (is (not (type-member? [42 "foobar"] my-set%)))
    (is (not (type-member? [42 false] my-set%)))))

(deftest numeric-type?-test
  (is (numeric-type? integer%))
  (is (numeric-type? double%))
  (is (not (numeric-type? boolean%)))
  (is (not (numeric-type? string%))))

(deftest ordered-type?-test
  (is (ordered-type? integer%))
  (is (ordered-type? double%))
  (is (ordered-type? string%))
  (is (not (ordered-type? boolean%))))

(deftest type=?-test
  (is (type=? string% string%))
  (is (not (type=? string%-nullable string%)))
  (let [string-5% (make-bounded-string-type 6)]
    (is (type=? string-5% string-5%))
    (is (not (type=? string% string-5%))))
  (let [my-product1% (make-product-type [string% integer%])
        my-product2% (make-product-type [integer% string%])]
    (is (type=? my-product1% my-product1%))
    (is (not (type=? my-product1% my-product2%)))
    (is (not (type=? string% my-product2%))))
  (let [my-set1% (make-set-type string%)
        my-set2% (make-set-type integer%)]
    (is (type=? my-set1% my-set1%))
    (is (not (type=? my-set1% string%)))))

(deftest type->datum-test
  (is (= (type->datum string%) (list 'string)))
  (is (= (type->datum integer%) (list 'integer)))
  (is (= (type->datum string%-nullable) (list 'nullable (list 'string))))
  (is (= (type->datum (make-product-type [string% integer%]))
         (list 'product [(list 'string) (list 'integer)])))
  (is (= (type->datum (make-set-type string%))
         (list 'set (list 'string))))
  (is (= (type->datum (make-bounded-string-type 5))
         (list 'bounded-string 5))))

(deftest datum->type-test
  (let [test-universe (u/make-universe)
        my-bounded-string% (make-bounded-string-type 6)
        my-product% (make-product-type [string% integer%])
        my-set% (make-set-type double%)]
    (is (= (datum->type (list 'string) test-universe) string%))
    (is (= (datum->type (list 'integer) test-universe) integer%))
    (is (= (datum->type (list 'double) test-universe) double%))
    (is (= (datum->type (list 'boolean) test-universe) boolean%))
    (is (= (datum->type (list 'blob) test-universe) blob%))
    (is (= (datum->type (list 'bounded-string 6) test-universe)
           my-bounded-string%))
    (is (= (datum->type (list 'nullable (list 'string)) test-universe)
           (make-nullable-type string%)
           string%-nullable))
    (is (= (datum->type (list 'product [(list 'string) (list 'integer)])
                        test-universe)
           my-product%))
    (is (= (datum->type (list 'set (list 'double)) test-universe)
           my-set%))
    (do
      (is (thrown? Exception (datum->type (list 'long) test-universe)))
      (u/register-type! test-universe 'long double%)
      (is (= (datum->type (list 'long) test-universe)
             double%)))))

(deftest const->datum-test
  (let [my-bounded% (make-bounded-string-type 5)
        my-product% (make-product-type [integer% string%])
        my-set% (make-set-type integer%)]
    (is (= (const->datum string% "foobar") "foobar"))
    (is (= (const->datum double% 42.0) 42.0))
    (is (= (const->datum double%-nullable 42.0) 42.0))
    (is (= (const->datum double%-nullable nil) nil))
    (is (= (const->datum my-bounded% "foobar") "foobar"))
    (is (= (const->datum my-product% [42 "foobar"]) [42 "foobar"]))
    (is (thrown? Exception (const->datum my-product% 42)))
    (is (= (const->datum my-set% [23 42]) [23 42]))
    (is (thrown? Exception (const->datum :non-existent-type 42.0)))))

(deftest datum->const-test
  (let [my-bounded% (make-bounded-string-type 5)
        my-product% (make-product-type [integer% string%])
        my-set% (make-set-type integer%)]
    (is (= (datum->const double% 42.0) 42.0))
    (is (= (datum->const string% "foobar") "foobar"))
    (is (= (datum->const my-bounded% "foobar") "foobar"))
    (is (= (datum->const my-product% [42 "foobar"]) [42 "foobar"]))
    (is (= (datum->const my-set% ["foo" "bar"]) ["foo" "bar"]))
    (is (thrown? Exception (datum->const my-product% 42)))
    (is (thrown? Exception (datum->const my-set% 42)))))
