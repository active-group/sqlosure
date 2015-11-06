(ns sqlosure.type-test
  (:require [sqlosure.type :refer :all]
            [sqlosure.universe :as u]
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
