(ns sqlosure.type-test
  (:require [sqlosure.type :refer :all]
            [clojure.test :refer :all]))

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

(deftest nullable-test
  (let [really-nullable (really-make-nullable-type 'string)]
    (is (nullable really-nullable))
    (is (nullable 'boolean))
    (is (= (nullable-type-underlying (nullable 'boolean)) 'boolean))
    (is (not= (nullable-type-underlying (nullable 'string)) nil))))

;; TODO: Test type-member?

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
