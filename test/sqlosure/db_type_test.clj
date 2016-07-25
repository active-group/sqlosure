(ns sqlosure.db-type-test
  (:require [sqlosure.db-type :as db-type]
            [sqlosure.core :as sq]
            [clojure.test :refer [deftest testing is]]
            [sqlosure.time :as time]
            [sqlosure.db-connection :as db]))

(deftest cons-id-field-test
  (testing "empty-ish sequences"
    (is (= {"id" sq/$integer-t} (db-type/cons-id-field nil)))
    (is (= {"id" sq/$integer-t} (db-type/cons-id-field {})))
    (is (= {"id" sq/$integer-t} (db-type/cons-id-field '()))))
  (testing "non-empty sequences"
    (testing "keywords are turned into strings"
      (is (= {"id" sq/$integer-t "first" sq/$string-t}
             (db-type/cons-id-field {:first sq/$string-t}))))
    (testing "well-formed maps"
      (is (= {"id" sq/$integer-t "first" sq/$string-t "second" sq/$timestamp-t}
             (db-type/cons-id-field {:first sq/$string-t
                                     "second" sq/$timestamp-t}))))))

(deftest all-sqlosure-types?-test
  (is (db-type/all-sqlosure-types? []))
  (is (db-type/all-sqlosure-types? [sq/$integer-t]))
  (is (db-type/all-sqlosure-types? [sq/$integer-t sq/$double-t]))
  (is (not (db-type/all-sqlosure-types? [42]))))

(deftest db-name-of-test
  (is (= "foo" (db-type/db-name-of "foo")))
  (is (= "foo_bar" (db-type/db-name-of "foo-bar")))
  (is (= "foo" (db-type/db-name-of :foo)))
  (is (= "foo_bar" (db-type/db-name-of :foo-bar)))
  (is (= "foo" (db-type/db-name-of 'foo)))
  (is (= "foo_bar" (db-type/db-name-of 'foo-bar))))
