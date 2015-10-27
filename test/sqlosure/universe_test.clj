(ns sqlosure.universe-test
  (:require [sqlosure.universe :refer :all]
            [clojure.test :refer :all]))

(deftest make-universe-test
  (is (= (really-make-universe {} {} {})
         (make-universe)))
  (is (= (universe-base-relation-table (make-universe)) {}))
  (is (= (universe-type-table (make-universe)) {}))
  (is (= (universe-rator-table (make-universe)) {})))

(deftest register-type-test
  (let [test-universe (make-universe)]
    (is (= {:key :value}
           (universe-type-table
            (register-type test-universe :key :value))))
    (is (let [new-universe (register-type test-universe :name :type)]
          (and (empty? (universe-base-relation-table new-universe))
               (= {:name :type} (universe-type-table new-universe))
               (empty? (universe-rator-table new-universe)))))))

(deftest universe-lookup-type-test
  (let [test-universe (register-type (make-universe) :name :type)]
    (is (= :type (universe-lookup-type test-universe :name)))
    (is (nil? (universe-lookup-type test-universe :non-in-universe)))))

(deftest register-base-relation-test
  (let [test-universe (make-universe)]
    (is (= {:name "some value"}
           (universe-base-relation-table (register-base-relation test-universe
                                                                 :name
                                                                 "some value"))))
    (is (let [new-universe (-> test-universe
                               (register-base-relation :name "some value")
                               (register-base-relation :other "other value"))]
          (and (= {:name "some value" :other "other value"}
                  (universe-base-relation-table new-universe))
               (empty? (universe-type-table new-universe))
               (empty? (universe-rator-table new-universe)))))))

(deftest universe-lookup-base-relation-test
  (let [test-universe
        (register-base-relation (make-universe) :name {:rel "ation"})]
    (is (= {:rel "ation"} (universe-lookup-base-relation test-universe :name)))
    (is (nil? (universe-lookup-base-relation test-universe :non-in-universe)))))

(deftest register-rator-test
  (let [test-universe (make-universe)]
    (is (= {:name "some value"}
           (universe-rator-table (register-rator test-universe
                                                 :name
                                                 "some value"))))
    (is (let [new-universe (-> test-universe
                               (register-rator :name "some value")
                               (register-rator :other "other value"))]
          (and (empty? (universe-base-relation-table new-universe))
               (empty? (universe-type-table new-universe))
               (= {:name "some value" :other "other value"}
                  (universe-rator-table new-universe)))))))

(deftest universe-lookup-rator-test
  (let [test-universe
        (register-rator (make-universe) :name {:rel "ation"})]
    (is (= {:rel "ation"} (universe-lookup-rator test-universe :name)))
    (is (nil? (universe-lookup-rator test-universe :non-in-universe)))))
