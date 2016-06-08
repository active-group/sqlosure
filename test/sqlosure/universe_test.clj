(ns sqlosure.universe-test
  (:require [clojure.test :refer :all]
            [sqlosure.universe :refer :all]))

(deftest make-universe-test
  (let [test-universe @(make-universe)]
    (is (= (really-make-universe {} {} {})
           test-universe))
    (is (= (universe-base-relation-table test-universe) {}))
    (is (= (universe-type-table test-universe) {}))
    (is (= (universe-rator-table test-universe) {}))))

(deftest register-type!-test
  (let [test-universe (make-universe)]
    (is (= {:key :value}
           (universe-get-type-table
            (register-type! test-universe :key :value)))))
  (let [new-universe (register-type! (make-universe) :name :type)]
    (is (empty? (universe-get-base-relation-table new-universe)))
    (is (= {:name :type} (universe-get-type-table new-universe)))
    (is (empty? (universe-get-rator-table new-universe)))))

(deftest universe-lookup-type-test
  (let [test-universe (register-type! (make-universe) :name :type)]
    (is (= :type (universe-lookup-type test-universe :name)))
    (is (nil? (universe-lookup-type test-universe :non-in-universe)))))

(deftest register-base-relation!-test
  (let [test-universe (make-universe)]
    (is (= {:name :value}
           (universe-base-relation-table
            @(register-base-relation! test-universe :name :value)))))
  (let [new-universe (-> (make-universe)
                         (register-base-relation! :name "some value")
                         (register-base-relation! :other "other value"))]
    (is (and (= {:name "some value" :other "other value"}
                (universe-base-relation-table @new-universe))
             (empty? (universe-type-table @new-universe))
             (empty? (universe-rator-table @new-universe))))))

(deftest universe-lookup-base-relation-test
  (let [test-universe
        (register-base-relation! (make-universe) :name {:rel "ation"})]
    (is (= {:rel "ation"} (universe-lookup-base-relation test-universe :name)))
    (is (nil? (universe-lookup-base-relation test-universe :non-in-universe)))))

(deftest register-rator!-test
  (let [test-universe (make-universe)]
    (is (= {:name "some value"}
           (universe-rator-table @(register-rator! test-universe
                                                   :name
                                                   "some value"))))
    (is (let [new-universe (-> test-universe
                               (register-rator! :name "some value")
                               (register-rator! :other "other value"))]
          (and (empty? (universe-base-relation-table @new-universe))
               (empty? (universe-type-table @new-universe))
               (= {:name "some value" :other "other value"}
                  (universe-rator-table @new-universe)))))))

(deftest universe-lookup-rator-test
  (let [test-universe
        (register-rator! (make-universe) :name {:rel "ation"})]
    (is (= {:rel "ation"} (universe-lookup-rator test-universe :name)))
    (is (nil? (universe-lookup-rator test-universe :non-in-universe)))))

(deftest universe-get-base-relation-table-test
  (let [test-universe (make-universe)]
    (is (empty? (universe-get-base-relation-table test-universe)))
    (do
      (register-base-relation! test-universe 'relation :relation)
      (= {'relation :wrong} (universe-get-base-relation-table test-universe)))))

(deftest universe-get-type-table-test
  (let [test-universe (make-universe)]
    (is (empty? (universe-get-type-table test-universe)))
    (do
      (register-type! test-universe 'long :long)
      (= {'relation :relation} (universe-get-type-table test-universe)))))
