(ns sqlosure.relational-algebra-test
  (:require [sqlosure.relational-algebra :refer :all]
            [sqlosure.universe :refer :all]
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
    (let [[rel u] (make-base-relation "name"
                                      "scheme"
                                      :universe test-universe)]
      (is (= {"name" rel} (universe-base-relation-table u)))
      (is (= (really-make-base-relation "name" "scheme" nil) rel)))
    (let [[rel u] (make-base-relation "name"
                                      "scheme"
                                      :universe test-universe
                                      :handle "some handle")]
      (is (= {"name" rel} (universe-base-relation-table u)))
      (is (= (really-make-base-relation "name" "scheme" "some handle") rel)))))

(deftest query?-test
  (is (query? '())
      (query? nil))
  (is (query? (make-base-relation "name" "scheme"))
      (query? (make-base-relation "name" "scheme" :universe (make-universe)
                                  :handle "some handle"))))
