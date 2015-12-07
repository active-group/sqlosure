(ns sqlosure.optimization-test
  (:require [sqlosure.optimization :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.type :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(deftest project-alist-substitute-attribute-refs-test
  (is (= {"one" (make-const integer% 42)}
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  {"one" (make-attribute-ref "one")})))
  (is (= {"one" (make-const integer% 42)
          "two" (make-attribute-ref "two")}
         (project-alist-substitute-attribute-refs {"one" (make-const integer% 42)}
                                                  {"one" (make-attribute-ref "one")
                                                   "two" (make-attribute-ref "two")}))))

(deftest order-alist-attribute-names-test
  (let [alist {(make-attribute-ref "one") :ascending
               (make-attribute-ref "two") :descending}]
    (is (= '("one" "two")
           (order-alist-attribute-names alist)))
    (is (empty?
           (order-alist-attribute-names nil)))))

(deftest intersect-live-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (make-rel-scheme {"one" string%
                                                   "two" integer%})
                                 :handle "tbl1")
        p (make-project {"one" (make-attribute-ref "one")
                         "two" (make-attribute-ref "two")}
                        tbl1)]
    (is (= ["one"]
           (intersect-live ["one"] p)))
    (is (= ["two" "one"]
           (intersect-live ["one" "two"] p)))))

(deftest remove-dead-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (make-rel-scheme {"one" string%
                                                   "two" integer%
                                                   "three" double%})
                                 :handle "tbl1")
        p1 (make-project {"one" (make-attribute-ref "one")
                          "two" (make-attribute-ref "two")}
                         tbl1)
        p2 (make-project {"one" (make-attribute-ref "one")}
                         (make-project {"one" (make-attribute-ref "one")
                                        "two" (make-attribute-ref "two")}
                                       tbl1))
        po1 (make-project {"one" (make-attribute-ref "one")}
                          (make-order {(make-attribute-ref "one") :ascending}
                                      tbl1))]
    (is (= p1 (remove-dead p1)))
    (is (not= p2 (remove-dead p2)))
    (is (= po1 (remove-dead po1)))))

(deftest merge-project-test
  (let [tbl1 (make-base-relation "tbl1"
                                 (make-rel-scheme {"one" string%
                                                   "two" integer%
                                                   "three" double%})
                                 :handle "tbl1")]
    (is (= nil (merge-project nil)))))