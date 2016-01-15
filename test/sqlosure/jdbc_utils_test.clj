(ns sqlosure.jdbc-utils-test
  (:require [clojure.test :refer :all]
            [sqlosure.jdbc-utils :refer :all]
            [sqlosure.type :as type]))

(deftest query-row-fn-test
  (is (= ["42"]
         (query-row-fn (fn [tt v] (str v))
                       [type/integer%]
                       [42]))))

(deftest result-set-fn-test
  ;; Basics
  (is (= [[13 42]]
         (result-set-fn true nil nil ["col1" "col2"] [[:col1 :col2] [[13 42]]])))
  (is (= [[42 13]]
         (result-set-fn true reverse nil ["col1" "col2"] [[:col1 :col2] [[13 42]]])))
  (is (= [{"col1" 13 "col2" 42}]
         (result-set-fn false nil nil ["col1" "col2"] [[:col1 :col2] [[13 42]]])))
  ;; Test that the resultset is actually a lazy-sequence
  (is (not (realized? (result-set-fn true identity identity ["col1"] [[:col1] (lazy-seq [[42] [21]])]))))
  )
