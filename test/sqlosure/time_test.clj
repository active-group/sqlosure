(ns sqlosure.time-test
  (:require [sqlosure.time :refer :all]
            [clojure.test :refer :all]))

(def d1 (java.time.LocalDate/of 1989 10 31))
(def t1 (java.time.LocalDateTime/of 1989 10 31 0 0))

(deftest date-identity-test
  (is (= d1
         (-> d1
             to-sql-date
             from-sql-date))))

(deftest timestamp-identity-test
  (is (= t1
         (-> t1
             to-sql-timestamp
             from-sql-timestamp))))

(deftest coerce-time-values-test
  (is (= (list (to-sql-date d1) (to-sql-timestamp t1) :foo :bar 42)
         (coerce-time-values [d1 t1 :foo :bar 42]))))
