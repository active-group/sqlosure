(ns sqlosure.sugar-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest function? is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [sugar :refer :all]
             [time :as time]]
            [sqlosure.galaxy.galaxy
             :as
             glxy
             :refer
             [*db-galaxies* initialize-db-galaxies! make&install-db-galaxy]]))

(deftest cons-id-field-test
  (testing "empty-ish sequences"
    (is (= {"id" '$integer-t} (cons-id-field nil)))
    (is (= {"id" '$integer-t} (cons-id-field {})))
    (is (= {"id" '$integer-t} (cons-id-field '()))))
  (testing "non-empty sequences"
    (testing "keywords are turned into strings"
      (is (= {"id" '$integer-t "first" '$string-t}
             (cons-id-field {:first '$string-t}))))
    (testing "well-formed maps"
      (is (= {"id" '$integer-t "first" '$string-t "second" '$timestamp-t}
             (cons-id-field {:first '$string-t "second" '$timestamp-t}))))))

(deftest check-types-test
  (testing "empty-ish sequences"
    (is (true? (check-types nil nil)))
    (is (true? (check-types '() '())))
    (is (true? (check-types [] []))))
  (testing "should throw"
    (testing "with unequal lenghts"
      (is (thrown? Exception (check-types [:id] [$integer-t $boolean-t])))
      (is (thrown? Exception (check-types [:id :first] [$integer-t]))))
    (testing "if types vector contains something other than sqlosure-types"
      (is (thrown? Exception (check-types [:id] [1])))))
  (testing "should return correct error report"
    (is (= [0 1 $string-t]
           (check-types [1 "foobar"] [$string-t $integer-t])))
    (is (= [2 "foobar" $date-t]
           (check-types [0 1 "foobar"] [$integer-t $integer-t $date-t]))))
  (testing "should word with lists and vectors"
    (is (true? (check-types [0 (time/make-date 2015 1 1)]
                            [$integer-t $date-t])))
    (is (true? (check-types (list 0 (time/make-date 2015 5 29))
                            (list $integer-t $date-t))))))

(define-product-type kv {:k $integer-t
                         :v $string-t})

(deftest make-db->val-test
  (let [db->kv (make-db->val $kv)]
    (is (function? db->kv))
    (is (= ($kv 0 1 "foo")
           (db->kv [0 1 "foo"])))
    (is (= ($kv 1 1 "bar")
           (db->kv '(1 1 "bar"))))))

(deftest make-val->db-test
  (let [kv->db (make-val->db [$integer-t $integer-t $string-t])]
    (is (function? kv->db))
    (is (= (glxy/make-tuple [($integer 0)
                             ($integer 23)
                             ($string "foobar")])
           (kv->db ($kv 0 23 "foobar")))))
  (testing "should throw if non sqlosure types are passed"
    (is (thrown? Exception (make-val->db [$integer-t 42])))
    (is (thrown? Exception (make-val->db [$integer-t (type 23)])))))

(deftest make-selector-test
  (let [sel (make-selector "kv-k" $kv-t $integer-t kv-k 1)
        rator (rel/application-rator
               (sel (glxy/make-tuple
                     [($integer 0) ($integer 1) ($string "foobar")])))]
    (is (function? sel))
    (let [res-rator (rel/make-rator "kv-k" (fn [fail t]
                                             (when-not (= t $kv-t)
                                               fail)
                                             $integer-t)
                                    kv-k
                                    :universe sql/sql-universe
                                    :data (glxy/make-db-operator-data
                                           nil
                                           (fn [kv & _]
                                             (first (glxy/tuple-expressions kv)))))]
      (is (= (rel/rator-name res-rator) (rel/rator-name rator)))
      (is (= (glxy/db-operator-data-base-query (rel/rator-data res-rator))
             (glxy/db-operator-data-base-query (rel/rator-data rator)))))))

(deftest db-installer-strings-test
  (testing "it should throw with either or both inputs empty-ish"
    (is (thrown? Exception (db-installer-strings nil nil)))
    (is (thrown? Exception (db-installer-strings nil [])))
    (is (thrown? Exception (db-installer-strings '() nil))))
  (is (= [["id" "INTEGER"]
          ["k" "INTEGER"]
          ["v" "TEXT"]]
         (db-installer-strings ["id" "k" "v"]
                               [$integer-t $integer-t $string-t]))))

;; -- stuff and stuff

(def ^{:dynamic true} *conn* (atom nil))
(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(define-product-type person {"fname" $string-t
                             "lname" $string-t
                             "sex" $boolean-t
                             "birthday" $date-t})

(defn with-person-db
  [spec func]
  (jdbc/with-db-connection [db spec]
    (let [conn (db-connect db)]
      (reset! *db-galaxies* nil)
      (reset! *conn* conn)
      (make&install-db-galaxy "person" $person-t install-person-table!
                              person-table)
      (initialize-db-galaxies! @*conn*)

      ;; Insert a few values
      (db/insert! @*conn* person-table 0 "Marco" "Schneider"
                  false (time/make-date 1989 10 31))
      (db/insert! @*conn* person-table 1 "Helen" "Ahner"
                  true (time/make-date 1990 10 15))
      (db/insert! @*conn* person-table 2 "Frederike" "Guggemos"
                  true (time/make-date 1989 12 4))
      (db/insert! @*conn* person-table 3 "Tim" "Rach"
                  false (time/make-date 1991 6 2))

      (func))))

(def $sex=female
  (fn [ps]
    ($= ($person-sex (! ps))
        ($boolean true))))

(def $can-buy-alcohol
  (fn [ps]
    ($> ($person-birthday (! ps))
        ($date
         (.minusYears (java.time.LocalDate/now)
                      18)))))

(def $older-than
  (fn [years ps]
    ($> ($person-birthday (! ps)) ($date (.minusYears (time/make-date)
                                                      years)))))

(def $women-older-than
  (fn [years ps]
    ($and ($= ($person-sex (! ps)) ($boolean true))
          ($older-than years ps))))

(with-person-db db-spec
  (fn []
    (db/db-query-reified-results
     @*conn*
     (query [ps (<- person-galaxy)]
            (restrict ($women-older-than 25 ps))
            (order {($person-fname (! ps)) :descending})
            (top 1)
            (project {"name" ($person-fname (! ps))})))
    (put-query
     (query [ps (<- person-galaxy)]
            (restrict ($women-older-than 25 ps))
            (order {($person-fname (! ps)) :descending})
            (top 1)
            (project {"name" ($person-fname (! ps))})))))
