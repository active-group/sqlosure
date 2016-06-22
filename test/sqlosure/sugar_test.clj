(ns sqlosure.sugar-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest function? is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [galaxy :as glxy :refer [*db-galaxies* initialize-db-galaxies! make&install-db-galaxy]]
             [relational-algebra :as rel]
             [sql :as sql]
             [sugar :refer :all]
             [time :as time]]))

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
  (let [db->kv (make-db->val $kv [$integer-t $integer-t $string-t])]
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
    (let [res-rator (rel/make-rator
                     "kv-k" (fn [fail t]
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
                               [$integer-t $integer-t $string-t])))
  (is (= [["id" "INTEGER"]
          ["kv" "INTEGER"]]
         (db-installer-strings ["id" "kv"]
                               [$integer-t $kv-t]))))

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
    (let [conn (db-connect db)
          ins-person (fn [p]
                       (db/insert! @*conn* person-galaxy p))]
      (reset! *db-galaxies* nil)
      (reset! *conn* conn)
      (make&install-db-galaxy "person" $person-t install-person-table!
                              person-table)
      (initialize-db-galaxies! @*conn*)

      ;; Insert a few values
      (ins-person ($person 0 "Marco" "Schneider" false (time/make-date 1989 10 31)))
      (ins-person ($person 1 "Helen" "Ahner" true (time/make-date 1990 10 15)))
      (ins-person ($person 2 "Frederike" "Guggemos" true (time/make-date 1989 12 4)))
      (ins-person ($person 3 "Tim" "Rach" false (time/make-date 1991 6 2)))
      (ins-person ($person 4 "John" "Doe" false (time/make-date 2016 6 9)))
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

#_(with-person-db db-spec
  (fn []
    (db/run-query
     @*conn*
     (query [ps (<- person-galaxy)]
            (restrict ($women-older-than 25 ps))
            (project ps)
           #_ (project {"name" ($person-fname (! ps))
                      "sex" ($person-sex (! ps))}))
     {:galaxy-query? true})))

(define-product-type point {:x $integer-t
                            :y $integer-t})

(define-product-type circle {:center $point-t
                             :radius $double-t})

(define-product-type rect {:bot_left $point-t
                           :top_right $point-t})

(defn with-shapes-db
  [spec func]
  (jdbc/with-db-connection [db spec]
    (let [conn (db-connect db)
          ins-point (fn [p]
                      (db/insert! @*current-db-connection* point-galaxy p))
          ins-circle (fn [c]
                       (db/insert! @*current-db-connection* circle-galaxy c))
          ins-rect (fn [r]
                     (db/insert! @*current-db-connection* rect-galaxy r))]
      (reset! *db-galaxies* nil)

      (set-db-connection! conn)

      (make&install-db-galaxy "point" $point-t install-point-table!
                              point-table)
      (make&install-db-galaxy "circle" $circle-t install-circle-table!
                              circle-table)
      (make&install-db-galaxy "rect" $rect-t install-rect-table!
                              rect-table)

      (initialize-db-galaxies! @*current-db-connection*)

      ;; Insert a few values
      (let [p0 ($point 0 0 0)
            p1 ($point 1 0 1)
            p2 ($point 2 1 0)
            p3 ($point 3 1 1)
            p4 ($point 4 2 2)
            p5 ($point 5 3 3)
            c1 ($circle 0 p0 1.0)
            c2 ($circle 1 p1 2.8)
            c3 ($circle 2 p3 0.5)
            r1 ($rect 0 p0 p3)
            r2 ($rect 1 p0 p4)
            r3 ($rect 2 p1 p5)
            ]
        (doall (map ins-point [p0 p1 p2 p3]))
        (doall (map ins-circle [c1 c2 c3]))
        (doall (map ins-rect [r1 r2 r3])))
      (func))))

(defn make-db-operator
  [name in-range out data]
  (rel/make-monomorphic-combinator name in-range out nil
                                   :universe sql/sql-universe
                                   :data data))

(def $point=
  (make-db-operator "point=" [$point-t $point-t] $boolean-t
                    (glxy/make-db-operator-data
                     (fn [p1 p2 & args]
                       (println "$point=2" p1 p2)
                       ($= p1 p2)))))

(defn $share-point?
  [circle rect]
  ($or ($point= ($circle-center (! circle))
                ($rect-bot_left (! rect)))
       ($point= ($circle-center (! circle))
                ($rect-top_right (! rect)))))

(def $square (fn [x] ($times x x)))
(def $area (fn [r] ($times ($double Math/PI) ($square r))))

(def $circle-area
  (make-db-operator "circle-area" [$circle-t] $double-t
                    (glxy/make-db-operator-data
                     (fn [c & _]
                       ($area (get (glxy/tuple-expressions c) 2))))))

(with-shapes-db db-spec
  (fn []
    (let [$areas (query [cs (<- circle-galaxy)]
                        (project {"area" ($circle-area (! cs))}))]
      (run (query [areas (<- $areas)]
                  (order {(! areas "area") :descending})
                  (top 1)
                  (project areas))
        {:galaxy-query? true}))))

