(ns sqlosure.galaxy.galaxy-test
  (:require [active.clojure.record :refer [define-record-type]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [time :as time]
             [test-utils :as utils]]
            [sqlosure.galaxy.galaxy :refer :all]))

;; -----------------------------------------------------------------------------
;; -- SETUP CEREMONY
;; -----------------------------------------------------------------------------
(define-record-type ^{:doc "A simple key->value type."} kv
  (make-kv k v) kv?
  [k kv-k v kv-v])

(def kv-scheme "The scheme for a key-value table."
  (rel/alist->rel-scheme {"k" $integer-t
                          "v" $string-t}))

(def kv-table "A table for key-value records."
  (table "kv" (rel/rel-scheme-map kv-scheme)))

(defn install-kv-db-tables!
  "Takes a db-connection and sets up the key-value table."
  [db]
  (jdbc/db-do-prepared
   (db/db-connection-conn db)
   (jdbc/create-table-ddl
    "kv"
    [["k" "INT"] ["v" "TEXT"]])))

(defn key->kv
  "Takes a db-connection and a key and returns the record with \"key\" = k or
  `nil` if missing."
  [db k]
  (db/run-query db (query [kv (<- kv-table)]
                          (restrict ($= (! kv "k")
                                        ($integer k)))
                          (project kv))))

(defn db->kv
  [db k]
  (let [kv (key->kv db k)]
    (when kv (apply make-kv (first kv)))))

(defn kv->db-expression
  [kv]
  (make-tuple [($integer (kv-k kv)) ($string (kv-v kv))]))

(def $kv-t (make-db-type "kv" kv? kv-k key->kv kv-scheme db->kv
                         kv->db-expression))

(def kv-galaxy (make&install-db-galaxy "kv" $kv-t install-kv-db-tables!
                                       kv-table))

(defn with-kv-db
  "Takes a db-spec and a function that takes a (db-connect spec) connection.
  Sets up "
  [spec func]
  (jdbc/with-db-connection [db spec]
    (let [conn (db-connect db)]
      (reset! *db-galaxies* nil)
      (make&install-db-galaxy
       "kv" $kv-t
       install-kv-db-tables!
       kv-table)
      (initialize-db-galaxies! conn)
      (println @*db-galaxies*)
      (func conn))))

(with-kv-db utils/db-spec
  (fn [conn]
    (db/insert! conn kv-table 1 "foo")
    (db/insert! conn kv-table 2 "bar")
    (db/run-query conn (query [kv (<- kv-table)]
                              (project kv)))))

;; -----------------------------------------------------------------------------
;; -- ACTUAL TESTING
;; -----------------------------------------------------------------------------
(deftest make&install-db-galaxy-test
  (let [int-g-args ["integer-galaxy" $integer-t (constantly false) nil]
        string-g-args ["string-galaxy" $string-t (constantly false) nil]]
    (reset! *db-galaxies* nil)
    (testing "there should be nothing registered"
      (is (= nil @*db-galaxies*)))
    (testing "make&install string-galaxy"
      (let [string-rel (apply make&install-db-galaxy string-g-args)]
        (is (= {"string-galaxy" string-rel}  ;; string-galaxy should be registered.
               @*db-galaxies*))
        (testing "additionally make&install integer-galaxy"
          (let [int-rel (apply make&install-db-galaxy int-g-args)]
            (is (= {"string-galaxy" string-rel
                    "integer-galaxy" int-rel}
                   @*db-galaxies*))
            (reset! *db-galaxies* nil)))))))

(defn- all
  [coll]
  (every? true? coll))

(deftest make-name-generator-test
  (let [gen (#'sqlosure.galaxy.galaxy/make-name-generator "foo")]
    (testing "multiple calls to the generator should result in diffrent names"
      (all (map #(is (= (str "foo_" %) (gen))) (range 0 10))))))

(deftest list->product-test
  (let [underlying (rel/make-project {"foo" (rel/make-attribute-ref "null")}
                                     rel/the-empty)
        q1 (rel/make-top
            0 5
            (rel/make-project {"foo" (rel/make-attribute-ref "v")} kv-table))
        q2 (rel/make-project {"fizz" (rel/make-attribute-ref "k")} kv-table)
        f #'sqlosure.galaxy.galaxy/list->product]
    (testing "with just queries as inputs"
      (is (= (rel/make-product underlying rel/the-empty)
             (f (list underlying))))
      (is (= (rel/make-product
              underlying
              (rel/make-product
               q1
               (rel/make-product q2 rel/the-empty)))
             (f (cons underlying (list q1 q2))))))
    (testing "with nil it should be the empty query"
      (is (= rel/the-empty (f nil))))
    (testing "with non query arguments it should throw"
      (is (thrown? Exception (f '(5)))))))

(deftest apply-restriction-test
  (let [apply-restrictions #'sqlosure.galaxy.galaxy/apply-restrictions
        underlying (rel/make-project {"key" (rel/make-attribute-ref "k")}
                                     kv-table)
        r1 ($= ($integer 5) (rel/make-attribute-ref "k"))
        r2 ($= ($string "foo") (rel/make-attribute-ref "v"))]
    ;; NOTE All restrictions are applied in reverse order!
    (testing "with just restrictions"
      (is (= (rel/make-restrict r1 underlying)
             (apply-restrictions [r1] underlying)))
      (is (= (rel/make-restrict
              r2
              (rel/make-restrict
               r1 underlying))
             (apply-restrictions [r1 r2] underlying))))
    (testing "with empty as restrictions it should be just the underlying query"
      (is (= underlying (apply-restrictions nil underlying)))
      (is (= underlying (apply-restrictions [] underlying))))
    (testing "with nil as the query, it should return the empty query"
      (is (= rel/the-empty (apply-restrictions [] nil)))
      (is (= rel/the-empty (apply-restrictions [r1 r2] nil))))
    (testing "with something other than a query it should fail"
      (is (thrown? Exception (apply-restrictions [] 5))))))

(deftest restrict-to-scheme-test
  (let [underlying (rel/make-project {"one" (rel/make-attribute-ref "k")}
                                     kv-table)]
    (is (= (rel/make-project
            {"k" (rel/make-attribute-ref "key")
             "v" (rel/make-attribute-ref "value")}
            underlying)
           (restrict-to-scheme (rel/alist->rel-scheme
                                {"k" "key" 
                                 "v" "value"})
                               underlying)))
    (testing "empty scheme should cause an assertion violation"
      (is (thrown? Exception (restrict-to-scheme nil underlying))))
    (testing "invalid query should cause an assertion violation"
      (is (thrown? Exception (restrict-to-scheme (rel/alist->rel-scheme
                                                  {"k" "key"}) nil))))))

(deftest make-new-names-test
  (is (= (list "foo_0" "foo_1" "foo_2")
         (make-new-names "foo" (range 0 3))))
  (is (= nil (make-new-names "foo" nil))))

(deftest rename-query-test
  (let [gen (#'sqlosure.galaxy.galaxy/make-name-generator "dbize")
        q (rel/make-project {"k" (rel/make-attribute-ref "k")}
                            kv-table)]
    (is (= [(list (rel/make-attribute-ref "dbize_0"))
            (rel/make-project {"dbize_0" (rel/make-attribute-ref "k")}
                              q)]
           (rename-query q gen)))
    (testing "should throw with non-function name-generator"
      (is (thrown? Exception (rename-query q nil)))
      (is (thrown? Exception (rename-query q 5))))
    (testing "should throw if q is not a query"
      (is (thrown? Exception (rename-query nil gen))))))

(comment

  (def p1 (make-person 0 "Marco" "Schneider" (time/make-date 1989 10 31) false))

  (def $person-id
    (rel/make-monomorphic-combinator "person-id"
                                     (list $person-t)
                                     $integer-t
                                     person-id
                                     :universe sql/sql-universe))

  (define-record-type person
    (make-person id first last birthday sex) person?
    [id person-id
     first person-first
     last person-last
     birthday person-birthday
     ^{:doc "false == male"}
     sex person-sex])

  (def person-scheme
    (rel/alist->rel-scheme {"id" $integer-t
                            "first" $string-t
                            "last" $string-t
                            "birthday" $date-t
                            "sex" $boolean-t}))

  (def person-table
    (table "person" (rel/rel-scheme-map person-scheme)))

  (defn install-person-db-tables!
    [db]
    (jdbc/db-do-prepared
     db
     (jdbc/create-table-ddl
      [[:id "INT"]
       [:first "TEXT"]
       [:last "TEXT"]
       [:birthday "DATE"]
       [:sex "BOOLEAN"]])))

  (defn id->person
    [db id]
    (db/run-query
     db
     (query [p (<- person-table)]
            (restrict ($= (! p "id")
                          ($integer id)))
            (project p))))

  (defn db->person
    [db id]
    (when-let [p (id->person db id)]
      (apply make-person (first p))))

  (defn person->db-expression
    [p]
    (make-tuple [($integer (person-id p))
                 ($string (person-first p))
                 ($string (person-last p))
                 ($date (person-birthday p))
                 ($boolean (person-sex p))]))

  (def $person-t (make-db-type "person" person?
                               person-id id->person
                               person-scheme
                               db->person
                               person->db-expression))

  (def person-galaxy
    (make&install-db-galaxy
     "person" $person-t
     (fn [db] (install-person-db-tables! db))
     person-table))

  )
