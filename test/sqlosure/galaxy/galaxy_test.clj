(ns sqlosure.galaxy.galaxy-test
  (:require [active.clojure.record :refer [define-record-type]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [universe :as universe]]
            [sqlosure.galaxy.galaxy :refer :all]))

(def ... nil)

(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(defn name-generator
  [n]
  (#'sqlosure.galaxy.galaxy/make-name-generator n))

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
  [kv]
  (apply make-kv kv))

(defn kv->db-expression
  [kv]
  (make-tuple [($integer (kv-k kv)) ($string (kv-v kv))]))

(def $kv-t (make-db-type "kv" kv?
                         nil
                         nil
                         kv-scheme
                         db->kv
                         kv->db-expression))

(defn $kv
  [k v]
  (rel/make-const $kv-t (make-kv k v)))

(def kv-galaxy (make&install-db-galaxy "kv" $kv-t install-kv-db-tables!
                                       kv-table))

(def $kv-k (rel/make-monomorphic-combinator "kv-k"
                                            [$kv-t]
                                            $integer-t
                                            kv-k
                                            :universe sql/sql-universe
                                            :data
                                            (make-db-operator-data
                                             nil
                                             (fn [k & args]
                                               (first (tuple-expressions k))))))

(def $kv-v (rel/make-monomorphic-combinator
            "kv-v"
            [$kv-t]
            $string-t
            kv-v
            :universe sql/sql-universe
            :data
            (make-db-operator-data
             nil
             (fn [key & args]
               (second (tuple-expressions key))))))

(defn with-kv-db
  "Takes a db-spec and a function that takes a (db-connect spec) connection.
  Sets up a kv-galaxy + tables, etc."
  [spec func]
  (jdbc/with-db-connection [db spec]
    (let [conn (db-connect db)]
      (reset! *db-galaxies* nil)
      (make&install-db-galaxy
       "kv" $kv-t
       install-kv-db-tables!
       kv-table)
      (initialize-db-galaxies! conn)
      (func conn))))

(deftest dbize-project-test
  (testing "project with base-relation"
    (is (= [[["k" (rel/make-attribute-ref "k")]] kv-table {}]
           (dbize-project
            {"k" (rel/make-attribute-ref "k")}
            kv-table
            (name-generator "foo")))))
  (testing "project with galaxy query"
    (is (= [[["kv_0" (rel/make-attribute-ref "kv_0")]
             ["kv_1" (rel/make-attribute-ref "kv_1")]]
            (rel/make-project {"kv_0" (rel/make-attribute-ref "k")
                               "kv_1" (rel/make-attribute-ref "v")}
                              kv-table)
            {"kv" (make-tuple (mapv rel/make-attribute-ref ["kv_0" "kv_1"]))}]
           (dbize-project {"kv" (rel/make-attribute-ref "kv")}
                          kv-galaxy (name-generator "dbize"))))))
(dbize-project {"kv" (rel/make-attribute-ref "kv")}
               kv-galaxy (name-generator "dbize"))
(deftest dbize-query-test
  (testing "empty query"
    (is (= [rel/the-empty-rel-scheme {}] (dbize-query rel/the-empty))))
  (testing "base-relation"
    (testing "'regular' base-relation"
      (is (= [kv-table {}] ;; Shouldn't change anything.
             (dbize-query kv-table))))
    (testing "galaxy"
      (is (= [(rel/make-project [["kv_0" (rel/make-attribute-ref "k")]
                                 ["kv_1" (rel/make-attribute-ref "v")]]
                                kv-table)
              {"kv"
               (make-tuple (map rel/make-attribute-ref ["kv_0" "kv_1"]))}]
             (dbize-query kv-galaxy)))))
  (testing "project"
    ;; NOTE This is an important case! -- Why?
    #_(is (= [(rel/make-project
             {"k" (rel/make-attribute-ref "k")
              "v" (rel/make-attribute-ref "v")}
             (rel/make-project {}))]
           (rel/make-project {"k" (rel/make-attribute-ref "k")
                              "v" (rel/make-attribute-ref "v")}
                             kv-galaxy)))
    (is (= [(rel/make-project
             [["k" (rel/make-attribute-ref "k")]]
             (rel/make-project [["kv_0" (rel/make-attribute-ref "k")]
                                ["kv_1" (rel/make-attribute-ref "v")]]
                               kv-table))
            {"kv" (make-tuple (map rel/make-attribute-ref ["kv_0" "kv_1"]))}]
           (dbize-query
            (rel/make-project {"k" (rel/make-attribute-ref "k")}
                              kv-galaxy)))))
  (testing "restrict"
    (let [r #(rel/make-restrict ($= % ($integer 0)) kv-table)]
      (is (= [(rel/make-project
               [["k" (rel/make-attribute-ref "k")]
                ["v" (rel/make-attribute-ref "v")]]
                (r (rel/make-attribute-ref "k"))) {}]
             (dbize-query (r (rel/make-attribute-ref "k")))))
      (is (= [(rel/make-project
               [["k" (rel/make-attribute-ref "k")]
                ["v" (rel/make-attribute-ref "v")]]
               (r ($integer 0))) {}]
             (dbize-query (r ($kv-k ($kv 0 "foo"))))))))
  (testing "combine"
    (let [c #(rel/make-combine :union %1 %2)]
      (is (= [(c (rel/make-project
                  [["k" (rel/make-attribute-ref "k")]]
                  (rel/make-project [["kv_0" (rel/make-attribute-ref "k")]
                                     ["kv_1" (rel/make-attribute-ref "v")]]
                                    kv-table))
                 (rel/make-project
                  [["k" (rel/make-attribute-ref "k")]
                   ["v" (rel/make-attribute-ref "v")]]
                  (rel/make-restrict ($= ($string "foo")
                                         (rel/make-attribute-ref "v"))
                                     kv-table)))
              {"kv" (make-tuple (map rel/make-attribute-ref ["kv_0" "kv_1"]))}]
             (dbize-query (c (rel/make-project {"k" (rel/make-attribute-ref "k")}
                                               kv-galaxy)
                             (rel/make-restrict ($= ($string "foo")
                                                    (rel/make-attribute-ref "v"))
                                                kv-table)))))))
  (testing "order"
    (is (= [(rel/make-order
             [[(rel/make-attribute-ref "k") :ascending]]
             (rel/make-project [["kv_0" (rel/make-attribute-ref "k")]
                                ["kv_1" (rel/make-attribute-ref "v")]]
                               kv-table))
            {"kv" (make-tuple (mapv rel/make-attribute-ref ["kv_0" "kv_1"]))}]
           (dbize-query (rel/make-order
                         {(rel/make-attribute-ref "k") :ascending}
                         kv-galaxy)))))
  (testing "top"
    (is (= [(rel/make-top 0 10 kv-table) {}]
           (dbize-query (rel/make-top 0 10 kv-table))))
    (is (= [(rel/make-top
             0 10
             (rel/make-project [["kv_0" (rel/make-attribute-ref "k")]
                                ["kv_1" (rel/make-attribute-ref "v")]]
                               kv-table))
            {"kv" (make-tuple (mapv rel/make-attribute-ref ["kv_0" "kv_1"]))}]
           (dbize-query (rel/make-top 0 10 kv-galaxy)))))
  (testing "anything else should fail"
    (is (thrown? Exception (dbize-query nil)))
    (is (thrown? Exception
                 (dbize-query (rel/make-project
                               {"k" (rel/make-attribute-ref "k")}
                               ;; Underlying query must not be nil.
                               nil))))))

(deftest dbize-expression-test
  (testing "attribute-ref"
    (testing "with empty environment"
      (is (= [(rel/make-attribute-ref "foo") '() '()]
             (dbize-expression (rel/make-attribute-ref "foo")
                               nil
                               kv-table
                               (name-generator "dbize")))))
    (testing "with environment lookup"
      (is (= [(rel/make-attribute-ref "bar") '() '()]
             (dbize-expression (rel/make-attribute-ref "foo")
                               {"foo" (rel/make-attribute-ref "bar")}
                               kv-table
                               (name-generator "dbize"))))))
  (testing "const"
    (testing "with non-db-type"
      (is (= [($integer 5) '() '()]
             (dbize-expression ($integer 5)
                               nil kv-table (name-generator "dbize"))))
      (is (= [($double 2.0) '() '()]
             (dbize-expression ($double 2.0)
                               {"foo" (rel/make-attribute-ref "bar")}
                               kv-table (name-generator "dbize"))))
      (testing "with nullable type"
        (is (= [($integer-null 42) '() '()]
               (dbize-expression ($integer-null 42)
                                 nil kv-table (name-generator "dbize"))))))
    (testing "with db-type"
      (is (= [(make-tuple [($integer 0) ($string "foo")]) '() '()]
             (dbize-expression ($kv 0 "foo")
                               nil kv-table (name-generator "dbize"))))))
  (testing "application"
    (testing "with 'regular' operator"
      (let [plus (universe/universe-lookup-rator sql/sql-universe '+)
            app1 (rel/make-application plus ($integer 23) ($integer 42))
            app2 (rel/make-application plus (rel/make-attribute-ref "foo")
                                       ($integer 42))]
        (is (= [app1 '() '()]
               (dbize-expression app1 nil kv-table
                                 (name-generator "dbize"))))
        (is (= [(rel/make-application plus ($integer 23) ($integer 42)) '() '()]
               (dbize-expression app2 {"foo" ($integer 23)} kv-table
                                 (name-generator "dbize"))))))
    (testing "with 'db-type' operator"
      (testing "without db-operator-data-query"
        (is (= [($integer 0) '() '()]
               (dbize-expression
                ($kv-k ($kv 0 "foo")) nil kv-table (name-generator "dbize"))))
        ;; A little more complex
        (is (= [($= ($integer 5) ($integer 0)) '() '()]
               (dbize-expression
                ($= ($integer 5)
                    ($kv-k ($kv 0 "foo")))
                nil kv-table (name-generator "dbize"))))))
    (testing "tuple"
      (is (= [(make-tuple [($integer 5)
                           (rel/make-attribute-ref "not found")
                           (make-tuple
                            [($integer 0) ($string "foo")])])
              '() '()]
             (dbize-expression
              (make-tuple [(rel/make-attribute-ref "five")
                           (rel/make-attribute-ref "not found")
                           ($kv 0 "foo")])
              {"five" ($integer 5)}
              kv-table (name-generator "dbize"))))
      (is (= [(make-tuple [(make-tuple [($integer 0) ($string "foo")])
                           ($integer 1)])
              '() '()]
             (dbize-expression
              (make-tuple [($kv 0 "foo")
                           ($kv-k ($kv 1 "bar"))])
              nil kv-table (name-generator "dbize")))))
    (testing "aggregate"
      (is (= [($count (rel/make-attribute-ref "k"))
              '() '()]
             (dbize-expression
              ($count (rel/make-attribute-ref "k"))
              nil kv-table (name-generator "dbize"))))
      (is (= [($sum (rel/make-attribute-ref "bar")) '() '()]
             (dbize-expression
              ($sum (rel/make-attribute-ref "foo"))
              {"foo" (rel/make-attribute-ref "bar")}
              kv-table (name-generator "foo")))))))

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
  "Is every element in a sequence true?"
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
            {"k" (rel/make-attribute-ref "k")
             "v" (rel/make-attribute-ref "v")}
            underlying)
           (restrict-to-scheme (rel/alist->rel-scheme
                                {"k" $integer
                                 "v" $string})
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

(with-kv-db db-spec
  (fn [conn]
    (let [count-query (query [kv (<- kv-galaxy)]
                             (project {"count" ($count (! kv))}))
          project-all-query (query [kv (<- kv-galaxy)]
                                   (project kv))
          restrict-query (query [kv (<- kv-galaxy)]
                                (restrict ($or ($< ($kv-k (! kv))
                                                   ($integer 1))
                                               ($= ($kv-k (! kv))
                                                   ($integer 42))))
                                (project kv))]
      (db/insert! conn kv-table 0 "foo")
      (db/insert! conn kv-galaxy 1 "bar")
      (db/insert! conn kv-galaxy (make-kv 42 "baz"))
      (filter #(even? (kv-k %))) (db/db-query-reified-results conn restrict-query))))

(put-query (first (dbize-query (query [kv (<- kv-galaxy)]
                                      (restrict ($= ($kv-v (! kv))
                                                    ($string "foo")))
                                      (project kv)))))
