(ns sqlosure.galaxy-test
  (:require [active.clojure.record :refer [define-record-type]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [sql :as sql]
             [time :as time]
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

(defn insert-kv!
  [conn kv]
  (db/insert! conn kv-table (kv-k kv) (kv-v kv)))

(defn query+string
  [conn q]
  (let [[q _] (dbize-query q)
        s (put-query q)
        s-cut [(first s)
               (map second (rest s))]
        res (db/db-query-reified-results conn q)]
    [s-cut res]))

(with-kv-db db-spec
  (fn [conn]
    (let [count-query (query [kv (<- kv-galaxy)]
                             (project {"count" ($count (! kv))}))
          project-all-query (query [kv (<- kv-galaxy)] (project kv))
          project-v-query (query [kv (<- kv-galaxy)] (project {"v" ($kv-v (! kv))}))
          restrict-query (query [kv (<- kv-galaxy)]
                                (restrict ($or ($< ($kv-k (! kv))
                                                   ($integer 2))
                                               ($= ($kv-k (! kv))
                                                   ($integer 42))))
                                (project kv))]
      (doall (map #(insert-kv! conn %)
                  [(make-kv 0 "foo")
                   (make-kv 1 "bar")
                   (make-kv 42 "baz")
                   (make-kv 23 "fizz")]))
      ;; (db/db-query-reified-results conn project-k-query)
      ;; (db/db-query-reified-results conn project-k-query)
      (db/db-query-reified-results conn project-v-query)
      (query+string conn (query [kv (<- kv-galaxy)]
                                (restrict ($< ($kv-k (! kv))
                                              ($integer 3)))
                                (top 2)
                                (project {"v" ($kv-v (! kv))}))))))

;; -----------------------------------------------------------------------------
;; -- MORE ELABORATE EXAMPLE
;; -----------------------------------------------------------------------------

(def ^{:dynamic true} *conn* (atom nil))

(comment
  "To illustracte and test the behaviour of galaxy types (in this case, only
product types), we'll construct a larger example.
This example will be a book with an isbn-number, an author, a title, a release
date and a publisher. The author and publisher are themselves record types with
their own db-types.")

;; -----------------------------------------------------------------------------
(comment
  "First, we'll define some basic record-types to represent the values in our
  program.")

(define-record-type book
  ^{:doc "A record for representing a book. This will be the base for a
db-type."}
  (make-book isbn author title release publisher) book?
  [^{:doc "The isbn-number of the book (`string`)."} isbn book-isbn
   ^{:doc "The author (author-record, see `make-author`)."} author book-author
   ^{:doc "The title of the book (`string`)."} title book-title
   ^{:doc "The release date (`java.time.LocalDateTime`)"} release book-release
   ^{:doc "The publisher (publisher-record, see `make-publisher`"}
   publisher book-publisher])

(define-record-type publisher
  ^{:doc "A record for representing publishers."}
  (make-publisher name operates-from) publisher?
  [^{:doc "The name of the publisher (`string`)."} name publisher-name
   ^{:doc "The city this publisher operates from (`string`)."}
   operates-from publisher-operates-from])

(define-record-type person
  ^{:doc "A record for representing a person. Is used as the author of a book."}
  (make-person fname lname birthday gender) person?
  [^{:doc "First name(s) (`string`)."} fname person-fname
   ^{:doc "Last name(s) (`string`)."} lname person-lname
   ^{:doc "Birthday of the person (`java.time.LocalDateTime`)."}
   birthday person-birthday
   ^{:doc "The gender of the person (`string`)."} gender person-gender])

;; -----------------------------------------------------------------------------
(comment
  "Next, we are going to define db-types for those record types, which can then
be used alongside primtive db-types. We'll also define the sql-table and scheme
values to use in the queries."

  (define-table+scheme publisher {"name" $string-t
                                  "operates_from" $string-t})

  (defn db->publisher
    "Takes the values returned by a query to get a `publisher` from the database
  and returns the corresponding `publisher` record."
    [publisher]
    (apply make-publisher publisher))

  (defn publisher->db
    "Takes a publisher-record `publisher` and returns a representation which can
  be inserted into the database."
    [publisher]
    (make-tuple [($string (publisher-name publisher))
                 ($string (publisher-operates-from publisher))]))

  (def $publisher-t
    (make-db-type "publisher"  ;; The name of the db-type
                  publisher?   ;; A predicate to check whether a value is a publisher
                  nil
                  nil
                  publisher-scheme  ;; The scheme of the record type
                  db->publisher  ;; How to "make" a publisher from a query result
                  publisher->db  ;; How to make a db-representation from a record
                  ))

  (defn install-publisher-table!
    [db]
    (jdbc/db-do-prepared (db/db-connection-conn db)
                         (jdbc/create-table-ddl
                          "publisher"
                          [["name" "TEXT"] ["operates_from" "TEXT"]])))

  (def publisher-galaxy
    (make&install-db-galaxy "publisher" $publisher-t install-publisher-table!
                            publisher-table))

  (def $publisher-name "sql-like operator to get the name of a publisher."
    (rel/make-monomorphic-combinator "publisher-name" [$publisher-t] $string-t
                                     publisher-name
                                     :universe sql/sql-universe
                                     :data (make-db-operator-data
                                            nil
                                            (fn [pub & _]
                                              (first (tuple-expressions pub))))))

  ;; Same thing for person
  (define-table+scheme person {"fname" $string-t
                               "lname" $string-t
                               "birthday" $date-t
                               "gender" $string-t})

  (defn db->person [person] (apply make-person person))

  (defn person->db
    [person]
    (make-tuple [($string (person-fname person))
                 ($string (person-lname person))
                 ($date (person-birthday person))
                 ($string (person-gender person))]))

  (def $person-t
    (make-db-type "person" person? nil nil person-scheme
                  db->person person->db))

  (defn install-person-table!
    [db]
    (jdbc/db-do-prepared (db/db-connection-conn db)
                         (jdbc/create-table-ddl
                          "person"
                          [["fname" "TEXT"] ["lname" "TEXT"]
                           ["birthday" "DATE"] ["gender" "TEXT"]])))

  (def person-galaxy
    (make&install-db-galaxy "person" $person-t install-person-table!
                            person-table))

  (defn- person-op
    [name out-type out-fn data-fn]
    (rel/make-monomorphic-combinator name [$person-t] out-type out-fn
                                     :universe sql/sql-universe
                                     :data data-fn))

  (def $person-lname "sql-like operator to get the last-name of a person."
    (person-op "person-lname" $string-t person-lname
               (make-db-operator-data
                nil (fn [p & _] (second (tuple-expressions p))))))

  ;; And again for book. Note the `$person-t` and `$publisher-t`, which can be
  ;; used just like primitive types.
  (define-table+scheme book {"isbn" $string-t
                             "author" $person-t
                             "title" $string-t
                             "release" $date-t
                             "publisher" $publisher-t})

  (defn db->book
    "This is a little more interesting but what it actually does is quite easy.
  It is taking the result for one book (as it is stored in the database) and
  reconstructs the record from those values by looking up the missing parts in
  the neighbor galaxies (in this case person and publisher)."
    [book]
    (let [[isbn author title release pub] book
          [pers pub] (first (db/db-query-reified-results
                             @*conn*
                             (query [pers (<- person-galaxy)
                                     pubs (<- publisher-galaxy)]
                                    (restrict ($= ($person-lname (! pers))
                                                  ($string author)))
                                    (restrict ($= ($publisher-name (! pubs))
                                                  ($string pub)))
                                    (project {"author" (! pers)
                                              "publisher" (! pubs)}))))]
      (make-book isbn pers title release pub)))

  (defn book->db
    [book]
    (make-tuple [($string (book-isbn book))
                 ($string (-> book book-author person-lname))
                 ($string (book-release book))
                 ($date (book-release book))
                 ($string (-> book book-publisher publisher-name))]))

  (defn title->book
    [title]
    nil)

  (def $book-t
    (make-db-type "book" book? book-title title->book book-scheme
                  db->book book->db))

  (defn install-book-table!
    [db]
    (jdbc/db-do-prepared (db/db-connection-conn db)
                         (jdbc/create-table-ddl
                          "book"
                          [["isbn" "TEXT"]
                           ;; It's just an example, so we'll use the last name as
                           ;; the foreign key.
                           ["author" "TEXT"] ["title" "TEXT"]
                           ["release" "DATE"]
                           ;; Again, we'll just use the publisher's name.
                           ["publisher" "TEXT"]])))

  (def book-galaxy
    (make&install-db-galaxy "book" $book-t install-book-table! book-table))

  (defn with-book-db
    "Takes a db-spec and a function that takes a (db-connect spec) connection.
  Sets up a kv-galaxy + tables, etc."
    [spec func]
    (jdbc/with-db-connection [db spec]
      (let [conn (db-connect db)]
        (reset! *db-galaxies* nil)
        (reset! *conn* conn)
        (make&install-db-galaxy "publisher" $publisher-t install-publisher-table!
                                publisher-table)
        (make&install-db-galaxy "person" $person-t install-person-table!
                                person-table)
        (make&install-db-galaxy "book" $book-t install-book-table! book-table)
        (initialize-db-galaxies! @*conn*)
        ;; Insert a few values
        (db/insert! @*conn* publisher-table "Rowohlt" "Berlin")
        (db/insert! @*conn* publisher-table "Kipenheuer & Witsch" "Köln")
        (db/insert! @*conn* person-table
                    "Wolfgang" "Herrndorf" (time/make-date 1965 6 12) "male")
        (db/insert! @*conn* book-table
                    "978-3-87134-781-8" "Herrndorf" "Arbeit und Struktur"
                    (time/make-date 2013 1 1) "Rowohlt")

        (func))))

  (comment
    "Now, instead of querying the tables, we don't need to care about constructing
and reconstructing records from the result sets.")

  (def $publisher-operates-from
    (rel/make-monomorphic-combinator "publisher-operates-from" [$publisher-t]
                                     $string-t
                                     publisher-operates-from
                                     :universe sql/sql-universe
                                     :data (make-db-operator-data
                                            nil
                                            (fn [pub & _]
                                              (second (tuple-expressions pub))))))

  ;; PERSON ----------------------------------------------------------------------

  (def $person-fname
    (person-op "person-name" $string-t person-fname
               (make-db-operator-data
                nil (fn [p & _] (first (tuple-expressions p))))))

  (def $person-birthday
    (person-op "person-birthday" $date-t person-birthday
               (make-db-operator-data
                nil (fn [p & _] (second (rest (tuple-expressions p)))))))

  (def $person-gender
    (person-op "person-gender" $string-t person-gender
               (make-db-operator-data
                nil (fn [p & _] (second (rest (rest (tuple-expressions p))))))))

  ;; BOOK ------------------------------------------------------------------------

  (def $book-title
    (rel/make-monomorphic-combinator "book-title" [$book-t] $string-t
                                     book-title
                                     :universe sql/sql-universe
                                     :data (make-db-operator-data
                                            nil
                                            (fn [book & _]
                                              (get (tuple-expressions book) 2)))))


  (def rowohlt (make-publisher "Rowohlt" "Berlin"))
  (def kiwi (make-publisher "Kipenheuer & Witsch" "Köln"))
  (def herrndorf (make-person "Wolfgang" "Herrndorf" (time/make-date 1965 6 12)
                              "Köln"))
  (def arbeit-und-struktur (make-book "978-3-87134-781-8"
                                      herrndorf
                                      "Arbeit und Struktur"
                                      (time/make-date 2013 1 1)
                                      rowohlt))

  #_(with-book-db db-spec
      (fn []
        (db/db-query-reified-results
         @*conn* (query [books (<- book-galaxy)]
                        (restrict ($= ($book-title (! books))
                                      ($string "Arbeit und Struktur")))
                        (project books)))))

  ;; SET TYPE

  (comment "with a cool name, this could be something like"
           (define-set-type string-v int-v)
           "which could then expand to"
           (define-record-type string-v-int-v
             (make-string-v-int-v id id_0 id_1) string-v-int-v?
             [id string-v-int-v-id
              id_0 string-v-int-v-id_0
              id_1 string-v-int-v-id_1])
           )

  )

(define-record-type string-v
  (make-string-v id v) string-v?
  [id string-v-id
   v string-v-v])

(define-record-type int-v
  (make-int-v id v) int-v?
  [id int-v-id
   v int-v-v])


(define-record-type int-or-string
    (really-make-int-or-string id i s) int-or-string?
    [id int-or-string-id
     i int-or-string-i
     s int-or-string-s])

  (defn make-int-or-string
    [id i-or-s]
    (if (integer? i-or-s)
      (really-make-int-or-string id i-or-s nil)
      (really-make-int-or-string id nil i-or-s)))

  (defn extract-typed-value
    [int-or-s]
    (let [id (int-or-string-id int-or-s)
          i (int-or-string-i int-or-s)
          s (int-or-string-s int-or-s)]
      (if i (make-int-v id i) (make-string-v id s))))

  (define-table+scheme int-or-string {"id" $integer-t
                                      "i" $integer-null-t
                                      "s" $integer-null-t})

  (define-table+scheme integer {"id" $integer-t
                                "val" $integer-t})

  (define-table+scheme string {"id" $integer-t
                               "val" $string-t})

  (defn db->i-or-s
    [i-or-s]
    (let [[id i s] i-or-s
          v (first (if i
                     (db/run-query @*conn* (query [is (<- integer-table)]
                                                  (restrict ($= (! is "id")
                                                                ($integer id)))
                                                  (project is)))
                     (db/run-query @*conn* (query [ss (<- string-table)]
                                                  (restrict ($= (! ss "id")
                                                                ($integer id)))
                                                  (project ss)))))]
      (extract-typed-value (make-int-or-string id (second v)))))

  (defn i-or-s->db
    [i-or-s]
    (make-tuple [($integer (int-or-string-id i-or-s))
                 ($integer-null-t (int-or-string-i i-or-s))
                 ($integer-null-t (int-or-string-s i-or-s))]))

  (defn key->i-or-s
    [k]
    nil)

  (def $int-or-string-t
    (make-db-type "int-or-string" int-or-string? int-or-string-id
                  key->i-or-s int-or-string-scheme
                  db->i-or-s i-or-s->db))

  (defn install-i-or-s-tables!
    [db]
    (doall
     [(jdbc/db-do-prepared (db/db-connection-conn db)
                           (jdbc/create-table-ddl
                            "int_or_string"
                            [["id" "INTEGER"] ["i" "INTEGER"] ["s" "INTEGER"]]))
      (jdbc/db-do-prepared (db/db-connection-conn db)
                           (jdbc/create-table-ddl
                            "integer"
                            [["id" "INTEGER"] ["val" "INTEGER"]]))
      (jdbc/db-do-prepared (db/db-connection-conn db)
                           (jdbc/create-table-ddl
                            "string"
                            [["id" "INTEGER"] ["val" "TEXT"]]))]))

  (def int-or-string-galaxy
    (make&install-db-galaxy "int_or_string"
                            $int-or-string-t
                            install-i-or-s-tables!
                            int-or-string-table))

  (defn with-i-or-s-db
    [spec func]
    (jdbc/with-db-connection [db spec]
      (let [conn (db-connect db)]
        (reset! *db-galaxies* nil)
        (reset! *conn* conn)
        (make&install-db-galaxy "int_or_string"
                                $int-or-string-t
                                install-i-or-s-tables!
                                int-or-string-table)
        (initialize-db-galaxies! @*conn*)

        (db/insert! @*conn* int-or-string-table 0 nil 0)
        (db/insert! @*conn* int-or-string-table 1 nil 1)
        (db/insert! @*conn* int-or-string-table 2 2 nil)
        (db/insert! @*conn* int-or-string-table 3 nil 3)
        (db/insert! @*conn* int-or-string-table 4 4 nil)

        (db/insert! @*conn* integer-table 2 42)
        (db/insert! @*conn* integer-table 4 23)

        (db/insert! @*conn* string-table 0 "foo")
        (db/insert! @*conn* string-table 1 "bar")
        (db/insert! @*conn* string-table 3 "baz")
        (func))))
(def $i-or-s-id (rel/make-monomorphic-combinator "ir-or-s-id"
                                                 [$int-or-string-t]
                                                 $integer-t
                                                 int-or-string-id
                                                 :universe sql/sql-universe
                                                 :data
                                                 (make-db-operator-data
                                                  nil
                                                  (fn [k & args]
                                                    (first (tuple-expressions k))))))
(with-i-or-s-db db-spec
    (fn []
      #_(db/run-query @*conn* (query [strings (<- string-table)]
                                     (restrict ($= (! strings "id")
                                                   ($integer 0)))
                                     (project strings)))
      (db/db-query-reified-results @*conn*
                                   (query [ios (<- int-or-string-galaxy)]
                                          (project {"id" ($i-or-s-id (! ios))})))))
