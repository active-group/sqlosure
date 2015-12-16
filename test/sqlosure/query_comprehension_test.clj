(ns sqlosure.query-comprehension-test
  (:require [sqlosure.query-comprehension :refer :all]
            [sqlosure.relational-algebra :refer :all]
            [sqlosure.database :refer :all]
            [sqlosure.db-postgresql :refer :all]
            [sqlosure.type :refer :all]
            [sqlosure.universe :refer :all]
            [sqlosure.sql :refer :all]
            [sqlosure.sql-put :as put]
            [active.clojure.monad :refer :all]
            [clojure.test :refer :all]))

(def tbl1 (sql/make-sql-table "tbl1"
                              (make-rel-scheme {"one" string%
                                                "two" integer%})))

(def tbl2 (sql/make-sql-table "tbl2"
                              (make-rel-scheme {"three" blob%
                                                "four" string%})))

(deftest get-query-test
  (is (rel-scheme=?
       (make-rel-scheme {"foo" integer%})
       (get-query (monadic
                   [t1 (embed tbl1)]
                   [t2 (embed tbl2)]
                   (restrict (=$ (! t1 "one")
                                     (! t2 "four")))
                   (project {"foo" (! t1 "two")}))))))

(get-query (monadic
            [t1 (embed tbl1)]
            [t2 (embed tbl2)]
            (restrict (=$ (! t1 "one")
                          (! t2 "four")))
            #_(project {"foo" (! t1 "two")})))

(let [movies-table (make-base-relation 'movies
                                       (make-rel-scheme {"title" string%
                                                         "director" string%
                                                         "year" integer%
                                                         "any_good" boolean%})
                                       :universe (make-universe)
                                       :handle "movies")
      conn (open-db-connection-postgresql "localhost" 5432 "marco" "marco" "")]
  (monadic [movies (embed movies-table)]
           (project {"title" (! movies "title")})))

