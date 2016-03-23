(ns sqlosure.db-sqlite3-test
  (:require [sqlosure.db-sqlite3 :as sql]
            [sqlosure.database :as dbs]
            [sqlosure.core :refer :all]
            [sqlosure.time :as time]
            [sqlosure.test-utils :refer :all]
            [active.clojure.monad :refer [return]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]]))


;; A set of example tests to illustrate one possible way to test with an
;; in-memory instance of sqlite3.
(deftest simple-test
  (with-actor-db db-spec
    (fn [db]
      (let [conn (sql/open-db-connection-sqlite3 db)]
        (testing "order"
          (let [row-fn (fn [row] (assoc row :release (time/from-sql-time-string (:release row))))]
            (is (= (jdbc-out db [(str "SELECT title, release "
                                       "FROM movie "
                                       "ORDER BY release DESC")]
                             row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (order {(! movie "release") :descending})
                                             (project {"title" (! movie "title")
                                                       "release" (! movie "release")})))))
            (is (= (jdbc-out db ["SELECT release FROM movie ORDER BY release ASC"] row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (order {(! movie "release") :ascending})
                                             (project {"release" (! movie "release")})))))))
        (testing "top"
          (let [row-fn (fn [row] (assoc row
                                        :good (= 1 (:good row))
                                        :release (time/from-sql-time-string (:release row))))]
            ;; NOTE: sqlite3 represents booleans a 0 and 1 -> need to convert to boolean manually.
            (is (= (jdbc-out db [(str "SELECT * FROM movie LIMIT 5")] row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (top 5)
                                             (return movie)))))
            ;; TODO: This seems to work for now but we need to investigate wheter the same query against
            ;; the same database state will always return the same order of elements.
            (is (= (jdbc-out db [(str "SELECT * FROM movie LIMIT 5 OFFSET 2")] row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (top 2 5)
                                             (return movie)))))))
        (testing "count(*)"
          (is (= (jdbc-out db "SELECT count(*) AS count FROM movie")
                 (sqlosure-out conn (query [movie (<- movie-table)]
                                           (project {"count" $count*})))))
          (is (= (jdbc-out db "SELECT count(*) AS count FROM movie, person")
                 (sqlosure-out conn (query [movie (<- movie-table)
                                            person (<- person-table)]
                                           (project {"count" $count*})))))
          (testing "with nested SELECTs"
            (is (= (jdbc-out db "SELECT count(*) AS count FROM movie, (SELECT * FROM person WHERE id < 10)")
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             [person (<- (query [people (<- person-table)]
                                                                (restrict ($< (! people "id") ($integer 10)))
                                                                (return people)))]
                                             (project {"count" $count*})))))))
        (testing "group"
          (testing "statement can be moved"
            (let [qstring (str "SELECT p.id, count(m.id) AS movies "
                               "FROM person AS p, movie AS m, actor_movie AS am "
                               "WHERE am.movie_id = m.id AND p.id = am.actor_id "
                               "GROUP BY p.id")]
              (is (jdbc-out db qstring)
                  (sqlosure-out conn (query [p  (<- person-table)
                                             m  (<- movie-table)
                                             am (<- actor-movie-table)]
                                            (restrict ($and ($= (! am "movie_id")
                                                                (! m "id"))
                                                            ($= (! p "id")
                                                                (! am "actor_id"))))
                                            (group [p "id"])  ;; Note the order of the statements.
                                            (project {"id" (! p "id")
                                                      "movies" ($count (! m "id"))}))))
              (is (jdbc-out db qstring)
                  (sqlosure-out conn (query [p  (<- person-table)
                                             m  (<- movie-table)
                                             am (<- actor-movie-table)]
                                            (group [p "id"])  ;; Note the order of the statements.
                                            (restrict ($and ($= (! am "movie_id")
                                                                (! m "id"))
                                                            ($= (! p "id")
                                                                (! am "actor_id"))))
                                            (project {"id" (! p "id")
                                                      "movies" ($count (! m "id"))})))))
            (testing "with grouping in nested table"
              (is (= (jdbc-out db (str "SELECT p.first, p.last, m.count "
                                       "FROM person AS p, (SELECT a.id, count(m.id) AS count "
                                       "                   FROM person AS a, movie AS m, actor_movie AS am "
                                       "                   WHERE a.id = am.actor_id AND m.id = am.movie_id "
                                       "                   GROUP BY a.id) AS m "
                                       "WHERE m.count >= 2 AND p.id = m.id"))
                     (sqlosure-out conn (query [p (<- person-table)
                                                m (<- (query [a  (<- person-table)
                                                              m  (<- movie-table)
                                                              am (<- actor-movie-table)]
                                                             (restrict ($and ($= (! a "id") (! am "actor_id"))
                                                                             ($= (! m "id") (! am "movie_id"))))
                                                             (group [a "id"])
                                                             (project {"id"    (! a "id")
                                                                       "count" ($count (! m "id"))})))]
                                               (restrict ($and ($>= (! m "count") ($integer 2))
                                                               ($=  (! m "id") (! p "id"))))
                                               (project {"first" (! p "first")
                                                         "last"  (! p "last")
                                                         "count" (! m "count")}))))))))))))
