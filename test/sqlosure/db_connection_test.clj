(ns sqlosure.db-connection-test
  (:require [active.clojure.monad :refer [return]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [relational-algebra :as rel]
             [test-utils :refer [actor-movie-table db-spec jdbc-out movie-table person-table sqlosure-out with-actor-db]]
             [time :as time]]))

(deftest insert!-test
  (with-actor-db db-spec
    (fn [db]
      (let [conn (db-connect db)
            axel [-1 "Axel" "Hacke" (time/make-date 1956 1 20) false]
            get-1 (query [person (<- person-table)]
                         (restrict ($= (! person "id") ($integer -1)))
                         (return person))
            delete-1 #(jdbc/delete! db "person" ["id = ?" -1])]
        (testing "without explicit rel-scheme"
          (do
            (is (empty? (db/run-query conn get-1)))
            (apply db/insert! conn person-table axel)
            (is (= #{axel} (set (db/run-query conn get-1))))
            ;; Remove the record for the next test.
            ))
        (testing "with explicit rel-scheme"
          (do
            ;; Clean the last insert
            (delete-1)
            (is (empty? (db/run-query conn get-1)))
            (apply db/insert! conn person-table
                   (rel/base-relation-scheme person-table)
                   axel)
            (is (= #{axel} (set (db/run-query conn get-1)))))
          (testing "should fail with underspecified rel-scheme"
            ;; Clean the last insert
            (delete-1)
            (is (thrown?
                 Exception
                 (db/insert! conn person-table
                             ;; Note missing keys which should cause this query to fail.
                             (rel/alist->rel-scheme {"id" $integer
                                                     "first" $string})
                             -1 "Cormac")))))))))

(deftest delete!-test
  (with-actor-db db-spec
    (fn [db]
      (let [conn (db-connect db)]
        (testing "deletion of one record"
          (let [r (first (db/run-query conn (query [p (<- person-table)]
                                                   (return p))))]
            (do
              (is r)  ;; We have on record now.
              (is (= r (first (db/run-query conn (query [p (<- person-table)]
                                                        (return p))))))
              (db/delete! conn person-table
                          (fn [id _ _ _ _]
                            ($= id ($integer 1)))))))
        (testing "deletion of a set of records"
          ;; Insert a few records we know the id's of.
          (doseq [i (range -5 0)]
            (db/insert! conn person-table
                        i "some" "name" (time/make-date 1989 10 31) false))
          (let [q (query [p (<- person-table)]
                         (restrict ($and
                                    ($>= (! p "id")
                                         ($integer -5))
                                    ($<= (! p "id")
                                         ($integer 0))))
                         (return p))]
            (is (= 5 (count (db/run-query conn q))))
            (db/delete! conn person-table
                        (fn [id _ _ _ _]
                          ($and ($>= id ($integer -5))
                                ($<= id ($integer 0)))))
            (is (empty? (db/run-query conn q)))))))))

;; A set of example tests to illustrate one possible way to test with an
;; in-memory instance of sqlite3.
(deftest simple-test
  (with-actor-db db-spec
    (fn [db]
      (let [conn (db-connect db)]
        (testing "order"
          (let [row-fn (fn [row] (assoc row 1 (time/from-sql-time-string (get row 1))))]
            (is (= (jdbc-out db [(str "SELECT title, release "
                                      "FROM movie "
                                      "ORDER BY release DESC")]
                             row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (order {(! movie "release") :descending})
                                             (project {"title" (! movie "title")
                                                       "release" (! movie "release")}))))))
          (let [row-fn (fn [[release]]
                         [(time/from-sql-time-string release)])]
            (is (= (jdbc-out db ["SELECT release FROM movie ORDER BY release ASC"] row-fn)
                   (sqlosure-out conn (query [movie (<- movie-table)]
                                             (order {(! movie "release") :ascending})
                                             (project {"release" (! movie "release")})))))))
        (testing "top"
          (let [row-fn (fn [[id title release good]]
                         [id title (time/from-sql-time-string release) (= 1 good)])]
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
