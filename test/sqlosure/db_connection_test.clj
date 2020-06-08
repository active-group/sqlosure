(ns sqlosure.db-connection-test
  (:require [active.clojure.monad :refer [return]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [sqlosure.core :as sql]
            [sqlosure.db-connection :as db]
            [sqlosure.relational-algebra :as rel]
            [sqlosure.time :as time]
            [sqlosure.test-utils :refer [actor-movie-table db-spec jdbc-out movie-table person-table sqlosure-out with-actor-db]]
            [sqlosure.sql-put :as put]
            [sqlosure.backends.h2 :as h2]
            [sqlosure.relational-algebra-sql :as rsql]))

(deftest insert!-test
  (with-actor-db db-spec h2/implementation
    (fn [db]
      (let [conn     (sql/db-connect db h2/implementation)
            axel     [-1 "Axel" "Hacke" (time/make-date 1956 1 20) false]
            get-1    (sql/query [person (sql/<- person-table)]
                                (sql/restrict! (sql/$= (sql/! person "id") (sql/$integer -1)))
                                (sql/project person))
            delete-1 #(jdbc/delete! db "person" ["id = ?" -1])]
        (testing "without explicit rel-scheme"
          (is (empty? (db/run-query conn get-1)))
          (apply db/insert! conn person-table axel)
          (is (= #{axel} (set (db/run-query conn get-1))))
          ;; Remove the record for the next test.
          )
        (testing "with explicit rel-scheme"
          ;; Clean the last insert
          (delete-1)
          (is (empty? (db/run-query conn get-1)))
          (apply db/insert! conn person-table
                 (rel/base-relation-scheme person-table)
                 axel)
          (is (= #{axel} (set (db/run-query conn get-1))))
          #_(testing "should fail with underspecified rel-scheme"
              ;; Clean the last insert
              (delete-1)
              (is (thrown?
                   Exception
                   (db/insert! conn person-table
                               ;; Note missing keys which should cause this query to fail.
                               (rel/alist->rel-scheme {"id"    $integer-t
                                                       "first" $string-t})
                               -1 "Cormac")))))))))

(deftest delete!-test
  (with-actor-db db-spec h2/implementation
    (fn [db]
      (let [conn (sql/db-connect db h2/implementation)]
        (testing "deletion of one record"
          (let [r (first (db/run-query conn (sql/query [p (sql/<- person-table)]
                                                       (sql/project p))))]
            (is r)  ;; We have one record now.
            (is (= r (first (db/run-query conn (sql/query [p (sql/<- person-table)]
                                                          (sql/project p))))))
            (db/delete! conn person-table
                        (fn [id _ _ _ _]
                          (sql/$= id (sql/$integer 1))))))
        (testing "deletion of a set of records"
          ;; Insert a few records we know the id's of.
          (doseq [i (range -5 0)]
            (db/insert! conn person-table
                        i "some" "name" (time/make-date 1989 10 31) false))
          (let [q (sql/query [p (sql/<- person-table)]
                             (sql/restrict! (sql/$and
                                             (sql/$>= (sql/! p "id")
                                                      (sql/$integer -5))
                                             (sql/$<= (sql/! p "id")
                                                      (sql/$integer 0))))
                             (sql/project p))]
            (is (= 5 (count (db/run-query conn q))))
            (db/delete! conn person-table
                        (fn [id _ _ _ _]
                          (sql/$and (sql/$>= id (sql/$integer -5))
                                    (sql/$<= id (sql/$integer 0)))))
            (is (empty? (db/run-query conn q)))))))))

(deftest update!-test
  (with-actor-db db-spec h2/implementation
    (fn [db]
      (let [conn      (sql/db-connect db h2/implementation)
            vian      [-1 "Boris" "Vian" (time/make-date 1920 3 10) false]
            foucault  [-1 "Michel" "Foucault" (time/make-date 1926 10 15) false]
            person=-1 (sql/query [p (sql/<- person-table)]
                                 (sql/restrict! (sql/$= (sql/! p "id") (sql/$integer -1)))
                                 (sql/project p))]
        ;; Insert a new person with id -1.
        (apply db/insert! conn person-table vian)
        ;; Make sure he's there.
        (is (= vian (first (db/run-query conn person=-1))))
        (db/update! conn person-table
                    (fn [id _ _ _ _]
                      (sql/$= id (sql/$integer -1)))
                    (fn [_ _ _ _ _]
                      {"first"    (sql/$string "Michel")
                       "last"     (sql/$string "Foucault")
                       "birthday" (sql/$date (time/make-date 1926 10 15))}))
        (is (= foucault (first (db/run-query conn person=-1))))))))

;; A set of example tests to illustrate one possible way to test with an
;; in-memory instance of sqlite3.
(deftest simple-test
  (with-actor-db db-spec h2/implementation
    (fn [db]
      (let [conn (sql/db-connect db h2/implementation)]
        (testing "order"
          (let [row-fn (fn [row]
                         (update-in row [1] time/from-sql-date))]
            (is (= (jdbc-out db [(str "SELECT title, release "
                                      "FROM movie "
                                      "ORDER BY release DESC")]
                             row-fn)
                   (sqlosure-out
                    conn
                    (sql/query [movie (sql/<- movie-table)]
                               (sql/order! {(sql/! movie "release") :descending})
                               (sql/project [["title" (sql/! movie "title")]
                                             ["release" (sql/! movie "release")]]))))))
          (is (= (into
                  #{}
                  (map #(->> % first time/from-sql-date vector)
                       (jdbc-out
                        db
                        ["SELECT release FROM movie ORDER BY release ASC"])))
                 (sqlosure-out
                  conn (sql/query [movie (sql/<- movie-table)]
                                  (sql/order! {(sql/! movie "release") :ascending})
                                  (sql/project [["release" (sql/! movie "release")]]))))))
        (testing "top"
          (let [row-fn (fn [row]
                         (update-in row [2] time/from-sql-date))]
            (is (= (jdbc-out db [(str "SELECT * FROM movie LIMIT 5")] row-fn)
                   (sqlosure-out conn (sql/query [movie (sql/<- movie-table)]
                                                 (sql/top 5)
                                                 (sql/project movie)))))
            ;; TODO: This seems to work for now but we need to investigate
            ;;       wheter the same query against the same database state will
            ;;       always return the same order of elements.
            (is (= (jdbc-out
                    db [(str "SELECT * FROM movie LIMIT 5 OFFSET 2")] row-fn)
                   (sqlosure-out conn (sql/query [movie (sql/<- movie-table)]
                                                 (sql/top 2 5)
                                                 (sql/project movie)))))))
        (testing "count(*)"
          (is (= (jdbc-out db "SELECT count(*) AS count FROM movie")
                 (sqlosure-out conn (sql/query [movie (sql/<- movie-table)]
                                               (sql/project [["count" sql/$count*]]))))))
        (testing "group"
          (testing "statement can be moved"
            (let [qs (str "SELECT p.id, count(m.id) AS movies "
                          "FROM person AS p, movie AS m, actor_movie AS am "
                          "WHERE am.movie_id = m.id AND p.id = am.actor_id "
                          "GROUP BY p.id")]
              (is (= (jdbc-out db qs)
                     (sqlosure-out
                      conn (sql/query [p  (sql/<- person-table)
                                       m  (sql/<- movie-table)
                                       am (sql/<- actor-movie-table)]
                                      (sql/restrict! (sql/$and (sql/$= (sql/! am "movie_id")
                                                                       (sql/! m "id"))
                                                               (sql/$= (sql/! p "id")
                                                                       (sql/! am "actor_id"))))
                                      ;; Note the order of the statements.
                                      (sql/group! [p "id"])
                                      (sql/project [["id" (sql/! p "id")]
                                                    ["movies" (sql/$count (sql/! m "id"))]])))))
              (is (= (jdbc-out db qs)
                     (sqlosure-out
                      conn
                      (sql/query [p  (sql/<- person-table)
                                  m  (sql/<- movie-table)
                                  am (sql/<- actor-movie-table)]
                                 (sql/group! [p "id"])  ;; Note the order of the statements.
                                 (sql/restrict! (sql/$and (sql/$= (sql/! am "movie_id")
                                                                  (sql/! m "id"))
                                                          (sql/$= (sql/! p "id")
                                                                  (sql/! am "actor_id"))))
                                 (sql/project [["id" (sql/! p "id")]
                                               ["movies" (sql/$count (sql/! m "id"))]]))))))
            (testing "with grouping in nested table"
              (let [subquery (sql/query [a  (sql/<- person-table)
                                         m  (sql/<- movie-table)
                                         am (sql/<- actor-movie-table)]
                                        (sql/restrict (sql/$and (sql/$= (sql/! a "id")
                                                                        (sql/! am "actor_id"))
                                                                (sql/$= (sql/! m "id")
                                                                        (sql/! am "movie_id"))))
                                        (sql/group! [a "id"])
                                        (sql/project [["id"    (sql/! a "id")]
                                                      ["count" (sql/$count (sql/! m "id"))]]))]
                (is (= (jdbc-out
                        db
                        (str "SELECT p.first, p.last, m.count "
                             "FROM person AS p, (SELECT a.id, count(m.id) AS count "
                             "                   FROM person AS a, movie AS m, actor_movie AS am "
                             "                   WHERE a.id = am.actor_id AND m.id = am.movie_id "
                             "                   GROUP BY a.id) AS m "
                             "WHERE m.count >= 2 AND p.id = m.id"))
                       (sqlosure-out
                        conn
                        (sql/query [p (sql/<- person-table)
                                    m (sql/<- subquery)]
                                   (sql/restrict! (sql/$and (sql/$>= (sql/! m "count") (sql/$integer 2))
                                                            (sql/$=  (sql/! m "id") (sql/! p "id"))))
                                   (sql/project [["first" (sql/! p "first")]
                                                 ["last"  (sql/! p "last")]
                                                 ["count" (sql/! m "count")]])))))))))))))
