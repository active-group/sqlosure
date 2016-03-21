(ns sqlosure.db-sqlite3-test
  (:require [sqlosure.db-sqlite3 :as sql]
            [sqlosure.database :as dbs]
            [sqlosure.core :refer :all]
            [sqlosure.time :as time]
            [active.clojure.monad :refer [return]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]]))

;; http://blog.find-method.de/index.php?/archives/210-In-memory-database-fixtures-with-Clojure-and-sqlite.html
;; https://github.com/clojure/java.jdbc/blob/master/src/test/clojure/clojure/java/test_jdbc.clj

(def current-person-id (atom 0))
(def current-movie-id (atom 0))
(defn next-person-id [] (swap! current-person-id inc))
(defn next-movie-id [] (swap! current-movie-id inc))

(defn characters
  []
  (map char (range (int \a) (inc (int \z)))))

(defn random-string
  []
  (apply str (take (inc (rand-int 15)) (shuffle (characters)))))

(defn random-date
  []
  (time/make-date (+ 1900 (rand-int 100))
                  (inc (rand-int 12))
                  (inc (rand-int 27))))

(defn random-boolean
  []
  (= 0 (rand-int 2)))

(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(defn make-person-table
  [db]
  (jdbc/db-do-prepared db
   (jdbc/create-table-ddl
    "person"
    [:id :int]
    [:first "VARCHAR(32)"]
    [:last "VARCHAR(32)"]
    [:birthday "DATE"]
    [:sex :boolean])))

(def person-table
  (table "person"
         {"id" $integer-t
          "first" $string-t
          "last" $string-t
          "birthday" $date-t
          "sex" $boolean-t}))

(defn create-people
  [db]
  (dotimes [_ 100]
    (jdbc/insert! db "person"
                  {:id (next-person-id)
                   :first (random-string)
                   :last (random-string)
                   :birthday (random-date)
                   :sex (random-boolean)})))

(defn make-movie-table
  [db]
  (jdbc/db-do-prepared db
                       (jdbc/create-table-ddl
                        "movie"
                        [:id :int]
                        [:title "TEXT"]
                        [:release "DATE"]
                        [:good :boolean])))

(def movie-table
  (table "movie"
         {"id" $integer-t
          "title" $string-t
          "release" $date-t
          "good" $boolean-t}))

(defn create-movies
  [db]
  (dotimes [_ 20]
    (jdbc/insert! db "movie"
                  {:id (next-movie-id)
                   :title (random-string)
                   :release (random-date)
                   :good (random-boolean)})))

(defn make-actor-movie-table
  [db]
  (jdbc/db-do-prepared db
                       (jdbc/create-table-ddl
                        "actor_movie"
                        [:actor_id :int]
                        [:movie_id :int])))

(def actor-movie-table
  (table "actor_movie"
         {"actor_id" $integer-t
          "movie_id" $integer-t}))

(defn create-actors
  [db]
  (dotimes [_ 30]
    (jdbc/insert! db "actor_movie"
                  {:actor_id (rand-int @current-person-id)
                   :movie_id (rand-int @current-movie-id)})))

(defn with-actor-db
  [spec func]
  (jdbc/with-db-connection [db spec]
    (make-person-table db)
    (make-movie-table db)
    (make-actor-movie-table db)
    (create-people db)
    (create-movies db)
    (create-actors db)
    (let [res (func db)]
      (reset! current-movie-id 0)
      (reset! current-person-id 0)
      res)))

(def actors-per-movie
  (query [movie (<- movie-table)
          actor (<- actor-movie-table)]
         (restrict ($= (! movie "id")
                       (! actor "movie_id")))
         (group [movie "title"])
         (project {"title" (! movie "title")
                   "actors" ($count (! actor "actor_id"))})))

;; A set of example tests to illustrate one possible way to test with an
;; in-memory instance of sqlite3.
(deftest simple-test
  (with-actor-db db-spec
    (fn [db]
      (let [conn (sql/open-db-connection-sqlite3 db)
            run #(dbs/run-query conn %)]
        (is (= (set (stringify-keys
                     (jdbc/query db [(str "SELECT m.title, count(a.actor_id) AS actors "
                                          "FROM movie AS m, actor_movie AS a "
                                          "WHERE a.movie_id = m.id "
                                          "GROUP BY m.title")])))
               (set (dbs/run-query conn actors-per-movie))))))))