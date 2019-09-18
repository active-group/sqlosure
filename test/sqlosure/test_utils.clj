(ns sqlosure.test-utils
  (:require [clojure.java.jdbc :as jdbc]
            [sqlosure.core :refer :all]
            [sqlosure.db-connection :as dbs]
            [sqlosure.time :as time]))

(defn jdbc-out
  [db q & row-fns]
  (let [res-seq (jdbc/query db q {:as-arrays? :cols-as-is})]
    (set (map (apply comp row-fns) (rest res-seq)))))

(defn sqlosure-out
  [db q]
  (set (dbs/run-query db q)))

;; This is just kept here as a reminder.
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

(def db-spec {:classname "org.h2.Driver"
              :subprotocol "h2:mem"
              :subname "tests;MODE=PostgreSQL"
              :mode "PostgreSQL"})

(defn make-person-table
  [db]
  (jdbc/db-do-prepared db
   (jdbc/create-table-ddl
    "person"
    [[:id :int]
     [:first "VARCHAR(32)"]
     [:last "VARCHAR(32)"]
     [:birthday "DATE"]
     [:sex :boolean]])))

(def person-table
  (table "person"
         {"id"       $integer-t
          "first"    $string-t
          "last"     $string-t
          "birthday" $date-t
          "sex"      $boolean-t}))

(defn create-people
  [db backend]
  (let [conn (db-connect db backend)]
    (dotimes [_ 100]
      (dbs/insert! conn
                   person-table
                   (next-person-id) (random-string) (random-string)
                   (random-date) (random-boolean)))))

(defn make-movie-table
  [db]
  (jdbc/db-do-prepared
   db
   (jdbc/create-table-ddl
    "movie"
    [[:id :int]
     [:title "varchar(100)"]
     [:release "DATE"]
     [:good :boolean]])))

(def movie-table
  (table "movie"
         [["id" $integer-t]
          ["title" $string-t]
          ["release" $date-t]
          ["good" $boolean-t]]))

(defn create-movies
  [db backend]
  (let [conn (db-connect db backend)]
    (dotimes [_ 20]
      (dbs/insert!
       conn movie-table
       (next-movie-id) (random-string) (random-date) (random-boolean)))))

(defn make-actor-movie-table
  [db]
  (jdbc/db-do-prepared
   db
   (jdbc/create-table-ddl
    "actor_movie"
    [[:actor_id :int]
     [:movie_id :int]])))

(def actor-movie-table
  (table "actor_movie"
         [["actor_id" $integer-t]
          ["movie_id" $integer-t]]))

(defn create-actors
  [db backend]
  (let [conn (db-connect db backend)]
    (dotimes [_ 30]
      (dbs/insert! conn actor-movie-table
                   (rand-int @current-person-id)
                   (rand-int @current-movie-id)))))

(defn with-actor-db
  "`with-actor-db` takes a db-spec and a function which takes the 'conneted' db
  as an argument. Use to run tests against an in-memory instance of sqlite3.
  This function creates three tables: 'person', 'movie' and 'actor_movie' and
  inserts some default records with random values and unique ids per table."
  [spec backend func]
  (jdbc/with-db-connection [db spec]
    (make-person-table db)
    (make-movie-table db)
    (make-actor-movie-table db)
    (create-people db backend)
    (create-movies db backend)
    (create-actors db backend)
    (let [res (func db)]
      (reset! current-movie-id 0)
      (reset! current-person-id 0)
      res)))

(defn constant-alias
  "Takes a string representing a SQL query and returns it
   will all the auto-generated aliases set to \"alias\"."
  [s]
  (clojure.string/replace s #"G__\d+" "alias"))
