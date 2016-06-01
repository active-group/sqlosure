(ns sqlosure.sugar-test
  (:require [clojure.java.jdbc :as jdbc]
            [sqlosure
             [core :refer :all]
             [db-connection :as db]
             [sugar :refer :all]
             [time :as time]]
            [sqlosure.galaxy.galaxy
             :refer
             [*db-galaxies* initialize-db-galaxies! make&install-db-galaxy]]))

(def ^{:dynamic true} *conn* (atom nil))
(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(define-product-type person {"fname" $string-t
                             "lname" $string-t
                             "birthday" $date-t})

(defn with-person-db
  [spec func]
  (jdbc/with-db-connection [db spec]
    (let [conn (db-connect db)]
      (reset! *db-galaxies* nil)
      (reset! *conn* conn)
      (make&install-db-galaxy "person" $person-t install-person-table! person-table)
      (initialize-db-galaxies! @*conn*)

      ;; Insert a few values
      (db/insert! @*conn* person-table 0 "Marco" "Schneider" (time/make-date 1989 10 31))
      #_(db/insert! @*conn* person-table "Helen" "Ahner" (time/make-date 1990 10 15))

      (func))))

(with-person-db db-spec
  (fn []
    #_(db/run-query @*conn* (query [ps (<- person-table)]
                                   (project ps)))
    (db/db-query-reified-results @*conn* (query [ps (<- person-table)]
                                                (project ps)))))
