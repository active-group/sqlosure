Currently supports [postgresql](http://www.postgresql.org/) and [sqlite](http://www.sqlite.org/).

# Minimal example

Suppose we have the following table in our database.

```sql
CREATE TABLE movies (
  title text,
  director text,
  year integer,
  any_good boolean
);
```

We can now execute queries against this table as follows
(for now, this will be made more comfortable in the future):

```clojure
(ns sqlosure.test
  (:require [sqlosure.sql :as sql :refer [=$ or$]]
            [sqlosure.relational-algebra :as rel :refer [make-const]]
            [sqlosure.type :as t]
            ;; [sqlosure.db-sqlite3 :refer :all]  ;; either postgres or sqlite3
            [sqlosure.db-postgresql :refer [open-db-connetion-postgresql]]
            [sqlosure.database :as db :refer [run-query insert delete update]]))

;; Our table we want to query.
(def movies-table (sql/make-sql-table
                   "movies"
                   (rel/make-rel-scheme {"title" t/string%
                                         "director" t/string%
                                         "year" t/integer%
                                         "any_good" t/boolean%})))

;; Query to count all movies (represented as a projection + aggregation).
;; Read: take all records of movies-table, aggregate via count and project the
;; count (as row called "sum") as the result
(def count-movies (rel/make-project
                   {"sum" (rel/make-aggregation :count (rel/make-attribute-ref "title"))}
                   movies-table))

;; Query to project title and year of all entries in movies-table.
;; Read: take all record of movies-table and project column title as "title" and column year as "releasedin"
(def project-title-and-year (rel/make-project {"title" (rel/make-attribute-ref "title")
                                               "releasedin" (rel/make-attribute-ref "year")}
                                              movies-table))

(let [conn (open-db-connection-postgresql "<db-host>" <db-port> "<db-user>" "<db-dbname>" "<db-password>")]
  (do
    (run-query conn project-title-and-year)  ;; => [{:title <title> :releasedin <year>} ...]
    (run-query conn count-movies)  ;; => {:sum <the-count>}
    ;; Insert a new record
    (insert conn movies-table "Leon the Professional" "Luc Besson" 1994 true)
    ;; Update records: update all records titles with
    ;; director='Some Director' OR year=1999
    ;; to 'Changed Title'
    (update conn movies-table
            (fn [title director year any-good?]
              (or$ (=$ director (make-const t/string% "Some Director"))
                   (=$ year (make-const t/integer% 1999))))
            (fn [title director year any-good?]
              {"title" (make-const t/string% "Changed title")}))
    ;; Delete all reecords with title='Changed title'
    (delete conn movies-table (fn [title director year any-good?]
                                (=$ title (make-const t/string% "Changed title"))))))
```
