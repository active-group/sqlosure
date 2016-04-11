`sqlosure` is a library for accessing SQL-Databases.
(currently supports [postgresql](http://www.postgresql.org/) and [sqlite](http://www.sqlite.org/)).

It provides a query DSL based on relational algebra in form of a query monad which itself ist loosely based on [HaskellDB's](https://hackage.haskell.org/package/haskelldb).

# Overview

Suppose we have the following tables in our database.

```sql
CREATE TABLE movie (
    id INT,
    title TEXT,
    release DATE,
    good BOOLEAN,
);

CREATE TABLE person (
    id INT,
    first VARCHAR(30),  -- first name
    last VARCHAR(30),   -- last name
    birthday DATE,
    sex BOOLEAN         -- true == female
);

CREATE TABLE actor_movie (
    actor_id INT,
    movie_id INT
);
```

To access those tables in Clojure, we first need to define them as a `table`.

```clojure
(ns your.name.space
  (:require [active.clojure.monad :refer [return]]
            [sqlosure.core :refer :all]))

(def person-table
  (table "person"
         {"id" $integer-t
          "first" $string-t
          "last" $string-t
          "birthday" $date-t
          "sex" $boolean-t}))

(def movie-table
  (table "movie"
         {"id" $integer-t
          "title" $string-t
          "release" $date-t
          "good" $boolean-t}))

(def actor-movie-table
  (table "actor_movie"
         {"actor_id" $integer-t
          "movie_id" $integer-t}))
```


First, we need to define our database spec. This is just a map containing the 
connection information.
```clojure
(def db-spec {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname ":memory:"})

(def conn (db-connect db-spec))
```

We can now use sqlosure to formulate queries. A few examples:

```clojure
;; SELECT * FROM person
(run-query conn (query [person (<- person-table)]
                       [return person]))

;; SELECT first, last FROM person WHERE id < 10
(run-query conn (query [person (<- person-table)]
                       ;; Restrict the result set.
                       (restrict ($< (! person "id")
                                     ($integer 10)))
                       ;; Finally, project the desired columns.
                       (project {"first" (! person "first")
                                 "last" (! person "last")})))
```

One can easily construct more complex queries containig multiple tables, for
example the get all actors for a given movie title, one might forumlate the
following sqlosure-query. To make it reusable, we'll define it as a function:

```clojure
(defn actors-by-movie
  [movie-title]
  (query [p (<- person-table)
          m (<- movie-table)
          ma (<- actor-movie-table)]
         (restrict ($= (! m "title")
                       ($string movie-title)))
         (restrict ($= (! m "id")
                       (! ma "movie_id")))
         (restrict ($= (! p "id")
                       (! ma "actor_id")))
         (project {"first" (! p "first")
                   "last" (! p "last")
                   "movie_title" (! m "title")})

(run-query conn (actors-by-movie "Moonrise Kingdom"))
```

We can also use this to create nested queries. For example, if you wanted to use
the result of this query to get ne number of actors for the given movie:

```clojure
(run-query conn (query [sub (<- (actors-by-movie "Moonrise Kingdom"))]
                       (project {"number_of_actors" $count*})))
```

More to come...

## License

Copyright © 2016 Active Group GmbH

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
