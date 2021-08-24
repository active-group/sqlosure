(ns sqlosure.runner.database
  "Definition of a 'database' query runner that operates on a RDBMS. Requires a
  [[sqlosure.db-connection/db-connection]].

  Example:

  (def table (sqlosure.core/table \"foo\" [[\"a\" sqlosure.core/$integer-t]
                                           [\"b\" sqlosure.core/$boolean-t]]))

  (def db-connection (sqlosure.core/db-connect {:connection-uri \"...\"
                                                :user \"...\"
                                                :password \"...\"}))
        

  (sqlosure.runner/run-query (runner db-connection) table)
  ;; => [[42 true], [23 false]]
  "
  (:require [active.clojure.monad :as monad]
            [sqlosure.db-connection :as db-connection]
            [sqlosure.lang :as lang]))

(defn run-db-action
  [run-any env state m]
  (let [db-conn (::db-conn env)]
    (try
      (cond
        (lang/create? m)
        [(apply db-connection/insert! db-conn
                (lang/create-table m)
                (lang/create-record m))
         state]

        (lang/read? m)
        [(db-connection/run-query db-conn (lang/read-query m))
         state]

        (lang/update? m)
        (let [affected-row-count (db-connection/update! db-conn
                                                        (lang/update-table m)
                                                        (lang/update-predicate-fn m)
                                                        (lang/update-update-fn m))]
          [(lang/make-write-result affected-row-count) state])

        (lang/delete? m)
        (let [affected-row-count (db-connection/delete! db-conn (lang/delete-table m) (lang/delete-predicate-fn m))]
          [(lang/make-write-result affected-row-count) state])

        :else
        monad/unknown-command)
      (catch Throwable t
        [(monad/make-exception-value t) state]))))

(defn command-config
  [db-connection]
  (monad/make-monad-command-config run-db-action {::db-conn db-connection} {}))
