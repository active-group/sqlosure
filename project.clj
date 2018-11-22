(defproject de.active-group/sqlosure "0.3.0-SNAPSHOT"
  :description "Compositional relational queries."
  :url "https://github.com/active-group/sqlosure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.8"]
                 ;; Refer to your required driver here.
                 ;; [com.h2database/h2 "1.4.195"]
                 ;; [org.postgresql/postgresql "9.4.1208"]
                 [active-clojure "0.26.0"]]
  :plugins [[lein-cloverage "1.0.6"]
            [lein-kibit "0.1.2"]
            [lein-codox "0.9.4"]]

  :codox {:metadata {:doc/format :markdown}}

  :profiles {:test {:dependencies [[com.h2database/h2 "1.4.197"]]}
             :repl {
                    :injections [(require 'active.clojure.condition-hooks)
                                 (active.clojure.condition-hooks/activate-clojure-test!)]}
             :dev {:dependencies [[com.h2database/h2 "1.4.197"]]
                   :injections [(require 'active.clojure.condition-hooks)
                                (active.clojure.condition-hooks/activate-clojure-test!)]}}

  :repl-options {:caught active.clojure.condition-hooks/repl-caught}

  ;; Use `lein cover` to run cloverage. Reason: cloverage is not happy with lets
  ;; in monadic code. cover excludes the files that use
  ;; active.clojure.monad/monadic. Coverage for these namespaces must be done by
  ;; hand.
  :aliases {"cover" ["cloverage" "-e" ".*query.*"]})
