(defproject de.active-group/sqlosure "0.4.0"
  :description "Compositional relational queries."
  :url "https://github.com/active-group/sqlosure"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [de.active-group/active-clojure "0.35.0"]]

  :plugins [[lein-cloverage "1.0.6"]
            [lein-kibit "0.1.2"]
            [lein-codox "0.9.4"]]

  :codox {:metadata {:doc/format :markdown}}

  :profiles {:test {:dependencies [[com.h2database/h2 "1.4.200"]]}
             :repl {:injections [(require 'active.clojure.condition-hooks)
                                 (active.clojure.condition-hooks/activate-clojure-test!)]}
             :dev  {:dependencies [[com.h2database/h2 "1.4.200"]]
                    :injections   [(require 'active.clojure.condition-hooks)
                                   (active.clojure.condition-hooks/activate-clojure-test!)]}}

  :test-selectors {:default   (complement :northwind)
                   :northwind :northwind
                   :all       (constantly true)}

  :repl-options {:caught active.clojure.condition-hooks/repl-caught}

  ;; Use `lein cover` to run cloverage. Reason: cloverage is not happy with lets
  ;; in monadic code. cover excludes the files that use
  ;; active.clojure.monad/monadic. Coverage for these namespaces must be done by
  ;; hand.
  :aliases {"cover" ["cloverage" "-e" ".*query.*"]})
