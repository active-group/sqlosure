(defproject de.active-group/sqlosure "0.1.0"
  :description "Compositional relational queries."
  :url "https://github.com/active-group/sqlosure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/java.jdbc "0.4.2"]
                 ;; uncomment only one
                 ;; [org.xerial/sqlite-jdbc "3.8.11"]
                 [org.postgresql/postgresql "9.4-1206-jdbc41"]
                 [active-clojure "0.12.0"]]
  :plugins [[lein-cloverage "1.0.6"]
            [lein-kibit "0.1.2"]]

  :profiles {:test {:dependencies [[pjstadig/humane-test-output "0.7.1"]]
                    :injections [(require 'pjstadig.humane-test-output)
                                 (pjstadig.humane-test-output/activate!)]}
             :repl {:dependencies [[pjstadig/humane-test-output "0.7.1"]]
                    :injections [(require 'pjstadig.humane-test-output)
                                 (pjstadig.humane-test-output/activate!)
                                 (require 'active.clojure.condition-hooks)
                                 (active.clojure.condition-hooks/activate-clojure-test!)]}}

  :repl-options {:caught active.clojure.condition-hooks/repl-caught}

  ;; Use `lein cover` to run cloverage. Reason: cloverage is not happy with lets
  ;; in monadic code. cover excludes the files that use
  ;; active.clojure.monad/monadic. Coverage for these namespaces must be done by
  ;; hand.
  :aliases {"cover" ["cloverage" "-e" ".*query.*"]})
