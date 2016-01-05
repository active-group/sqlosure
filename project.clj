(defproject sqlosure "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [clj-time "0.11.0"]
                 ;; uncomment only one
                 ;; [org.xerial/sqlite-jdbc "3.8.11"]
                 [org.postgresql/postgresql "9.4-1206-jdbc41"]
                 [active-clojure "0.11.0"]]
  :plugins [[lein-cloverage "1.0.6"]
            [lein-kibit "0.1.2"]])
