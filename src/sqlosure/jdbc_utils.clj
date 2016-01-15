(ns sqlosure.jdbc-utils
  "Utilities for using clojure.java.jdbc as a backend."
  (:require [sqlosure.relational-algebra :as rel]))

(defn query-row-fn
  "Takes a relational scheme and a row returned by a query, then returns a map
  of the key-value pairs with values converted via convert-base-value."
  [convert-base-value scheme row]
  (let [alist (rel/rel-scheme-alist scheme)]
    (into {} (map (fn [[k v] tt]
                    ;; assert (base-type? tt) ?
                    [k (convert-base-value tt v)])
                  row
                  (vals alist)))))
