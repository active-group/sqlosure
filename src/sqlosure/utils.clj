(ns sqlosure.utils)

(defn third [xs]
  (-> xs rest rest first))
