(ns sqlosure.utils)

(defn third [xs]
  (-> xs rest rest first))

(defn zip
  [xs ys]
  (mapv (fn [k v] [k v]) xs ys))
