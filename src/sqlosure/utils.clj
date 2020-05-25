(ns sqlosure.utils)

(defn third [xs]
  (-> xs rest rest first))

(defn fourth [xs]
  (-> xs rest rest rest first))

(defn zip
  [xs ys]
  (mapv (fn [k v] [k v]) xs ys))

(defn print-deprecation-warning!
  [the-sym replace-with]
  (println (format "WARNING: %s is deprecated. Replace with %s" the-sym replace-with)))
