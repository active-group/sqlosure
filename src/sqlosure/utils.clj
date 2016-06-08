(ns sqlosure.utils)

(defn third [xs]
  (-> xs rest rest first))

(defn fourth [xs]
  (-> xs rest rest rest first))

(defn zip
  [xs ys]
  (mapv (fn [k v] [k v]) xs ys))

(defn count=
  "Are the lengths of `xs` and `ys` equal?"
  [xs ys]
  (= (count xs) (count ys)))

(defn and?
  "`and?` returns the conjunction of a seq of boolean(ish) values."
  [bs]
  (every? true? bs))
