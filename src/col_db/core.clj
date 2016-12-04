(ns col-db.core
  (:require [clojure.string :as str]))

(defn new-db [name]
  (let [db (atom {:name    name
                  :schema  {}})]
    (spit (str name ".db") "")
    (spit (str name ".schema") (pr-str @db))
    db))

(defn type-definition->type
  [definition]
  (case definition
    :int (type 1)))

(defn type-definition->cast-fn
  [definition]
  (case definition
    :int #(Long/parseLong %)))

(defn transact! [db attr val]
  (let [attr-type (get-in @db [:schema attr])]
    (assert (= (type val) (type-definition->type attr-type)) "Schema not matched")
    (spit (str (:name @db) "-" (name attr) ".col") (str val ",") :append true)
    :ok))

#_(defn fetch-col [db attr])

(defn new-schema [db attr type]
  (assert (keyword? attr) "Attr must be a keyword")
  (spit (str (:name @db) ".col") "")
  (swap! db assoc-in [:schema attr] type))

(defn col-seq [db attr]
  (let [attr-type (get-in @db [:schema attr])]
    (map-indexed #(vector ((type-definition->cast-fn attr-type) %2) %1)
                 (str/split (slurp (str (:name @db) "-" (name attr) ".col")) #","))))

(defn compress-col [db attr]
  (let [s (col-seq db attr)
        size (count s)]
    (->> s
        (group-by first)
        (map (fn [[k v]] [k (map second v)]))
        (into {})
        (map (fn [[k v]] [k ])))))
