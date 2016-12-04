(ns col-db.core)

(defn new-db [name]
  (spit (str name ".db") "")
  (atom {:name   name
         :schema {}
         :columns []}))

(defn type-definition->type
  [definition]
  (case definition
    :int (type 1)))

(defn transact [db attr val]
  (let [attr-type (attr db)]
    (assert (= (type val) (type-definition->type attr-type)))
    ))

(defn new-schema [db attr type]
  (assert (keyword? attr) "Attr must be a keyword")
  (swap! db assoc-in [:schema attr] type)
  (swap! db update-in [:columns] conj attr))
