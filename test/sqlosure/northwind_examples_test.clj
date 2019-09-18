(ns sqlosure.northwind-examples-test
  (:require [active.clojure.monad :refer [monadic]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [sqlosure.core :refer :all]
            [sqlosure.backends.postgresql :as postgresql]
            [sqlosure.query-comprehension :as qc]
            [sqlosure.db-connection :as db]))

(def db-spec {:dbtype   "postgresql"
              :dbname   "northwind"
              :host     "localhost"
              :port     5432
              :user     "northwind_user"
              :password "thewindisblowing"})

;;;; TABLE DEFINITIONS.
(def categories-table
  (table "categories" [["category_id" $integer-t]
                       ["category_name" $string-t]
                       ["description" $string-null-t]
                       ["picture" $bytea-null-t]]))

;;; Customers
(def customer-customer-demo-table
  (table "customer_customer_demo" [["customer_id_" $string-t]
                                   ["customer_type_id" $string-t]]))

(def customer-demographics-table
  (table "customer_demographics" [["customer_type_id" $string-t]
                                  ["customer_desc" $string-null-t]]))

(def customers-table
  (table "customers" [["customer_id" $string-t]
                      ["company_name" $string-t]
                      ["contact_name" $string-t]
                      ["contact_title" $string-t]
                      ["address" $string-t]
                      ["city" $string-t]
                      ["region" $string-t]
                      ["postal_code" $string]
                      ["country" $string-t]
                      ["phone" $string-t]
                      ["fax" $string-t]]))

;; Employees
(def employees-table
  (table "employees" [["employee_id" $integer-t]
                      ["last_name" $string-t]
                      ["first_name" $string-t]
                      ["title" $string-null-t]
                      ["title_of_courtesy" $string-null-t]
                      ["birth_date" $date-null-t]
                      ["hire_date" $date-null-t]
                      ["address" $string-null-t]
                      ["city" $string-null-t]
                      ["region" $string-null-t]
                      ["postal_code" $string-null-t]
                      ["country" $string-null-t]
                      ["home_phone" $string-null-t]
                      ["extension" $string-null-t]
                      ["photo" $bytea-null-t]
                      ["notes" $string-null-t]
                      ["reports_to" $integer-null-t]
                      ["photo_path" $string-null-t]]))

(def employee-territories-table
  (table "employee_territories" [["employee_id" $integer-t]
                                 ["territory_id" $string-t]]))

(def order-details-table
  (table "order_details" [["order_id" $integer-t]
                          ["product_id" $integer-t]
                          ["unit_price" $double-t]
                          ["quantity" $integer-t]
                          ["discount" $double-t]]))

(def orders-table
  (table "orders" [["order_id" $integer-t]
                   ["customer_id" $string-null-t]
                   ["employee_id" $integer-null-t]
                   ["order_date" $date-null-t]
                   ["required_date" $date-null-t]
                   ["shipped_date" $date-null-t]
                   ["ship_via" $integer-null-t]
                   ["freight" $double-null-t]
                   ["ship_name" $string-null-t]
                   ["ship_address" $string-null-t]
                   ["ship_city" $string-null-t]
                   ["ship_region" $string-null-t]
                   ["ship_postal_code" $string-null-t]
                   ["ship_country" $string-null-t]]))

(def products-table
  (table "products" [["product_id" $integer-t]
                     ["product_name" $string-t]
                     ["supplier_id" $integer-null-t]
                     ["category_id" $integer-null-t]
                     ["quantity_per_unit" $string-null-t]
                     ["unit_price" $double-null-t]
                     ["units_in_stock" $integer-null-t]
                     ["units_on_order" $integer-null-t]
                     ["reorder_level" $integer-null-t]
                     ["discontinued" $integer-t]]))

(def shippers-table
  (table "shippers" [["shipper_id" $integer-t]
                     ["company_name" $string-t]
                     ["phone" $string-null-t]]))

(def suppliers-table
  (table "suppliers" [["supplier_id" $integer-t]
                      ["company_name" $string-t]
                      ["contact_name" $string-null-t]
                      ["contact_title" $string-null-t]
                      ["address" $string-null-t]
                      ["city" $string-null-t]
                      ["region" $string-null-t]
                      ["postal_code" $string-null-t]
                      ["country" $string-null-t]
                      ["phone" $string-null-t]
                      ["fax" $string-null-t]
                      ["homepage" $string-null-t]]))

(def territories-table
  (table "territories" [["territory_id" $string-t]
                        ["territory_description" $string-t]
                        ["region_id" $integer-t]]))

(def region-table
  (table "region" [["region_id" $integer-t]
                   ["region_description" $string-t]]))

(def us-states-table
  (table "us_states" [["state_id" $integer-t]
                      ["state_name" $string-null-t]
                      ["state_abbr" $string-null-t]
                      ["state_region" $string-null-t]]))

(def employee
  (query [employees            (<- employees-table)
          employee-territories (<- employee-territories-table)
          territories          (<- territories-table)
          regions              (<- region-table)]
         (restrict ($= (! employees "employee_id")
                       (! employee-territories "employee_id")))
         (restrict ($= (! employee-territories "territory_id")
                       (! territories "territory_id")))
         (restrict ($= (! territories "region_id")
                       (! regions "region_id")))
         (project [["employee_id"       (! employees "employee_id")]
                   ["last_name"         (! employees "last_name")]
                   ["first_name"        (! employees "first_name")]
                   ["title"             (! employees "title")]
                   ["title_of_courtesy" (! employees "title_of_courtesy")]
                   ["birth_date"        (! employees "birth_date")]
                   ["hire_date"         (! employees "hire_date")]
                   ["address"           (! employees "address")]
                   ["city "             (! employees "city")]
                   ["region"            (! employees "region")]
                   ["postal_code"       (! employees "postal_code")]
                   ["country"           (! employees "country")]
                   ["home_phone"        (! employees "home_phone")]
                   ["extension"         (! employees "extension")]
                   ["photo"             (! employees "photo")]
                   ["notes"             (! employees "notes")]
                   ["reports_to"        (! employees "reports_to")]
                   ["photo_path"        (! employees "photo_path")]
                   ["employee_territory_id" (! territories "territory_id")]
                   ["employee_territory_description" (! territories "territory_description")]
                   ["employee_region_id" (! regions "region_id")]
                   ["employee_region_description" (! regions "region_description")]])))

(defn run-query
  [q]
  (jdbc/with-db-connection [conn db-spec]
    (let [connection (db-connect db-spec postgresql/implementation)]
      (db/run-query connection q))))

;; Examples taken from here: https://www.w3resource.com/mysql-exercises/northwind/products-table-exercises/

;; 1. Write a query to get product name and quantity/unit.
(def q1 (query [p (<- products-table)]
               (project [["product_name" (! p "product_name")]
                         ["quantity_per_unit" (! p "quantity_per_unit")]])))

(deftest query-1-test
  (is (= 77 (count (run-query q1)))))

;; 2. Write a query to get current product list (product id and name).

(defn products
  [discontinued?]
  (query [p (<- products-table)]
         (restrict ($= (! p "discontinued") ($integer (if discontinued? 1 0))))
         (order [[(! p "product_name") :ascending]])
         (project [["product_id" (! p "product_id")]
                   ["product_name" (! p "product_name")]])) )

(def q2 (products false))

(deftest query-2-test
  (is (= 67 (count (run-query q2)))))

;; 3. Write a query to get discontinued Product list (Product ID and name). 
(def q3 (products true))

(def q3-res
  (list [17 "Alice Mutton"]
        [1 "Chai"]
        [2 "Chang"]
        [5 "Chef Anton's Gumbo Mix"]
        [24 "Guaraná Fantástica"]
        [9 "Mishi Kobe Niku"]
        [53 "Perth Pasties"]
        [28 "Rössle Sauerkraut"]
        [42 "Singaporean Hokkien Fried Mee"]
        [29 "Thüringer Rostbratwurst"]))

(deftest query-3-test
  (is (= q3-res (run-query q3))))

;; 4. Write a query to get most expense and least expensive Product list (name and unit price).
(def q4 (query [p (<- products-table)]
               (order [[(! p "unit_price") :descending]])
               (project [["product_name" (! p "product_name")]
                         ["unit_price" (! p "unit_price")]])))

(deftest query-4-test
  (let [res (run-query q4)]
    (is (= ["Côte de Blaye" 263.5] (first res)))
    (is (= ["Geitost" 2.5] (last res)))))

;; 5. Write a query to get Product list (id, name, unit price) where current products cost less than $20.
(def q5 (query [p (<- products-table)]
               (restrict ($and ($< (! p "unit_price") ($double 20))
                               ($= (! p "discontinued") ($integer 0))))
               (order [[(! p "unit_price") :descending]])
               (project [["product_id" (! p "product_id")]
                         ["product_name" (! p "product_name")]
                         ["unit_price" (! p "unit_price")]])))

(deftest query-5-test
  (let [res (run-query q5)]
    (is (= [57 "Ravioli Angelo" 19.5] (first res)))
    (is (= [33 "Geitost" 2.5] (last res)))))

;; 6. Write a query to get Product list (name, unit price) where products cost between $15 and $25.
(def q6 (query [p (<- products-table)]
               (restrict ($and ($and ($>= (! p "unit_price") ($double 15))
                                     ($<= (! p "unit_price") ($double 25)))
                               ($= (! p "discontinued") ($integer 0))))
               (order [[(! p "unit_price") :descending]])
               (project [["product_name" (! p "product_name")]
                         ["unit_price" (! p "unit_price")]])))

(deftest query-6-test
  (let [res (run-query q6)]
    (is (= ["Grandma's Boysenberry Spread" 25.0] (first res)))
    (is (= ["Röd Kaviar" 15.0] (last res)))))

;; 7. Write a query to get Product list (id, name, unit price) of above average price.
#_(def q7 (query [p (<- products-table)
                avg (<- (monadic [ps (<- products-table)]
                                 (project [["avg" ($avg (! ps "unit_price"))]])))]
               (restrict ($> (! p "unit_price") (! avg "avg")))
               (order [[(! p "unit_price") :ascending]])
               distinct!
               (project [["product_name" (! p "product_name")]
                         ["unit_price" (! p "unit_price")]])))

#_(deftest query-7-test
  (let [res (run-query q7)]
    (is (= ["Uncle Bob's Organic Dried Pears" 30.0] (first res)))
    (is (= ["Côte de Blaye" 263.5] (last res)))))

;; 8. Write a query to get Product list (name, unit price) of twenty most expensive products.
#_(def q8 (query [p (<- products-table)
                inner (<- (monadic [ps (<- products-table)]
                                   (restrict ($>= (! ps "unit_price")
                                                  (! p "unit_price")))
                                   distinct!
                                   (project [["count" ($count (! ps "unit_price"))]])))]
               (restrict ($>= ($integer 20)
                              (! inner "count")))
               distinct!
               (project [["twenty_most_expensive_products" (! p "product_name")]
                         ["unit_price" (! p "unit_price")]])))
