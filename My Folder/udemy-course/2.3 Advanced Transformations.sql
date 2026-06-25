-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To handle a string type containing JSON, we can access the JSON string with a colon. E.g. `profile:first_name`

-- COMMAND ----------

SELECT
    customer_id,
    profile:first_name as first_name,
    profile:address:country as country
FROM 
    customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `from_json` can read JSON data. But it **must** strictly be JSON, not a JSON *string datatype*. See the following examples

-- COMMAND ----------

-- Sample for one row
SELECT profile
FROM customers
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT
    customer_id,
    from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) AS profile_struct
FROM 
    customers
;


-- COMMAND ----------

SELECT * FROM parsed_customers;
-- json in profile is now of STRUCT type. We can interact with nested objects. Expanding, collapsing hierarchies.

-- COMMAND ----------

DESCRIBE parsed_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, instead of colons we've used above to extract JSON string datatype, we use period "." with `STRUCT` types.

-- COMMAND ----------

select
    customer_id,
    profile_struct.first_name as first_name,
    profile_struct.address.country as country
from parsed_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flatten fields into columns
-- MAGIC
-- MAGIC Once we can access JSON fields (with either : or . for string and struct datatypes respectively), we can flatten them.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customer_final AS
SELECT
    customer_id,
    profile_struct.* -- short and quick way to extract all fields at the top in a struct type. 
FROM 
    parsed_customers
;

-- COMMAND ----------

SELECT * FROM customer_final;

-- COMMAND ----------

SELECT
    customer_id,
    profile_struct.*, -- short and quick way to extract all fields at the top in a struct type. 
    profile_struct.address.* -- access 1 level deeper.
FROM 
    parsed_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Another example with Books
-- MAGIC
-- MAGIC The `orders` table has an array of struct type for `books` column. 
-- MAGIC
-- MAGIC ## Function: explode()
-- MAGIC
-- MAGIC Spark has functions to handle arrays, with the most important one being `explode()`, which allows us to put each element of an array on its own row.

-- COMMAND ----------

SELECT
    order_id,
    customer_id,
    books
FROM 
    orders
;

-- COMMAND ----------

DESCRIBE orders;

-- COMMAND ----------

SELECT
    order_id,
    customer_id,
    explode(books) as book
FROM 
    orders
;
-- notice customer_id C0..2 has two rows, instead of one. This customer has purchased two books.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Function: collect_set_aggregation()
-- MAGIC Another interesting function is the `collect_set_aggregation()` function that allows us to collect unique values for a field, including fields within arrays.

-- COMMAND ----------

SELECT
    customer_id,
    collect_set(order_id) as orders_set,
    collect_set(books.book_id) as books_set
FROM 
    orders
GROUP BY
    customer_id
;
-- See results below. Can we then flatten this array? Yes, we can!

-- COMMAND ----------

SELECT
    customer_id,
    collect_set(books.book_id) AS before_flattenning,
    array_distinct( -- flatten the array, but remove duplicates. cust_id 4 has B08 appearing twice. Need only one.
        flatten(collect_set(books.book_id))
    ) AS after_flatten
FROM
    orders
GROUP BY 
    customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS 
SELECT 
    *
FROM (
    SELECT *, explode(books) as book
    FROM orders
) o
INNER JOIN
    books b
ON
    o.book.book_id = b.book_id
;

-- COMMAND ----------

SELECT * FROM orders_enriched;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates AS 
    SELECT * FROM parquet.`${dataset.bookstore}/orders-new`
;
SELECT * FROM orders
UNION 
SELECT * FROM orders_updates

-- COMMAND ----------

select * from orders
intersect -- essentially the same as the JOIN operation. The only difference is that the JOIN operation returns all columns, whereas the INTERSECT operation returns only the common columns
select * from orders_updates

-- COMMAND ----------

select * from orders o
JOIN orders_updates ou
on
o.order_id = ou.order_id 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## PIVOT
-- MAGIC Spark SQL also supports `PIVOT`, which is used to change data perspective.

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
    SELECT 
        * 
    FROM (
        SELECT
            customer_id,
            book.book_id AS book_id,
            book.quantity AS quantity
        FROM
            orders_enriched
    )
    PIVOT (
        sum(quantity) FOR book_id IN (
        'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 
        'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
    )
);

-- COMMAND ----------

-- Before pivot. Let us first understand what we're going to pivot
SELECT
  customer_id,
  book.book_id AS book_id,
  book.quantity AS quantity
FROM
  orders_enriched
WHERE
  customer_id = 'C00394'
ORDER BY
  book_id ASC

  -- After pivot
  -- As you can see, instead of 6 records of one customer_id, it has been flattened out to a single record with 12 columns

-- COMMAND ----------

SELECT * FROM transactions;