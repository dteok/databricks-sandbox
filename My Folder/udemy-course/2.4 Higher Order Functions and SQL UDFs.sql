-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

-- Note again that the books column is of STRUCT type. We need to use higher order functions to access the elements of the struct.
SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # HOF
-- MAGIC One of the most common Higher Order Function (HOF) is the **`FILTER`** function, which filters an array using a given lambda function.
-- MAGIC
-- MAGIC ## FILTER function
-- MAGIC The following example shows we are creating a new column called `multiple_copies`, where we **filter** the books column to extract only those books having a quantity >= 2

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER(books, i -> i.quantity >= 2) AS multiple_copies -- accessing the STRUCT type by  assigning it an alias 'i'
FROM 
  orders
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SIZE function
-- MAGIC Think of this as an array that is not empty will have a size > 0. 
-- MAGIC
-- MAGIC Similarly, in Python, a non-empty list has a len > 0.

-- COMMAND ----------

/*
Notice that the FILTER function also returns empty arrays when the condition is not met.
We can filter them out by making the main query as a subquery or as a CTE.
*/
WITH cte_multiple_copies AS (
    SELECT
      order_id,
      books,
      FILTER(books, i -> i.quantity >= 2) AS multiple_copies -- accessing the STRUCT type by  assigning it an alias 'i'
    FROM 
      orders
)

SELECT 
  order_id,
  multiple_copies
FROM
  cte_multiple_copies
WHERE
  size(multiple_copies) > 0 -- size() is the equivalent of len() in python. This is a higher order function
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TRANSFORM
-- MAGIC The `TRANSFORM` functoin is used to apply a transformation on all items in an array and extract the transformed value.

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM
  orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # User Defined Functions, UDF
-- MAGIC
-- MAGIC UDF leverage spark SQL directly, maintaining all the optimization of Spark.
-- MAGIC
-- MAGIC ```
-- MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
-- MAGIC                            |       |     +--------> data type
-- MAGIC                            |       +--------------> optional parameters
-- MAGIC                            +----------------------> function name
-- MAGIC         +-----------------------------------------> the type to be returned
-- MAGIC         |
-- MAGIC RETURNS STRING
-- MAGIC RETURN concat("https://www.", split(email, "@")[1]) 
-- MAGIC        +-------------------------------------------> and some custom logics, if applicable in your application.
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Cell 10
CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

--let us start using the UDF
SELECT
  email,
  get_url(email) AS domain
FROM 
  customers
WHERE 
  email IS NOT NULL

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

-- A slightly more complicated UDF

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN 
  CASE 
    WHEN email like "%.com" THEN "Commercial Business"
    WHEN email like "%.org" THEN "Non-profits Organisation"
    WHEN email like "%.edu" THEN "Educational Institution"
    ELSE concat("Unknown extension for domain: ", split(email, "@")[1])
  END
;
  

-- COMMAND ----------

--let us start using the UDF
SELECT
  email,
  get_url(email) AS domain,
  site_type(email) AS domain_category
FROM 
  customers
WHERE 
  email IS NOT NULL

-- COMMAND ----------

