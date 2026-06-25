-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # SQL to insert and update records in Delta Tables

-- COMMAND ----------

-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

create table if not exists orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS 
SELECT * FROM parquet.`${dataset.bookstore}/orders`;


-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Appending records

-- COMMAND ----------

INSERT INTO orders
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MERGE INTO
-- MAGIC With `INSERT INTO` we can insert existing records resulting in duplicates. However, with `MERGE`, we can upsert data from source table, view, or dataframe into the target data table

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED 
    AND c.email is NULL 
    AND u.email IS NOT NULL 
THEN
    UPDATE
    SET 
        email = u.email,
        updated = u.updated
WHEN NOT MATCHED
THEN
    INSERT *

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Another example of using CORV with file options

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates (
    book_id string,
    title string,
    author string,
    category string,
    price double)
USING CSV
OPTIONS (
    path = "${dataset.bookstore}/books-csv-new",
    header = "true",
    delimiter = ";"
);

SELECT * FROM books_updates;
-- Say, we are only interested in inserting the Computer Science books. We'll leverage the MERGE statement

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
    ON b.book_id = u.book_id
    AND b.title = u.title
WHEN NOT MATCHED 
    AND u.category = 'Computer Science'
THEN
    INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the above cell, if we rerun the SQL, zero records will be inserted -- because 3 records were previously inserted the first time. The `book_id` and `title` are existing.
-- MAGIC
-- MAGIC This effectively avoids duplicate inserts.

-- COMMAND ----------

