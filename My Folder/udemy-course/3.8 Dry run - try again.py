# Databricks notebook source
# MAGIC %md
# MAGIC #Delta Lake Tables (DLT)

# COMMAND ----------

print("Hello world! 同志们辛苦了！")

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_bronze_layer;
# MAGIC DROP TABLE IF EXISTS orders_bronze_layer;
# MAGIC DROP TABLE IF EXISTS orders_cleaned;
# MAGIC DROP TABLE IF EXISTS orders_silver_unnest_tmp;

# COMMAND ----------

# chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/checkpoints/")
# display(chkpt_files)

dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/bronze_customers_schema", recurse=True)
dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/bronze_orders_schema", recurse=True)
dbutils.fs.rm("dbfs:/mnt/demo/checpoints/bronze-orders-raw", recurse=True)
dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/silver_orders_cleaned", recurse=True)
dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/orders_raw", recurse=True)
dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/orders_silver", recurse=True)


# COMMAND ----------


dbutils.fs.rm(f"dbfs:/user/hive/warehouse/bronze_customers_schema", recurse=True)
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/bronze_orders_schema", recurse=True)
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/bronze_orders_raw", recurse=True)

dbutils.fs.rm(f"dbfs:/user/hive/warehouse/silver_orders_cleaned", recurse=True)
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/orders_raw", recurse=True)
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/orders_silver", recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze layer
# MAGIC Ingest raw data files
# MAGIC 1. Orders -- best practise is to leverage temporary table with Spark readStream.
# MAGIC     - `bronze_orders_tmp`
# MAGIC 2. Customers -- best practise is to leverage temporary table with Spark readStream.
# MAGIC     - `bronze_customers_raw_tmp`

# COMMAND ----------

order_files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(order_files)

# COMMAND ----------


# dbutils.fs.rm("dbfs:/mnt/demo/mymultihop", recurse=True)
# chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/mymultihop/")
# display(chkpt_files)


# COMMAND ----------

# Ingest raw Orders
# Materialising data ingestion stream to write to "bronze_orders_raw"
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", f"{dataset_bookstore}/mymultihop/bronze_orders_raw_schema")
  .load(f"{dataset_bookstore}/orders-raw")
  .writeStream
  .option("checkpointLocation", f"{dataset_bookstore}/mymultihop/bronze_orders_raw")
  .table("bronze_orders_raw")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW bronze_orders_tmp AS
# MAGIC   WITH cte_orders_raw AS (
# MAGIC     select DISTINCT
# MAGIC       order_id,
# MAGIC       order_timestamp,
# MAGIC       customer_id,
# MAGIC       quantity,
# MAGIC       total,
# MAGIC       explode(books) AS book
# MAGIC     from
# MAGIC       bronze_orders_raw
# MAGIC   )
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     order_timestamp,
# MAGIC     customer_id,
# MAGIC     quantity,
# MAGIC     total,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS book_quantity,
# MAGIC     book.subtotal AS subtotal,
# MAGIC     current_timestamp() AS arrival_time,
# MAGIC     input_file_name() AS source_file
# MAGIC   FROM
# MAGIC     cte_orders_raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bronze_orders_tmp

# COMMAND ----------

cust_files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(cust_files)

# COMMAND ----------

# Ingest raw customers data and materialised it to bronze_customers_raw
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", f"{dataset_bookstore}/mymultihop/bronze_customers_raw_schema")
  .load(f"{dataset_bookstore}/customers-json")
  .writeStream
  .option("checkpointLocation", f"{dataset_bookstore}/mymultihop/bronze_customers_raw")
  .table("bronze_customers_raw")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW bronze_customers_raw_tmp AS
# MAGIC WITH cte_customers_raw AS (
# MAGIC   select
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     from_json(
# MAGIC       profile,
# MAGIC       schema_of_json(
# MAGIC         '{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}'
# MAGIC       )
# MAGIC     ) AS profile_struct
# MAGIC   from
# MAGIC     bronze_customers_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   email,
# MAGIC   profile_struct.first_name AS firstname,
# MAGIC   profile_struct.last_name AS lastname,
# MAGIC   profile_struct.gender AS gender,
# MAGIC   profile_struct.address.city AS city,
# MAGIC   profile_struct.address.country AS country
# MAGIC FROM
# MAGIC   cte_customers_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_customers_raw_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: Ingesting from Bronze into Silver
# MAGIC
# MAGIC Note: `bronze_orders_raw` and `bronze_customers_raw` are readStreams, not the ones I have created after ingesting them - `bronze_orders_tmp` and `bronze_customers_raw_tmp`
# MAGIC
# MAGIC This means I will have to "redo" above sections where I have CTEs.
# MAGIC - Just run Spark readStreams.
# MAGIC - Do not create temporary tables as I have for `bronze_orders_tmp` and `bronze_customers_raw_tmp`.
# MAGIC - Run the cell just below this, and continue.

# COMMAND ----------


# dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/silver_orders_cleaned", recurse=True)
# chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/checkpoints/")
# display(chkpt_files)

# dbutils.fs.rm(f"dbfs:/user/hive/warehouse/silver_orders_cleaned", recurse=True)

# COMMAND ----------

# Read the streaming orders
orders_stream = spark.readStream.table("bronze_orders_raw")

# Read customers as a static table for enrichment
customers_static = spark.read.table("bronze_customers_raw")

# Join them together on customer_id
enriched_orders = orders_stream.join( 
    customers_static, 
    orders_stream.customer_id == customers_static.customer_id,
    "inner"
).select(
    orders_stream.order_id,
    orders_stream.customer_id,
    orders_stream.order_timestamp,
    customers_static.profile, #matching up profile info
    orders_stream.books
)

# Write out to a temporary silver table
(enriched_orders.writeStream
   .option("checkpointLocation", f"{dataset_bookstore}/mymultihop/silver_orders_cust_combined") ## checkpoint location. Should have updated it to .../demo/mymultihop/..
   .table("orders_customers_combined")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from orders_customers_combined
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_customers_orders AS
# MAGIC WITH cte_orders_raw AS (
# MAGIC     SELECT DISTINCT
# MAGIC       order_id,
# MAGIC       order_timestamp,
# MAGIC       customer_id,
# MAGIC       quantity,
# MAGIC       total,
# MAGIC       explode(books) AS book,
# MAGIC       current_timestamp() AS arrival_time,
# MAGIC       input_file_name() AS source_file
# MAGIC     from
# MAGIC       bronze_orders_raw
# MAGIC )
# MAGIC , cte_customers_raw AS (
# MAGIC     SELECT
# MAGIC       customer_id,
# MAGIC       email,
# MAGIC       from_json(
# MAGIC       profile,
# MAGIC       schema_of_json(
# MAGIC         '{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}'
# MAGIC       )
# MAGIC     ) AS profile_struct
# MAGIC   from
# MAGIC     bronze_customers_raw
# MAGIC )
# MAGIC
# MAGIC , cte_customers_flattened AS (
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   email,
# MAGIC   profile_struct.first_name AS firstname,
# MAGIC   profile_struct.last_name AS lastname,
# MAGIC   profile_struct.gender AS gender,
# MAGIC   profile_struct.address.city AS city,
# MAGIC   profile_struct.address.country AS country
# MAGIC FROM
# MAGIC   cte_customers_raw
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.email,
# MAGIC     c.firstname,
# MAGIC     c.lastname,
# MAGIC     c.gender,
# MAGIC     c.city,
# MAGIC     c.country,
# MAGIC     o.order_id,
# MAGIC     CAST(from_unixtime(o.order_timestamp, 'yyyy-MM-dd HH:mm') AS timestamp) AS order_timestamp,
# MAGIC     -- o.customer_id,
# MAGIC     o.total,
# MAGIC     o.book.book_id AS book_id,
# MAGIC     o.book.quantity AS book_quantity,
# MAGIC     o.book.subtotal AS subtotal
# MAGIC FROM 
# MAGIC   cte_customers_flattened c 
# MAGIC INNER JOIN 
# MAGIC   cte_orders_raw o
# MAGIC ON 
# MAGIC   c.customer_id = o.customer_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_customers_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   customer_id,
# MAGIC   email,
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   gender,
# MAGIC   city,
# MAGIC   country,
# MAGIC   book_id,
# MAGIC   order_timestamp,
# MAGIC   book_quantity,
# MAGIC   subtotal
# MAGIC FROM 
# MAGIC   silver_customers_orders
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold layer: Aggregation of data
# MAGIC Best practise is to create views off silver tables.
# MAGIC But we are not doing that.
# MAGIC
# MAGIC Most of the heavy lifting should be done in the Silver layer.
# MAGIC
# MAGIC As far as this demo is concerned, we conclude here.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   f_name,
# MAGIC   l_name,
# MAGIC   date_trunc("DD", order_timestamp) AS order_date,
# MAGIC   SUM(quantity) AS books_counts
# MAGIC FROM
# MAGIC   orders_cleaned
# MAGIC WHERE
# MAGIC   country = 'France'
# MAGIC GROUP BY
# MAGIC   customer_id,
# MAGIC   f_name,
# MAGIC   l_name,
# MAGIC   date_trunc("DD", order_timestamp)

# COMMAND ----------

# terminate all spark sessions

for s in spark.streams.active:
    print(f"Stopping stream: {s.id}")
    s.stop()
    s.awaitTermination()
