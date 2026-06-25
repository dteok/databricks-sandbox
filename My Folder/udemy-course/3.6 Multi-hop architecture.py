# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Multi-hop Architecture
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="./image_1781688422311.png" alt="My Image" width="800px")
# MAGIC </div>
# MAGIC
# MAGIC Basically, it is the medallion architecture -- bronze (ingestion layer), silver (filtered, cleansed, and curated), and gold (aggregated dataset)

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)
# we should have 3 files following from previous notebook 3.3

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will enrich our raw data without additional metadata describing the source file and the time of ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_tmp as (
# MAGIC     SELECT 
# MAGIC       *,
# MAGIC       current_timestamp() AS arrival_time,
# MAGIC       input_file_name() AS source_file
# MAGIC     FROM
# MAGIC       orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's check; NB. Dependent on above two cells. `orders_tmp` is a Spark stream session
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM orders_tmp

# COMMAND ----------

# MAGIC %md 
# MAGIC Now, pass this enriched data back to PySpark API to process an incremental write to `Delta Lake` table called `orders_bronze`.

# COMMAND ----------

(spark.table("orders_tmp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders")
    .outputMode("append")
    .table("orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver layer
# MAGIC For the purpose of this demo, we need a static lookup table to joining it with our bronze table.
# MAGIC
# MAGIC Below, we create a customer static temporary view from JSON file

# COMMAND ----------

# we are doing with PySpark API, but we can also use Spark SQL
(spark.read
    .format("json")
    .load(f"{dataset_bookstore}/customers-json")
    .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC To work with the bronze layer, we start by creating a streaming temporary view against our bronze table
# MAGIC

# COMMAND ----------

(spark.readStream
    .table("orders_bronze")
    .createOrReplaceTempView("orders_bronze_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creates `orders_enriched_tmp` from `orders_tmp` and `customers_lookup, which are in bronze.
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC     SELECT 
# MAGIC       order_id,
# MAGIC       quantity,
# MAGIC       o.customer_id,
# MAGIC       c.profile:first_name AS f_name,
# MAGIC       c.profile:last_name AS l_name,
# MAGIC       CAST(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) AS order_timestamp,
# MAGIC       books
# MAGIC     FROM 
# MAGIC       orders_bronze_tmp o 
# MAGIC     INNER JOIN 
# MAGIC       customers_lookup c
# MAGIC     ON o.customer_id = c.customer_id 
# MAGIC     WHERE
# MAGIC       quantity > 0
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver layer

# COMMAND ----------

# now stream write for this orders enriched data into a silver table

(spark.table("orders_enriched_tmp") # READ FROM this table
    .writeStream 
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
    .outputMode("append")
    .table("orders_silver") # WRITE TO this table
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold layer

# COMMAND ----------

(spark.readStream
    .table("orders_silver") # read from this table
    .createOrReplaceTempView("orders_silver_tmp") # write to this temp view
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table daily_customer_books;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NB. This temporary view reads from `orders_silver_tmp`, which is a readStream. Hence, querying this view will
# MAGIC -- start a streaming query
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS ( 
# MAGIC     SELECT 
# MAGIC       customer_id,
# MAGIC       f_name,
# MAGIC       l_name,
# MAGIC       date_trunc("DD", order_timestamp) AS order_date,
# MAGIC       SUM(quantity) AS book_counts
# MAGIC     FROM
# MAGIC       orders_silver_tmp
# MAGIC     GROUP BY 
# MAGIC       customer_id,
# MAGIC       f_name,
# MAGIC       l_name,
# MAGIC       date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books_tmp
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's write this aggregated data into a gold table called `daily_customer_books`

# COMMAND ----------

# chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/checkpoints/daily_customer_books")
# display(chkpt_files)

# dbutils.fs.rm("dbfs:/mnt/demo/checkpoints/daily_customer_books", recurse=True)

# COMMAND ----------

(spark.table("daily_customer_books_tmp") # READ FROM this table
    .writeStream
    .format("delta")
    .outputMode("complete") # overwrites full table each run
#    .option("mergeSchema", "true")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
    .trigger(availableNow=True) # trigger availablenow=true is a batch job
    .table("daily_customer_books")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can now query the gold table
# MAGIC
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM
# MAGIC   daily_customer_books
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC # Terminate all active streams
# MAGIC Run as Python
# MAGIC
# MAGIC ```python
# MAGIC for s in spark.streams.active:
# MAGIC     print(f"Stopping stream: {s.id}")
# MAGIC     s.stop()
# MAGIC     s.awaitTermination()
# MAGIC ```

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping stream: {s.id}")
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# concludes