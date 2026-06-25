# Databricks notebook source
# autoloader
print("hello work! Do you best!")

# COMMAND ----------

# MAGIC %md
# MAGIC # COPY INTO vs. AUTO LOADER
# MAGIC
# MAGIC ## COPY INTO
# MAGIC - Thousands of files
# MAGIC - Less efficient at scale
# MAGIC
# MAGIC ## Auto Loader
# MAGIC - Millions of files over time
# MAGIC - Efficient at scale: can split the processing into multiple batches to achieve this efficiency.
# MAGIC - Databricks recommends this approach when ingesting data from cloud storages.

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1781688422311.png](./image_1781688422311.png "image_1781688422311.png")

# COMMAND ----------

# Let's explore our data source dir
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
    .load(f"{dataset_bookstore}/orders-raw")
    .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- above execution is a streaming operation. Once data is loaded, we can query it as usual.
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC # Oops! I only meant to ingest ONE file
# MAGIC Let's fix that
# MAGIC
# MAGIC ## A clean start (recommended)

# COMMAND ----------

dbutils.notebook.exit('')

# COMMAND ----------

chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/orders_checkpoint")
display(chkpt_files)

# COMMAND ----------

# Remove old checkpoint
dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", recurse=True)

# Remove the target table files because I want a complete fresh table
# Note: Verify with DROP TABLE in sql
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/orders_updates", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's now load one file

# COMMAND ----------

# Development Helper: Clean environment on demand
DEBUG_CLEANUP = False

if DEBUG_CLEANUP:
    dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", recurse=True)
    print("Checkpoint cleared for fresh start.")

# COMMAND ----------

chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/orders_checkpoint")
display(chkpt_files)
# should return no such file or directory

# COMMAND ----------

# move 02.parquet to its own directory
# dbutils.fs.mv(f"{dataset_bookstore}/orders-raw/02.parquet", 
#               f"{dataset_bookstore}/orders-raw/batch2/02.parquet")

dbutils.fs.mv(f"{dataset_bookstore}/orders-raw/batch2/02.parquet", 
              f"{dataset_bookstore}/orders-raw_batch/02.parquet")

# COMMAND ----------

# dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
# dbutils.fs.rm(f"{dataset_bookstore}/orders-raw/batch2", recurse=True)

# COMMAND ----------

# Let's explore our data source dir
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
    .format("cloudFiles") # will ingest all files in the directory recursively, including subdirectories
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
    .load(f"{dataset_bookstore}/orders-raw")
    .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

chkpt_files = dbutils.fs.ls(f"dbfs:/mnt/demo/orders_checkpoint")
display(chkpt_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates; -- expecting 1,000 rows. Not 2,000.

# COMMAND ----------

# MAGIC %md
# MAGIC # Now we load new files
# MAGIC We will now run `load_new_data()` twice to load 1,000 records each time -- adding additional 2,000, totalling 3,000.

# COMMAND ----------

load_new_data()

# COMMAND ----------

# Let's explore our data source dir again. We should see 2 new files.
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM orders_updates
# MAGIC ; -- expecting 3,000 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up
# MAGIC Remove checkpoint location

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------

