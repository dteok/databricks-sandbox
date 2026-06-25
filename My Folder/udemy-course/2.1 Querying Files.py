# Databricks notebook source
# MAGIC %md
# MAGIC # Extracting data directly from files

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## We can read JSON data files
# MAGIC enclose the path with double-backticks \`${}\`
# MAGIC Remember the dollar $ symbol after first backtick
# MAGIC
# MAGIC We can also use wildcard character, the asterisk *, to query multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     *,
# MAGIC     input_file_name() as source_file -- helpful to troubleshoot as we can see which file the record orignated
# MAGIC from json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`${dataset.bookstore}/books-csv`
# MAGIC
# MAGIC -- note that the default delimited is a comma. Output shows the delimiter is a semicolon, hence, not parsed correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS books_csv;
# MAGIC
# MAGIC CREATE TABLE books_csv (
# MAGIC     book_id STRING,
# MAGIC     title STRING,
# MAGIC     author STRING,
# MAGIC     category STRING,
# MAGIC     price DOUBLE
# MAGIC     )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv" -- essentially creating an external table pointing to this location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended books_csv

# COMMAND ----------

# check how many CSV files we have in the directory
files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")

display(files)

# COMMAND ----------

(spark.read
        .table("books_csv")
        .write
        .mode("append")
        .format("csv")
        .option('header', 'true')
        .option('delimiter', ';')
        .save(f"{dataset_bookstore}/books-csv"))


# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")

display(
    files
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ***************************
# MAGIC Use the following two commands if necessary, otherwise, skip.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE books_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum books_csv

# COMMAND ----------

dbutils.fs.rm(f"{dataset_bookstore}/books-csv", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ***************************

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Delta Tables From External Sources
# MAGIC .. we use CTAS
# MAGIC
# MAGIC Note that in `DESCRIBE EXTENDED`, the row "Type" and "Provider" has the values MANAGED and DELTA respectively.
# MAGIC And, data types created by CTAS are `string` by default.
# MAGIC
# MAGIC CTAS do not support specifying additional file options.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers;
# MAGIC
# MAGIC CREATE TABLE customers AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS books_unparsed;
# MAGIC
# MAGIC CREATE TABLE books_unparsed AS
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`;
# MAGIC
# MAGIC SELECT * FROM books_unparsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended books_unparsed;

# COMMAND ----------

# MAGIC %md
# MAGIC Even though we have successfully created a delta table here, the data is not well parsed. To fix this, we first need to use a reference to the files that allow us to specify options.
# MAGIC
# MAGIC To achieve that, we create a temporary view that allows us to specify file options.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_unparsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_books_tmp 
# MAGIC     (book_id string, title string, author string, category string, price double) 
# MAGIC     USING CSV
# MAGIC     OPTIONS (
# MAGIC         path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC         header = "true",
# MAGIC         delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS books AS SELECT * FROM v_books_tmp;
# MAGIC     
# MAGIC select * from books;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended books;
# MAGIC
# MAGIC -- check that it is still a delta and managed table. OK!

# COMMAND ----------

# MAGIC %md
# MAGIC # Simplified File Querying
# MAGIC Instead of creating temp views like we have above, Databricks introduced a new function called `read_files`.
# MAGIC
# MAGIC ```SQL
# MAGIC SELECT * FROM read_files(
# MAGIC     '${dataset_bookstore}/books-csv/export_*.csv',
# MAGIC     format => 'csv',
# MAGIC     header => 'true',
# MAGIC     delimiter => ';'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files(
# MAGIC     '${dataset.bookstore}/books-csv/export_*.csv',
# MAGIC     format => 'csv',
# MAGIC     header => 'true',
# MAGIC     delimiter => ';'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC The _metadata Column
# MAGIC
# MAGIC The `input_file_name()` function is no longer supported in newer versions of the Databricks Runtime. Use `_metadata.file_path` attribute to retrieve the file path information as a substitute.
# MAGIC
# MAGIC ```sql
# MAGIC     SELECT *,
# MAGIC         _metadata.file_path AS source_file, --displays file path
# MAGIC         _metadata.file_name AS source_file_name,
# MAGIC         _metadata.file_size AS file_size,
# MAGIC         _metadata.file_modification_time AS file_last_modified
# MAGIC     FROM json.'${dataset.bookstore}/customers_json'
# MAGIC ```

# COMMAND ----------

