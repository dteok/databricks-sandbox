# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Streaming 
# MAGIC
# MAGIC To work with data streaming in SQL, we must first use `spark.readStream` method in PySpark API.
# MAGIC
# MAGIC We can think of a streaming query as an *always-on* incremental query.
# MAGIC
# MAGIC Always remember to cancel active streaming queries when done, otherwise, the cluster will not auto-terminate from inactivity.
# MAGIC
# MAGIC *NB: python notebook*

# COMMAND ----------

(spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Generally, we don't display a streaming result unless a human is actively monitoring the output.
# MAGIC This is because after SELECT execution of a streaming query, the query will continue to run indefinitely.
# MAGIC To stop the query, click the stop icon in the query results pane
# MAGIC */
# MAGIC SELECT * FROM books_streaming_tmp_vw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's add aggregation to the streaming query
# MAGIC -- Sorting in the streaming query is not supported. E.g. ORDER BY
# MAGIC SELECT
# MAGIC   author,
# MAGIC   count(book_id) AS total_books
# MAGIC FROM 
# MAGIC  books_streaming_tmp_vw
# MAGIC GROUP BY
# MAGIC   author

# COMMAND ----------

# MAGIC %md
# MAGIC Note that streaming queries are not persisted anywhere.
# MAGIC
# MAGIC In order to persist incremental results (write it to somewhere), we need to first pass our logic back to PySpark DataFrame API.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Because the subquery is a streaming temp view, this CORTV is also a streaming temp view
# MAGIC CREATE OR REPLACE TEMP VIEW author_countrs_tmp_vw AS (
# MAGIC     SELECT
# MAGIC       author,
# MAGIC       count(book_id) AS total_books
# MAGIC     FROM 
# MAGIC      books_streaming_tmp_vw
# MAGIC     GROUP BY
# MAGIC       author
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Now, the solution here is to feed from the CORTV streaming view into a DataFrame.

# COMMAND ----------

(spark.table("author_countrs_tmp_vw")
    .writeStream #writeStream method to persis the result of a streaming query to a durable storage
    .trigger(processingTime='4 seconds')
    .outputMode("complete") #outputMode("complete") to write all the data in the table. Choice between complete or append. For aggregation streaming queries, always use compelte to overwrite the table with new results
    .option("checkpointLocation", "dbfs:/mnt/demo/author_countrs_checkpoint") #checkpointLocation is to help tracking the progress of the streaming processing.
    .table("author_countrs")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   author_countrs --not a streaming query, nor a streaming table.

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding new data to source files (being streamed)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC With the above 2 cells still running, let's us add new data to the streaming table.
# MAGIC REMINDER: 
# MAGIC   - `books_streaming_tmp_vw`` is a streaming temp view, looking at books.
# MAGIC   - from that, we created `author_countrs_tmp_vw``
# MAGIC */
# MAGIC INSERT INTO books
# MAGIC VALUES 
# MAGIC     ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC     ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC     ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch mode streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES 
# MAGIC     ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC     ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC     ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_countrs_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True) # this method will trigger the stream once to process all new available data and STOP after execution.
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination() # this method blocks the execution of any cell in this notebook until this incremental batch's write succeeded.
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts
# MAGIC -- expects 18 rows in total

# COMMAND ----------

