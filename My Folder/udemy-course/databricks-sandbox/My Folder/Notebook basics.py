# Databricks notebook source
print("Hello world! I have finished watching The Pursuit of Jade")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "hello database world!" as greetings

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC - list item
# MAGIC - list item
# MAGIC - list item
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC Images: 
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And, of course, tables:
# MAGIC | user_id | user name |
# MAGIC |---------|-----------|
# MAGIC |  1      |  Adel        |
# MAGIC |  1      |  Bethany      |
# MAGIC |  1      |  Casey        |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank">  Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# dbutils is more useful than %fs 's magic command: ls because dbutils can be used as part of python code.

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

