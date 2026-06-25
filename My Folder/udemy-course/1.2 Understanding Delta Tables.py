# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employees 
# MAGIC   (id INT,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE)

# COMMAND ----------

# MAGIC %sql describe detail employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update senario
# MAGIC UPDATE employees
# MAGIC SET salary = salary - 1000
# MAGIC WHERE name like "A%"
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql describe history employees 

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json'

# COMMAND ----------

