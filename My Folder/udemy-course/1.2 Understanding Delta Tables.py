# Databricks notebook source
# MAGIC %sql
# MAGIC -- USE CATALOG hive_metastore
# MAGIC USE CATALOG udemycourseworkspace

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employees 
# MAGIC   (id INT,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE)

# COMMAND ----------

# MAGIC %sql describe detail employees

# COMMAND ----------

# MAGIC %fs ls 'abfss://unity-catalog-storage@dbstorageidbsj3qlxzbnc.dfs.core.windows.net/7405617380997222/__unitystorage/catalogs/e877625d-e858-4afa-94f1-b4af78bb416a/tables/6baceedd-bf0d-4f1f-9794-30e30e7da37c'

# COMMAND ----------

# MAGIC
# MAGIC %fs ls 'dbfs:/Workspace/Users/dantvli@hotmail.com/databricks-sandbox/My Folder/udemy-course'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (5, "Anna", 2500.0);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (6, "Kim", 6200.3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

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

# MAGIC %fs ls '

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json'

# COMMAND ----------


