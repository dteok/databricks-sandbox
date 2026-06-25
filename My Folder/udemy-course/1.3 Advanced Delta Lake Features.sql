-- Databricks notebook source
use catalog hive_metastore

-- COMMAND ----------

describe history employees

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 4

-- COMMAND ----------

-- same as above, but abbreviated.
select * from employees@v4

-- COMMAND ----------

delete from employees

-- COMMAND ----------

-- to confirm all data has been removed, we check now...
select * from employees

-- describe history will have a new version number

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- Restore the table to the previous version.
RESTORE TABLE employees TO VERSION AS OF 4

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

-- Z-order indexing
-- Optimize command and how to compact small files and do Z-order indexing
-- See output of describe detail above. Scroll to numFiles column. There is only ONE file.
-- If >1, then it means we have many small data files. This can negatively affect the performance of the delta table.
-- To resolve this issue, we use Optimize command.
Optimize employees 
zorder by id;


-- COMMAND ----------

-- Check the number of files again. It should be 1.
describe detail employees
-- Vacuum command
-- Vacuum command is used to delete files that are no longer needed.

-- COMMAND ----------

describe history employees
-- if numFiles were more than 1, and executed optimize command above, then history will have a new entry for OPTIMIZE.
-- We did not. So, no new entry in history

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- we can see there are four data files. But we know our current table references only one file after optimize operation.
-- This means other data files are unused. We can clean them up.
-- Let's vacuum the table.
-- Vacuum command is used to delete files that are no longer needed, and is older than 7 days (default)
vacuum employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

-- now, let's drop the table and understand what happens
drop table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

