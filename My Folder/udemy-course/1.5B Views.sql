-- Databricks notebook source
USE CATALOG hive_metastore;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take notice that the temporary view created with `CREATE TEMP VIEW temp_view_phones_brands` is no longer available. 
-- MAGIC This is because this new notebook. A new notebook means a new spark session. A new spark session means all `TEMP VIEW` creations has been purged.
-- MAGIC
-- MAGIC However, running `SHOW TABLES IN global_temp` will show that the `global_temp_view_latest_phones` still persists. As long as the cluster (compute) **is still running, and any notebooks attached the the cluster** can access its global temporary views.

-- COMMAND ----------

-- if cluster (compute) is restarted, this statement will not work.
select * from global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

DROP TABLE smartphones;


-- COMMAND ----------

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

