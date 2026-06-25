-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

create table if not exists smartphones (id INT,
  name STRING,
  brand STRING,
  year INT);

-- COMMAND ----------

insert into smartphones
values (1, 'iPhone 14', 'Apple', 2022),
       (2, 'iPhone 13', 'Apple', 2021),
       (3, 'iPhone 6', 'Apple', 2014),
       (4, 'iPad Air', 'Apple', 2013),
       (5, 'Galaxy S22', 'Samsung', 2022),
       (6, 'Galaxy Z Fold', 'Samsung', 2022),
       (7, 'Galaxy S9', 'Samsung', 2016),
       (8, 'XiaoMi 12 Pro', 'Xiaomi', 2022),
       (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
       (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

show tables;

-- COMMAND ----------

create view view_apple_phones
AS SELECT * FROM smartphones WHERE brand = 'Apple';

-- COMMAND ----------

select * from view_apple_phones

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that the view object `view_apple_phones` is not temporary. Each time a query against this view, it executes against its underlying table.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Create a temporary view

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS SELECT DISTINCT brand 
    FROM smartphones;

-- COMMAND ----------

SELECT * FROM temp_view_phones_brands

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Global temporary view
-- MAGIC
-- MAGIC To query a global temporary table, we have to include `global_temp.` followed by the name of the table. 
-- MAGIC
-- MAGIC It will not be listed in `SHOW TABLES` command.

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;


-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To view all global temporary tables, issue the following command `SHOW TABLES IN global_temp`.

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

