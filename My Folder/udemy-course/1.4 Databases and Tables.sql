-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

CREATE TABLE managed_default (width INT, length INT, height INT);

INSERT INTO managed_default VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_default;

-- COMMAND ----------

-- NOTICE Location: dbfs:/user/hive/warehouse/managed_default
-- and the type of the table is MANAGED



-- COMMAND ----------

-- NOW we create an example of an EXTERNAL table
CREATE TABLE external_default (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED external_default;

-- COMMAND ----------

-- we can drop the managed table
drop table managed_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehosue/managed_default'

-- COMMAND ----------

-- we can drop the external table
-- ls the location will show the data is still there, even though external table is dropped
-- this is because the underlying data is not managed by Hive.
drop table external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating new database / schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS new_default

-- COMMAND ----------

describe database extended new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Let's create some tables

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default (width INT, length INT, height INT);

INSERT INTO managed_new_default VALUES (3 INT, 2 INT, 1 INT);
--------------

CREATE TABLE external_new_default (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';

INSERT INTO external_new_default VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

describe database extended new_default

-- we can drop the managed table
--drop table managed_new_default;
--)

-- COMMAND ----------

describe extended external_new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DROPPING tables -- both managed and external tables

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## CREATE a table outside of the Hive directory

-- COMMAND ----------

DROP SCHEMA IF EXISTS custom;
CREATE SCHEMA IF NOT EXISTS custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom (width INT, length INT, height INT);

INSERT INTO managed_custom VALUES (3 INT, 2 INT, 1 INT);
--------------

CREATE TABLE external_custom (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';

INSERT INTO external_custom VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

drop table managed_custom;
drop table external_custom;

-- COMMAND ----------

