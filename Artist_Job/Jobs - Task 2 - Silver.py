# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC ![databricks_academy_logo.png](../Includes/images/databricks_academy_logo.png "databricks_academy_logo.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2 - Silver - Gold Table
# MAGIC This notebook is used for task 2 in the job from the directions in notebook: **Jobs - Creating a Simple Lakeflow Job**

# COMMAND ----------

####################################################################################
#catalog_name = "dbpraxis"
#schema_name = "create_job_artist"
#volume_name = "myfiles"
####################################################################################


# COMMAND ----------

# MAGIC %md
# MAGIC ## Capture Job Parameters

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Your Environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Set the default catalog and schema.

# COMMAND ----------

# Set the catalog and schema
spark.sql(f'USE CATALOG {catalog_name}')
spark.sql(f'USE SCHEMA {schema_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER
# MAGIC **Objective**: Transform the bronze table and create the silver table.
# MAGIC
# MAGIC 1. Create a table named **current_employees_silver_job** from the **current_employees_bronze_job** table. 
# MAGIC
# MAGIC     The table will:
# MAGIC     - Select the columns **ID**, **FirstName**, **Country**.
# MAGIC     - Convert the **Role** column to uppercase.
# MAGIC     - Add two new columns: **CurrentTimeStamp** and **CurrentDate**.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO album (title) SELECT DISTINCT album FROM track;
# MAGIC INSERT INTO artist (name) SELECT DISTINCT artist FROM track;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH track_silver_view AS (
# MAGIC SELECT
# MAGIC   t.id,
# MAGIC   t.title,
# MAGIC   t.count,
# MAGIC   t.rating,
# MAGIC   t.len,
# MAGIC   al.id AS album_id,
# MAGIC   ar.id AS artist_id
# MAGIC FROM track AS t
# MAGIC JOIN album  AS al ON t.album = al.title
# MAGIC JOIN artist AS ar ON t.artist = ar.name)
# MAGIC SELECT * FROM track_silver_view limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW track_silver_view AS
# MAGIC SELECT
# MAGIC   t.id,
# MAGIC   t.title,
# MAGIC   t.count,
# MAGIC   t.rating,
# MAGIC   t.len,
# MAGIC   al.id AS album_id,
# MAGIC   ar.id AS artist_id
# MAGIC FROM track AS t
# MAGIC JOIN album  AS al ON t.album = al.title
# MAGIC JOIN artist AS ar ON t.artist = ar.name;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM track_silver_view limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS track_silver;
# MAGIC CREATE OR REPLACE table track_silver AS
# MAGIC SELECT
# MAGIC   t.id,
# MAGIC   t.title,
# MAGIC   t.count,
# MAGIC   t.rating,
# MAGIC   t.len,
# MAGIC   al.id AS album_id,
# MAGIC   ar.id AS artist_id
# MAGIC FROM track AS t
# MAGIC JOIN album  AS al ON t.album = al.title
# MAGIC JOIN artist AS ar ON t.artist = ar.name;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM track_silver limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD
# MAGIC **Objective:** Aggregate the silver table to create the final gold table.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create a temporary view named **temp_view_total_roles_job** that aggregates the total number of employees by role. Then, display the results of the view.

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_total_roles_job AS 
# MAGIC SELECT
# MAGIC   Role, 
# MAGIC   count(*) as TotalEmployees
# MAGIC FROM current_employees_silver_job
# MAGIC GROUP BY Role;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Create the final gold table named **total_roles_gold_job** with the specified columns.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Insert all rows from the aggregated temporary view **temp_view_total_roles_job** into the **total_roles_gold_job** table, overwriting the existing data in the table. This overwrites the data in a table but keeps the existing schema and table definition and properties.
# MAGIC
# MAGIC     Confirm the following:
# MAGIC     - **num_affected_rows** is *4*
# MAGIC     - **num_inserted_rows** is *4*
