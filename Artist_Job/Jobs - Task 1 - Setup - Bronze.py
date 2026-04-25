# Databricks notebook source
# MAGIC %md
# MAGIC ![databricks_academy_logo.png](../Includes/images/databricks_academy_logo.png "databricks_academy_logo.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 1 - Setup and Bronze Table
# MAGIC This notebook is used for task 1 in the job from the directions in notebook: **02-Creating a Simple Lakeflow Job**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capture Job Parameters

# COMMAND ----------

####################################################################################
#catalog_name = "dbpraxis"
#schema_name = "create_job_artist"
#volume_name = "myfiles"
####################################################################################


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
# MAGIC ### BRONZE
# MAGIC **Objective:** Create a table using all of the CSV files in the **myfiles** volume.

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS album;
# MAGIC CREATE OR REPLACE TABLE album (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) ,
# MAGIC     title VARCHAR(128),
# MAGIC     PRIMARY KEY(id)
# MAGIC );
# MAGIC --product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
# MAGIC --DROP TABLE IF EXISTS track;
# MAGIC CREATE OR REPLACE TABLE track (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     title STRING, 
# MAGIC     artist STRING, 
# MAGIC     album STRING, 
# MAGIC     album_id BIGINT,
# MAGIC     count INTEGER, 
# MAGIC     rating INTEGER, 
# MAGIC     len INTEGER,
# MAGIC     PRIMARY KEY(id),
# MAGIC     CONSTRAINT fk_album FOREIGN KEY (album_id) REFERENCES album(id) --ON DELETE CASCADE
# MAGIC );
# MAGIC
# MAGIC --DROP TABLE IF EXISTS artist;
# MAGIC CREATE OR REPLACE TABLE artist (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     name VARCHAR(128),
# MAGIC     PRIMARY KEY(id)
# MAGIC );
# MAGIC
# MAGIC --DROP TABLE IF EXISTS tracktoartist;
# MAGIC CREATE OR REPLACE TABLE tracktoartist (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     track VARCHAR(128),
# MAGIC     track_id  BIGINT ,
# MAGIC     artist VARCHAR(128),
# MAGIC     artist_id BIGINT,
# MAGIC     PRIMARY KEY(id),
# MAGIC     CONSTRAINT fk_track FOREIGN KEY (track_id) REFERENCES track(id), --ON DELETE CASCADE,
# MAGIC     CONSTRAINT fk_artist FOREIGN KEY (artist_id) REFERENCES artist(id) --ON DELETE CASCADE
# MAGIC );

# COMMAND ----------

# Create the bronze raw ingestion table and include the file name for the rows
spark.sql(f'''
  COPY INTO track(title,artist,album,count,rating,len)
  FROM '/Volumes/{catalog_name}/{schema_name}/myfiles/track/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'False', 
    'inferSchema' = 'true'
)
''').display()

# COMMAND ----------

spark.sql(f'''SELECT * FROM track limit 10;''').show(5)

# COMMAND ----------

spark.sql(f'SHOW TABLES').display()

# COMMAND ----------

spark.sql(f'''DESCRIBE TABLE track;''').display(5)
