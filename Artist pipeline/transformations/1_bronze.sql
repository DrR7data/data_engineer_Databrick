--Pipeline source code

--Imports
--When using python, we import the pipelines and the --functions modules
--from pyspark import pipelines as dp
--from pyspark.sql.functions import *

--- Get pipeline configs
-- We set these configs when we created the pipeline
-- The code below captures these configs as python variables
--catalog_name = spark.conf.get("catalog_name")
--schema_name = spark.conf.get("schema_name")

--Define the source path using the configs
--source_path = f'/Volumes/{catalog_name}/{schema_name}/myfiles/'
USE CATALOG library;
use schema create_pipeline_library;

CREATE OR REPLACE MATERIALIZED VIEW track_raw
AS
SELECT *
FROM read_files(
  '/Volumes/library/create_pipeline_library/my_library/',
  format => 'csv',
  header => false,
  inferSchema => true
 );

--CREATE OR REFRESH STREAMING TABLE library_raw 
--COMMENT "Raw rides data streamed in from CSV files."
--AS SELECT * FROM 
--  STREAM READ_FILES("/Volumes/${catalog}/${schema}/my_library/*.csv", FORMAT => "csv");
--/Volumes/library/create_pipeline_library/my_library