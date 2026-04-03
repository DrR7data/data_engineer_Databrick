from pyspark import pipelines as dp
from pyspark.sql.functions import *


# Please edit the sample below


@dp.table
def flights_stat():
    df = spark.read.table("ingest_flight")
    return(
        df.agg(
            count("*").alias("nums_events"),
            countDistinct("icao24").alias("distinct_aircraft"),
            max("velocity").alias("max_velocity"),
        )
    )
