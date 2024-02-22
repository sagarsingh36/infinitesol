# Databricks notebook source

from pyspark.sql.types import IntegerType, StringType, StructType,  StructField

# COMMAND ----------

pitstop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("stop", IntegerType(), False),
                                    StructField("lap", IntegerType(), False),
                                    StructField("time", StringType(), False),
                                    StructField("duration", StringType(), False),
                                    StructField("milliseconds", IntegerType(), False)
])

# COMMAND ----------

pitstop_df = spark.read \
            .schema(pitstop_schema) \
            .option("multiline", True) \
            .json("/mnt/raw/pit_stops.json")
 

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_df.write.mode("overwrite").parquet("/mnt/processed/pits_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/pits_stops"))

# COMMAND ----------


