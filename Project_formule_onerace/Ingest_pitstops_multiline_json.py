# Databricks notebook source
# MAGIC %md
# MAGIC Step-1 Read Pitstop Json

# COMMAND ----------


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

# MAGIC %md
# MAGIC Step-2 Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamed("driverId", "driver_id") \
                         .withColumn("ingestion_date", current_timestamp())                  
   

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3 Write back to ADLS

# COMMAND ----------

pitstop_final_df.write.mode("overwrite").parquet("/mnt/processed/pits_stops")

# COMMAND ----------

display(pitstop_final_df)

# COMMAND ----------


