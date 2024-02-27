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



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamed("driverId", "driver_id") \
                         .withColumn("ingestion_date", current_timestamp())                  
   

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write deta as delta formate(As only delta is supported).

# COMMAND ----------

pitstop_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops

# COMMAND ----------


