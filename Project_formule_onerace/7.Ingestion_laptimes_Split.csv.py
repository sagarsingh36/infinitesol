# Databricks notebook source
# MAGIC %md
# MAGIC ### step 1. Read file
# MAGIC

# COMMAND ----------

spark.read.csv("/mnt/raw/lap_times")


# COMMAND ----------

laptimes_df=spark.read.csv("/mnt/raw/lap_times")

# COMMAND ----------

type(laptimes_df)

# COMMAND ----------

laptimes_df.show()

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1 Read the multiple csv file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

lapstimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("lap", IntegerType(), False),
                                      StructField("positoin", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False)
])

# COMMAND ----------

laptimes_df = spark.read \
          .schema(lapstimes_schema) \
          .csv("/mnt/raw/lap_times")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

laptimes_renamed_df= laptimes_df.withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("driverId", "driver_id") \
                             .withColumn("ingest_date", current_timestamp())
                            

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

laptimes_final_df=laptimes_renamed_df.write.mode("overwrite").parquet("/mnt/processed/lap_timess")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/lap_timess"))


# COMMAND ----------

display(laptimes_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write data in ADLS.
# MAGIC

# COMMAND ----------

laptimes_final_df = spark.read.format("parquet").load("/mnt/processed/lap_times")

# COMMAND ----------

laptimes_final_df.write.mode("overwrite").parquet("/mnt/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/lap_times"))

# COMMAND ----------


