# Databricks notebook source
# MAGIC %md
# MAGIC step-1 read qualifying Json

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DoubleType,FloatType,IntegerType 

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)
])

# COMMAND ----------

multiline_df = spark.read \
          .option("multiline", True) \
          .schema(qualifying_schema) \
          .json("/mnt/raw/qualifying")

# COMMAND ----------

display(multiline_df)

# COMMAND ----------

qualify_corrected_df =qualifying_df.withColumnRenamed("qualifyID", "qualify_id") \
                                   .withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorID", "constructor_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 corrected column

# COMMAND ----------

display(qualify_corrected_df)

# COMMAND ----------

qualify_corrected_df.write.mode("overwrite").parquet("/mnt/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/qualifying"))

# COMMAND ----------


