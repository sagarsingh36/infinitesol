# Databricks notebook source
from  pyspark.sql.types import *

# COMMAND ----------

spark.read.csv("/mnt/raw/races.csv")

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceid", IntegerType(), False ),
                                  StructField("year", IntegerType(), False ),
                                  StructField("round", IntegerType(), False ),
                                  StructField("circuteid", IntegerType(), False ),
                                  StructField("name", StringType(), False ),
                                  StructField("date", DateType(), False ),
                                  StructField("time", StringType(), False ),
                                  StructField("url", StringType(), False )
])

# COMMAND ----------

race_df = spark.read \
         .option("header", True) \
         .schema(races_schema)\
         .csv("/mnt/raw/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_droped_df = race_df.drop("url")

# COMMAND ----------

display(race_droped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp,lit, concat


# COMMAND ----------

races_with_timestamp_df = race_droped_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                  .withColumnRenamed("raceid" , "race_id")\
                                  .withColumnRenamed("year" , "race_year") \
                                  .withColumnRenamed("circuteid", "circute_id") \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").parquet("/mnt/processed/races")

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------


