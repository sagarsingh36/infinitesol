# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races file

# COMMAND ----------

# MAGIC %md
# MAGIC ##step1 read the races file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType

# COMMAND ----------

spark.read.csv("/mnt/raw/races.csv")

# COMMAND ----------



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

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
        .csv("/mnt/raw/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_url_df=races_df.drop("url")


# COMMAND ----------

races_df=race_url_df

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step2 Add ingestion Date and race_timestapmp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, col, concat

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                  .withColumnRenamed("raceid" , "race_id")\
                                  .withColumnRenamed("year" , "race_year") \
                                  .withColumnRenamed("circuteid", "circute_id") \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                  .withColumnRenamed("raceid" , "race_id")\
                                  .withColumnRenamed("year" , "race_year") \
                                  .withColumnRenamed("circuteid", "circute_id") \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##stpe 3 - Select only columns we need

# COMMAND ----------

##races_selected_df = races_with_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuteid").alias("circute_id"),col("name"),col("ingestion_timestamp"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

#final_race_df=races_selected_df.drop("url")

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Partition the data on race year so it will create separate folder for each year on the ADLS.

# COMMAND ----------

races_with_timestamp_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------


