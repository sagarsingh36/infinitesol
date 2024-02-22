# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructor file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# COMMAND ----------

spark.read.csv("/mnt/raw/constructors.json")

# COMMAND ----------



# COMMAND ----------

races_schema = StructType(fields=[StructField("constructorId", IntegerType(), False ),
                                  StructField("constructorRef", StringType(), False ),
                                  StructField("name", StringType(), False ),
                                  StructField("nationality", StringType(), False ),
                                  StructField("url", StringType(), False )
])

# COMMAND ----------

constructor_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
        .csv("/mnt/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_selection_df=constructor_df.drop("url")

# COMMAND ----------

display(constructor_selection_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step2 Add ingestion Date and race_timestapmp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, col, concat

# COMMAND ----------



# COMMAND ----------

construct_final_df= constructor_selection_df.withColumnRenamed("constructorId", "constructor_id") \
                                  .withColumnRenamed("constructorRef", "constructor_ref")\
                                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(construct_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##stpe 3 - Select only columns we need

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuteid").alias("circute_id"),col("name"),col("ingestion_timestamp"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

construct_final_df.write.mode("overwrite").parquet("/mnt/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/construct_final_df"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Partition the data on race year so it will create separate folder for each year on the ADLS.

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------


