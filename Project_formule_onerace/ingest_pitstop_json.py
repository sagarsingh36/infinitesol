# Databricks notebook source
# MAGIC %md
# MAGIC Ingest Pitstop file

# COMMAND ----------

# MAGIC %md
# MAGIC ##step1 read the races file

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType,  StructField

# COMMAND ----------

from pyspark.sql.types import IntegerType, StructType,DataType,StringType,FloatType,DoubleType

# COMMAND ----------

dbutils.fs.ls("/mnt/raw/")

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

spark.read.json("/mnt/raw/pit_stops.json")

# COMMAND ----------

spark.read.json("/mnt/raw/pit_stops.json")

# COMMAND ----------

display(spark.read.json("/mnt/raw/pit_stops.json"))

# COMMAND ----------

pitstop_schema=StructType(fields=[StructField()])

# COMMAND ----------

pitstop_schema = StructType(fields=[StructField("raceid", IntegerType(), False ),
                                  StructField("driverId", IntegerType(), False ),
                                  StructField("stop", IntegerType(), False ),
                                  StructField("lap", IntegerType(), False ),
                                  StructField("time", StringType(), False ),
                                  StructField("duration",StringType(), False ),
                                  StructField("milliseconds",  IntegerType(), False ),
                                 
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
# MAGIC ##Step2 Add ingestion Date and race_timestapmp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, col, concat

# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamd("driverId", "driver_id") \
                         .withColumn("ingestion_date", current_timestamp())                  
   

# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("driverId", "driver_id") \
                             .withColumn("ingestion_date", current_timestamp())               


# COMMAND ----------

display(pitstop_final_df)

# COMMAND ----------



# COMMAND ----------

pitstop_final_df = pitstop_df.withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("driverId", "driver_id") \
                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pitstop_df = spark.read \
            .schema(pitstop_schema) \
            .option("multiline", True) \
            .json("/mnt/raw/pit_stops.json")
 

# COMMAND ----------

display(pitstop_final_df )

# COMMAND ----------

# MAGIC %md
# MAGIC ##stpe 3 - Select only columns we need

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuteid").alias("circute_id"),col("name"),col("ingestion_timestamp"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Partition the data on race year so it will create separate folder for each year on the ADLS.

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/races"))

# COMMAND ----------


