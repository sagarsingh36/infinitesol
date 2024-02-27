# Databricks notebook source
# MAGIC %md
# MAGIC Step-1 Read Result json file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DataType,FloatType

# COMMAND ----------

#create schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(),False),
                                    StructField("constructorId",IntegerType(), False),
                                    StructField("number", IntegerType(), False),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), False),
                                    StructField("positionText", StringType(), False),
                                     StructField("positionOrder", IntegerType(),False),
                                    StructField("points",FloatType(), False),
                                    StructField("laps", IntegerType(), False ),
                                    StructField("time", StringType(), False),
                                    StructField("fastestLap",  IntegerType(), False),
                                    StructField("rank",  IntegerType(), False),
                                    StructField("fastestLapTime", StringType(),False),
                                    StructField("fastestLapSpeed", FloatType(), False),
                                    StructField("statusId", IntegerType(), False)
                                   ])


# COMMAND ----------

results_df = spark.read \
             .schema(results_schema) \
             .json("/mnt/raw/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_selected_df = results_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 Rename columns and add new column

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp

# COMMAND ----------

results_renamed_df = results_selected_df.withColumnRenamed("resultId", "result_id") \
                                        .withColumnRenamed("raceId", "race_id") \
                                        .withColumnRenamed("driverId" ,"driver_id")\
                                        .withColumnRenamed("constructorId", "constructor_id")\
                                        .withColumnRenamed("driverIde","driver_id")\
                                        .withColumnRenamed("positionText", "position_text")\
                                        .withColumnRenamed("positionOrder","position_order")\
                                        .withColumnRenamed("fastestLap", "fastest_lap")\
                                        .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                        .withColumnRenamed("positionOrder","position_order") \
                                        .withColumn("ingestion_date", current_timestamp())
            
            
  

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-4 Stored in ADLS

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/processed/results")

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------


