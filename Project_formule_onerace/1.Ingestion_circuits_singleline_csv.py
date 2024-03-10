# Databricks notebook source
# MAGIC %md
# MAGIC ### step 1. Read file
# MAGIC ### step 2. Rename column name
# MAGIC ### step 3. select the needed column
# MAGIC ### step 4. Add new column (ingestion_date)
# MAGIC #### step 5. wr data back

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .csv("/mnt/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

#create schema
circutes_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)
                                    ])


# COMMAND ----------

circutes_df = spark.read\
        .option("header", True)\
        .schema(circutes_schema)\
        .csv("/mnt/raw/circuits.csv")

# COMMAND ----------

display(circutes_df )

# COMMAND ----------

# MAGIC %md 
# MAGIC select only fields we need

# COMMAND ----------

circute_selected_df = circuits_df.select("circuitID","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circute_selected_df)

# COMMAND ----------

# for import col
from pyspark.sql.functions import col

# COMMAND ----------

circute_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt") )

# COMMAND ----------

display(circute_selected_df)

# COMMAND ----------

circute_renamed_df = circute_selected_df.withColumnRenamed("circuitId", "circute_id")\
                                        .withColumnRenamed("circuitRef", "circute_ref")\
                                        .withColumnRenamed("lat", "latitude")\
                                        .withColumnRenamed("lng", "longitude")\
                                        .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circute_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion data column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circute_final_df = circute_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circute_final_df )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write data as parquet formate
# MAGIC

# COMMAND ----------

df=spark.read.parquet("/mnt/processed/circutes/")

# COMMAND ----------

display(df)

# COMMAND ----------

circute_final_df.write.mode("overwrite").parquet("/mnt/processed/circutes/")

# COMMAND ----------

display(circute_final_df)

# COMMAND ----------


