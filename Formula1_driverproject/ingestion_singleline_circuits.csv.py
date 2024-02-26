# Databricks notebook source
data_frame = spark.read.format('csv').options(header=True, inferSchema=True).load('dbfs:/mnt/raw/circuits.csv')


# COMMAND ----------



# COMMAND ----------

dataFrame = spark.read.format('csv').options(header=True, inferSchema=True).load('dbfs:/mnt/raw/circuits.csv')

# COMMAND ----------

display(dataFrame)

# COMMAND ----------

dataFrame.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType,FloatType,IntegerType,DoubleType

# COMMAND ----------

circuits_schema= StructType(fields=[StructField("circuitId", IntegerType(), False),
                                StructField("circuitRef", StringType(), True),
                                StructField("name", StringType(),True),
                                StructField("location", StringType(), True),
                                StructField("country", StringType(), True),
                                StructField("lat", DoubleType(), True),
                                StructField("lng", DoubleType(),True),
                                StructField(" url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv("/mnt/raw/circuits.csv")

# COMMAND ----------

circuits_selected_df= circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_rename_df=circuits_selected_df.withColumnRenamed("circuitId", "circuite_id")\
                               .withColumnRenamed("circuitRef", "circuite_rf") \
                                .withColumnRenamed("lat","latitude")\
                                .withColumnRenamed("lng","lonitude")\
                                .withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

circuits_rename_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

circuit_drop_df= circuits_rename_df.drop("url")

# COMMAND ----------

circuit_final_df=circuit_drop_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

circuit_final_df.write.parquet("/mnt/processed/circuits")

# COMMAND ----------

df=spark.read.parquet("/mnt/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/processed/circutes")

# COMMAND ----------

display(df)

# COMMAND ----------


