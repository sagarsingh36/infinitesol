# Databricks notebook source
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

df= spark.read.json("/mnt/raw/qualifying")

# COMMAND ----------

multiline_df = spark.read \
          .option("multiline", True) \
          .schema(qualifying_schema) \
          .json("/mnt/raw/qualifying")

# COMMAND ----------


multiline_df.write.parquet("/mnt/processed/qualifying/")

# COMMAND ----------

df = spark.read.parquet("/mnt/processed/qualifying/")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------


