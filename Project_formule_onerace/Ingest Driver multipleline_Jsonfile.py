# Databricks notebook source
# MAGIC %md
# MAGIC Step-1 Read the json file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DataType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

#create schema
driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema ),
                                    StructField("dob", StringType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])


# COMMAND ----------

drivers_df = spark.read \
        .schema(driver_schema) \
        .json("/mnt/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3 Rename the column and add column

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp, lit

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("driverref", "driver_ref")\
                                   .withColumn("ingestion_date",current_timestamp())\
                                   .withColumn("name",concat(col("name.forename"),lit(" "),
                                    col("name.surname")))
  

# COMMAND ----------

display(drivers_with_column_df )

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3 Drop the URL and send to ADLS

# COMMAND ----------

drivers_selected_df = drivers_with_column_df.drop("url")

# COMMAND ----------

drivers_final_df= drivers_selected_df .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/processed/drivers")

# COMMAND ----------

spark.read.parquet("/mnt/processed/drivers")

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------


