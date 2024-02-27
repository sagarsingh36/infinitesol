# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructor file

# COMMAND ----------

#%run "https://adb-8340668157867098.18.azuredatabricks.net/?o=8340668157867098#notebook/1667449695352798"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType,FloatType,DoubleType

# COMMAND ----------

spark.read.json("dbfs:/mnt/raw/constructors.json")

# COMMAND ----------

constructor_schema = StructType(fields=[StructField("constructorId", IntegerType(), False ),
                                  StructField("constructorRef", StringType(), False ),
                                  StructField("name", StringType(), False ),
                                  StructField("nationality", StringType(), False ),
                                  StructField("url", StringType(), False )
])

# COMMAND ----------

constructor_df = spark.read \
    .option("header", True) \
    .schema(constructor_schema) \
    .json("/mnt/raw/constructors.json")

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

construct_final_df= constructor_selection_df.withColumnRenamed("constructorId", "constructor_id") \
                                  .withColumnRenamed("constructorRef", "constructor_ref")\
                                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(construct_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##stpe 3 - Select only columns we need

# COMMAND ----------

df=construct_final_df.write.mode("overwrite").parquet("/mnt/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/constructors"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Partition the data on race year so it will create separate folder for each year on the ADLS.

# COMMAND ----------

construct_final_df.printSchema()

# COMMAND ----------

 #construct_final_df = construct_final_df.withColumn("race_year", lit(" "))


# COMMAND ----------

construct_final_df.write.mode("overwrite").parquet("/mnt/processed/constructor")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/constructor"))

# COMMAND ----------


