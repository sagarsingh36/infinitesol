# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructor file

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

# MAGIC %md
# MAGIC step 2- Remove unwanted url column

# COMMAND ----------

constructor_selection_df=constructor_df.drop("url")

# COMMAND ----------

display(constructor_selection_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step-3 Rename the column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat

# COMMAND ----------

construct_final_df= constructor_selection_df.withColumnRenamed("constructorId", "constructor_id") \
                                  .withColumnRenamed("constructorRef", "constructor_ref")\
                                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(construct_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-4 Send the file to processed in ADLS

# COMMAND ----------

df=construct_final_df.write.mode("overwrite").parquet("/mnt/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/constructors"))

# COMMAND ----------

construct_final_df.printSchema()

# COMMAND ----------

construct_final_df.write.mode("overwrite").parquet("/mnt/processed/constructor")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/constructor"))

# COMMAND ----------


