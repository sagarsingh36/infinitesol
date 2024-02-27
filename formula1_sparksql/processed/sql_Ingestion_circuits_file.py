# Databricks notebook source
# MAGIC %md
# MAGIC ### step 1. Read file
# MAGIC ### step 2. Rename column name
# MAGIC ### step 3. select the needed column
# MAGIC ### step 4. Add new column (ingestion_date)
# MAGIC #### step 5. wr data back

# COMMAND ----------

#[10:29 AM] Sameer (Guest)
client_id = dbutils.secrets.get(scope="sagarsecretscope",key="clientserviceid")
tenant_id = dbutils.secrets.get(scope="sagarsecretscope",key="tenantserviceid")
client_secret = dbutils.secrets.get(scope="sagarsecretscope",key="clientservicesecretid")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

display("/mnt/raw/circuits.csv/")

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

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

# MAGIC %md 
# MAGIC select only fields we need

# COMMAND ----------

# for import col
from pyspark.sql.functions import col

# COMMAND ----------

circute_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt") )

# COMMAND ----------

display(circute_selected_df)

# COMMAND ----------

# use aliasing
circute_selected_df = circuits_df.select(col("circuitId").alias("circuit_ID"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt") )

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
# MAGIC  Write data as delta format(As only delta is supported)
# MAGIC

# COMMAND ----------

circute_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circutes")

# COMMAND ----------

display(circute_final_df)

# COMMAND ----------


