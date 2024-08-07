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

##dbutils.fs.unmount("/mnt/raw/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/container1/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/container1",
  extra_configs = configs)

# COMMAND ----------

spark.read.csv("/mnt/raw/circuits.csv")


# COMMAND ----------



# COMMAND ----------

display(spark.read.csv("/mnt/raw/circuits.csv"))

# COMMAND ----------

circutes_df=spark.read.csv("/mnt/raw/circuits.csv")

# COMMAND ----------

type(circutes_df)

# COMMAND ----------

circutes_df.show()

# COMMAND ----------

display(circutes_df)

# COMMAND ----------

circutes_df.printSchema() # to show the string typs

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("/mnt/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df = spark.read.\
    option("header", True)\
        .option("inferSchema",True)\
            .csv("/mnt/raw/circuits.csv")


# COMMAND ----------

circuits_df = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .csv("/mnt/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema

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
# MAGIC #### Write data as parquet formate
# MAGIC

# COMMAND ----------

circute_final_df.write.parquet("/mnt/processed/circutes/")

# COMMAND ----------

df = spark.read.parquet("/mnt/processed/circutes/")

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/circuits/"))

# COMMAND ----------

display(df)

# COMMAND ----------

circute_final_df.write.mode("overwrite").parquet("/mnt/processed/circutes")

# COMMAND ----------


