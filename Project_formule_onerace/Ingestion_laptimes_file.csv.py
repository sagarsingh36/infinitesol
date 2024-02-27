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

#configs = {"fs.azure.account.auth.type": "OAuth",
         # "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
         # "fs.azure.account.oauth2.client.id": client_id,
         # "fs.azure.account.oauth2.client.secret": client_secret,
          #"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

##dbutils.fs.unmount("/mnt/raw/")

# COMMAND ----------

#dbutils.fs.mount(
  #source = "abfss://raw@fractalstorage9661.dfs.core.windows.net/",
 # mount_point = "/mnt/raw",
  #extra_configs = configs)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/container1/")

# COMMAND ----------

spark.read.csv("/mnt/raw/lap_times")


# COMMAND ----------

laptimes_df=spark.read.csv("/mnt/raw/lap_times")

# COMMAND ----------

type(laptimes_df)

# COMMAND ----------

laptimes_df.show()

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.printSchema()


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

lapstimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("lap", IntegerType(), False),
                                      StructField("positoin", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False)
])

# COMMAND ----------

laptimes_df = spark.read \
          .schema(lapstimes_schema) \
          .csv("/mnt/raw/lap_times")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

aptimes_renamed_df= laptimes_df.withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("driverId", "driver_id") 
                            

# COMMAND ----------

aptimes_renamed_df.read.parqet("/mnt/processed/lap_times/")

# COMMAND ----------

laptimes_final_df = spark.read.parquet("/mnt/processed/lap_times/")


# COMMAND ----------

display(laptimes_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write deta as delta formate(As only delta is supported).
# MAGIC

# COMMAND ----------

display(spark.read.parquet("/mnt/processed/lap_times/"))

# COMMAND ----------

laptimes_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times
