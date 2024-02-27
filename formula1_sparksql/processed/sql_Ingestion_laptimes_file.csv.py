# Databricks notebook source
# MAGIC %md
# MAGIC ### step 1. Read file
# MAGIC ### step 2. Rename column name
# MAGIC ### step 3. select the needed column
# MAGIC ### step 4. Add new column (ingestion_date)
# MAGIC #### step 5. wr data back

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

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

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

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

# MAGIC %md 
# MAGIC select only fields we need

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion data column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write deta as delta formate(As only delta is supported).
# MAGIC

# COMMAND ----------

df = spark.read.parquet("/mnt/processed/lap_times/")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.laptimes")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.laptimes
