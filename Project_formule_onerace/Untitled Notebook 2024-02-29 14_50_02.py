# Databricks notebook source
client_id = dbutils.secrets.get(scope="sagarsecretscope",key="clientserviceid")
tenant_id = dbutils.secrets.get(scope="sagarsecretscope",key="tenantserviceid")
client_secret = dbutils.secrets.get(scope="sagarsecretscope",key="clientservicesecretid")

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": client_id,
"fs.azure.account.oauth2.client.secret": client_secret,
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
    source="abfss://raw1@fractalstorage9661.dfs.core.windows.net",
    mount_point="/mnt/raw1/",
    extra_configs = configs)


# COMMAND ----------

dbutils.fs.mount(
    source="abfss://processed1@fractalstorage9661.dfs.core.windows.net",
    mount_point="/mnt/processed1",
    extra_configs=configs)


# COMMAND ----------

dbutils.fs.mount(
    source="abfss://presentetion1@fractalstorage9661.dfs.core.windows.net",
    mount_point="/mnt/presentetion1n1",
    extra_configs=configs

)

# COMMAND ----------

circuits_df=spark.read.format('csv').options(header = True, inferSchema = True).load("/mnt/raw1/circuits.csv")


# COMMAND ----------

circuits_df=spark.read.format("csv").options(header= True, inferSchema= True).load("/mnt/raw1/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

schema_df=StructType(fields=([StructField("circuitId", IntegerType(),True),
                                          StructField("circuitRef", StringType(), False),
                                          StructField("name", StringType(), True),
                                          StructField("location",StringType(),True),
                                          StructField("country", StringType(),True),
                                          StructField("lat",DoubleType(),True),
                                          StructField("lng",DoubleType(),True),
                                          StructField("alt", FloatType(),True),
                                          StructField("url", StringType(),True)
                                           ]))

# COMMAND ----------

circuit_infer_df=spark.read.options(header=True) \
         .schema(schema_df) \
        .csv("/mnt/raw1/circuits.csv")

# COMMAND ----------

circuit_drop_df=circuit_infer_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col, concat

# COMMAND ----------

circuit_correccted_df=circuit_drop_df.withColumnRenamed("circuitId", "circuit_id") \
                                      .withColumnRenamed("circuitRef", "circuit_ref")\
                                      .withColumnRenamed("lat","latitude")\
                                      .withColumnRenamed("lng", "longitude")\
                                      .withColumnRenamed("alt", "altitude")\
                                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuit_correccted_df.write.parquet("/mnt/processed1/circutes")


# COMMAND ----------

df=spark.read.parquet("/mnt/processed1/circutes")

# COMMAND ----------

circuit_correccted_df.write.mode("overwrite").parquet("/mnt/processed1/circutes")



# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC for raceingestion

# COMMAND ----------

df=spark.read.format("csv").options(header=True, inferSchema= True).load("/mnt/raw1/races.csv")

# COMMAND ----------

df = spark.read.format('csv').options(header=True, inferSchema=True).load('/mnt/raw1/races.csv').show(2)

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StringType,DoubleType,FloatType

# COMMAND ----------

df_schema=StructType(fields=([StructField("raceId",IntegerType(), False),
                            StructField("year",IntegerType(), False),
                            StructField("round",IntegerType(), False),
                           StructField("circuitId",IntegerType(), False),
                           StructField("name",StringType(), False),
                           StructField("date",IntegerType(), False),
                           StructField("time",IntegerType(), False),
                           StructField("url",StringType(), False)
]))

# COMMAND ----------

races_df=spark.read \
         .option("header", True) \
          .schema(df_schema) \
            .csv("/mnt/raw1/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

corrected_df=races_with_timestamp_df.withColumnRenamed("raceID","race_id") \
                       .withColumnRenamed("year","race_year") 
                           

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceid").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circute_id"),col("name"),col("ingestion_timestamp"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.parquet("/mnt/processed1/races")
df=spark.read.parquet("/mnt/processed1/races")



# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed1/races")

# COMMAND ----------

spark.read.format("json").options(header=True, inferSchema=True).json("/mnt/raw1/constructors.json").show(2)

# COMMAND ----------



# COMMAND ----------

constructors_schema=( "constructorId INT, constructorRef STRING ,nationality STRING, url STRING")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------



# COMMAND ----------

constructor_schema = StructType(fields=[StructField("constructorId", IntegerType(), False ),
                                  StructField("constructorRef", StringType(), False ),
                                  
                                  StructField("nationality", StringType(), False ),
                                  StructField("url", StringType(), False )
])

# COMMAND ----------

constructors_df=spark.read.option("header", True).schema(constructors_schema).json("/mnt/raw1/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_drop_df=constructors_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructors_renamed_df=constructors_drop_df.withColumnRenamed("construstorID", "constructor_id") \
                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                 .withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

constructors_renamed_df.write.parquet("/mnt/processed1/constructor")

# COMMAND ----------

constructor_final_df=spark.read.parquet("/mnt/processed1/constructor")

# COMMAND ----------

constructors_renamed_df.write.mode("overwrite").parquet("/mnt/processed1/constructor")

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md ####driver ingestion
# MAGIC

# COMMAND ----------

driver_schema_df= ("driverId INT,driverRef STRING, number INT, code INT, name.forename STRING, name.surname STRING, dob DOUBLE, nationality STRING")

# COMMAND ----------



# COMMAND ----------

spark.read.format("json").options(inferSchema =True, header=True).json("/mnt/raw1/drivers.json").show(2)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,DataType

# COMMAND ----------



name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema_df= StructType(fields=([ StructField("driverId", IntegerType(), True),
                                      StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema ),
                                    StructField("dob", StringType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ]))

# COMMAND ----------



# COMMAND ----------

driver_read_schema_df= spark.read.schema(driver_schema_df).json("/mnt/raw1/drivers.json")

# COMMAND ----------

driver_df=driver_read_schema_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import concat,col,lit,current_timestamp

# COMMAND ----------

driver_corrected_df=driver_df.withColumnRenamed("driverId", "driver_id") \
                              .withColumnRenamed("driverRef", "driver_ref") \
                               .withColumn("ingestion_date", current_timestamp()) \
                               .withColumn("name", concat(col("name.forename"), lit(" "),
                                                           col("name.surname")))

# COMMAND ----------

driver_corrected_df.write.parquet("/mnt/processed1/drivers")


# COMMAND ----------

driver_corrected_df.write.mode("overwrite").parquet("/mnt/processed1/drivers")

# COMMAND ----------

driver_final_df=spark.read.parquet("/mnt/processed1/drivers")

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ingestion result

# COMMAND ----------

from pyspark.sql.types import FloatType, StructType, StructField, StringType,IntegerType, DoubleType


# COMMAND ----------

result_schema=StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(),False),
                                    StructField("constructorId",IntegerType(), False),
                                    StructField("number", IntegerType(), False),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), False),
                                    StructField("positionText", StringType(), False),
                                     StructField("positionOrder", IntegerType(),False),
                                    StructField("points",FloatType(), False),
                                    StructField("laps", IntegerType(), False ),
                                    StructField("time", StringType(), False),
                                    StructField("fastestLap",  IntegerType(), False),
                                    StructField("rank",  IntegerType(), False),
                                    StructField("fastestLapTime", StringType(),False),
                                    StructField("fastestLapSpeed", FloatType(), False),
                                    StructField("statusId", IntegerType(), False)
                                   ])

# COMMAND ----------

result_read_df= spark.read.option("header", True).schema(result_schema).json("/mnt/raw1/results.json")

# COMMAND ----------

results_selected_df=result_read_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

results_renamed_df = results_selected_df.withColumnRenamed("resultId", "result_id") \
                                        .withColumnRenamed("raceId", "race_id") \
                                        .withColumnRenamed("driverId" ,"driver_id")\
                                        .withColumnRenamed("constructorId", "constructor_id")\
                                        .withColumnRenamed("driverIde","driver_id")\
                                        .withColumnRenamed("positionText", "position_text")\
                                        .withColumnRenamed("positionOrder","position_order")\
                                        .withColumnRenamed("fastestLap", "fastest_lap")\
                                        .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                        .withColumnRenamed("positionOrder","position_order") \
                                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_renamed_df.write.parquet("/mnt/processed1/result")

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/processed1/result")

# COMMAND ----------

result_final_df=spark.read.parquet("/mnt/processed1/result")

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ingest pitstop json
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

spark.read.format("json").options(header=True, inferSchema= True).load("/mnt/raw1/pit_stops.json")

# COMMAND ----------

pitstop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("stop", IntegerType(), False),
                                    StructField("lap", IntegerType(), False),
                                    StructField("time", StringType(), False),
                                    StructField("duration", StringType(), False),
                                    StructField("milliseconds", IntegerType(), False)
])

# COMMAND ----------

pitstop_read_df=spark.read \
     .schema(pitstop_schema) \
    .option("multiline", True) \
    .json("/mnt/raw1/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

pit_stop_corrected_df=pitstop_read_df.withColumnRenamed("raceId", "race_id") \
                                      .withColumnRenamed("driverId", "driver_id") \
                                          .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stop_corrected_df.write.parquet("/mnt/processed1/pit_stops")

# COMMAND ----------

pit_stop_corrected_df.write.mode("overwrite").parquet("/mnt/processed1/pit_stops")

# COMMAND ----------

pit_stops_final_df=spark.read.parquet("/mnt/processed1/pit_stops")

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ingest laps time

# COMMAND ----------

from pyspark.sql.types import * 

# COMMAND ----------

spark.read.csv("/mnt/raw1/lap_times").show(2)

# COMMAND ----------

spark.read.format("csv").options(inferSchema=True, header=True).load("/mnt/raw1/lap_times")

# COMMAND ----------

laps_time_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), False),
                                       StructField("lap", IntegerType(), False),
                                      StructField("positoin", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False)])

# COMMAND ----------

laps_time_read_df= spark.read \
    .schema(laps_time_schema) \
    .csv("/mnt/raw1/lap_times")

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

lap_corrected_df=laps_time_read_df.withColumnRenamed("raceId", "race_id") \
                                  .withColumnRenamed("driverId", "driver_id") \
                                  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_corrected_df.write.parquet("/mnt/processed1/lap_times")

# COMMAND ----------

lap_corrected_df.write.mode("overwrite").parquet("/mnt/processed1/lap_times")

# COMMAND ----------

lap_times_final=spark.read.parquet("/mnt/processed1/lap_times")

# COMMAND ----------

display(lap_times_final)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ingestion of qualifying multiline json

# COMMAND ----------

("")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType

# COMMAND ----------



# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_read_df= spark.read \
    .option("multiline", True)\
     .schema(qualifying_schema) \
    .json("/mnt/raw1/qualifying")

# COMMAND ----------

qualify_corrected_df =qualifying_read_df.withColumnRenamed("qualifyID", "qualify_id") \
                                   .withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorID", "constructor_id")

# COMMAND ----------

qualify_corrected_df.write.parquet("/mnt/processed1/qualifying")

# COMMAND ----------

qualify_corrected_df.write.mode("overwrite").parquet("/mnt/processed1/qualifying")

# COMMAND ----------

qualifying_final_df=spark.read.parquet("/mnt/processed1/qualifying")

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------


