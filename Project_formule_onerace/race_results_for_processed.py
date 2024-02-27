# Databricks notebook source
# MAGIC %md
# MAGIC #####create race results table

# COMMAND ----------

# MAGIC %run "./configurations/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality") 


# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("driver_number", "number") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("name", "team")
    

# COMMAND ----------

circutes_df = spark.read.parquet(f"{processed_folder_path}/circutes") \
    .withColumnRenamed("location", "circute_location") \
    .withColumnRenamed("cicute_id", "circute_id")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time","race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC #####join circutes and races dataframes

# COMMAND ----------

display(circutes_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circute_df = races_df.join(circutes_df, races_df.circute_id == circutes_df.circute_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circutes_df.circute_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #####join results to all other dataframes

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_results_df = results_df.join(race_circute_df, results_df.race_id == race_circute_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circute_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points" ,"position") \
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


