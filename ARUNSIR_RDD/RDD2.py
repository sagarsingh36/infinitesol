# Databricks notebook source
# MAGIC %md
# MAGIC ReduceByKey

# COMMAND ----------

username = [("user1", 1), ("user2",2), ("user3",3),("user4",4)]



# COMMAND ----------

rdd = sc.parallelize(username)

# COMMAND ----------

count_users=rdd.reduce(lambda a,b:a+b)

# COMMAND ----------

count_users_df = spark.createDataFrame

# COMMAND ----------

count_users_rdd = spark.sparkContext.parallelize(count_users)

# COMMAND ----------

count_users.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC GroupByKey

# COMMAND ----------

username1 = [("user1", 1), ("user2",2), ("user3",3),("user4"4)]
rdd1 = sc.parallelize(username1)

# COMMAND ----------

groupBYuserrdd=rrd1.groupByKey()

# COMMAND ----------

count_users=rdd1.groupByKey(lambda a,b:a+b)

# COMMAND ----------

count_users.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Enforcement and Schema Evolutions

# COMMAND ----------

columns=["first_name","age"]
data=[("bob",47),("li",23),("learned",51)]
rdd=sc.parallelize(data)
df= rdd.toDF(columns)

# COMMAND ----------

df.show()

# COMMAND ----------

columns=["first_name","favourate_colour"]
data=[("arun","red"),("dolly","black")]
rdd2=sc.parallelize(data)
df2= rdd2.toDF(columns)

# COMMAND ----------

df2.show()

# COMMAND ----------

dbutils.fs.ls('/mnt/raw/')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.ls("/mnt/demo")

# COMMAND ----------

spark.load.csv("/mnt/demo/circuits.csv")

# COMMAND ----------

df.write.mode("overwrite").format("parquet").saveAsTables("/mnt/demo/circuits.csv \")

# COMMAND ----------


