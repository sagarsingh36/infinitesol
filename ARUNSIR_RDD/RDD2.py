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
