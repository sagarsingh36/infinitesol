# Databricks notebook source
a=10
b=20
c=a+b
print (c)

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM table_name
# MAGIC  

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "b6ce9116-3e22-4db9-bad3-6a0b45bda083",
          "fs.azure.account.oauth2.client.secret": "pZA8Q~llM0GXcwNRA21Scz4DRo~oN5Qv22q7zcar",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/c3d0f98e-c740-4d84-859d-b0b418502757/oauth2/token"}
 
dbutils.fs.refreshMounts()


# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://container1@storagesagar.dfs.core.windows.net/",
  mount_point = "/mnt/sacon2",
  extra_configs = configs)    

# COMMAND ----------

dbutils.fs.unmount()    

# COMMAND ----------

dbutils.fs.ls('/mnt/sacon2')

# COMMAND ----------

df = spark.read.format('csv').options(header=True, inferSchema=True).load('dbfs:/mnt/sacon2/2022/10/01/circuits.csv')

# COMMAND ----------

dbutils.fs.ls

# COMMAND ----------

dbutils.fs.ls

# COMMAND ----------


data=[("Arun",90),("sagar",100)]
y[0][1]

# COMMAND ----------

type(data)

# COMMAND ----------

st.paral(data)

# COMMAND ----------

rdd=st.parallelize(data)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

def.collect
