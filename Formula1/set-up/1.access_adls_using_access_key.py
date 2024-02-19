# Databricks notebook source
Access Azure Data Lake using access keys
1. Set the spark config fs.azure.account.key 
List files from demo container
3. Read data from circuits.csv file

# COMMAND ----------

# for replacing the secret key 

dbutils.secrets.get(scope='storagesagar-scope',key='storagesagardl-account-key')

# COMMAND ----------

# formula1dl-account-key get replacet to the key which we get as link
formula1dl_account_key=dbutils.secrets.get(scope='formula1-scope',key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storagesagar.dfs.core.windows.net",
    "formula1dl_account_key"
)

# COMMAND ----------


spark.conf.set(
    "fs.azure.account.key.storagesagar.dfs.core.windows.net",
    "NzrkoCCj9ZMzRa5OAOLWcH+6CkDt0j0rfVYm6Gb0f0cjS6oKnNT3qEQuj05xNX1oPlItCoNOQdFX+ASt1cH5VQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(butils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------

dbutils.fs.ls("abfss://demo@storagesagar.dfs.core.windows.net")

# COMMAND ----------



# COMMAND ----------


dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://demo@storagesagar.dfs.core.windows.net/circuits.csv')
dataframe.display()

# COMMAND ----------


