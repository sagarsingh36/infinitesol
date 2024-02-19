# Databricks notebook source

spark.conf.set(
    "fs.azure.account.key.storagesagar.dfs.core.windows.net",
    "sv=2023-01-03&st=2024-01-27T16%3A12%3A04Z&se=2024-01-28T16%3A12%3A04Z&sr=c&sp=rl&sig=xW0Zwnqku0U0U65hSuCFYY9eSl9Y0PmRklVDbQ2ufoY%3D"
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@storagesagar.dfs.core.windows.net")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@storagesagar.dfs.core.windows.net")

# COMMAND ----------



# COMMAND ----------


dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://demo@storagesagar.dfs.core.windows.net/circuits.csv')
dataframe.display()

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storagesagar.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl.dfs.core.windows.net", "sp=rl&st=2024-01-27T15:14:57Z&se=2024-01-27T23:14:57Z&spr=https&sv=2022-11-02&sr=c&sig=pBc0bsG9vpEII4PJwPd3xoqOgV%2FAxyPGODUcKjIUYJ4%3D")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@storagesagar.dfs.core.windows.net"))

# COMMAND ----------

di
