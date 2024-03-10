# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.fractalstorage9661.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.fractalstorage9661.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.fractalstorage9661.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2024-02-20T23:18:01Z&st=2024-02-20T15:18:01Z&spr=https&sig=mrRK0V%2BOewTn%2Bw7axgONz5ewxGhF1qhnib0GetHNYZw%3D") ### SAS token generate and create

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/"))

# COMMAND ----------

dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/")

# COMMAND ----------


