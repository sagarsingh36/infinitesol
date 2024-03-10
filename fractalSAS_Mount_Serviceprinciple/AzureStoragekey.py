# Databricks notebook source
spark.conf.set()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####connecting to adls using storage key 
# MAGIC ####connect to storage account using config
# MAGIC ##list files under the storage account

# COMMAND ----------

spark.conf.set("fs.azure.account.key.fractalstorage9661.dfs.core.windows.net", "PRG6fP41QiiIBXlNG0wphsJUaI7fw26sNlvpfpfyXGpOzS+O4tW2QxwKBgj2y0l/wlyDKE8i+azL+AStd+vYbQ==")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/"))

# COMMAND ----------

spark.conf.set("fs.azure.account.key.datalakestoragekey", "accesskey")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/"))

# COMMAND ----------


