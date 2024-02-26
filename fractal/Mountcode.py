# Databricks notebook source
#[10:29 AM] Sameer (Guest)
client_id = dbutils.secrets.get(scope="sagarsecretscope",key="clientserviceid")
tenant_id = dbutils.secrets.get(scope="sagarsecretscope",key="tenantserviceid")
client_secret = dbutils.secrets.get(scope="sagarsecretscope",key="clientservicesecretid")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/raw/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/processed/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/raw/")

# COMMAND ----------

dbutils.fs.unmount("/mnt/processed/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/presentation/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/presentation/")

# COMMAND ----------

display(spark.read.csv("/mnt/raw/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/raw/circuits.csv"))

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------


