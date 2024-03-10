# Databricks notebook source
# MAGIC %md mounting row, processed and presentation containar
# MAGIC

# COMMAND ----------

client_id = "80408156-0171-4db5-b754-acfde32938ab"
tenant_id = "de1fd05e-3bdc-40f4-b1ac-95a5627a510c"
client_secret = "nD58Q~kE8SuGXc-0DmptUZHul2S0NQpz5ra2aa-z"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sagarstorage9.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sagarstorage9.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sagarstorage9.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sagarstorage9.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sagarstorage9.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.secrets.list(scope = 'sagarsecretscope')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list(scope='sagarsecretscope'))


# COMMAND ----------

client_id = dbutils.secrets.get(scope="sagarsecretscope",key="clientserviceid")
tenant_id = dbutils.secrets.get(scope="sagarsecretscope",key="tenantserviceid")
client_secret = dbutils.secrets.get(scope="sagarsecretscope",key="clientservicesecretid")

# COMMAND ----------

dbutils.secrets.list(scope = 'sagarsecretscope')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

dbutils.fs.unmount("/mnt/raw")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@sagarstorage9.dfs.core.windows.net/",
  mount_point = "/mnt/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/presentation")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@sagarstorage9.dfs.core.windows.net/",
  mount_point = "/mnt/presentation",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/processed")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@sagarstorage9.dfs.core.windows.net/",
  mount_point = "/mnt/processed",
  extra_configs = configs)

# COMMAND ----------


