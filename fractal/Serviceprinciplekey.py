# Databricks notebook source
client_id = "80408156-0171-4db5-b754-acfde32938ab"
tenant_id = "de1fd05e-3bdc-40f4-b1ac-95a5627a510c"
client_secret = "1yT8Q~m_BrOjkIEOJ5xpfVv1M5u.vLmQN0XWYb2Y"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.fractalstorage9661.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.fractalstorage9661.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.fractalstorage9661.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.fractalstorage9661.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.fractalstorage9661.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/")

           

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@fractalstorage9661.dfs.core.windows.net/"))


# COMMAND ----------

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
  mount_point = "/mnt/demo",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.unmount("/mnt/demo/")

# COMMAND ----------


display(dbutils.fs.mounts())


# COMMAND ----------

display(spark.read.csv("/mnt/demo/test/Orders.csv"))

# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------



# COMMAND ----------

dbutils.secrets.list(scope = 'sagarsecretscope')

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/presentation",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------


