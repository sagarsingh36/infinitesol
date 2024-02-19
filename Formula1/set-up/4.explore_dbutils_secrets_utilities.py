# Databricks notebook source
Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='storagesagar-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope',key='formula1dl-account-key')

# COMMAND ----------


