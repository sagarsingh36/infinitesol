# Databricks notebook source
dbutils.fs.mount(
  source = "abfss://demo@fractalstorage9661.dfs.core.windows.net/",
  mount_point = "/mnt/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/demo/")

# COMMAND ----------


