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

# MAGIC %md
# MAGIC Transformation only excution when action is called

# COMMAND ----------


data= sc.parallelize([10,20,30])

# COMMAND ----------

type(data)

# COMMAND ----------

data.collect()

# COMMAND ----------

type(data.collect())

# COMMAND ----------

x=[4,5,6,7,8]

# COMMAND ----------

xrdd=sc.parallelize(x)

# COMMAND ----------

xrdd.collect()[2]

# COMMAND ----------

def summation(a,b):
    result=a+b
    return result

# COMMAND ----------

summation(6,5)

# COMMAND ----------

# MAGIC %md 
# MAGIC LAmda function
# MAGIC

# COMMAND ----------

# lambda function are anonymous functions
x=lambda a,b:a+b
print(x(5,6))

# COMMAND ----------

data = sc.parallelize([2,4,5,6,9])

# COMMAND ----------

data = sc.parallelize([10,20,30])
# [(10,60),(20,70),(30,80)]

# COMMAND ----------

#map
output=data.map(lambda x:(x,x+50))

# COMMAND ----------

output.collect()

# COMMAND ----------

data=[("Arun",100),("sagar",60),("Santosh",78)]

# COMMAND ----------

type(data)

# COMMAND ----------

rdd=sc.parallelize(data)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

def double_marks(marks):
     return marks*2

# COMMAND ----------

y=[("Arun",100),("sagar",60),("Santosh",78)]
y[1][0]

# COMMAND ----------

  rdd_new = rdd.map(lambda x:(x[0],double_marks(x[1])))

# COMMAND ----------

rdd_new.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC list of transformation and actions

# COMMAND ----------

DF1=rdd_new.toDF(["name","marks"])  # name the column header

# COMMAND ----------

DF1.show()

# COMMAND ----------

DF=rdd_new.toDF() 

# COMMAND ----------

DF.show()

# COMMAND ----------

#flat MAP each input items map 0 or more output function items
data = ["Arun Kumar","Santosh Jha ", "sagar Kumar "]
#rdd=sc.parallelize(data)
rdd= spark.sparkContext.parallelize(data)


# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd2=rdd.flatMap(lambda x:x.split(" "))
rdd2.collect()

# COMMAND ----------

#filetr ( ) transformation
#collect , reduce is an actions
# differentiate between actions and transformations
rdd1=sc.parallelize(["arun","amit","atul","sagar", "santosh"])
rdd2=rdd1.filter(lambda x:x.satartWith("a")) # no output due to no actions is called

# COMMAND ----------

rdd2.collect() # got error due to W is small and error not shown because actions is not called

# COMMAND ----------


