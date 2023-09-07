# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df.display()

# COMMAND ----------

df1=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.filter("Year=2008").display()

# COMMAND ----------

df1.groupBy("Year").count().display()

# COMMAND ----------


