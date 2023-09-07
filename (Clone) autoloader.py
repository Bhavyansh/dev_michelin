# Databricks notebook source
input_stream_path="dbfs:/mnt/nlyadls/raw/input_stream/"

# COMMAND ----------

output="dbfs:/mnt/nlyadls/raw/output_stream"

# COMMAND ----------

(
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output}/bhavyansh/autoloader/1/schemalocation")
.load(input_stream_path)
.writeStream
.option("checkpointLocation", f"{output}/bhavyansh/autoloader/1/checkpoint")
.option("path",f"{output}/bhavyansh/autoloader/1/table1")
.table("stream.autoloader1")
)

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

(
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output}/bhavyansh/autoloader/2/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.load(input_stream_path)
.writeStream
.option("checkpointLocation", f"{output}/bhavyansh/autoloader/2/checkpoint")
.option("path",f"{output}/bhavyansh/autoloader/2/table1")
.option("mergeSchema", "true")
.table("stream.autoloader2")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from stream.autoloader2

# COMMAND ----------


