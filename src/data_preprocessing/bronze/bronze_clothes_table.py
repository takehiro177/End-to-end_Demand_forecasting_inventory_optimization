# Databricks notebook source
# Retrieve the secret from the secret scope
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

query = (spark.readStream.format("cloudFiles")
                         .option("cloudFiles.format", "binaryFile")
                         .option("pathGlobfilter", "*.png")
                         .option("cloudFiles.schemaLocation", "dbfs:/twinllm/checkpoint/bronze/clothes_retail_image")
                         .load("abfss://clothes-retail-image-datasource@takehiropersonal.dfs.core.windows.net/*/*/")
                        .writeStream
                         .format("delta")
                         .outputMode("append")
                         .option("checkpointLocation", "dbfs:/twinllm/checkpoint/bronze/clothes_retail_image")
                         .trigger(availableNow=True)
                         .table("portfolio.twinllm.bronze_clothes_table")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.twinllm.bronze_clothes_table

# COMMAND ----------

#%sql
#DROP TABLE portfolio.twinllm.bronze_clothes_table

# COMMAND ----------

#dbutils.fs.rm("dbfs:/twinllm/checkpoint/bronze/clothes_retail_image", True)
