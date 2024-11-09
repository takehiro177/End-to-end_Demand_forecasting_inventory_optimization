# Databricks notebook source
# Retrieve the secret from the secret scope
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

query = (spark.readStream.format("cloudFiles")
                         .option("cloudFiles.format", "binaryFile")
                         .option("pathGlobfilter", "*.png")
                         .option("cloudFiles.schemaLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/clothes_retail_image")
                         .load("abfss://clothes-retail-image-datasource@takehiropersonal.dfs.core.windows.net/*/*/")
                        .writeStream
                         .format("delta")
                         .outputMode("append")
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/clothes_retail_image")
                         .trigger(availableNow=True)
                         .table("portfolio.end_to_end_demand_forecast.bronze_clothes_table")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.bronze_clothes_table
# MAGIC LIMIT 7;

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.bronze_clothes_table

# COMMAND ----------

#dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/bronze/clothes_retail_image", True)
