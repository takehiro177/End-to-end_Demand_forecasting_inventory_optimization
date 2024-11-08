# Databricks notebook source
# Retrieve the secret from the secret scope to access data lake storage container
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Configure Auto Loader to read from your Azure Blob Storage Gen2 with folder structure
query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true") 
              .option("pathGlobfilter", "*.csv")
              .option("cloudFiles.schemaLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/price")
              .load("abfss://price-datasource@takehiropersonal.dfs.core.windows.net/")
            .writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/price")
              .trigger(availableNow=True)
              .table("portfolio.end_to_end_demand_forecast.bronze_price_table")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.bronze_price_table

# COMMAND ----------

# MAGIC %fs ls dbfs:/end_to_end_demand_forecast/checkpoint/bronze/price

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.bronze_price_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/bronze/price", True)
