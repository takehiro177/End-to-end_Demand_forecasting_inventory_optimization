# Databricks notebook source
# Retrieve the secret from the secret scope to access data lake storage container
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

# Define the schema
schema = StructType([
    StructField("locality", StringType(), True),
    StructField("date", StringType(), True),
    StructField("avg_temp_C", FloatType(), True),
    StructField("min_temp_C", FloatType(), True),
    StructField("max_temp_C", FloatType(), True),
    StructField("dew_point_C", FloatType(), True),
    StructField("humidity_percent", FloatType(), True),
    StructField("visibility_km", FloatType(), True),
    StructField("avg_wind_kmh", FloatType(), True),
    StructField("max_wind_kmh", FloatType(), True),
    StructField("gust_kmh", FloatType(), True),
    StructField("slm_pressure_mb", FloatType(), True),
    StructField("avg_pressure_mb", FloatType(), True),
    StructField("rain_mm", FloatType(), True)
])




# COMMAND ----------

# Configure Auto Loader to read from your Azure Blob Storage Gen2 with folder structure
query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true") 
              .option("pathGlobfilter", "*.csv")
              .option("cloudFiles.schemaLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/weather")
              .schema(schema)
              .load("abfss://weather-datasource@takehiropersonal.dfs.core.windows.net/")
            .writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/weather")
              .trigger(availableNow=True)
              .table("portfolio.end_to_end_demand_forecast.bronze_weather_table")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.bronze_weather_table

# COMMAND ----------

# MAGIC %fs ls dbfs:/end_to_end_demand_forecast/checkpoint/bronze/weather

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.bronze_weather_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/bronze/weather", True)
