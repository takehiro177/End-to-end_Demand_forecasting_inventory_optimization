# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------

def batch_upsert(microBatchDF, batchId):

    microBatchDF = (microBatchDF.drop("_rescued_data")
                    .melt(ids=["external_code", "retail", "price"], values=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"], 
                          variableColumnName="week", 
                          valueColumnName="discount")
                    .withColumn("discount", F.col("discount").cast(FloatType()))
                    .withColumn("price", F.col("price").cast(FloatType()))
                    .withColumn("week", F.col("week").cast(IntegerType()))
                 )

    (microBatchDF.write.format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .saveAsTable("portfolio.end_to_end_demand_forecast.silver_price_table")
                 )

# COMMAND ----------

query = (spark.readStream.table("portfolio.end_to_end_demand_forecast.bronze_price_table")
                        .writeStream
                         .foreachBatch(batch_upsert)
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/silver/price")
                         .trigger(availableNow=True)
                         .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.silver_price_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_price_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/price", True)
