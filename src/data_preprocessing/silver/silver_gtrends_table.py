# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------

def batch_upsert(microBatchDF, batchId):

    microBatchDF = (microBatchDF
                    .withColumn("month", F.month(F.col("date")))
                    .withColumn("year", F.year(F.col("date")))
                    .withColumn("week_of_year", F.weekofyear(F.col("date")))
                 )

    (microBatchDF.write.format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .saveAsTable("portfolio.end_to_end_demand_forecast.silver_gtrends_table")
                 )

# COMMAND ----------

query = (spark.readStream.table("portfolio.end_to_end_demand_forecast.bronze_gtrends_table")
                        .writeStream
                         .foreachBatch(batch_upsert)
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/silver/gtrends")
                         .trigger(availableNow=True)
                         .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.silver_gtrends_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_gtrends_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/gtrends", True)
