# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------

#spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
def batch_upsert(microBatchDF, batchId):

    microBatchDF = (microBatchDF.drop("_rescued_data")
                    .withColumn("date", F.to_date(F.col("data"), 'yyyy-MM-dd HH:mm:ss'))
                    .withColumn("year", F.year(F.col("date")))
                    .withColumn("month", F.month(F.col("date")))
                    .withColumn("weekofyear", F.weekofyear(F.col("date")))
                    .withColumn("qty", F.col("qty").cast(IntegerType()))
                    .drop("data")
                 )

    (microBatchDF.write.format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .saveAsTable("portfolio.end_to_end_demand_forecast.silver_customer_table")
                 )

# COMMAND ----------

query = (spark.readStream.table("portfolio.end_to_end_demand_forecast.bronze_customer_table")
                        .writeStream
                         .foreachBatch(batch_upsert)
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/silver/customer")
                         .trigger(availableNow=True)
                         .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.silver_customer_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_customer_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/customer", True)
