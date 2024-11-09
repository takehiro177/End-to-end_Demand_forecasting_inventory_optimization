# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
def batch_upsert(microBatchDF, batchId):

    microBatchDF = (microBatchDF.drop("_rescued_data")
                    .melt(
                      ids=["external_code", "retail", "season", "category", "color", "image_path",
                           "fabric", "release_date", "restock"],
                      values=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
                      variableColumnName="week",
                      valueColumnName="sales"
                    )
                    .withColumn("release_day", F.split(F.col("release_date"), '-').getItem(2).cast(IntegerType()))
                    .withColumn("release_month", F.split(F.col("release_date"), '-').getItem(1).cast(IntegerType()))
                    .withColumn("release_year", F.split(F.col("release_date"), '-').getItem(0).cast(IntegerType()))
                    .withColumn("release_date", F.to_date(F.col("release_date"), 'yyyy-M-d'))
                    .withColumn("restock", F.col("restock").cast(IntegerType()))
                    .withColumn("week", F.col("week").cast(IntegerType()))
                    .withColumn("sales", F.col("sales").cast(IntegerType()))
                    .withColumn("release_week_of_year", F.weekofyear(F.col("release_date")))
                    .withColumn("current_week_of_year", (F.col("release_week_of_year") + F.col("week")).cast(IntegerType()))
                    .withColumn("current_date_of_week", F.when(F.col("current_week_of_year") < 54, F.to_date(F.concat(F.col("release_year").cast(StringType()), F.lpad(F.col("current_week_of_year"), 2, '0'), F.lit("1")), "yyyywwu")
                                                               ).otherwise(F.to_date(F.concat((F.col("release_year") + F.lit(1)).cast(StringType()), F.lpad((F.col("current_week_of_year") - F.lit(53)).cast(StringType()), 2, '0'), F.lit("1")), "yyyywwu")))
                    .withColumn("season_category", F.substring("season", 1, 2))
                 )

    (microBatchDF.write.format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .saveAsTable("portfolio.end_to_end_demand_forecast.silver_sales_table")
                 )

# COMMAND ----------

query = (spark.readStream.table("portfolio.end_to_end_demand_forecast.bronze_sales_table")
                        .writeStream
                         .foreachBatch(batch_upsert)
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/silver/sales")
                         .trigger(availableNow=True)
                         .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.silver_sales_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_sales_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/sales", True)
