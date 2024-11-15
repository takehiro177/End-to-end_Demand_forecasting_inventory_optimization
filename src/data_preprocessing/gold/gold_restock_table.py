# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
preprocessed_restock = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_restock_table")
                         .withColumn("year_week_str", F.concat(F.col("year").cast(StringType()), F.lpad(F.col("week"), 2, '0'), F.lit("2")))
                         .withColumn("date", F.expr("to_date(year_week_str, 'yyyywwu')"))
                         .drop("year_week_str")
                )

# COMMAND ----------

preprocessed_restock.createOrReplaceTempView("gold_restock_preview")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_restock_preview

# COMMAND ----------

preprocessed_restock.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("portfolio.end_to_end_demand_forecast.gold_restock_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_restock_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_restock_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/restock", True)
