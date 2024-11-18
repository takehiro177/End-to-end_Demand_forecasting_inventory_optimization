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
                         #.withColumn("date", F.expr("to_date(year_week_str, 'yyyywwu')"))
                         .drop("year_week_str")
                )
        
initial_restock = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_sales_table")
                         .select(F.col("external_code"),
                                 F.col("retail"),
                                 F.col("current_week_of_year").alias("week"),
                                 F.col("current_date_of_week").alias("date"),
                                 F.col("restock").alias("qty"))
                         .withColumn("year", F.year(F.col("date")))
                         #.withColumn("date", F.to_date(F.col("date")))
                         .drop("date")
                         .dropDuplicates(["external_code", "retail"])
                )

preprocessed_restock = (initial_restock.unionByName(preprocessed_restock))

sales = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_sales_table")
                         .select(F.col("external_code"),
                                 F.col("retail"),
                                 F.col("current_week_of_year").alias("week"),
                                 F.col("current_date_of_week").alias("date"),
                                 F.col("sales"))
                         .withColumn("year", F.year(F.col("date")))
                         #.drop("date")
                         .withColumn("date", F.to_date(F.col("date")))
                )

# Define window specification 
current_inventory_window = Window.partitionBy(["external_code", "retail"]).orderBy("week")
initial_inventory_amend_window = Window.partitionBy(["external_code", "retail", "date"]).orderBy(F.asc("qty"))

inventory = (sales.join(preprocessed_restock, on=["external_code", "retail", "week", "year"], how="left")
                  .fillna(0, subset=["qty"])
                  .withColumn('row', F.row_number().over(initial_inventory_amend_window))
                  .filter(F.col('row') == 1)
                  .withColumn("accumulated_inventory", F.sum(F.col("qty") - F.col("sales")).over(current_inventory_window))
             )

# COMMAND ----------

inventory.createOrReplaceTempView("gold_inventory_preview")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_inventory_preview
# MAGIC WHERE external_code = 1 AND retail = 3;

# COMMAND ----------

inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("portfolio.end_to_end_demand_forecast.gold_inventory_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_inventory_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.gold_inventory_table
