# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

# COMMAND ----------


sales_table = spark.read.table("portfolio.end_to_end_demand_forecast.silver_sales_table")
price_table = spark.read.table("portfolio.end_to_end_demand_forecast.silver_price_table")

customer_table_retail_statistics = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_customer_table")
                             .GroupBy("retail", "year", "weekofyear")
                             .agg(F.sum("qty").alias("retail_total_sales_qty"),
                                  F.count("*").alias("retail_total_customer_purchase")),
                             )

(sales_table.withColumn("unique_id", F.concat(F.col("external_code"),
                                              F.col("retail"),
                                              F.col("season"),
                                              F.col("category"),
                                              F.col("color"),
                                              F.col("fabric"),
                                              F.col("release_date").cast(StringType())))
            .select(
                    F.col("unique_id"),
                    F.col("current_date_of_week").alias("ds"),
                    F.year(F.col("current_date_of_week")).cast(IntegerType()).alias("current_year"),
                    F.month(F.col("current_date_of_week")).cast(IntegerType()).alias("current_month"),
                    F.weekofyear(F.col("current_date_of_week")).cast(IntegerType()).alias("current_week_of_year"),
                    F.col("sales").alias("y"),
                    F.col("retail").cast(IntegerType()),
                    F.col("category"),
                    F.col("color"),
                    F.col("fabric"),
                    F.col("release_year"),
                    F.col("release_month"),
                    F.col("release_day"),
                    F.dayofweek(F.col("release_date")).alias("release_day_of_week").cast(IntegerType()),
                    F.col("release_week_of_year"),
                    F.col("week"),
                    F.col("season_category"),
                    )
            .join(price_table, (sales_table.external_code == price_table.external_code) & (sales_table.retail == price_table.retail) & (sales_table.week == price_table.week), "left")
            .join(customer_table_retail_statistics, (sales_table.external_code == customer_table_retail_statistics.external_code) & (sales_table.retail == customer_table_retail_statistics.retail) & (sales_table.current_week_of_year == customer_table_retail_statistics.weekofyear), "left")
         .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "True")
            .saveAsTable("portfolio.end_to_end_demand_forecast.gold_demandforecast2to10_table")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_demandforecast2to10_table
# MAGIC LIMIT 15;

# COMMAND ----------

#%sql
#DROP TABLE portfolio.end_to_end_demand_forecast.gold_demandforecast2to10_table

# COMMAND ----------


