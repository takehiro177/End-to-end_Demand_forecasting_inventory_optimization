# Databricks notebook source
# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

import sklearn
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
sklearn.set_config(transform_output="pandas")

import mlflow

# COMMAND ----------

# Read tables
sales_table = spark.read.table("portfolio.end_to_end_demand_forecast.silver_sales_table")
price_table = spark.read.table("portfolio.end_to_end_demand_forecast.silver_price_table")
gtrends_table = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_gtrends_table")
                 .withColumnRenamed("week_of_year", "current_week_of_year")
                 .withColumnRenamed("year", "current_year")
                 .withColumnRenamed("month", "current_month")
                 )

# Aggregate customer table
customer_table_retail_statistics = (
    spark.read.table("portfolio.end_to_end_demand_forecast.silver_customer_table")
    .groupBy("retail", "year", "weekofyear")
    .agg(
        F.sum("qty").alias("retail_total_sales_qty"),
        F.count("*").alias("retail_total_customer_purchase")
    )
    .withColumn("retail", F.col("retail").cast(IntegerType()))
    .withColumnRenamed("weekofyear", "current_week_of_year")
    .withColumnRenamed("year", "current_year")
)

# Process sales table and join with price and customer tables
processed_sales = (
    sales_table.withColumn(
        "unique_id",
        F.concat(
            F.col("external_code"),
            F.col("retail"),
            F.col("season"),
            F.col("category"),
            F.col("color"),
            F.col("fabric"),
            F.col("release_date").cast(StringType())
        )
    )
    .select(
        F.col("unique_id"),
        F.col("current_date_of_week").alias("ds"),
        F.col("sales").alias("y"),
        F.year(F.col("current_date_of_week")).cast(IntegerType()).alias("current_year"),
        F.month(F.col("current_date_of_week")).cast(IntegerType()).alias("current_month"),
        F.weekofyear(F.col("current_date_of_week")).cast(IntegerType()).alias("current_week_of_year"),
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
        F.col("external_code")
    )
    .join(price_table, on=["external_code", "retail", "week"], how="left")
    .join(customer_table_retail_statistics, on=["retail", "current_week_of_year", "current_year"], how="left")
    .join(gtrends_table, on=[ "current_week_of_year", "current_month", "current_year"], how="left")
)


# filter test data (feature sample which satisfies forecasting condition)
test_path = 'dbfs:/FileStore/tables/stfore_test.csv' 
test_ids = spark.read.options(header=True).csv(test_path).toPandas()

def get_unique_id(df):
  df['release_date'] = pd.to_datetime(df['release_date'], format='%Y-%m-%d')
  df['unique_id'] = df['external_code'].astype(str) + df['retail'].astype(str) + df['season'] + df['category'] + df['color'] + df['fabric'] + df['release_date'].astype(str)

  return df['unique_id']

test_ids = get_unique_id(test_ids)

# below sample new sample which has at least 2 week sales for forecasting next week sales, and all new records' forecast should be generated
processed_sales = preprocessed_sales.toPandas()

processed_sales = processed_sales[processed_sales['unique_id'].isin(test_ids)].reset_index(drop=True)

# forecast each unique_id which has 2 week sales records

# use sklearn pipeline to get preprocessed data
# use spark udf to get prediction data and save to table
import mlflow.pyfunc

apply_model_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}/production")
new_data = new_data.withColumn(
  "prediction",
  apply_model_udf(udf_inputs)
)

# Write the result to a Delta table
processed_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("portfolio.end_to_end_demand_forecast.gold_forecasted_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_forecasted_table
# MAGIC LIMIT 15;

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.gold_forecasted_table
