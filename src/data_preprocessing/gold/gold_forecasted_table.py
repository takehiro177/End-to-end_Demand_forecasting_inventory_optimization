# Databricks notebook source
# MAGIC %pip install polars
# MAGIC %pip install mlforecast
# MAGIC %pip install lightgbm
# MAGIC %pip install flask  # install flask for mlflow udf inference

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType, StructType, DoubleType, StructField

import gc
from typing import Literal

import numpy as np
import pandas as pd
import polars as pls

import sklearn
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
sklearn.set_config(transform_output="pandas")

from lightgbm import LGBMRegressor

from mlforecast import MLForecast
from mlforecast.feature_engineering import transform_exog

import mlflow
mlflow.autolog(disable=True)
mlflow.set_registry_uri("databricks-uc")

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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading test data and mlflow batch inference using spark udf

# COMMAND ----------

class CFG:
    horizon=1
    feat_cols=['current_week_of_year', 'current_year', 'current_month', 'retail', 'week', 'release_year', 'release_month', 'release_day', 'release_day_of_week', 'release_week_of_year', 'price', 'discount']
    customer_cols=['retail_total_sales_qty', 'retail_total_customer_purchase']
    gtrends_cols= [
    "long_sleeve", "culottes", "miniskirt", "short_sleeves",
    "printed_shirt", "short_cardigan", "solid_color_top", 
    "trapeze_dress", "sleeveless", "long_cardigan", 
    "sheath_dress", "short_coat", "medium_coat", 
    "doll_dress", "long_dress", "shorts", 
    "long_coat", "jumpsuit", "drop_sleeve", 
    "patterned_top", "kimono_dress", "medium_cardigan", 
    "shirt_dress", "maxi", "capris", "gitana_skirt", 
    "long_duster", "yellow", "brown", "blue", 
    "grey", "green", "black", "red", 
    "white", "orange", "violet", "acrylic", 
    "scuba_crepe", "tulle", "angora", "faux_leather", 
    "georgette", "lurex", "nice", "crepe", 
    "satin_cotton", "silky_satin", "fur", 
    "matte_jersey", "plisse", "velvet", 
    "lace", "cotton", "piquet", "plush", 
    "bengaline", "jacquard", "frise", 
    "technical", "cady", "dark_jeans", 
    "light_jeans", "ity", "plumetis", 
    "polyviscous", "dainetto", "webbing", 
    "foam_rubber", "chanel", "marocain", 
    "macrame", "embossed", "heavy_jeans", 
    "nylon", "tencel", "paillettes", 
    "chambree", "chine_crepe", 
    "muslin_cotton_or_silk", "linen", 
    "tactel", "viscose_twill", "cloth", 
    "mohair", "mutton", "scottish", 
    "milano_stitch", "devore", "hron", 
    "ottoman", "fluid", "flamed", 
    "fluid_polyviscous", "shiny_jersey", "goose"
    ]
    cat_cols=['category', 'color', 'fabric', 'season_category']

class Preprocessor(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
    
    def set_output(self, transform: None | Literal['default'] | Literal['pandas'] = None) -> BaseEstimator:
        pass

    def reduce_memory_usage_pls(self, df):
      """ Reduce memory usage by polars dataframe {df} with name {name} by changing its data types.
          Original pandas version of this function: https://www.kaggle.com/code/arjanso/reducing-dataframe-memory-size-by-65 """
      print(f"Memory usage of dataframe is {round(df.estimated_size('mb'), 2)} MB")
      Numeric_Int_types = [pls.Int8,pls.Int16,pls.Int32,pls.Int64]
      Numeric_Float_types = [pls.Float32,pls.Float64]    
      for col in df.columns:
          col_type = df[col].dtype
          c_min = df[col].min()
          c_max = df[col].max()
          if col_type in Numeric_Int_types:
              if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                  df = df.with_columns(df[col].cast(pls.Int8))
              elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                  df = df.with_columns(df[col].cast(pls.Int16))
              elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                  df = df.with_columns(df[col].cast(pls.Int32))
              elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                  df = df.with_columns(df[col].cast(pls.Int64))
          elif col_type in Numeric_Float_types:
              if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                  df = df.with_columns(df[col].cast(pls.Float32))
              else:
                  pass
          elif col_type == pls.Utf8:
              df = df.with_columns(df[col].cast(pls.Categorical))
          else:
              pass
      print(f"Memory usage of dataframe became {round(df.estimated_size('mb'), 2)} MB")
      return df

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):

        X['ds'] = pd.to_datetime(X['ds'])

        base_X = X.loc[:, ['ds', 'unique_id', 'y'] + CFG.cat_cols + CFG.feat_cols].copy()
        base_X = pls.from_pandas(base_X)
        X = pls.from_pandas(X)
        gc.collect()

        transformed_feats = transform_exog(X.select(['ds', 'unique_id'] + CFG.customer_cols + CFG.gtrends_cols),
                                           lags=[1, 2], 
                                           #lag_transforms={
                                           #    1: [(rolling_mean, 2), (rolling_average_days_with_sales, 2), (rolling_mean_positive_only, 2)],
                                           #    },
                                           num_threads=4)
        transformed_feats = transformed_feats.fill_nan(-1)

        transformed_X = base_X.join(transformed_feats, on=['unique_id', 'ds'], how='left')
        transformed_X = transformed_X.to_pandas()
        del base_X, transformed_feats;gc.collect()

        # only use 1 lag gtrends to avoid data leakage
        CFG.feat_cols.extend([col for col in transformed_X.columns if col not in ['ds', 'unique_id', 'y'] + CFG.cat_cols + CFG.feat_cols + CFG.gtrends_cols + CFG.customer_cols])

        # fill out of stock quantity to 0 (reqired if tweedie loss)
        transformed_X.loc[transformed_X['y'] < 0, 'y'] = 0

        # convert date column to datetime64[ns] is nixtla requirement
        transformed_X['ds'] = pd.to_datetime(transformed_X['ds'], dayfirst=True)
      
        return transformed_X

    def fit_transform(self, X, y=None):
        self.fit(X, y)
        X = self.transform(X, y)
        return X
    
def make_pipeline(df):

    cat_pipeline = ColumnTransformer([
        ('cat_features', OrdinalEncoder(categories='auto', handle_unknown='error'), CFG.cat_cols)
    ], verbose_feature_names_out=False, remainder='passthrough')

    pipeline = Pipeline([
        ('train_data', Preprocessor()),
        ('cat_features', cat_pipeline),
    ]).set_output(transform='pandas')

    return pipeline

# COMMAND ----------

# filter test data (feature sample which satisfies forecasting condition)
test_path = 'dbfs:/FileStore/tables/stfore_test.csv' 
test_ids = spark.read.options(header=True).csv(test_path).toPandas()

def get_unique_id(df):
  df['release_date'] = pd.to_datetime(df['release_date'], format='%Y-%m-%d')
  df['unique_id'] = df['external_code'].astype(str) + df['retail'].astype(str) + df['season'] + df['category'] + df['color'] + df['fabric'] + df['release_date'].astype(str)

  return df['unique_id']

test_ids = get_unique_id(test_ids)

# COMMAND ----------

# below sample new sample which has at least 2 week sales for forecasting next week sales, and all new records' forecast should be generated
processed_sales = processed_sales.toPandas()
processed_sales = processed_sales[processed_sales['unique_id'].isin(test_ids)].reset_index(drop=True)

# COMMAND ----------

# forecast each unique_id which has 2 week sales records at least
# use sklearn pipeline to get preprocessed data
pipeline = make_pipeline(processed_sales)
test_data = pipeline.fit_transform(processed_sales)

# use mlforecast to get target feature
fcst = MLForecast(
        models={'dummy_model': LGBMRegressor()},
        freq='W-MON',
        lags=[1],
        date_features=None,
        num_threads=14,
    )
  
test_data_prep = fcst.preprocess(
        test_data[['unique_id', 'ds', 'y'] + CFG.cat_cols + CFG.feat_cols],
        static_features=CFG.cat_cols,
        max_horizon=CFG.horizon
        )
X_te, y_te = test_data_prep[CFG.cat_cols + CFG.feat_cols], test_data_prep['y0']

# COMMAND ----------

test_data_prep = spark.createDataFrame(test_data_prep)

# COMMAND ----------

# use spark udf to get batched prediction data and save to table
model_name = 'portfolio.end_to_end_demand_forecast.lgbm-regress-tweedie_2w1'
model_version = '1'
model_uri = f"models:/{model_name}/{model_version}"

mlflow.pyfunc.get_model_dependencies(model_uri)
model_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double', env_manager="virtualenv")
spark.udf.register("predict", model_udf)

test_data_prep = test_data_prep.withColumn("forecast", model_udf(F.struct(*map(F.col, X_te.columns))))

# COMMAND ----------

model_name = 'portfolio.end_to_end_demand_forecast.mapie-lgbm-regress-tweedie_2w1'
model_version = '1'
model_uri = f"models:/{model_name}/{model_version}"
dependencies_path = mlflow.pyfunc.get_model_dependencies(model_uri)

# COMMAND ----------

# MAGIC %pip install -r {dependencies_path}

# COMMAND ----------

# define custom UDF
from pyspark.sql.functions import pandas_udf, PandasUDFType

loaded_model = mlflow.pyfunc.load_model(model_uri)

schema = StructType([
  StructField("lo_ci", DoubleType(), True),
  StructField("hi_ci", DoubleType(), True)
  ])

# Define the UDF
@pandas_udf(schema, PandasUDFType.SCALAR)
def model_udf(*cols):
  # Convert the input columns to a DataFrame
  input_df = pd.concat(cols, axis=1)
  # Predict using the loaded model
  predictions = loaded_model.predict(input_df)
  return pd.DataFrame(predictions)

test_data_prep = (test_data_prep.withColumn("forecast_confidence_interval", model_udf(F.struct(*map(F.col, X_te.columns))))
                  .withColumn("lo_ci", F.col("forecast_confidence_interval.lo_ci"))
                  .withColumn("hi_ci", F.col("forecast_confidence_interval.hi_ci"))
                  .drop("forecast_confidence_interval")
)

# COMMAND ----------

# Write the result to a Delta table
selected_columns = ["unique_id", "ds", "forecast", "hi_ci", "lo_ci"]
processed_sales = spark.createDataFrame(processed_sales)
(test_data_prep.select(*selected_columns)
               .join(processed_sales, on=["unique_id", "ds"], how="left")
               .withColumn("lo_ci", F.round(F.when(F.col("lo_ci") < 0, 0).otherwise(F.col("lo_ci")), 0))
               .withColumn("hi_ci", F.round(F.col("hi_ci"), 0))
               .withColumn("forecast", F.round(F.col("forecast"), 0))
              .write.format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "True")
               .saveAsTable("portfolio.end_to_end_demand_forecast.gold_forecasted_table")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_forecasted_table
# MAGIC LIMIT 15;

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.gold_forecasted_table
