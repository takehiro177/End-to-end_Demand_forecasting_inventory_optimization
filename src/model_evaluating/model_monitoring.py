# Databricks notebook source
# MAGIC %md
# MAGIC ### envently ai does not work under 15.4 LTS environment, use 14.3 LTS runtime

# COMMAND ----------

# MAGIC %pip install evidently
# MAGIC #%pip install nannyml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

import pandas as pd
import numpy as np

from evidently.spark.engine import SparkEngine
from evidently import ColumnMapping

from evidently.report import Report
from evidently.metrics.base_metric import generate_column_metrics
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import *

from evidently.test_suite import TestSuite
from evidently.tests.base_test import generate_column_tests
from evidently.test_preset import DataStabilityTestPreset, NoTargetPerformanceTestPreset, RegressionTestPreset
from evidently.tests import *

#import nannyml as nml
from IPython.display import display

import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

# COMMAND ----------

train_table = spark.read.table("portfolio.end_to_end_demand_forecast.gold_demandforecast2to1_table")
test_table = spark.read.table("portfolio.end_to_end_demand_forecast.gold_forecasted_table")

# COMMAND ----------

class CFG:
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

# COMMAND ----------

column_mapping = ColumnMapping()

#column_mapping.task = 'regression'
column_mapping.target = None
column_mapping.prediction = None
column_mapping.numerical_features = CFG.customer_cols + ["price", "discount", "retail"] # + CFG.gtrends_cols
column_mapping.categorical_features = CFG.cat_cols
#column_mapping.datetime_features = ['current_week_of_year', 'current_year', 'current_month', 'week', 'release_year', 'release_month', 'release_day', 'release_day_of_week', 'release_week_of_year']

# COMMAND ----------

train_drift_monitor_table = train_table.select(*column_mapping.numerical_features, *column_mapping.categorical_features)
test_drift_monitor_table = test_table.select(*column_mapping.numerical_features, *column_mapping.categorical_features)

train_drift_monitor_table_pandas = train_drift_monitor_table.toPandas()
test_drift_monitor_table_pandas = test_drift_monitor_table.toPandas()

# COMMAND ----------

report = Report(metrics=[
    DataDriftPreset(), 
])

#report.run(reference_data=train_drift_monitor_table, current_data=test_drift_monitor_table, column_mapping=column_mapping, engine=SparkEngine)
report.run(reference_data=train_drift_monitor_table_pandas, current_data=test_drift_monitor_table_pandas, column_mapping=column_mapping)
report.show()

# COMMAND ----------

tests = TestSuite(tests=[
    TestNumberOfColumnsWithMissingValues(),
    TestNumberOfRowsWithMissingValues(),
    TestNumberOfConstantColumns(),
    TestNumberOfDuplicatedRows(),
    TestNumberOfDuplicatedColumns(),
    TestColumnsType(),
    TestNumberOfDriftedColumns(),
])

tests.run(reference_data=train_drift_monitor_table_pandas, current_data=test_drift_monitor_table_pandas, column_mapping=column_mapping)
tests

# COMMAND ----------

suite = TestSuite(tests=[
    NoTargetPerformanceTestPreset(),
])

suite.run(reference_data=train_drift_monitor_table_pandas, current_data=test_drift_monitor_table_pandas, column_mapping=column_mapping)
suite

# COMMAND ----------

suite.as_dict()

# COMMAND ----------

suite.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### NannyML advanced proactive monitoring ML accuracy

# COMMAND ----------

#estimator = nml.DLE(
#    feature_column_names=['car_age', 'km_driven', 'price_new', 'accident_count', 'door_count', 'fuel', 'transmission'],
#    y_pred='y_pred',
#    y_true='y_true',
#    timestamp_column_name='timestamp',
#    metrics=['rmse', 'mae'],
#    chunk_size=6000,
#    tune_hyperparameters=False
#)

# COMMAND ----------

#estimator.fit(reference_df)
#results = estimator.estimate(analysis_df)

# COMMAND ----------

#display(results.filter(period='analysis').to_df())

# COMMAND ----------

#display(results.filter(period='reference').to_df())

# COMMAND ----------

#metric_fig = results.plot()
#metric_fig.show()
