# Databricks notebook source
# MAGIC %pip install imgbeddings

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType, BooleanType

import io
import numpy as np
import pandas as pd

from PIL import Image, ImageOps

from imgbeddings import imgbeddings

# COMMAND ----------

def is_valid_image(image_data):
    try:
        Image.open(io.BytesIO(image_data)).verify()
        return True
    except:
        return False

is_valid_image_udf = udf(is_valid_image, BooleanType())

def extract_img_embed(content):
  """Extract image embedding from its raw content."""
  image = Image.open(io.BytesIO(content))
  img_embed = imgbeddings()
  embed = img_embed.to_embeddings(image)

  return embed[0].tolist()

@pandas_udf(ArrayType(FloatType()))
def extract_img_embed_udf(content_series):
  """Extract image embedding from its raw content."""
  embeds = content_series.apply(lambda content: extract_img_embed(content))

  return embeds

# COMMAND ----------

preprocessed_clothes_table = (spark.read.table("portfolio.end_to_end_demand_forecast.silver_clothes_table")
                        .select(F.col("content"),
                                F.col("category"),
                                F.col("file_name"),
                                )
                        .filter(F.col("content").isNotNull())
                        .filter((is_valid_image_udf(F.col("content"))))
                        .withColumn("img_path", F.concat(F.col("category"), F.lit("/"), F.col("file_name")))
                        .withColumn("embed", extract_img_embed_udf(F.col("content")))
                        .drop("content")
                )
        


# COMMAND ----------

preprocessed_clothes_table.show()

# COMMAND ----------

preprocessed_clothes_table.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("portfolio.end_to_end_demand_forecast.gold_imgembed_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.gold_imgembed_table

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.gold_imgembed_table
