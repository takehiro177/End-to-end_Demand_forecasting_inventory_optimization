# Databricks notebook source
# Retrieve the secret from the secret scope
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Install Library
%pip install fastembed-gpu

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Library
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType

import io
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from PIL import Image, ImageOps

from fastembed import ImageEmbedding

# COMMAND ----------


def extract_category(path_col):
  """Extract category from file path using built-in SQL functions."""
  return F.regexp_extract(path_col, "[\/]([^\/]+[\/][^\/]+)$", 1)

def extract_size(content):
  """Extract image size from its raw content."""
  image = Image.open(io.BytesIO(content))
  return image.size

@pandas_udf("width: int, height: int")
def extract_size_udf(content_series):
  sizes = content_series.apply(extract_size)
  return pd.DataFrame(list(sizes))

def extract_img_embed(content):
  """Extract image embedding from its raw content."""
  image = Image.open(io.BytesIO(content))
  image = ImageOps.fit(image, (128, 128), Image.LANCZOS)
  model = ImageEmbedding(model_name="Qdrant/clip-ViT-B-32-vision", cuda=True)
  embed = model.embed(image)

  return embed

@pandas_udf("array<decimal>")
def extract_img_embed_udf(content_series):
  """Extract image embedding from its raw content."""
  embeds= content_series.apply(extract_img_embed)

  return pd.DataFrame(list(embeds))

# COMMAND ----------

def batch_upsert(microBatchDF, batchId):

    microBatchDF = (microBatchDF
                    .withColumn("category", F.split(extract_category("path"), '/').getItem(0))
                    .withColumn("file_name", F.split(extract_category("path"), '/').getItem(1))
                    .withColumn("size", extract_size_udf(F.col("content")))
                    #.withColumn("embed", extract_img_embed_udf("content"))

                 )

    (microBatchDF.write.format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .saveAsTable("portfolio.end_to_end_demand_forecast.silver_clothes_table")
                 )

# COMMAND ----------

query = (spark.readStream.option("maxBytesPerTrigger", "1g").table("portfolio.end_to_end_demand_forecast.bronze_clothes_table")
                        .writeStream
                         .foreachBatch(batch_upsert)
                         .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/silver/clothes_retail_image")
                         .trigger(availableNow=True)
                         .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.silver_clothes_table
# MAGIC LIMIT 7;

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.silver_clothes_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/silver/clothes_retail_image", True)
