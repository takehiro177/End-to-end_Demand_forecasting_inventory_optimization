# Databricks notebook source
# Retrieve the secret from the secret scope to access data lake storage container
storage_account_key = dbutils.secrets.get(scope="tk-personal", key="substackposts-storage-account-key")
spark.conf.set("fs.azure.account.key.takehiropersonal.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, FloatType, DateType

# Define the schema
schema = StructType([
    StructField("date", DateType(), True),
    StructField("long_sleeve", FloatType(), True),
    StructField("culottes", FloatType(), True),
    StructField("miniskirt", FloatType(), True),
    StructField("short_sleeves", FloatType(), True),
    StructField("printed_shirt", FloatType(), True),
    StructField("short_cardigan", FloatType(), True),
    StructField("solid_color_top", FloatType(), True),
    StructField("trapeze_dress", FloatType(), True),
    StructField("sleeveless", FloatType(), True),
    StructField("long_cardigan", FloatType(), True),
    StructField("sheath_dress", FloatType(), True),
    StructField("short_coat", FloatType(), True),
    StructField("medium_coat", FloatType(), True),
    StructField("doll_dress", FloatType(), True),
    StructField("long_dress", FloatType(), True),
    StructField("shorts", FloatType(), True),
    StructField("long_coat", FloatType(), True),
    StructField("jumpsuit", FloatType(), True),
    StructField("drop_sleeve", FloatType(), True),
    StructField("patterned_top", FloatType(), True),
    StructField("kimono_dress", FloatType(), True),
    StructField("medium_cardigan", FloatType(), True),
    StructField("shirt_dress", FloatType(), True),
    StructField("maxi", FloatType(), True),
    StructField("capris", FloatType(), True),
    StructField("gitana_skirt", FloatType(), True),
    StructField("long_duster", FloatType(), True),
    StructField("yellow", FloatType(), True),
    StructField("brown", FloatType(), True),
    StructField("blue", FloatType(), True),
    StructField("grey", FloatType(), True),
    StructField("green", FloatType(), True),
    StructField("black", FloatType(), True),
    StructField("red", FloatType(), True),
    StructField("white", FloatType(), True),
    StructField("orange", FloatType(), True),
    StructField("violet", FloatType(), True),
    StructField("acrylic", FloatType(), True),
    StructField("scuba_crepe", FloatType(), True),
    StructField("tulle", FloatType(), True),
    StructField("angora", FloatType(), True),
    StructField("faux_leather", FloatType(), True),
    StructField("georgette", FloatType(), True),
    StructField("lurex", FloatType(), True),
    StructField("nice", FloatType(), True),
    StructField("crepe", FloatType(), True),
    StructField("satin_cotton", FloatType(), True),
    StructField("silky_satin", FloatType(), True),
    StructField("fur", FloatType(), True),
    StructField("matte_jersey", FloatType(), True),
    StructField("plisse", FloatType(), True),
    StructField("velvet", FloatType(), True),
    StructField("lace", FloatType(), True),
    StructField("cotton", FloatType(), True),
    StructField("piquet", FloatType(), True),
    StructField("plush", FloatType(), True),
    StructField("bengaline", FloatType(), True),
    StructField("jacquard", FloatType(), True),
    StructField("frise", FloatType(), True),
    StructField("technical", FloatType(), True),
    StructField("cady", FloatType(), True),
    StructField("dark_jeans", FloatType(), True),
    StructField("light_jeans", FloatType(), True),
    StructField("ity", FloatType(), True),
    StructField("plumetis", FloatType(), True),
    StructField("polyviscous", FloatType(), True),
    StructField("dainetto", FloatType(), True),
    StructField("webbing", FloatType(), True),
    StructField("foam_rubber", FloatType(), True),
    StructField("chanel", FloatType(), True),
    StructField("marocain", FloatType(), True),
    StructField("macrame", FloatType(), True),
    StructField("embossed", FloatType(), True),
    StructField("heavy_jeans", FloatType(), True),
    StructField("nylon", FloatType(), True),
    StructField("tencel", FloatType(), True),
    StructField("paillettes", FloatType(), True),
    StructField("chambree", FloatType(), True),
    StructField("chine_crepe", FloatType(), True),
    StructField("muslin_cotton_or_silk", FloatType(), True),
    StructField("linen", FloatType(), True),
    StructField("tactel", FloatType(), True),
    StructField("viscose_twill", FloatType(), True),
    StructField("cloth", FloatType(), True),
    StructField("mohair", FloatType(), True),
    StructField("mutton", FloatType(), True),
    StructField("scottish", FloatType(), True),
    StructField("milano_stitch", FloatType(), True),
    StructField("devore", FloatType(), True),
    StructField("hron", FloatType(), True),
    StructField("ottoman", FloatType(), True),
    StructField("fluid", FloatType(), True),
    StructField("flamed", FloatType(), True),
    StructField("fluid_polyviscous", FloatType(), True),
    StructField("shiny_jersey", FloatType(), True),
    StructField("goose", FloatType(), True)
])


# COMMAND ----------

# Configure Auto Loader to read from your Azure Blob Storage Gen2 with folder structure
query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true") 
              .option("pathGlobfilter", "*.csv")
              .schema(schema)
              .load("abfss://gtrends-datasource@takehiropersonal.dfs.core.windows.net/")
            .writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", "dbfs:/end_to_end_demand_forecast/checkpoint/bronze/gtrends")
              .trigger(availableNow=True)
              .table("portfolio.end_to_end_demand_forecast.bronze_gtrends_table")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM portfolio.end_to_end_demand_forecast.bronze_gtrends_table

# COMMAND ----------

# MAGIC %fs ls dbfs:/end_to_end_demand_forecast/checkpoint/bronze/gtrends

# COMMAND ----------

#%sql
##DROP TABLE portfolio.end_to_end_demand_forecast.bronze_gtrends_table

# COMMAND ----------

##dbutils.fs.rm("dbfs:/end_to_end_demand_forecast/checkpoint/bronze/gtrends", True)
