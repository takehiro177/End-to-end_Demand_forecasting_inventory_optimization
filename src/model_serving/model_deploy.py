# Databricks notebook source
dataSourceInfo = mlflow.search_runs(filter_string='tags.mlflow.runName = "untuned_random_forest"').iloc[0]["tags.sparkDatasourceInfo"]


# COMMAND ----------

run_id = mlflow.search_runs(filter_string='tags.mlflow.runName = "untuned_random_forest"').iloc[0].run_id


# COMMAND ----------

model_version = mlflow.register_model(f"runs:/{run_id}/random_forest_model", model_name)


# COMMAND ----------

# モデルの説明文を追加します
client = mlflow.tracking.MlflowClient()
client.update_registered_model(name=model_name, description="""**ワイン品質予測モデル**

![](https://sajpstorage.blob.core.windows.net/demo20210903-ml/22243068_s.jpg)

- **特徴量** ワインの特性を示す特徴量
- **出力** ワインが高品質である確率
- **承認者** Taro Yamada
""")

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
client.transition_model_version_stage(
  name=model_name,
  version=model_version.version,
  stage="Production",
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


